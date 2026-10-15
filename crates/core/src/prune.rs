//! 构建产物的**确定性回收**（票 runner-offload/03 / B5 收口 + 票 106-stability/07）。
//!
//! 这个模块管两件事，同为「磁盘上的字节从哪条确定的路径被收回来」：
//!
//! **① 共享缓存**（票 runner-offload/03）。任务 worktree 的 cargo 构建经
//! `CARGO_TARGET_DIR` 收敛到 `{home}/shared-target`
//! （[`crate::home::Home::shared_target_path`]），worktree 自身不再各养一份 1.4GB 的
//! `target/`。缓存的价值在**复用**——registry 依赖的编译产物跨任务免重编（106 上全量
//! ~20 分钟），所以回收不能「任务结束一锅端」：
//!
//! - **增量树**（`{debug,release}/incremental`）按 worktree 路径区分指纹，跨任务
//!   复用率近零、却往往是大头（实测 1.4GB 里 491MB）——任务终态时**必删**，重建便宜；
//! - **总量上限**：剩余部分（deps / build）跨任务复用率高，平时保留；总量越过
//!   [`SHARED_TARGET_CAP_BYTES`] 时整体清空——这是机制保证的兜底，不是「恰好被清」
//!   （01M3X472FJF8NW9082K6BFZPKC 巡检的教训：那次 1.4GB 是被碰巧清掉的，无路径可循）。
//!
//! 调用点：任务进入终态（done / 归档 / 取消）与项目删除——即「这个任务的构建
//! 活动确定结束了」的所有时刻。删除目录是逐项尽力而为：单个条目删除失败不阻断
//! 任务收口，只留 WARN（缓存回收失败不该让已完成的任务报错）。
//!
//! **② 终态 worktree 里的运行期产物**（票 106-stability/07）。终态那三条路本来会把
//! worktree 整个删掉，但「整个删掉」会失败（[`crate::git::Git::remove_worktree`] 报错时
//! 调用点只 WARN 后继续），于是 106 上留下过一个 **2.8GB** 的 cancelled 任务目录
//! （票 106-stability/11 手工删掉的那次）。这一半给两个出口：
//!
//! - [`reclaim_terminal_worktree`]：只按名字删 [`WORKTREE_ARTIFACT_DIRS`] 里那几个
//!   **确定是产物**的目录，其余一概不碰（`.scratch/` 与未提交改动只**列**出来，
//!   一个字都不改），并把释放量报给调用点去发事件；
//! - [`should_sweep_under_disk_pressure`]：盘压兜底的判据——磁盘剩余低于
//!   [`DISK_PRESSURE_FLOOR_BYTES`] 时，维护作业对**已终态**任务残留的 worktree 先下手
//!   （执行点在 `scheduler::maintenance`）。读不到 disk_free 按「不收」判（决策 209
//!   的降级姿态：宁可这次不动，也不在瞎的情况下删东西）。
//!
//! [② 的两个出口]都不越界：只碰 `{home}/worktrees/` 直属的那一层（部署跑的是项目主仓，
//! 永远不会引用 worktree 里的产物——票面要的「确认无部署引用」在这一层是**机械成立**的，
//! 不需要第二次判断），也不追符号链接背后的东西。

use crate::home::Home;
use serde::Serialize;
#[cfg(unix)]
use std::os::fd::AsRawFd;

/// 共享缓存的总量上限：10 GiB。40G 盘上给构建缓存留这个量级有历史依据——
/// 单任务全量 target 实测 1.4GB，十个并发深度也到不了这个帽。
pub const SHARED_TARGET_CAP_BYTES: u64 = 10 * 1024 * 1024 * 1024;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct PruneStats {
    /// 本次删除的增量树字节数（0 = 没有增量树可删）。
    pub incremental_bytes: u64,
    /// 是否触发了总量兜底清空（true 时 deps/build 也被清，下次构建全量重编）。
    pub full_reset: bool,
    /// 回收后共享目录剩余字节数（目录不存在时为 0）。
    pub remaining_bytes: u64,
}

/// 任务终态时的构建缓存回收：删增量树 + 总量兜底。**尽力而为**：任何删除失败
/// 只记 WARN，不向上报错——调用点都在任务收口的关键路径上。
pub async fn prune_build_cache(home: &Home) -> PruneStats {
    prune_with_cap(home, SHARED_TARGET_CAP_BYTES).await
}

/// 共享目录的**占锁哨兵**：回收前先试拿非阻塞排它锁。拿不到 = 还有任务在构建
/// （票据的硬要求：清理不得影响在跑任务），本次跳过、留给下一个终态事件。
/// 锁文件常驻不删——删了会出现「持锁 inode 与后来者打开的 inode 不是同一个」的竞态。
#[cfg(unix)]
fn try_lock_guard(root: &std::path::Path) -> Option<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(".prune-lock"))
        .ok()?;
    // flock(LOCK_EX|LOCK_NB):拿不到立刻走人——这是哨兵,不是闸门。
    // std 的 File::try_lock 要 1.89,仓库 MSRV 声明 1.80,故走 libc
    // (io_budget 的 statvfs 同款先例)。
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Some(file)
    } else {
        None
    }
}

/// 非 unix 没有这套哨兵:宁可不收,也不冒「删掉在跑构建」的险。
#[cfg(not(unix))]
fn try_lock_guard(_root: &std::path::Path) -> Option<std::fs::File> {
    None
}

/// cap 注入的内部形态：测试用它把「总量兜底」逼出来。
async fn prune_with_cap(home: &Home, cap_bytes: u64) -> PruneStats {
    let root = home.shared_target_path();
    let stats = tokio::task::spawn_blocking(move || {
        // 有别的任务正持锁构建 → 本次不收（WARN 留痕，机会留给下一个终态事件）。
        // 票据的硬要求：清理不得影响在跑任务——拿不到锁就整段让路。
        let Some(_guard) = try_lock_guard(&root) else {
            tracing::warn!("共享构建缓存正被在跑的构建占用，本次回收跳过");
            return PruneStats::default();
        };
        let incremental_bytes = remove_incremental_trees(&root);
        let total_after_incremental = dir_size(&root);
        let mut full_reset = false;
        let mut remaining_bytes = total_after_incremental;
        if total_after_incremental > cap_bytes {
            full_reset = true;
            match std::fs::remove_dir_all(&root) {
                Ok(()) => remaining_bytes = 0,
                Err(e) => {
                    // 清空失败如实记：full_reset 仍为真（意图已发生），读数按实重测——
                    // 不许把「没删掉」报成「已归零」。
                    tracing::warn!(error = %e, "共享构建缓存整体清空失败");
                    remaining_bytes = dir_size(&root);
                }
            }
        }
        if incremental_bytes > 0 || full_reset {
            tracing::info!(
                incremental_mb = incremental_bytes / (1024 * 1024),
                full_reset,
                remaining_mb = remaining_bytes / (1024 * 1024),
                "共享构建缓存已回收"
            );
        }
        PruneStats {
            incremental_bytes,
            full_reset,
            remaining_bytes,
        }
    })
    .await
    .unwrap_or_default();
    stats
}

/// 删除 `{debug,release}/incremental`（尽力而为），返回删掉的字节数。
fn remove_incremental_trees(root: &std::path::Path) -> u64 {
    let mut freed = 0;
    for profile in ["debug", "release"] {
        let incremental = root.join(profile).join("incremental");
        if incremental.exists() {
            freed += dir_size(&incremental);
            if let Err(e) = std::fs::remove_dir_all(&incremental) {
                tracing::warn!(path = %incremental.display(), error = %e, "增量树删除失败（不阻断）");
            }
        }
    }
    freed
}

/// 目录总大小（尽力而为；打不开的条目按 0 计）。
fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                total += dir_size(&entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    total
}

/// 目录下的**文件数**（尽力而为；目录不存在为 0）。用来把「保留了什么」说成一个数。
fn count_files(path: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| match e.metadata() {
            Ok(m) if m.is_dir() => count_files(&e.path()),
            Ok(_) => 1,
            Err(_) => 0,
        })
        .sum()
}

// ─────────────── 终态 worktree 里的运行期产物（票 106-stability/07）───────────────

/// 终态 worktree 里**确定是产物**的顶层目录：只有这几个名字会被删。
///
/// 名单收窄到这一份是刻意的。`node_modules` 之类同样吃盘（106 那次实录里 101MB），但它
/// 不是「跑一次就作废」的东西，删它等于删依赖；而终态那三条路本来会把**整个** worktree
/// 删掉，那些字节由 `remove_worktree` 收。这条路径服务的是「它没删成」的那种现场
/// （票 11 手工删掉的那个 2.8GB 目录），所以要动的只有明确无主的大头。
pub const WORKTREE_ARTIFACT_DIRS: &[&str] = &["target", ".rows"];

/// 盘压兜底的地板：数据卷剩余低于这个量时，维护作业先回收**已终态**任务的产物。
///
/// 量级取自 106 的 40G 盘（两天内两次逼近告警线、82% 用量、3.5 分钟掉 740MB）：一个
/// 全量 `target/` 是 1.4GB 量级，5 GiB 留三个任务的余量。**写死不配**——与
/// [`SHARED_TARGET_CAP_BYTES`] 同一姿态：水位线散成四处可配的数字，就没有人能对照
/// 「这次为什么动手的」读到一条确定的判据。
pub const DISK_PRESSURE_FLOOR_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// 一次 worktree 产物回收的读数。
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct ReclaimStats {
    /// **逐项**读数，顺序与 [`WORKTREE_ARTIFACT_DIRS`] 一致（含 0 的那些——「这个名字下
    /// 没有东西」也是读数的一部分，缺项会让人以为名单和实删是两套）。
    /// 不设 `target_bytes` / `rows_bytes` 两个具名字段：那份平行清单会和名单本身漂移，
    /// 而名单是判据、读数是结果（决策 258 的同一课）。
    pub per_dir: Vec<(String, u64)>,
    /// 本次收回的总字节数（= 逐项之和）。
    pub freed_bytes: u64,
    /// 保留下来的 `.scratch/` 文件数（只数、不改）。
    pub kept_scratch_files: usize,
    /// 保留下来的**未提交改动**清单（`XY path`，上限见 `git.rs` 的 `DIRTY_FILES_MAX`）。
    /// 只列不改：这些是人的东西，回收没有权限动它——列出来是为了让人知道有什么在等人。
    pub uncommitted: Vec<String>,
    /// 拒绝执行的原因（`Some` = 一个字都没动）。
    pub refused: Option<String>,
}

impl ReclaimStats {
    /// 这条读数值不值得播报：什么都没收回、也没什么在等人的东西时，不发事件、不落步骤。
    pub fn worth_reporting(&self) -> bool {
        self.freed_bytes > 0 || !self.uncommitted.is_empty() || self.refused.is_some()
    }

    /// 某个名字下的读数（名单里没有 = 0）。
    pub fn bytes_of(&self, name: &str) -> u64 {
        self.per_dir
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, b)| *b)
            .unwrap_or(0)
    }

    /// 一句话读数（台账步骤 / 维护日志用；措辞只有这一处）。
    pub fn summary_line(&self) -> String {
        if let Some(why) = &self.refused {
            return format!("产物回收未执行：{why}");
        }
        let parts = self
            .per_dir
            .iter()
            .map(|(n, b)| format!("{n} {}/MB", b / (1024 * 1024)))
            .collect::<Vec<_>>()
            .join(" / ");
        format!(
            "产物回收：释放 {} MB（{parts}），保留 .scratch {} 个文件、未提交改动 {} 处",
            self.freed_bytes / (1024 * 1024),
            self.kept_scratch_files,
            self.uncommitted.len(),
        )
    }
}

/// 盘压判据（纯函数，水位由调用点读——`io_budget::disk_free_bytes` 是那条线）。
///
/// 读不到 disk_free 按**不收**判：宁可这一趟不动，也不在瞎的情况下删东西（决策 209 的
/// 降级姿态，与超时守卫、脏态读数同款）。
pub fn should_sweep_under_disk_pressure(disk_free_bytes: Option<u64>, floor_bytes: u64) -> bool {
    disk_free_bytes.is_some_and(|free| free < floor_bytes)
}

/// 终态 worktree 的产物回收（票 07①②）。**尽力而为**：删除失败只 WARN、不向上报错——
/// 调用点全在任务收口的关键路径上，回收失败不该让一个已完成的任务报错。
///
/// 只删 `<worktree>/{WORKTREE_ARTIFACT_DIRS}` 里点名的目录，其余一个字不动：
/// - 路径必须是 `{home}/worktrees/` 的**直属**子目录，否则整段拒绝并把原因放进读数——
///   「release 档在确认无部署引用后清」这一条落在这里，而且是**机械成立**的：部署跑的
///   是项目主仓（`/opt/AgentPipeline` 那一层的 target 由边界条款明令不碰），
///   引用不到家目录 worktree 里的产物。不靠调用点自觉。
/// - 顶层那个名字若是**符号链接**，不删——链接背后的东西不在这次授权范围内
///   （与决策 395 的符号链接逃逸同一个方向：允许根拦住它）。
/// - 目录不存在 = 无事可做（幂等，重复调用不报错）。
///
/// 未提交清单走 [`crate::git::Git::dirty_files`]，读不出来按空表——它换来的只是一段
/// 文本，不允许有能力挂住终态收口（决策 209 的教训，与 `is_dirty` 同一个短上限）。
pub async fn reclaim_terminal_worktree(home: &Home, worktree: &std::path::Path) -> ReclaimStats {
    let worktrees_dir = home.worktrees_dir();
    if worktree.parent() != Some(worktrees_dir.as_path()) {
        return ReclaimStats {
            refused: Some(format!(
                "不是 {{{home}}}/worktrees 的直属目录：{p}",
                home = worktrees_dir.display(),
                p = worktree.display()
            )),
            ..Default::default()
        };
    }
    if !worktree.is_dir() {
        return ReclaimStats::default();
    }
    let uncommitted = crate::git::Git
        .dirty_files(worktree)
        .await
        .unwrap_or_default();
    let w = worktree.to_path_buf();
    let per_dir = tokio::task::spawn_blocking(move || {
        // 保留面先数（删除动作会改变目录内容，读数要说的是「动手前那儿有什么」）。
        let kept = count_files(&w.join(".scratch"));
        let per: Vec<(String, u64)> = WORKTREE_ARTIFACT_DIRS
            .iter()
            .map(|name| ((*name).to_string(), remove_artifact_dir(&w.join(name))))
            .collect();
        (per, kept)
    })
    .await
    .unwrap_or_else(|_| (Vec::new(), 0));
    let (per_dir, kept_scratch_files) = per_dir;
    let freed_bytes = per_dir.iter().map(|(_, b)| b).sum();
    ReclaimStats {
        per_dir,
        freed_bytes,
        kept_scratch_files,
        uncommitted,
        refused: None,
    }
}

/// 删除一个产物目录并返回**确实**收回的字节数（与本模块的总量兜底同一姿态：
/// 没删掉的东西不许报成已释放——失败时重测剩余，只报差值）。
fn remove_artifact_dir(dir: &std::path::Path) -> u64 {
    let Ok(meta) = std::fs::symlink_metadata(dir) else {
        return 0;
    };
    if meta.file_type().is_symlink() {
        tracing::warn!(path = %dir.display(), "产物目录是符号链接，不删（背后的东西不在本次授权范围内）");
        return 0;
    }
    let before = dir_size(dir);
    match std::fs::remove_dir_all(dir) {
        Ok(()) => before,
        Err(e) => {
            let after = dir_size(dir);
            tracing::warn!(path = %dir.display(), error = %e, "产物目录删除失败（不阻断终态）");
            before.saturating_sub(after)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn home_in(tmp: &std::path::Path) -> Home {
        Home::new(tmp.join("home"))
    }

    #[tokio::test]
    async fn prune_removes_incremental_trees_and_keeps_deps() {
        let tmp = tempfile::tempdir().unwrap();
        let home = home_in(tmp.path());
        let root = home.shared_target_path();
        let deps = root.join("debug/deps");
        std::fs::create_dir_all(&deps).unwrap();
        std::fs::write(deps.join("libfoo.rlib"), vec![0u8; 100]).unwrap();
        let incremental = root.join("debug/incremental");
        std::fs::create_dir_all(&incremental).unwrap();
        std::fs::write(incremental.join("chunk.bin"), vec![0u8; 200]).unwrap();

        let stats = prune_build_cache(&home).await;

        assert_eq!(stats.incremental_bytes, 200);
        assert!(!stats.full_reset);
        assert_eq!(stats.remaining_bytes, 100);
        assert!(
            deps.join("libfoo.rlib").exists(),
            "deps 是缓存本体,必须保留"
        );
        assert!(!incremental.exists(), "增量树必须删");
    }

    #[tokio::test]
    async fn prune_full_resets_when_over_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let home = home_in(tmp.path());
        let root = home.shared_target_path();
        let deps = root.join("debug/deps");
        std::fs::create_dir_all(&deps).unwrap();
        std::fs::write(deps.join("libbig.rlib"), vec![0u8; 64]).unwrap();

        // cap 注入:64 字节的缓存、32 字节的帽——兜底必须整体清空。
        let stats = prune_with_cap(&home, 32).await;
        assert!(stats.full_reset);
        assert!(!deps.join("libbig.rlib").exists(), "越过帽必须整体清空");
        assert_eq!(stats.remaining_bytes, 0);

        // 同一缓存、正常帽——不动。
        let stats = prune_build_cache(&home).await;
        assert!(!stats.full_reset, "64 字节不该触发 10GiB 的帽");
    }

    #[tokio::test]
    async fn prune_on_missing_dir_is_a_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let home = home_in(tmp.path());
        let stats = prune_build_cache(&home).await;
        assert_eq!(stats, PruneStats::default());
    }

    // ─────────── 终态 worktree 的产物回收（票 106-stability/07）───────────

    /// 造一个「像终态任务 worktree」的目录：产物 + 过程草稿 + 源码 + 未提交改动各有各的位置。
    fn fake_worktree(home: &Home, task_id: &str) -> std::path::PathBuf {
        let wt = home.worktree_path(task_id);
        let mk = |rel: &str, bytes: usize| {
            let p = wt.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, vec![7u8; bytes]).unwrap();
        };
        std::fs::create_dir_all(&wt).unwrap();
        mk("target/debug/libfoo.rlib", 1000);
        mk("target/debug/incremental/chunk.bin", 200);
        mk(".rows/201.md", 300);
        mk(".scratch/ux-audit-3/handoff.md", 40);
        mk("src/lib.rs", 10);
        wt
    }

    #[tokio::test]
    async fn reclaim_takes_only_the_named_artifact_dirs_and_reports_the_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let home = home_in(tmp.path());
        let wt = fake_worktree(&home, "t1");

        let stats = reclaim_terminal_worktree(&home, &wt).await;

        assert_eq!(stats.freed_bytes, 1500, "target(1200) + .rows(300)");
        assert_eq!(stats.bytes_of("target"), 1200);
        assert_eq!(stats.bytes_of(".rows"), 300);
        assert!(stats.worth_reporting());
        assert!(!wt.join("target").exists(), "target 必须整目录删掉");
        assert!(
            !wt.join(".rows").exists(),
            ".rows 是 ux-audit 那族运行期工件"
        );
        // 票面要的「保留」：过程草稿与源码一个字不动。
        assert_eq!(
            std::fs::read(wt.join(".scratch/ux-audit-3/handoff.md"))
                .unwrap()
                .len(),
            40,
            ".scratch/ 是人的过程草稿，回收无权动它"
        );
        assert!(wt.join("src/lib.rs").exists(), "源码不在名单上");
        assert_eq!(stats.kept_scratch_files, 1, "保留面要数得出来");
        assert!(stats.refused.is_none());
    }

    #[tokio::test]
    async fn the_per_dir_reading_covers_every_name_on_the_list() {
        // 只有 target、没有 .rows 时，读数**照样给两项**——缺项会让人以为名单与实删是两套。
        let tmp = tempfile::tempdir().unwrap();
        let home = home_in(tmp.path());
        let wt = home.worktree_path("t2");
        std::fs::create_dir_all(wt.join("target")).unwrap();
        std::fs::write(wt.join("target/x"), vec![0u8; 50]).unwrap();

        let stats = reclaim_terminal_worktree(&home, &wt).await;
        let names: Vec<_> = stats.per_dir.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, WORKTREE_ARTIFACT_DIRS.to_vec());
        assert_eq!(stats.bytes_of(".rows"), 0);
        assert_eq!(stats.freed_bytes, 50);
    }

    #[tokio::test]
    async fn reclaim_refuses_anything_that_is_not_a_direct_worktree_child() {
        // 「release 档在确认无部署引用后清」的机械形态：判据住在回收函数里，不靠调用点自觉。
        let tmp = tempfile::tempdir().unwrap();
        let home = home_in(tmp.path());
        let outside = tmp.path().join("elsewhere");
        std::fs::create_dir_all(outside.join("target")).unwrap();
        std::fs::write(outside.join("target/big"), vec![0u8; 1234]).unwrap();

        let stats = reclaim_terminal_worktree(&home, &outside).await;

        assert_eq!(stats.freed_bytes, 0);
        assert!(stats.refused.is_some(), "不在 worktrees 直属层 → 拒绝");
        assert!(
            outside.join("target/big").exists(),
            "拒绝必须是「一个字都没动」，不是边删边报错"
        );
        assert!(
            stats.worth_reporting(),
            "拒绝也要播报——静默的兜底等于没有兜底"
        );
        assert!(stats.summary_line().contains("产物回收未执行"));
    }

    #[tokio::test]
    async fn reclaim_on_a_vanished_worktree_is_a_quiet_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let home = home_in(tmp.path());
        let stats = reclaim_terminal_worktree(&home, &home.worktree_path("gone")).await;
        assert_eq!(stats, ReclaimStats::default());
        assert!(
            !stats.worth_reporting(),
            "目录本来就不在（幂等）不该发一条空事件"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlinked_artifact_name_is_not_followed() {
        // 链接背后的东西不在这次授权范围内（与决策 395 的符号链接逃逸同一个方向）。
        let tmp = tempfile::tempdir().unwrap();
        let home = home_in(tmp.path());
        let wt = home.worktree_path("t3");
        std::fs::create_dir_all(&wt).unwrap();
        let elsewhere = tmp.path().join("precious");
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(elsewhere.join("keep.me"), b"important").unwrap();
        std::os::unix::fs::symlink(&elsewhere, wt.join("target")).unwrap();

        let stats = reclaim_terminal_worktree(&home, &wt).await;

        assert_eq!(stats.freed_bytes, 0, "符号链接不计入释放量");
        assert!(
            elsewhere.join("keep.me").exists(),
            "链接背后的东西一个字不许动"
        );
        assert!(
            wt.join("target").exists(),
            "链接本身也不删——那不在名单的授权里"
        );
    }

    #[test]
    fn the_disk_pressure_judgement_is_silent_when_it_cannot_read_the_water_level() {
        let floor = DISK_PRESSURE_FLOOR_BYTES;
        assert!(should_sweep_under_disk_pressure(Some(floor - 1), floor));
        assert!(
            !should_sweep_under_disk_pressure(Some(floor), floor),
            "恰好在水位线上不算越线（严格低于才动手）"
        );
        assert!(!should_sweep_under_disk_pressure(Some(floor * 4), floor));
        assert!(
            !should_sweep_under_disk_pressure(None, floor),
            "读不到水位按「不收」判——瞎的时候不许删东西"
        );
    }
}
