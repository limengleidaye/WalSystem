use std::os::fd::AsRawFd;

use anyhow::{Ok, Result};
use forge_wal::wal::system::{WalSystem, WalSystemMetrics};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    let file = WalSystem::create_target("wal.log", 0, false)?;
    let token = CancellationToken::new();

    // slab_capacity=32, slab_amount=3 → 只有 3 个物理 slab 组成环
    let (wal, sq, cq) = WalSystem::bootstrap(token.clone(), file.as_raw_fd(), 32, 3, 0, 0)?;

    static METRICS: WalSystemMetrics = WalSystemMetrics;

    // 写满第 1 个 slab（逻辑编号 0）
    wal.reserve(32, |w| w[..3].copy_from_slice(b"AAA"), &METRICS, &token)
        .await?;
    println!("[slab 0] filled, inflight = {}", wal.inflight());

    // 写满第 2 个 slab（逻辑编号 1）
    wal.reserve(32, |w| w[..3].copy_from_slice(b"BBB"), &METRICS, &token)
        .await?;
    println!("[slab 1] filled, inflight = {}", wal.inflight());

    // 写满第 3 个 slab（逻辑编号 2）
    wal.reserve(32, |w| w[..3].copy_from_slice(b"CCC"), &METRICS, &token)
        .await?;
    println!("[slab 2] filled, inflight = {}", wal.inflight());

    // 第 4 次写入 → 触发背压，必须先刷盘才能继续
    // 这里验证了环的核心：物理 slab[0] 被回收复用，逻辑编号变为 3
    wal.reserve(32, |w| w[..3].copy_from_slice(b"DDD"), &METRICS, &token)
        .await?;
    println!(
        "[slab 3] filled (reused physical 0), inflight = {}",
        wal.inflight()
    );

    // 再来一次，部分写入（不满），验证 drop 时刷盘
    wal.reserve(10, |w| w[..3].copy_from_slice(b"EEE"), &METRICS, &token)
        .await?;
    println!("[slab 4] partial write, inflight = {}", wal.inflight());

    println!("done");

    wal.ring.flush_remaining_for_shutdown();

    while wal.inflight() > 0 {
        std::hint::spin_loop();
    }

    token.cancel();

    sq.join().expect("SQ thread panicked")?;
    cq.join().expect("CQ thread panicked")?;
    Ok(())
}
