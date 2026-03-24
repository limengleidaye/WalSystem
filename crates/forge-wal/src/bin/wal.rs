use std::{
    os::fd::AsRawFd,
    sync::{Arc, atomic::AtomicUsize},
    time::Instant,
};

use anyhow::{Ok, Result};
use forge_wal::wal::system::{WalSystem, WalSystemMetrics};
use tokio_util::sync::CancellationToken;

static METRICS: WalSystemMetrics = WalSystemMetrics;

#[tokio::main]
async fn main() -> Result<()> {
    let total_tasks: usize = 10_000;
    let producer_count: usize = 64;
    let slab_capacity: usize = 4096;
    let slab_amount: usize = 64;
    let write_size: usize = 128;

    let preallocate = (total_tasks * write_size) as u64;
    let file = WalSystem::create_target("wal.log", preallocate, false)?;
    let token = CancellationToken::new();

    let (wal, sq, cq) = WalSystem::bootstrap(
        token.clone(),
        file.as_raw_fd(),
        slab_capacity,
        slab_amount,
        64,
        32,
    )?;

    let completed = Arc::new(AtomicUsize::new(0));
    let tasks_per_producer = total_tasks / producer_count;

    let start = Instant::now();

    let handles: Vec<_> = (0..producer_count)
        .map(|id| {
            let wal = wal.clone();
            let token = token.clone();
            let completed = completed.clone();

            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    for i in 0..tasks_per_producer {
                        let seq = id * tasks_per_producer + i;
                        wal.reserve(
                            write_size,
                            |w| {
                                let bytes = seq.to_le_bytes();
                                w[..8].copy_from_slice(&bytes);
                            },
                            &METRICS,
                            &token,
                        )
                        .await
                        .unwrap();
                        completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                })
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("producer panicked");
    }

    let elapsed = start.elapsed();
    let total = completed.load(std::sync::atomic::Ordering::Relaxed);

    println!(
        "done: {} tasks in {:.2?}, throughput = {:.0} ops/s",
        total,
        elapsed,
        total as f64 / elapsed.as_secs_f64(),
    );

    // 刷盘 + 等待 inflight 归零
    wal.ring.flush_remaining_for_shutdown();
    while wal.inflight() > 0 {
        std::hint::spin_loop();
    }

    token.cancel();
    sq.join().expect("SQ panicked")?;
    cq.join().expect("CQ panicked")?;
    Ok(())
}
