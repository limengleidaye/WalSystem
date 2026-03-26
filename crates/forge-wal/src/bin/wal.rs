use rand::RngExt;
use rand::SeedableRng;
use rand::rng;
use rand::rngs::SmallRng;
use rand::seq;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::time::Duration;
use std::{
    iter::repeat_n,
    os::fd::AsRawFd,
    sync::{Arc, atomic::AtomicUsize},
};

use anyhow::{Ok, Result};
use forge_wal::wal::system::{WalSystem, WalSystemMetrics};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

static METRICS: WalSystemMetrics = WalSystemMetrics;

#[inline]
fn distribute(amount: usize, part: usize) -> impl Iterator<Item = usize> {
    let v = amount / part;
    let k = amount % part;
    repeat_n(v + 1, k).chain(repeat_n(v, part - k))
}

fn main() -> Result<()> {
    let slab_capacity: usize = 256 * 1024;
    let slab_amount: usize = 256;
    let coop: usize = 3;
    let producer: usize = 16384;

    let duration = Duration::from_secs(10);

    let preallocate = 2 * 1024 * 1024 * 1024u64; // 2 GiB
    let file = WalSystem::create_target("wal.log", preallocate, false)?;
    let wal_token = CancellationToken::new();
    let tpc_token = CancellationToken::new();

    let (wal, sq, cq) = WalSystem::bootstrap(
        wal_token.clone(),
        file.as_raw_fd(),
        slab_capacity,
        slab_amount,
        64,
        32,
    )?;

    let completed = AtomicUsize::new(0);

    let threads = num_cpus::get().saturating_sub(coop).max(1);
    let handles = distribute(threads.max(producer), threads)
        .map(|tasks| {
            let wal = wal.clone();
            let token = tpc_token.clone();
            let ops: &'static AtomicUsize = unsafe { std::mem::transmute(&completed) };

            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to build runtime");
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async {
                    let tracker = TaskTracker::new();
                    for _ in 0..tasks {
                        tracker.spawn_local(producer_task(ops, wal.clone(), token.clone()));
                    }
                    tracker.close();
                    tracker.wait().await;
                })
            })
        })
        .collect::<Vec<_>>();

    let (ctrlc_tx, ctrlc_rx) = mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = ctrlc_tx.send(());
    })?;

    for elapsed in 1..=duration.as_secs() {
        if ctrlc_rx.recv_timeout(Duration::from_secs(1)).is_ok() {
            println!("ctrl+c received, shutting down...");
            break;
        }
        let total = completed.load(Ordering::Relaxed);
        println!(
            "[{elapsed}s] total={total}, throughput={:.0} ops/s, inflight={}",
            total as f64 / elapsed as f64,
            wal.inflight(),
        );
    }

    tpc_token.cancel();

    for handle in handles {
        handle.join().expect("producer thread panicked");
    }

    while wal.inflight() > 0 {
        std::hint::spin_loop();
    }

    wal_token.cancel();

    let total = completed.load(Ordering::Relaxed);
    println!("done: {total} tasks completed");

    sq.join().expect("SQ panicked")?;
    cq.join().expect("CQ panicked")?;
    Ok(())
}

async fn producer_task(ops: &'static AtomicUsize, wal: Arc<WalSystem>, token: CancellationToken) {
    let mut rng = SmallRng::from_rng(&mut rng());
    while !token.is_cancelled() {
        let count: u16 = rng.random_range(1..=1000);
        let values: Vec<u32> = (0..count).map(|_| rng.random_range(1..=100000)).collect();
        let sum: u32 = values.iter().sum();

        let write_size = (count as usize + 2) * 4 + 2;
        let result = wal
            .reserve(
                write_size,
                |w, seq| {
                    let mut off = 0;
                    w[off..off + 2].copy_from_slice(&count.to_le_bytes());
                    off += 2;
                    for &v in &values {
                        w[off..off + 4].copy_from_slice(&v.to_le_bytes());
                        off += 4;
                    }
                    w[off..off + 4].copy_from_slice(&sum.to_le_bytes());
                    off += 4;
                    w[off..off + 4].copy_from_slice(&seq.to_le_bytes());
                },
                &METRICS,
                &token,
            )
            .await;
        if result.is_err() {
            break;
        }
        ops.fetch_add(1, Ordering::Relaxed);
    }
}
