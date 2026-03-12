use std::{
    cell::UnsafeCell,
    ffi::OsString,
    mem::transmute,
    os::fd::AsRawFd,
    ptr::copy_nonoverlapping,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread::{JoinHandle, spawn},
    time::Duration,
};

use anyhow::{Result, bail};
use bytesize::{ByteSize, kib, mib};
use clap::{Parser, value_parser};
use hdrhistogram::Histogram;
use minstant::Instant;
use rand::{RngExt, SeedableRng, rng, rngs::SmallRng};
use tokio::runtime::{Builder, LocalOptions};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use oxalium::wal::{CQR, SQR, WalSystem, WalSystemMetrics};
use oxalium_infra::{convert::IntoLossy, iter::distribute, process::get_process_peak_memory_usage};

type TR = Result<(WalSystemMetrics, Histogram<u64>)>;
type TRR = std::thread::Result<TR>;
type JR = JoinHandle<TR>;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "wal.snap")]
    pub path: OsString,

    #[arg(long, default_value = "60s", value_parser = humantime::parse_duration)]
    pub duration: Duration,

    #[arg(long, default_value_t = 16384, value_parser = value_parser!(u32).range(32..))]
    pub producer: u32,

    #[arg(long, default_value_t = false)]
    pub disable_rng_payload: bool,

    #[arg(long, default_value = "128GiB")]
    pub preallocate: ByteSize,

    #[arg(long, default_value_t = false)]
    pub disable_physical_preallocate: bool,

    #[arg(long, default_value = "256KiB")]
    pub slab_capacity: ByteSize,

    #[arg(long, default_value_t = 64, value_parser = value_parser!(u64).range(8..))]
    pub slab_amount: u64,

    #[arg(long, default_value_t = 4096, value_parser = value_parser!(u32).range(256..=4096))]
    pub uring_queue_depth: u32,

    #[arg(long, default_value_t = 32, value_parser = value_parser!(u32).range(4..1024))]
    pub uring_inflight_limit: u32,

    #[arg(long, default_value_t = 32768, value_parser = value_parser!(u64).range(8192..))]
    pub idle_spin_limit: u64,

    #[arg(long, default_value_t = 3, value_parser = value_parser!(u32).range(3..=1024))]
    pub coop: u32,
}

#[derive(Clone, Copy)]
struct TaskMetrics {
    wal_system_metrics: &'static WalSystemMetrics,
    task_lat_hist: &'static UnsafeCell<Histogram<u64>>,
    ops: &'static AtomicUsize,
}

impl TaskMetrics {
    fn record_task_lat(&self, reserve_inst: &Instant) {
        unsafe { &mut *self.task_lat_hist.get() }
            .saturating_record(reserve_inst.elapsed().as_micros().into_lossy());
    }

    fn record_ops(&self) {
        self.ops.fetch_add(1, Ordering::Relaxed);
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.duration < Duration::from_secs(3) {
        bail!("duration must be at least 3s");
    }
    if args.preallocate.0 != 0 && args.preallocate.0 < mib(256u64) {
        bail!("preallocate must be at least 256MiB");
    }
    if args.slab_capacity.0 < kib(4u64) {
        bail!("slab_capacity must be at least 4KiB");
    }

    let target = WalSystem::create_target(
        &args.path,
        args.preallocate.0.into_lossy(),
        !args.disable_physical_preallocate,
    )?;

    let wal_cancellation_token = CancellationToken::new();

    let (wal, sq, cq) = WalSystem::bootstrap(
        wal_cancellation_token.clone(),
        target.as_raw_fd(),
        args.slab_capacity.0.into_lossy(),
        args.slab_amount.into_lossy(),
        args.uring_queue_depth,
        args.uring_inflight_limit.into_lossy(),
        args.idle_spin_limit.into_lossy(),
    )?;

    let ops = AtomicUsize::new(0);

    let tpc_cancellation_token = CancellationToken::new();

    let threads = num_cpus::get()
        .saturating_sub(args.coop.into_lossy())
        .max(1);
    let tpc = distribute(threads.max(args.producer.into_lossy()), threads)
        .map(|tasks| {
            spawn_tpc_thread(
                unsafe { transmute(&ops) },
                !args.disable_rng_payload,
                wal.clone(),
                tasks,
                tpc_cancellation_token.clone(),
            )
        })
        .collect::<Vec<_>>();

    let (ctrlc_sender, ctrlc_receiver) = mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = ctrlc_sender.send(());
    })?;

    let mut ops_amount = 0;
    for duration in 0..args.duration.as_secs() {
        let ctrlc = ctrlc_receiver.recv_timeout(Duration::from_secs(1));
        if matches!(ctrlc, Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected)) {
            break;
        }

        let ops = ops.load(Ordering::Relaxed);
        let ops_per_sec = ops - ops_amount;
        ops_amount = ops;

        let inflight = wal.inflight();

        println!(
            "[{:>2}/{}] inflight: {} ops: {}",
            duration + 1,
            args.duration.as_secs(),
            inflight,
            ops_per_sec
        );
    }

    tpc_cancellation_token.cancel();

    let tpc = tpc.into_iter().map(|v| v.join()).collect();

    wal_cancellation_token.cancel();

    print_thread_histogram(sq, cq, tpc)?;

    drop(wal);
    drop(target);

    println!("Tasks amount ops {}", ops.load(Ordering::Relaxed));
    println!(
        "Tasks peak memory usage {}",
        get_process_peak_memory_usage().display().iec()
    );

    Ok(())
}

fn spawn_tpc_thread(
    ops: &'static AtomicUsize,
    use_rng_payload: bool,
    wal: Arc<WalSystem>,
    tasks: usize,
    cancellation_token: CancellationToken,
) -> JR {
    spawn(move || {
        let wal_system_metrics = WalSystemMetrics::default();
        let task_lat_hist = UnsafeCell::new(build_task_lat_histogram()?);
        Builder::new_current_thread()
            .enable_all()
            .build_local(LocalOptions::default())
            .expect("Failed building the Runtime")
            .block_on(async {
                let metrics = TaskMetrics {
                    wal_system_metrics: unsafe { transmute(&wal_system_metrics) },
                    task_lat_hist: unsafe { transmute(&task_lat_hist) },
                    ops,
                };
                let tracker = TaskTracker::new();
                for _ in 0..tasks {
                    tracker.spawn_local(producer_task(
                        metrics,
                        use_rng_payload,
                        wal.clone(),
                        cancellation_token.clone(),
                    ));
                }
                tracker.close();
                tracker.wait().await;
            });
        Ok((wal_system_metrics, task_lat_hist.into_inner()))
    })
}

async fn producer_task(
    metrics: TaskMetrics,
    use_rng_payload: bool,
    wal: Arc<WalSystem>,
    cancellation_token: CancellationToken,
) {
    let mut rng = SmallRng::from_rng(&mut rng());
    let mut b = vec![0u8; 4096];
    payload::<false>(use_rng_payload, &mut rng, &mut b);

    loop {
        let capacity = payload::<true>(use_rng_payload, &mut rng, &mut b);
        let reserve_inst = Instant::now();
        let fut = wal.reserve(
            capacity,
            |w| unsafe { copy_nonoverlapping(b.as_ptr(), w.as_mut_ptr(), capacity) },
            metrics.wal_system_metrics,
            &cancellation_token,
        );
        let is_cancelled = cancellation_token.is_cancelled();
        let Ok(_) = fut.await else {
            break;
        };
        metrics.record_task_lat(&reserve_inst);
        metrics.record_ops();
        if is_cancelled {
            break;
        }
    }
}

fn payload<const L: bool>(use_rng_payload: bool, rng: &mut SmallRng, b: &mut [u8]) -> usize {
    const P_RANGE_S: usize = 1;
    const P_RANGE_E: usize = 1000;
    const R: usize = (P_RANGE_S + P_RANGE_E) / 2;
    const C: usize = (2 + R) * 4;

    if L {
        if use_rng_payload {
            let amount = rng.random_range(P_RANGE_S..=P_RANGE_E);
            rng_payload(rng, amount, b)
        } else {
            C
        }
    } else {
        if !use_rng_payload {
            rng_payload(rng, R, b)
        } else {
            C
        }
    }
}

fn rng_payload(rng: &mut SmallRng, c: usize, b: &mut [u8]) -> usize {
    const P_L_RANGE_S: u32 = 1;
    const P_L_RANGE_E: u32 = 10_0000;

    unsafe {
        let mut amount = 0;
        let ptr = b.as_mut_ptr().cast::<u32>();
        let mut wptr = ptr.byte_add(8);
        for _ in 0..c {
            let v = rng.random_range(P_L_RANGE_S..P_L_RANGE_E);
            *wptr = v;
            amount += v;
            wptr = wptr.byte_add(4);
        }
        let capacity = (2 + c) * 4;
        *ptr = capacity.into_lossy();
        *ptr.byte_add(4) = amount;
        capacity
    }
}

fn print_thread_histogram(sq: SQR, cq: CQR, tpc: Vec<TRR>) -> Result<()> {
    const MULTIPLIER: f64 = 1000.0f64;

    let mut sq_idle_spin_perc = f64::NAN;
    let mut cq_idle_spin_perc = f64::NAN;

    if let Ok(Ok((mut_hist, retire_hist, sq_batch_hist, sq_idle_spin_perc0))) = sq.join() {
        print_histogram("slab ready", &mut_hist, "us");
        print_histogram("slab retire", &retire_hist, "bytes");
        print_histogram("submission batch", &sq_batch_hist, "");
        sq_idle_spin_perc = sq_idle_spin_perc0;
    }
    if let Ok(Ok((submit_hist, cq_batch_hist, cq_idle_spin_perc0))) = cq.join() {
        print_histogram("IO complete lat", &submit_hist, "us");
        print_histogram("completion batch", &cq_batch_hist, "");
        cq_idle_spin_perc = cq_idle_spin_perc0;
    }
    let mut wal_system_metrics = WalSystemMetrics::default();
    let mut task_lat_hist = build_task_lat_histogram()?;
    for tr in tpc {
        if let Ok(Ok((metrics, hist))) = tr {
            wal_system_metrics += metrics;
            task_lat_hist += hist;
        }
    }
    print_histogram("task lat", &task_lat_hist, "us");

    println!(
        "slab ring backpressure: {}",
        wal_system_metrics.backpressure()
    );
    println!(
        "slab owner mismatch: {}",
        wal_system_metrics.owner_mismatch()
    );
    println!(
        "slab claim contention: {}",
        wal_system_metrics.claim_contention()
    );
    println!("slab notify await: {}", wal_system_metrics.notify_await());
    println!(
        "submission idle spin perc: {:.3}",
        (sq_idle_spin_perc * MULTIPLIER).trunc() / MULTIPLIER
    );
    println!(
        "completion idle spin perc: {:.3}",
        (cq_idle_spin_perc * MULTIPLIER).trunc() / MULTIPLIER
    );

    Ok(())
}

fn print_histogram(report: &str, hist: &Histogram<u64>, unit: &str) {
    const BAR_WIDTH_LIMIT: u64 = 40;
    const TEQ: &str = "===========================================================================";
    const TASTERISK: &str =
        "***************************************************************************";
    const TSUB: &str =
        "-------------------------+--------------+----------------------------------";

    println!("{}", TEQ);
    println!("histogram: {} ({})", report, unit);
    if !hist.is_empty() {
        let samples = hist.len();
        let min = hist.min();
        let max = hist.max();
        let mean = hist.mean();
        let stdev = hist.stdev();

        let mut buckets = [0u64; 65];
        let mut low = 65;
        let mut high = 0;
        for v in hist.iter_recorded() {
            let c = v.count_at_value();
            let v = v.value_iterated_to();
            let b = match v {
                0 => 0,
                _ => 64usize.saturating_sub(v.leading_zeros().into_lossy()),
            };
            buckets[b] += c;
            low = low.min(b);
            high = high.max(b);
        }
        let peak = unsafe { buckets.iter().copied().max().unwrap_unchecked() };

        println!("{}", TASTERISK);
        println!("samples : {}", samples);
        println!("min     : {} {}", min, unit);
        println!("max     : {} {}", max, unit);
        println!("mean    : {:.2} {}", mean, unit);
        println!("stddev  : {:.2} {}", stdev, unit);
        println!("{}", TASTERISK);
        println!("{:>20} | {:>9} | {}", "range", "amount", "distribution");
        println!("{}", TSUB);
        for b in low..=high {
            let c = buckets[b];
            let range = match b {
                0 => "[0, 1)".into(),
                _ => {
                    let low = 1u64 << (b - 1);
                    let high = 1u64 << b;
                    format!("[{}, {})", low, high)
                }
            };
            let bar = "@".repeat((BAR_WIDTH_LIMIT * c / peak).into_lossy());
            println!("{:>20} | {:>9} | {}", range, c, bar);
        }
    }
    println!("{}", TEQ);
}

fn build_task_lat_histogram() -> Result<Histogram<u64>> {
    Ok(Histogram::new_with_max(6000_0000, 4)?)
}
