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

// 定义类型别名，方便处理统计结果
type TR = Result<(WalSystemMetrics, Histogram<u64>)>;
type TRR = std::thread::Result<TR>;
type JR = JoinHandle<TR>;

/// 命令行参数配置
#[derive(Parser, Debug)]
struct Args {
    /// WAL 文件的存储路径
    #[arg(long, default_value = "wal.snap")]
    pub path: OsString,

    /// 测试持续时间
    #[arg(long, default_value = "60s", value_parser = humantime::parse_duration)]
    pub duration: Duration,

    /// 并发生产者的任务总数
    #[arg(long, default_value_t = 16384, value_parser = value_parser!(u32).range(32..))]
    pub producer: u32,

    /// 是否禁用随机负载填充（禁用后使用固定负载大小）
    #[arg(long, default_value_t = false)]
    pub disable_rng_payload: bool,

    /// 预分配的日志文件大小
    #[arg(long, default_value = "128GiB")]
    pub preallocate: ByteSize,

    /// 是否禁用物理预分配 (fallocate)
    #[arg(long, default_value_t = false)]
    pub disable_physical_preallocate: bool,

    /// 每个 Slab（内存块）的大小
    #[arg(long, default_value = "256KiB")]
    pub slab_capacity: ByteSize,

    /// 环形缓冲区中 Slab 的数量
    #[arg(long, default_value_t = 64, value_parser = value_parser!(u64).range(8..))]
    pub slab_amount: u64,

    /// io_uring 的队列深度
    #[arg(long, default_value_t = 4096, value_parser = value_parser!(u32).range(256..=4096))]
    pub uring_queue_depth: u32,

    /// io_uring 正在进行中 (in-flight) 的 IO 限制
    #[arg(long, default_value_t = 32, value_parser = value_parser!(u32).range(4..1024))]
    pub uring_inflight_limit: u32,

    /// 空闲时的自旋限制（用于平衡延迟和 CPU 消耗）
    #[arg(long, default_value_t = 32768, value_parser = value_parser!(u64).range(8192..))]
    pub idle_spin_limit: u64,

    /// 为系统核心预留的 CPU 数量（协作核心数）
    #[arg(long, default_value_t = 3, value_parser = value_parser!(u32).range(3..=1024))]
    pub coop: u32,
}

/// 任务性能指标记录器
#[derive(Clone, Copy)]
struct TaskMetrics {
    /// WAL 系统底层指标（如背压次数）
    wal_system_metrics: &'static WalSystemMetrics,
    /// 任务延迟直方图（微秒）
    task_lat_hist: &'static UnsafeCell<Histogram<u64>>,
    /// 完成的操作总数
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
    
    // 基础参数校验
    if args.duration < Duration::from_secs(3) {
        bail!("duration must be at least 3s");
    }
    if args.preallocate.0 != 0 && args.preallocate.0 < mib(256u64) {
        bail!("preallocate must be at least 256MiB");
    }
    if args.slab_capacity.0 < kib(4u64) {
        bail!("slab_capacity must be at least 4KiB");
    }

    // 1. 准备/创建 WAL 文件
    let target = WalSystem::create_target(
        &args.path,
        args.preallocate.0.into_lossy(),
        !args.disable_physical_preallocate,
    )?;

    let wal_cancellation_token = CancellationToken::new();

    // 2. 初始化 WAL 系统及其 IO 线程 (SQ/CQ)
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

    // 3. 根据可用 CPU 核心数启动生产者线程 (Thread Per Core)
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

    // 4. 设置信号处理 (Ctrl+C 退出)
    let (ctrlc_sender, ctrlc_receiver) = mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = ctrlc_sender.send(());
    })?;

    // 5. 主监控循环：每秒打印一次实时性能数据
    let mut ops_amount = 0;
    for duration in 0..args.duration.as_secs() {
        let ctrlc = ctrlc_receiver.recv_timeout(Duration::from_secs(1));
        if matches!(ctrlc, Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected)) {
            break;
        }

        let ops_now = ops.load(Ordering::Relaxed);
        let ops_per_sec = ops_now - ops_amount;
        ops_amount = ops_now;

        let inflight = wal.inflight(); // 当前正在进行中（未刷盘）的 IO 数量

        println!(
            "[{:>2}/{}] inflight: {} ops: {}",
            duration + 1,
            args.duration.as_secs(),
            inflight,
            ops_per_sec
        );
    }

    // 6. 停止压力测试
    tpc_cancellation_token.cancel();
    let tpc = tpc.into_iter().map(|v| v.join()).collect();

    // 7. 停止 WAL 系统并收集性能报表
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

/// 启动核心绑定的生产者线程
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
        
        // 每个线程运行一个本地的 Tokio Runtime，以最大化单核利用率
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
                    // 启动多个异步生产者任务
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

/// 生产者任务逻辑：不断生成负载并请求 WAL 写入
async fn producer_task(
    metrics: TaskMetrics,
    use_rng_payload: bool,
    wal: Arc<WalSystem>,
    cancellation_token: CancellationToken,
) {
    let mut rng = SmallRng::from_rng(&mut rng());
    let mut b = vec![0u8; 4096]; // 数据缓冲区
    payload::<false>(use_rng_payload, &mut rng, &mut b);

    loop {
        let capacity = payload::<true>(use_rng_payload, &mut rng, &mut b);
        let reserve_inst = Instant::now();
        
        // 核心操作：请求 WAL 空间并写入
        // wal.reserve 是零拷贝的：它直接给你一个目标内存切片 w，你只需要把数据拷进去
        let fut = wal.reserve(
            capacity,
            |w| unsafe { copy_nonoverlapping(b.as_ptr(), w.as_mut_ptr(), capacity) },
            metrics.wal_system_metrics,
            &cancellation_token,
        );
        
        let is_cancelled = cancellation_token.is_cancelled();
        
        // await 等待磁盘持久化完成
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

/// 生成测试负载数据
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

/// 填充随机数据并计算校验和（模拟真实计算开销）
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

/// 打印汇总的性能统计信息
fn print_thread_histogram(sq: SQR, cq: CQR, tpc: Vec<TRR>) -> Result<()> {
    const MULTIPLIER: f64 = 1000.0f64;

    let mut sq_idle_spin_perc = f64::NAN;
    let mut cq_idle_spin_perc = f64::NAN;

    // 1. 打印 Submission Queue (写入提交) 的统计
    if let Ok(Ok((mut_hist, retire_hist, sq_batch_hist, sq_idle_spin_perc0))) = sq.join() {
        print_histogram("slab ready", &mut_hist, "us"); // Slab 准备就绪的耗时
        print_histogram("slab retire", &retire_hist, "bytes"); // Slab 回收时的数据量
        print_histogram("submission batch", &sq_batch_hist, ""); // 单次提交给 io_uring 的批大小
        sq_idle_spin_perc = sq_idle_spin_perc0;
    }
    
    // 2. 打印 Completion Queue (IO 完成) 的统计
    if let Ok(Ok((submit_hist, cq_batch_hist, cq_idle_spin_perc0))) = cq.join() {
        print_histogram("IO complete lat", &submit_hist, "us"); // 磁盘 IO 真实的持久化延迟
        print_histogram("completion batch", &cq_batch_hist, ""); // 单次处理的 IO 完成批大小
        cq_idle_spin_perc = cq_idle_spin_perc0;
    }
    
    // 3. 汇总所有生产者线程的统计数据
    let mut wal_system_metrics = WalSystemMetrics::default();
    let mut task_lat_hist = build_task_lat_histogram()?;
    for tr in tpc {
        if let Ok(Ok((metrics, hist))) = tr {
            wal_system_metrics += metrics;
            task_lat_hist += hist;
        }
    }
    print_histogram("task lat", &task_lat_hist, "us"); // 最终用户侧感受到的写入总延迟

    // 4. 打印各种内部系统指标
    println!(
        "slab ring backpressure: {}",
        wal_system_metrics.backpressure() // 缓冲区满导致的阻塞次数
    );
    println!(
        "slab owner mismatch: {}",
        wal_system_metrics.owner_mismatch()
    );
    println!(
        "slab claim contention: {}",
        wal_system_metrics.claim_contention() // 并发抢占 Slab 的竞争次数
    );
    println!("slab notify await: {}", wal_system_metrics.notify_await());
    println!(
        "submission idle spin perc: {:.3}",
        (sq_idle_spin_perc * MULTIPLIER).trunc() / MULTIPLIER // SQ 线程自旋（空转）比例
    );
    println!(
        "completion idle spin perc: {:.3}",
        (cq_idle_spin_perc * MULTIPLIER).trunc() / MULTIPLIER // CQ 线程自旋比例
    );

    Ok(())
}

/// 格式化并打印直方图
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
