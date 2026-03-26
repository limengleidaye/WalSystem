# Repository Map

## What this repo does
这个项目是解决一个比赛题目的高性能并发写入项目

比赛题目如下：
```
实现一个多生产者（每个生产者对应1个线程，必须在消费者线程完成计算任务后再产生新的任务）、多消费者（每个消费者对应1个线程，在完成计算任务后通知对应的生产者线程产生下一个任务）的任务队列（必须所有生产者、消费者共用这一个任务队列），任务队列最大长度为10000（队列满则生产者需要等待，队列为空则消费者要等待），生产者随机产生1到1000个整数（整数范围在1-100000之间），消费者计算这些整数的和，持续60秒，统计每秒钟完成的任务数。

在上面的条件下，不限制任务队列个数，但是每个任务需要原子写入（即不同任务之间的写入内容不能有交叉）到a.data文件中，写入内容包括每个任务的所有整数个数、所有整数、整数的和、顺序号（顺序号从1开始递增，每个任务落盘时分配一个唯一递增的值，先写入的任务的顺序号比后写入的任务的顺序号小，全局单调递增）
```
## Main entry points
- crates/oxalium——主函数入口以及wal的实现
- crates/oxalium-infra——工具类
- crates/oxalium-mock——学习仿照的项目工程

```
oxalium/
  ├── AI_CONTEXT.md
  ├── Cargo.toml                      # workspace 根
  ├── README
  └── crates/
      ├── oxalium/                    # 主业务 crate
      │   ├── Cargo.toml
      │   └── src/
      │       ├── lib.rs              # 导出 wal，设置全局分配器 mimalloc
      │       ├── wal.rs              # WAL 核心实现
      │       └── bin/
      │           └── pcr-2601.rs     # 压测/主程序入口
      ├── oxalium-infra/              # 基础工具 crate
      │   ├── Cargo.toml
      │   └── src/
      │       ├── lib.rs
      │       ├── convert.rs          # 数值 lossy 转换 trait
      │       ├── iter.rs             # distribute 任务分配
      │       └── process.rs          # 进程峰值内存读取
```

## Important modules
- Workspace 入口是 Cargo.toml，通过 members = ["crates/*"] 聚合 3 个 crate。
- 主执行入口是 pcr-2601.rs:107，流程是：
    1. 解析 CLI 参数
    2. 调用 WalSystem::create_target 创建/预分配 WAL 文件
    3. 调用 WalSystem::bootstrap 启动 WAL 系统
    4. 启动 TPC producer 线程，每线程内跑单线程 Tokio runtime
    5. 周期打印吞吐与 inflight
    6. 结束后汇总 SQ/CQ/任务延迟直方图
- 主库入口是 lib.rs，只暴露 pub mod wal。
- 核心门面是 wal.rs:666 里的 WalSystem，公开能力：
    - create_target
    - bootstrap
    - reserve
    - inflight
- WAL 内部核心模块是 wal.rs 里的私有 mod slab，负责：
    - Slab
    - SlabRing
    - 状态机 NONE/WRITING/READY/IN_FLIGHT/COMPLETED
    - producer 与 SQ/CQ 之间的共享内存环

## 依赖关系
```
pcr-2601 (bin)
    -> oxalium::wal::{WalSystem, WalSystemMetrics, SQR, CQR}
    -> oxalium-infra::{convert, iter, process}

oxalium (lib)
    -> oxalium-infra
    -> anyhow, io-uring, tokio, tokio-util
    -> hdrhistogram, clap, rand, ctrlc
    -> mimalloc, minstant, num_cpus, humantime, bytesize
    -> libc (linux)

oxalium::wal
    -> 私有 slab 模块
    -> io_uring 封装
    -> RegisteredMemory 注册缓冲区
    -> SQ 线程 / CQ 线程
    -> SlabRing 作为 producer 与 IO 线程之间的桥

oxalium-infra
    -> bytesize, itertools, num_cpus
    -> libc (linux) / windows-sys (windows)
```
## Data flow

```
producer_task
    -> WalSystem::reserve()
    -> SlabRing::reserve()
    -> 写入当前 slab
    -> SQ thread 扫描 READY slab
    -> io_uring submit
    -> CQ thread 收割完成事件
    -> SlabRing::advance_persisted_lsn()
    -> 唤醒等待持久化的 producer
```
