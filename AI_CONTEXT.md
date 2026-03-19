# Repository Map

## What this repo does
这个项目是解决一个比赛题目的高性能并发写入项目

比赛题目如下：
```
1、基于任意语言实现一个跳表（SkipList）；
2、1中的跳表支持并发插入和查询；
3、多线程并发向1中的跳表插入200万条key-value对，记录插入时间，key随机生成（小写字母+数字组成），长度在[20-60]字节，value为8字节随机整数，对于每个线程，要求每插入一条记录后，再生成下一个key和value，并将所有生成的key保存在一个全局的数组中，数组共200万个单元，每个单元存放key的指针；
4、3中可以在内存中创建多个跳表，但是要求单个跳表插满后（达到自己设定的单个跳表最大记录数），再向另一个跳表中插入数据，也就是始终只有1个跳表处于插入状态；
5、基于3中的数组，单线程随机生成[0-2000000)的序号，然后基于该序号读取数组对应单元的key，并基于该key在跳表中进行查找，每查找到之后，继续生成下一个随机序号，重复10万次，记录查询时间；

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
