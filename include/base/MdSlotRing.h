#pragma once
//
// MdSlotRing.h —— db 行情接收路径的定长槽索引环
//
// 单生产者（WS IO 线程）/ 单消费者（consume 线程）、定长槽、索引式。
// 取代 BaseMarket.h 的 pubsub::SPSCQueue<std::string, BUFF_SIZE>
// （BUFF_SIZE = 524288 ⇒ 16 MiB/unit）。
//
//   * 没有 seqlock、没有 per-slot 版本号、没有 fence、没有"覆盖最旧"
//   * 满则丢【最新】那条 —— 与 boost::lockfree::spsc_queue 语义一致
//   * 偏离教科书 SPSC 的两点都只为性能，不改语义：
//       ① 槽定长、字符串构造时 reserve ⇒ 热路径 0 次 malloc/free
//       ② 消费端两段式 peek/release（不是一次 pop）—— 见下"用法"
//
// 一帧行情：3 次 malloc/free + 3 次 memcpy（含 1 次跨线程 free）
//        → 0 次 malloc/free + 2 次 memcpy（跨线程 free 归零）
//
// 本文件只依赖 std（无 boost / simdjson / fmt / tbb），所以能在开发机
// （macOS/arm64）直接编译并跑功能测试 + TSan + ASan + x86-64 codegen
// （脚本 db/test/run_md_slot_ring_test.sh）。完整推导见
// docs/db行情接收路径定长槽环改造方案.md §4。
//
// ===========================================================================
// 用法
// ===========================================================================
//
// 构造（每个 unit 一个环；depth / maxMsg 都按该 unit 的 marketType 配，见下）：
//
//   md::MdSlotRing mRing{mdRingDepth(marketType), mdRingMaxMsg(exch, marketType)};
//
// ---- 生产者：WS IO 线程，每收到一条报文调一次 write() ----
//
//   void BinanceUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t ns) {
//       const md::WriteResult r = mRing.write(reinterpret_cast<const char*>(data), len, ns);
//       if (r == md::WriteResult::kOverflow && !overflowLogged_) {
//           LOG_ERROR("msg overflow: len={} maxMsg={}", len, mRing.maxMsg());
//           overflowLogged_ = true;      // 超长 = 配置错，每条 unit 只打一次
//       }
//       // kDropped 是背压，不必每条处理；周期上报 dropped() / readable() 即可
//   }
//
// ---- 消费者：consume 线程，两段式 peek / release（**必须照抄这个形状**）----
//
//   const std::string* msg = nullptr;
//   int64_t recvNs = 0;
//   bool    pending = false;                    // 已 peek、还没 release
//   while (1) {
//       try {
//           if (mRing.peek(msg, recvNs)) {
//               pending = true;
//               parseMarketData(*msg, recvNs);   // ← 解析发生在 r 被钉住的期间
//               mRing.release();                 // ← 读完才发布 r_
//               pending = false;
//               continue;
//           }
//       } catch (const std::exception& e) {
//           if (pending) { mRing.release(); pending = false; }   // ← 异常也必须推进
//           LOG_ERROR("... {}", e.what());
//       }
//       spin::cpu_relax();
//   }
//
// ⚠️ 不能"先出队再解析"：peek 返回的是指向槽内的指针。若先推进 r_，r 立刻逼近 w，
//    生产者再写 1 条就会覆盖正在解析的槽 ⇒ 撕裂。把 r_ 钉到解析结束，生产者最多
//    前进到 r+depth-1，写的是 {(r+1)%depth … (r+depth-1)%depth}，永不含 r%depth。
// ⚠️ 异常分支必须 release()：否则游标不动 ⇒ 坏包被无限重解析 ⇒ 活锁。
//
// 两个 release/acquire 配对（标准 SPSC，缺一不可）：
//   生产者 w_.store(release) ←→ 消费者 w_.load(acquire)   发布"有新数据"
//   消费者 r_.store(release) ←→ 生产者 r_.load(acquire)   发布"槽已腾空"
//
// ===========================================================================
// depth 与 maxMsg 的建议大小（都按 marketType 分别配）
// ===========================================================================
//
//   md::MdSlotRing ring(depth, maxMsg);
//
// **两个都是构造参数、都不是模板参数** —— 理由见文件末尾"为什么不做模板参数"。
//
// depth —— 槽数，构造时向上取整到 2 的幂（内部存 mask_ = depth-1 ⇒ w & mask_ 一条 and）。
//      它的语义**不是"缓冲深度"**，而是"**消费者被卡住多久之后生产者开始丢**"。
//      所以按【时间】定，不按密度定：
//
//          depth ≈ 目标容忍停顿秒数 × 该 unit 的聚合速率
//          聚合速率 = 每 symbol 速率 × 该 unit 的 symbol 数（≤ tokenLot）
//          再向上取 2 的幂，下限 64（避免"写两条就满"的退化）
//
//      每 symbol 速率（量级）：TRADES 10²/s（唯一的高密度流）· DEPTH1 10¹/s ·
//      DEPTH5/10/20 10¹/s（@100ms）· KLINE_* / FUNDING_RATE 1/s。
//
//      ✅ 推荐（按"每 unit 1 个 symbol"）：
//
//         marketType       每 symbol 速率   depth    maxMsg   内存
//         --------------   -------------   ------   -------   --------
//         TRADES           10²/s           1024      512      576 KiB
//         DEPTH1           10¹/s            256      256       80 KiB
//         DEPTH5           10¹/s            128      512       72 KiB
//         DEPTH10          10¹/s            128     1024      136 KiB
//         DEPTH20          10¹/s            256     2048      528 KiB
//         KLINE_*          1/s               64      512       36 KiB
//         FUNDING_RATE     1/s               64      512       36 KiB
//
//      ⚠️ 三个坑：
//        1. **必须按【规划的 symbol 数】定 depth，不是今天的 1 个**。tokenLot 扩到 N 个
//           symbol 时聚合速率 ×N ⇒ 同样 depth 的容忍时间除以 N。
//        2. **环满丢的是【最新】那条**（与 boost spsc 一致）。对 KLINE / FUNDING_RATE /
//           深度档这类**快照流**，被卡住时会"处理完旧的、把最新的丢掉" —— 恰好最不该丢。
//           ⇒ depth 定小了在快照流上比在 TRADES 上更危险；要么留足，要么上"跳过积压"。
//        3. **最终靠每个 unit 的 dropped() 标定**：恒 0 ⇒ 够；> 0 ⇒ 翻倍重测。
//           `overflow()`（maxMsg 定小 = 配置错）与 `dropped()`（depth 定小 = 背压）
//           是两个独立计数，别混。
//
// maxMsg —— 单条报文字节上限，必须 >= 该 unit 实际订阅的【最大】报文。
//      【实测 2026-10-14】REST 全量快照取 10 个主流币对的最大 len
//      （Binance 侧含 ~46 B 合并流 wrapper {"stream":...,"data":{...}}）：
//
//        Binance DEPTH1 / TRADES / FUNDING_RATE / KLINE_*   ≈ 120–300 B
//        Binance spot    DEPTH10                            ≈   704 B
//        Binance futures DEPTH20                            ≈   980 B
//        Binance spot    DEPTH20    ← Binance 最大的一档     ≤ 1366 B
//        Gateio spot     limit=100                          ≈  4.5 KB  ⚠️
//        Bybit linear    limit=200                          ≈  8.6 KB  ⚠️
//        OKX   books     sz=400                             ≈ 25.9 KB  ⚠️
//
//      ⚠️ "4096 够"是 Binance 的性质，不是环的性质。OKX/Bybit/Gateio 的深档快照
//         要按上表单独配（建议 32768 / 16384 / 8192）。
//      ⚠️ maxMsg 定小了走 kOverflow（不写入、计数 +1），不是静默截断 ⇒ 上线后盯 overflow()：
//         恒 0 ⇒ 定对了；> 0 ⇒ 日志里带实际 len，取最大值向上取 2 的幂重设。
//
// 内存 = depth × maxMsg（槽数组 + 每槽字符串缓冲），【实测】：
//        depth=1024, maxMsg=4096  ⇒ 4.06 MiB/unit（对照今天 16 MiB，省 4×）
//        depth=1024, maxMsg=1024  ⇒ 1.06 MiB/unit（省 15×）
//
// ---------------------------------------------------------------------------
// 为什么 depth / maxMsg 都不做模板参数（不用 MdSlotRing<N> 或 <N, MaxMsg>）
// ---------------------------------------------------------------------------
//   收益是 0，传染面却很大：
//
//   * **收益为 0**：占用内存由"传进去的数值"决定，与它是模板参数还是构造参数无关。
//     `MdSlotRing<1024> r(512)` 与 `MdSlotRing r(1024, 512)` 的 sizeof/堆占用完全一样。
//   * **指令数也一样**：depth 是 2 的幂时 `w & mask_` 与 `w % N` 都是一条 `and`。
//     实测（探针 db/test/md_slot_ring_depth_probe.cpp）：`write` 62→62、`peek` 26→26、
//     `release` 12→12 条指令，locked RMW 全 0；只多一次 L1 load（`mask_` 与环头同 cache line）。
//   * **传染面**：环是 BaseUnit 的成员 ⇒ 模板化就得把 BaseUnit 跟着模板化 ⇒ 7 个 unit
//     子类（Binance/BinanceSbe/Bybit/Okx/OkxSbe/Gateio/GateioSbe）全部模板化 ⇒ 约 1000 行的
//     parseMarketData 会按 (交易所 × marketType) 实例化多份（11 种 marketType），
//     编译时间与二进制体积双双膨胀；7 个 `new XxxUnit(...)` 构造点也要改成运行时 switch。
//   * **唯一"只有编译期才拿得到"的东西 —— 槽内联存储 + 原地 simdjson 解析 —— 用运行时
//     尺寸也能拿到**：把槽存储从 `std::vector<Slot>`（每槽一个 std::string 堆块）
//     改成一个扁平 `std::vector<char> buf_(depth * (maxMsg + SIMDJSON_PADDING))`，
//     每槽 `maxMsg + SIMDJSON_PADDING` 字节连续可读，peek 直接交出可解析指针。
//     不新增类型、不传染，还省掉第 2 次 memcpy（方案文档 §3 / §2.5）。
//

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace md {

// write() 的结果。调用方可以忽略（那就等价于今天的"静默丢"），也可以据此计数/告警。
enum class WriteResult : uint8_t {
    kOk       = 0,   // 已入队
    kDropped  = 1,   // 环满，丢弃最新那条（背压；与今天 boost spsc 行为一致）
    kOverflow = 2,   // len > maxMsg，未写入（配置错；调用方应打一次性 LOG_ERROR）
};

// 单生产者 / 单消费者、定长槽、索引式环。
// depth / maxMsg 都是构造参数（不是模板参数），怎么取见文件头。
class MdSlotRing {
public:
    // 向上取整到 2 的幂（depth 是 2 的幂时 `w & mask_` 只需一条 and）。
    static size_t nextPow2(size_t n) noexcept {
        size_t p = 1;
        while (p < n) p <<= 1;
        return p;
    }

    // 槽对齐到 cache line：避免"生产者写槽 i+1"打脏"消费者正在读的槽 i"所在的行。
    // 代价 depth × 64 B，相对载荷 depth × maxMsg 可忽略。
    struct alignas(64) Slot {
        std::string msg;
        int64_t     recvNs{0};
    };

    // depth 会被向上取整到 2 的幂；maxMsg 是单条上限。
    MdSlotRing(size_t depth, size_t maxMsg)
        : depth_(nextPow2(depth)), mask_(depth_ - 1), maxMsg_(maxMsg) {
        slots_.resize(depth_);
        for (auto& s : slots_) s.msg.reserve(maxMsg);   // ← 整个生命周期唯一一次分配
    }

    MdSlotRing(const MdSlotRing&)            = delete;
    MdSlotRing& operator=(const MdSlotRing&) = delete;

    // ======================= 生产者：WS IO 线程 =======================
    //
    // noexcept：len <= maxMsg_ ⇒ assign 不重分配、不抛。
    // ⚠️ 本函数【不做日志】—— 环只依赖 std，日志由调用方在 kOverflow 分支做。
    WriteResult write(const char* data, size_t len, int64_t recvNs) noexcept {
        if (len > maxMsg_) {                     // 超长：不写坏槽、不推进 w
            overflow_.fetch_add(1, std::memory_order_relaxed);
            return WriteResult::kOverflow;
        }
        const uint64_t w = w_.load(std::memory_order_relaxed);
        if (w - r_.load(std::memory_order_acquire) >= depth_) {   // 满
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return WriteResult::kDropped;
        }
        Slot& s = slots_[w & mask_];
        s.msg.assign(data, len);                 // capacity 够 ⇒ 0 分配，1 次 memcpy
        s.recvNs = recvNs;
        w_.store(w + 1, std::memory_order_release);          // 发布新数据
        return WriteResult::kOk;
    }

    // ======================= 消费者：consume 线程 =======================

    // 当前积压深度 = w - r。不参与控制流，只用于观测（建议在 consume() 里维护
    // 本地 max，不要在环里加原子计数）。
    uint64_t readable() const noexcept {
        const uint64_t w = w_.load(std::memory_order_acquire);
        const uint64_t r = r_.load(std::memory_order_relaxed);
        return w > r ? w - r : 0;
    }

    // 看一条（不推进游标）。返回的指针只在【下一次 release() 之前】有效。
    bool peek(const std::string*& out, int64_t& recvNs) const noexcept {
        const uint64_t r = r_.load(std::memory_order_relaxed);
        if (r == w_.load(std::memory_order_acquire)) return false;   // 空
        const Slot& s = slots_[r & mask_];
        out    = &s.msg;
        recvNs = s.recvNs;
        return true;
    }

    // 读完/解析完之后调 —— 发布 r_（release-store），告诉生产者"这个槽已腾空、可复用"。
    // ⚠️ 【异常也必须走到这里】，否则游标不动 ⇒ 坏包被无限重解析 ⇒ 活锁。
    //    调用方必须用一个 pending 标志在 catch 里兜底推进（见文件头"用法"）。
    void release() noexcept {
        r_.store(r_.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }

    // ======================= 统计与自省 =======================

    uint64_t dropped()  const noexcept { return dropped_.load(std::memory_order_relaxed); }
    uint64_t overflow() const noexcept { return overflow_.load(std::memory_order_relaxed); }
    uint64_t writeSeq() const noexcept { return w_.load(std::memory_order_acquire); }
    uint64_t readSeq()  const noexcept { return r_.load(std::memory_order_relaxed); }

    size_t depth()  const noexcept { return depth_; }    // 实际槽数（已取整到 2 的幂）
    size_t maxMsg() const noexcept { return maxMsg_; }

    // 近似常驻内存：对象头 + 槽数组 + 每槽的字符串缓冲（≈ maxMsg）。
    // reserve(n) 在 libstdc++/libc++ 上按 n 精确分配，所以这是准的而不是上限。
    size_t bytes() const noexcept {
        return sizeof(*this) + depth_ * sizeof(Slot) + depth_ * maxMsg_;
    }

private:
    // 各计数器独占 cache line（生产者只碰 w_/dropped_/overflow_，消费者只碰 r_）。
    alignas(64) std::atomic<uint64_t> w_{0};
    alignas(64) std::atomic<uint64_t> r_{0};
    alignas(64) std::atomic<uint64_t> dropped_{0};
    alignas(64) std::atomic<uint64_t> overflow_{0};

    size_t depth_{0};    // 已取整到 2 的幂
    size_t mask_{0};     // depth_ - 1
    size_t maxMsg_{0};
    std::vector<Slot> slots_;
};

} // namespace md
