#pragma once
//
// MdSlotRing.h —— db 行情接收路径的定长槽索引环（"刀 2"主交付）
//
// 完整方案与推导见 docs/db行情接收路径定长槽环改造方案.md。
// 本文件取代 BaseMarket.h:70 的
//     pubsub::SPSCQueue<std::string, BUFF_SIZE>;   // BUFF_SIZE = 524288 ⇒ 16 MiB/unit
//
// ===========================================================================
// 这就是一个【普通的单生产者/单消费者环】，没有花活
// ===========================================================================
//   * 没有 seqlock、没有 per-slot 版本号、没有 fence、没有"覆盖最旧"
//   * 满则丢【最新】那条 —— 与今天 boost::lockfree::spsc_queue 完全一致
//   * 只有两个偏离"教科书 SPSC"的地方，都是为了性能，不是语义：
//       ① 槽是定长的、字符串在构造时 reserve ⇒ 热路径 0 次 malloc
//       ② 消费端是【两段式】peek/release（见下"用法"），而不是一次 pop
//
// ---------------------------------------------------------------------------
// 为什么换（一帧行情的堆操作，方案文档 §1.1 / §1.4）
// ---------------------------------------------------------------------------
//   今天：3 次 malloc/free + 3 次载荷 memcpy，其中 free #2 跨线程
//         （push 收 const& ⇒ boost placement-new 拷贝构造 ⇒ malloc #2；
//           pop 会析构槽 ⇒ capacity 不跨轮次保留；pop 的 move-assign 释放的是
//           "上一轮 IO 线程分配"的缓冲 ⇒ 跨线程 free）
//   本环：0 次 malloc/free + 2 次载荷 memcpy，跨线程 free 归零
//
// ---------------------------------------------------------------------------
// 用法：两段式 peek / release（**必须照抄这个形状**）
// ---------------------------------------------------------------------------
//   const std::string* msg = nullptr;
//   int64_t recvNs = 0;
//   bool    pending = false;                 // 已 peek、还没 release
//   while (1) {
//       try {
//           if (mRing.peek(msg, recvNs)) {
//               pending = true;
//               parseMarketData(*msg, recvNs);   // ← 解析发生在 r 被钉住的期间
//               mRing.release();                 // ← 读完才"发布" r_
//               pending = false;
//               continue;
//           }
//       } catch (const std::exception& e) {
//           if (pending) { mRing.release(); pending = false; }  // ← 异常也必须推进
//           LOG_ERROR(...);
//       }
//       spin::cpu_relax();
//   }
//
// **为什么不能像普通队列那样"先出队再解析"**：peek 返回的是指向槽内的指针。
// 若先推进 r_，r 立刻逼近 w，生产者只需再写 1 条就会覆盖正在解析的那个槽 ⇒ 撕裂。
// 把 r_ 钉住到解析结束，生产者最多前进到 r+N-1，写的是
// {(r+1)%N … (r+N-1)%N}，**永远不含 r%N**。
//
// **两个方向的 release/acquire 配对**（标准 SPSC，缺一不可）：
//   生产者 w_.store(release) ←→ 消费者 w_.load(acquire)   发布"槽里有新数据"
//   消费者 r_.store(release) ←→ 生产者 r_.load(acquire)   发布"槽已腾空、可复用"
// 少了第一个会读到半写的槽；少了第二个生产者会误判满而白白丢包。
//
// ---------------------------------------------------------------------------
// 本文件【只依赖 std】—— 不依赖 boost / simdjson / fmt / tbb
// ---------------------------------------------------------------------------
//   所以能在开发机（macOS/arm64）上直接编译、跑功能测试 + TSan + ASan
//   （方案文档 §7.1，脚本 db/test/run_md_slot_ring_test.sh）。
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
    kDropped  = 1,   // 环满，丢弃最新那条（与今天 boost spsc 行为一致）
    kOverflow = 2,   // len > maxMsg，未写入（方案文档 §4.2：应打一次性 LOG_ERROR）
};

// 单生产者 / 单消费者、定长槽、索引式环。
//
//   N      —— 槽数，必须是 2 的幂（模板参数 ⇒ w % N 编译成一条 and）
//   maxMsg —— 单条消息字节上限；构造时按此 reserve，之后不再分配
//
// **N 的语义**（方案文档 §4.1）：它【不是缓冲深度】，而是
// "消费者卡住多久之后生产者开始丢"。取 1024 的唯一目的是让 dropped 恒为 0。
//
// **maxMsg 怎么定**：必须 >= 该 unit 实际订阅的【最大】报文。
// 【实测】2026-10-14，REST 快照，含合并流 wrapper 约 46 B：
//     Binance spot  depth20  ≤ 1366 B   （10 个币对取最大）
//     Binance spot  depth10  ≈  704 B
//     Binance futures depth20 ≈  980 B
//     Binance bookTicker / markPrice / aggTrade / kline ≈ 120–300 B
//     OKX   books sz=400     ≈ 25.9 KB   ⚠️ 超
//     Bybit linear limit=200 ≈  8.6 KB   ⚠️ 超
//     Gateio spot limit=100  ≈  4.5 KB   ⚠️ 超
//   ⇒ **4096 覆盖 Binance 全部深度档**（depth20 有 ~2.9× 余量）；
//     要上 OKX/Bybit/Gateio 的深度快照，就得按上面的值单独配。
//   ⚠️ **内存 = N × maxMsg**，所以 maxMsg 应该**按 marketType 分别配**，
//      不要用一个全局常量把所有 unit 都抬到最大那个值。
template <uint32_t N>
class MdSlotRing {
    static_assert(N > 0, "N must be > 0");
    static_assert((N & (N - 1)) == 0, "N must be a power of two");

public:
    static constexpr uint32_t capacity() noexcept { return N; }

    // 槽对齐到 cache line：避免"生产者写槽 i+1"把"消费者正在读的槽 i"所在的行打脏。
    // 代价 1024 × 64 B = 64 KiB，相对载荷 N × maxMsg（1–4 MiB）可忽略。
    struct alignas(64) Slot {
        std::string msg;
        int64_t     recvNs{0};
    };

    explicit MdSlotRing(size_t maxMsg) : maxMsg_(maxMsg) {
        slots_.resize(N);
        for (auto& s : slots_) s.msg.reserve(maxMsg);   // ← 整个生命周期唯一一次分配
    }

    MdSlotRing(const MdSlotRing&)            = delete;
    MdSlotRing& operator=(const MdSlotRing&) = delete;

    // ======================= 生产者：WS IO 线程 =======================
    //
    // noexcept 是成立的：len <= maxMsg_ <= capacity() ⇒ assign 不会重分配、
    // 不会抛（len <= max_size 恒真）。这也是把分配彻底移出热路径的意义。
    //
    // ⚠️ 本函数【不做日志】—— 环只依赖 std，日志由调用方在 kOverflow 分支做
    //    （方案文档 §2.4 / §4.2）。
    WriteResult write(const char* data, size_t len, int64_t recvNs) noexcept {
        if (len > maxMsg_) {                     // 超长：不写坏槽、不推进 w
            overflow_.fetch_add(1, std::memory_order_relaxed);
            return WriteResult::kOverflow;
        }
        const uint64_t w = w_.load(std::memory_order_relaxed);
        if (w - r_.load(std::memory_order_acquire) >= N) {   // 满
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return WriteResult::kDropped;
        }
        Slot& s = slots_[w % N];
        s.msg.assign(data, len);                 // capacity 够 ⇒ 0 分配，1 次 memcpy
        s.recvNs = recvNs;
        w_.store(w + 1, std::memory_order_release);          // 发布新数据
        return WriteResult::kOk;
    }

    // ======================= 消费者：consume 线程 =======================

    // 当前积压深度 = w - r。
    // 它【不参与控制流】，只用于观测（方案文档 §8 第 2.5 步要打 max/p99）。
    // 建议在 consume() 的循环里维护一个本地 max，不要在环里加原子计数。
    uint64_t readable() const noexcept {
        const uint64_t w = w_.load(std::memory_order_acquire);
        const uint64_t r = r_.load(std::memory_order_relaxed);
        return w > r ? w - r : 0;
    }

    // 看一条（不推进游标）。返回的指针只在【下一次 release() 之前】有效。
    bool peek(const std::string*& out, int64_t& recvNs) const noexcept {
        const uint64_t r = r_.load(std::memory_order_relaxed);
        if (r == w_.load(std::memory_order_acquire)) return false;   // 空
        const Slot& s = slots_[r % N];
        out    = &s.msg;
        recvNs = s.recvNs;
        return true;
    }

    // 读完/解析完之后调 —— 这一步"发布" r_（release-store），
    // 告诉生产者"这个槽已经腾空、可以复用了"。
    //
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

    size_t maxMsg() const noexcept { return maxMsg_; }

    // 近似常驻内存：对象头 + 槽数组 + 每槽的字符串缓冲（≈ maxMsg）。
    // reserve(n) 在 libstdc++/libc++ 上按 n 精确分配，所以这是准的而不是上限。
    size_t bytes() const noexcept {
        return sizeof(*this) + N * sizeof(Slot) + N * maxMsg_;
    }

private:
    // 各计数器独占 cache line（生产者只碰 w_/dropped_/overflow_，
    // 消费者只碰 r_，避免伪共享）。
    alignas(64) std::atomic<uint64_t> w_{0};
    alignas(64) std::atomic<uint64_t> r_{0};
    alignas(64) std::atomic<uint64_t> dropped_{0};
    alignas(64) std::atomic<uint64_t> overflow_{0};

    size_t maxMsg_{0};
    std::vector<Slot> slots_;
};

} // namespace md
