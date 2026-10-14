// md_slot_ring_test.cpp —— MdSlotRing 功能测试
//
// 只依赖 std：不需要 boost / simdjson / fmt / tbb，所以能在开发机（macOS/arm64）
// 直接编译并运行。方案文档 §7.1 的"本机能验的"第 1 项。
//
//   clang++ -std=c++17 -O2 -pthread -I<db/include> md_slot_ring_test.cpp -o /tmp/t && /tmp/t
//   ... 再加 -fsanitize=thread / -fsanitize=address,undefined 各跑一遍
//
// 覆盖：
//   t1  单生产单消费：不丢、不重、有序、载荷不被撕裂（TSan 的重点）
//   t2  满则丢【最新】、环里保留【最旧】的 N 条（与今天 boost spsc 一致）
//   t3  溢出：len > maxMsg 不写入、不推进 w、计数 +1、槽内容未被破坏（含边界）
//   t4  异常不活锁：pending 兜底推进后游标必须前进
//   t5  N = 1 的边界
//   t6  maxMsg 定多大：用放大过的 Binance depth20 帧验证 4096 够（方案文档 §4.2）
//   t7  ABI / 内存实测值，对上方案文档 §4.3
//   t8  核心安全属性：持有 peek 期间生产者绝不碰该槽（确定性，不依赖调度）

#include "base/MdSlotRing.h"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

int g_fail = 0;

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            ++g_fail;                                                          \
            std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond);      \
        }                                                                      \
    } while (0)

#define CHECK_EQ_U64(a, b)                                                     \
    do {                                                                       \
        const uint64_t _a = static_cast<uint64_t>(a);                          \
        const uint64_t _b = static_cast<uint64_t>(b);                          \
        if (_a != _b) {                                                        \
            ++g_fail;                                                          \
            std::printf("  FAIL %s:%d  %s == %s  (%llu != %llu)\n",            \
                        __FILE__, __LINE__, #a, #b,                            \
                        static_cast<unsigned long long>(_a),                   \
                        static_cast<unsigned long long>(_b));                  \
        }                                                                      \
    } while (0)

constexpr size_t kMaxMsg = 256;

// 造一条"可自证"的载荷：前 8 字节是 seq，其余字节是 seq 与下标的确定性函数。
// 任何撕裂 / 串槽 / 复用残留都会让 verify() 失败。
size_t fill(char* buf, uint64_t seq) {
    const size_t len = 32 + (seq % 200);            // 32..231，全部 < kMaxMsg
    std::memcpy(buf, &seq, sizeof(seq));
    for (size_t i = sizeof(seq); i < len; ++i) {
        buf[i] = static_cast<char>(static_cast<uint8_t>(seq * 31u + i * 7u));
    }
    return len;
}

bool verify(const std::string& m, uint64_t seq) {
    if (m.size() < sizeof(seq)) return false;
    uint64_t got = 0;
    std::memcpy(&got, m.data(), sizeof(got));
    if (got != seq) return false;
    for (size_t i = sizeof(seq); i < m.size(); ++i) {
        if (static_cast<uint8_t>(m[i]) != static_cast<uint8_t>(seq * 31u + i * 7u)) return false;
    }
    return true;
}

uint64_t seqOf(const std::string& m) {
    uint64_t s = 0;
    if (m.size() >= sizeof(s)) std::memcpy(&s, m.data(), sizeof(s));
    return s;
}

// ---------------------------------------------------------------------------
// t1 —— 不丢、不重、有序、载荷不撕裂
// ---------------------------------------------------------------------------
template <uint32_t N>
void t1_fifo(uint64_t total, const char* tag) {
    md::MdSlotRing<N> ring(kMaxMsg);
    std::atomic<uint64_t> retries{0};
    std::atomic<uint64_t> consumed{0};
    std::atomic<uint64_t> bad{0};

    std::thread producer([&] {
        char buf[kMaxMsg];
        for (uint64_t seq = 0; seq < total; ++seq) {
            const size_t len = fill(buf, seq);
            for (;;) {
                const md::WriteResult r = ring.write(buf, len, static_cast<int64_t>(seq));
                if (r == md::WriteResult::kOk) break;
                if (r == md::WriteResult::kOverflow) { bad.fetch_add(1); break; }
                retries.fetch_add(1, std::memory_order_relaxed);   // 满了，重试（不丢）
            }
        }
    });

    std::thread consumer([&] {
        const std::string* m = nullptr;
        int64_t ns = 0;
        uint64_t expect = 0;
        while (expect < total) {
            if (!ring.peek(m, ns)) continue;
            if (!verify(*m, expect)) bad.fetch_add(1, std::memory_order_relaxed);
            if (static_cast<uint64_t>(ns) != expect) bad.fetch_add(1, std::memory_order_relaxed);
            ring.release();
            consumed.store(++expect, std::memory_order_relaxed);
        }
    });

    producer.join();
    consumer.join();

    CHECK_EQ_U64(consumed.load(), total);                  // 一条不少
    CHECK_EQ_U64(bad.load(), 0);                           // 一条不错、一条不撕
    CHECK_EQ_U64(ring.dropped(), retries.load());          // dropped 计数精确
    CHECK_EQ_U64(ring.overflow(), 0);
    CHECK_EQ_U64(ring.readable(), 0);                      // 排空
    CHECK_EQ_U64(ring.writeSeq(), total);
    std::printf("  t1 %-22s N=%-5u total=%-9llu dropped=%-9llu (全部重试成功)\n",
                tag, N, static_cast<unsigned long long>(total),
                static_cast<unsigned long long>(ring.dropped()));
}

// ---------------------------------------------------------------------------
// t2 —— 满则丢最新，环里保留最旧的 N 条
// ---------------------------------------------------------------------------
void t2_drop_newest_keep_oldest() {
    constexpr uint32_t N = 64;
    constexpr uint64_t kExtra = 37;
    md::MdSlotRing<N> ring(kMaxMsg);
    char buf[kMaxMsg];

    uint64_t ok = 0, drop = 0;
    for (uint64_t seq = 0; seq < N + kExtra; ++seq) {          // 消费者完全没跑 ⇒ r 恒为 0
        const size_t len = fill(buf, seq);
        const md::WriteResult r = ring.write(buf, len, static_cast<int64_t>(seq));
        if (r == md::WriteResult::kOk) ++ok;
        else if (r == md::WriteResult::kDropped) ++drop;
        else { CHECK(false); }
    }
    CHECK_EQ_U64(ok, N);
    CHECK_EQ_U64(drop, kExtra);
    CHECK_EQ_U64(ring.dropped(), kExtra);
    CHECK_EQ_U64(ring.writeSeq(), N);                          // w 没有越过 N

    // 留下的必须是【最旧】的 0..N-1（丢最新 ⇒ 与 boost spsc 的语义一致）
    const std::string* m = nullptr;
    int64_t ns = 0;
    uint64_t expect = 0;
    while (ring.peek(m, ns)) {
        CHECK(verify(*m, expect));
        CHECK_EQ_U64(static_cast<uint64_t>(ns), expect);        // recvNs 跟着槽一起搬
        ++expect;
        ring.release();
    }
    CHECK_EQ_U64(expect, N);
    std::printf("  t2 %-22s ok=%llu dropped=%llu，留下的是最旧的 0..%u\n",
                "drop-newest", static_cast<unsigned long long>(ok),
                static_cast<unsigned long long>(drop), N - 1);
}

// ---------------------------------------------------------------------------
// t3 —— 溢出：不写入、不推进 w、计数 +1、槽内容未被破坏；含 maxMsg 边界
// ---------------------------------------------------------------------------
void t3_overflow() {
    constexpr uint32_t N = 8;
    constexpr size_t kMax = 64;
    md::MdSlotRing<N> ring(kMax);
    char buf[512];

    const size_t len0 = fill(buf, 7);                          // 39 B
    CHECK(ring.write(buf, len0, 7) == md::WriteResult::kOk);

    const uint64_t w0 = ring.writeSeq();
    CHECK(ring.write(buf, kMax + 1, 99) == md::WriteResult::kOverflow);
    CHECK_EQ_U64(ring.overflow(), 1);
    CHECK_EQ_U64(ring.writeSeq(), w0);                         // w 未推进
    CHECK_EQ_U64(ring.readable(), 1);
    CHECK_EQ_U64(ring.dropped(), 0);                           // 溢出不算 dropped

    // 边界：恰好 maxMsg 可以，maxMsg + 1 不行
    std::vector<char> exact(kMax, 'x');
    CHECK(ring.write(exact.data(), kMax, 11) == md::WriteResult::kOk);
    CHECK(ring.write(exact.data(), kMax + 1, 12) == md::WriteResult::kOverflow);
    CHECK_EQ_U64(ring.overflow(), 2);
    CHECK_EQ_U64(ring.readable(), 2);

    // 第一条槽的内容没有被溢出尝试破坏
    const std::string* m = nullptr;
    int64_t ns = 0;
    CHECK(ring.peek(m, ns));
    CHECK(verify(*m, 7));
    CHECK_EQ_U64(static_cast<uint64_t>(ns), 7);
    ring.release();

    CHECK(ring.peek(m, ns));
    CHECK_EQ_U64(m->size(), kMax);
    CHECK(m->find_first_not_of('x') == std::string::npos);
    ring.release();
    std::printf("  t3 %-22s 溢出不写入、不推进 w、计数精确；边界 maxMsg 可 / maxMsg+1 不可\n",
                "overflow");
}

// ---------------------------------------------------------------------------
// t4 —— 异常不活锁（peek/release 必须靠 pending 兜底推进）
// ---------------------------------------------------------------------------
void t4_exception_no_livelock() {
    constexpr uint32_t N = 16;
    md::MdSlotRing<N> ring(kMaxMsg);
    char buf[kMaxMsg];
    for (uint64_t s = 0; s < 3; ++s) {
        CHECK(ring.write(buf, fill(buf, s), static_cast<int64_t>(s)) == md::WriteResult::kOk);
    }

    std::vector<uint64_t> seen;
    const std::string* m = nullptr;
    int64_t ns = 0;
    bool pending = false;
    uint64_t guard = 0;

    // 这就是 BaseMarket.cpp consume() 的循环形状（方案文档 §2.3.1）
    while (ring.readable() > 0 && guard < 100) {
        ++guard;
        try {
            if (ring.peek(m, ns)) {
                pending = true;
                const uint64_t s = seqOf(*m);
                if (s == 1) throw std::runtime_error("bad frame");   // 模拟坏包
                seen.push_back(s);
                ring.release();
                pending = false;
            }
        } catch (const std::exception&) {
            if (pending) { ring.release(); pending = false; }        // ← 兜底推进
        }
    }

    CHECK(guard < 100);                     // 没有活锁（否则这里必然超限）
    CHECK_EQ_U64(ring.readable(), 0);       // 坏包那条被丢弃，游标正常前进
    CHECK_EQ_U64(seen.size(), 2);
    CHECK_EQ_U64(seen.empty() ? 0 : seen[0], 0);
    CHECK_EQ_U64(seen.size() > 1 ? seen[1] : 9, 2);
    std::printf("  t4 %-22s 坏包只被解析一次（guard=%llu），游标正常前进\n",
                "exception", static_cast<unsigned long long>(guard));
}

// ---------------------------------------------------------------------------
// t5 —— N = 1 的边界
// ---------------------------------------------------------------------------
void t5_n1() {
    md::MdSlotRing<1> ring(kMaxMsg);
    char buf[kMaxMsg];
    const size_t len = fill(buf, 0);

    CHECK(ring.write(buf, len, 0) == md::WriteResult::kOk);
    CHECK(ring.write(buf, len, 1) == md::WriteResult::kDropped);   // N=1 ⇒ 立刻满
    CHECK_EQ_U64(ring.dropped(), 1);

    const std::string* m = nullptr;
    int64_t ns = 0;
    CHECK(ring.peek(m, ns));
    CHECK(verify(*m, 0));
    ring.release();
    CHECK(!ring.peek(m, ns));                                      // 空了
    CHECK_EQ_U64(ring.readable(), 0);

    CHECK(ring.write(buf, len, 2) == md::WriteResult::kOk);        // 空出来就能再写
    CHECK(ring.peek(m, ns));
    CHECK_EQ_U64(static_cast<uint64_t>(ns), 2);
    ring.release();
    std::printf("  t5 %-22s N=1 写-满-丢-读-再写 正常\n", "N=1");
}

// ---------------------------------------------------------------------------
// t6 —— maxMsg 该定多大：用【真实报文形状】验证 4096 够不够（方案文档 §4.2）
//
// 构造一条"最坏形状"的 Binance 合并流 depth20 帧（20 档 × 2 侧，price/qty 都取
// 最宽字段），量出字节数，再让它真的过一遍环。
// 这样"4096 够不够"就是可复现的，不靠猜。
// ---------------------------------------------------------------------------
void t6_payload_sizing() {
    std::string frame =
        R"({"stream":"dogeusdt@depth20@100ms","data":{"lastUpdateId":9999999999999,"bids":[)";
    for (int i = 0; i < 20; ++i) {
        if (i) frame += ',';
        frame += R"(["117432.12000000","12345.12345678"])";   // 最宽形态：price/qty 各 16 字符
    }
    frame += R"(],"asks":[)";
    for (int i = 0; i < 20; ++i) {
        if (i) frame += ',';
        frame += R"(["117432.12000001","12345.12345679"])";
    }
    frame += R"(]}})";

    // 【实测】2026-10-14 真实快照：Binance spot depth20 ≤ 1366 B、futures ≤ 980 B。
    // 上面这条是**刻意放大过**的（每个字段都取满），仍应远小于 4096。
    CHECK(frame.size() < 2048);
    std::printf("  t6 %-22s 放大版 depth20 帧 = %zu B（真实最大 1366 B）⇒ 4096 余量 %.1fx\n",
                "payload sizing", frame.size(), 4096.0 / double(frame.size()));

    constexpr uint32_t N = 8;

    // maxMsg = 4096 必须装得下，且逐字节一致
    md::MdSlotRing<N> ring(4096);
    CHECK(ring.write(frame.data(), frame.size(), 12345) == md::WriteResult::kOk);
    const std::string* m = nullptr;
    int64_t ns = 0;
    CHECK(ring.peek(m, ns));
    CHECK_EQ_U64(m->size(), frame.size());
    CHECK(*m == frame);
    CHECK_EQ_U64(static_cast<uint64_t>(ns), 12345);
    ring.release();

    // 反证：同样的帧喂给 maxMsg = 1024，必须被 kOverflow 挡下（**不静默损坏**）
    md::MdSlotRing<N> small(1024);
    CHECK(small.write(frame.data(), frame.size(), 1) == md::WriteResult::kOverflow);
    CHECK_EQ_U64(small.overflow(), 1);
    CHECK_EQ_U64(small.readable(), 0);            // 没进环
    CHECK_EQ_U64(small.dropped(), 0);             // 也不算"满"——两个故障分得很清楚
}

// ---------------------------------------------------------------------------
// t8 —— 核心安全属性：消费者持有 peek 期间，生产者【绝不能】碰那个槽
//
// 这是两段式 peek/release 的全部意义。确定性验证（不依赖调度）：
// 手动把 r 钉住，让生产者把环写满并大量丢弃，然后检查被持有的槽逐字节未变，
// 同时检查其它槽【确实】被写过了（否则这个测试是空转的）。
// ---------------------------------------------------------------------------
void t8_hold_peek_across_wrap() {
    constexpr uint32_t N = 4;
    md::MdSlotRing<N> ring(kMaxMsg);
    char buf[kMaxMsg];

    CHECK(ring.write(buf, fill(buf, 0), 0) == md::WriteResult::kOk);   // w = 1

    const std::string* m = nullptr;
    int64_t ns = 0;
    CHECK(ring.peek(m, ns));                       // 钉住槽 0（r = 0）
    CHECK_EQ_U64(static_cast<uint64_t>(ns), 0);

    // 生产者继续写 500 条；r 被钉在 0 ⇒ 只有 3 条能成功（w: 1 → 4），其余全丢
    for (uint64_t s = 100; s < 600; ++s) ring.write(buf, fill(buf, s), static_cast<int64_t>(s));

    CHECK(ring.dropped() > 0);                     // 确实满了（测试不是空转）
    CHECK_EQ_U64(ring.writeSeq(), N);              // w 停在 N，没有绕回槽 0
    CHECK_EQ_U64(ring.readable(), N);

    CHECK(verify(*m, 0));                          // ← 被持有的槽逐字节未变
    CHECK_EQ_U64(static_cast<uint64_t>(ns), 0);    // recvNs 也没变

    // 反证：其它槽【确实】被生产者写过了（100/101/102 落在槽 1,2,3）——
    // 否则"被持有的槽没变"可能只是因为生产者根本没动过，测试就是空转的。
    ring.release();                                // 才推进
    CHECK_EQ_U64(ring.readable(), N - 1);
    {
        const std::string* m2 = nullptr;
        int64_t ns2 = 0;
        uint64_t expect = 100;
        while (ring.peek(m2, ns2)) {
            CHECK_EQ_U64(seqOf(*m2), expect);
            CHECK_EQ_U64(static_cast<uint64_t>(ns2), expect);
            ++expect;
            ring.release();
        }
        CHECK_EQ_U64(expect, 103);                 // 槽 1/2/3 = seq 100/101/102
        CHECK_EQ_U64(ring.readable(), 0);
    }
    std::printf("  t8 %-22s 持有 peek 期间生产者写满并丢弃 %llu 次，被持有的槽未变\n",
                "hold-peek", static_cast<unsigned long long>(ring.dropped()));
}

// ---------------------------------------------------------------------------
// t7 —— ABI / 内存实测（对上方案文档 §4.3）
// ---------------------------------------------------------------------------
void t7_abi() {
    using R1024 = md::MdSlotRing<1024>;
    std::printf("  t7 %-22s sizeof(Slot)=%zu  sizeof(Ring<1024>)=%zu\n",
                "ABI", sizeof(R1024::Slot), sizeof(R1024));

    {
        R1024 r(4096);
        std::printf("     N=1024 maxMsg=4096  bytes()=%8zu B (%.2f MiB)  "
                    "对照今天 SPSCQueue<std::string,524288> = 16 MiB\n",
                    r.bytes(), double(r.bytes()) / (1024.0 * 1024.0));
        CHECK_EQ_U64(r.capacity(), 1024);
        CHECK_EQ_U64(r.maxMsg(), 4096);
    }
    {
        R1024 r(1024);
        std::printf("     N=1024 maxMsg=1024  bytes()=%8zu B (%.2f MiB)\n",
                    r.bytes(), double(r.bytes()) / (1024.0 * 1024.0));
    }
}

} // namespace

int main(int argc, char** argv) {
    bool quick = false;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--quick") quick = true;
    }
    const uint64_t big   = quick ? 100000ULL : 1000000ULL;
    const uint64_t small = quick ? 20000ULL  : 200000ULL;

    std::printf("== MdSlotRing 功能测试（%s）==\n", quick ? "quick" : "full");

    t1_fifo<1024>(big,   "N=1024 生产配置");
    t1_fifo<8>(small,    "N=8 高压重试");      // 小环 ⇒ 大量满/重试，压 release/acquire
    t1_fifo<2>(small,    "N=2 极端");
    t2_drop_newest_keep_oldest();
    t3_overflow();
    t4_exception_no_livelock();
    t5_n1();
    t6_payload_sizing();
    t8_hold_peek_across_wrap();
    t7_abi();

    if (g_fail == 0) {
        std::printf("\n全部通过。\n");
        return 0;
    }
    std::printf("\n失败 %d 项。\n", g_fail);
    return 1;
}
