// ring_depth_probe.cpp —— 只用于 x86-64 codegen 反汇编（不运行）
//
// 目的：量化「把 N 从模板参数改成运行时构造参数」的代价。
//
//   A 组 = 改造前：template <uint32_t N>，槽数编译期（w % N → and imm）
//   B 组 = 改造后：运行时 depth_，存 mask_ = depth-1（w & mask_ → and mem）
//
// ✅ 2026-10-14 已按 B 组落地到 db/include/base/MdSlotRing.h（去掉了 template）。
//    生产环重测：write 62 / peek 26 / release 12 / readable 18 insns，locked RMW 全 0 ——
//    与改造前逐条一致。本探针保留作为"两组并存"的可复现对照。
//
//   clang++ -std=c++17 -O2 -arch x86_64 -S ring_depth_probe.cpp -o probe.s
//   python3 tests/shm_queue/codegen/rmwcount.py probe.s

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace tpl {

enum class WriteResult : uint8_t { kOk = 0, kDropped = 1, kOverflow = 2 };

template <uint32_t N>
class Ring {
    static_assert(N > 0 && (N & (N - 1)) == 0, "N must be a power of two");

public:
    struct alignas(64) Slot {
        std::string msg;
        int64_t     recvNs{0};
    };

    explicit Ring(size_t maxMsg) : maxMsg_(maxMsg) {
        slots_.resize(N);
        for (auto& s : slots_) s.msg.reserve(maxMsg);
    }

    WriteResult write(const char* data, size_t len, int64_t recvNs) noexcept {
        if (len > maxMsg_) { overflow_.fetch_add(1, std::memory_order_relaxed); return WriteResult::kOverflow; }
        const uint64_t w = w_.load(std::memory_order_relaxed);
        if (w - r_.load(std::memory_order_acquire) >= N) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return WriteResult::kDropped;
        }
        Slot& s = slots_[w % N];
        s.msg.assign(data, len);
        s.recvNs = recvNs;
        w_.store(w + 1, std::memory_order_release);
        return WriteResult::kOk;
    }

    uint64_t readable() const noexcept {
        const uint64_t w = w_.load(std::memory_order_acquire);
        const uint64_t r = r_.load(std::memory_order_relaxed);
        return w > r ? w - r : 0;
    }

    bool peek(const std::string*& out, int64_t& recvNs) const noexcept {
        const uint64_t r = r_.load(std::memory_order_relaxed);
        if (r == w_.load(std::memory_order_acquire)) return false;
        const Slot& s = slots_[r % N];
        out = &s.msg;
        recvNs = s.recvNs;
        return true;
    }

    void release() noexcept {
        r_.store(r_.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }

private:
    alignas(64) std::atomic<uint64_t> w_{0};
    alignas(64) std::atomic<uint64_t> r_{0};
    alignas(64) std::atomic<uint64_t> dropped_{0};
    alignas(64) std::atomic<uint64_t> overflow_{0};
    size_t maxMsg_{0};
    std::vector<Slot> slots_;
};

} // namespace tpl

namespace run {

enum class WriteResult : uint8_t { kOk = 0, kDropped = 1, kOverflow = 2 };

// 槽数改成运行时；保持 2 的幂 ⇒ w & mask_（mask_ = depth-1）仍是一条 and。
class Ring {
public:
    struct alignas(64) Slot {
        std::string msg;
        int64_t     recvNs{0};
    };

    Ring(size_t depth, size_t maxMsg) : depth_(depth), mask_(depth - 1), maxMsg_(maxMsg) {
        slots_.resize(depth);
        for (auto& s : slots_) s.msg.reserve(maxMsg);
    }

    WriteResult write(const char* data, size_t len, int64_t recvNs) noexcept {
        if (len > maxMsg_) { overflow_.fetch_add(1, std::memory_order_relaxed); return WriteResult::kOverflow; }
        const uint64_t w = w_.load(std::memory_order_relaxed);
        if (w - r_.load(std::memory_order_acquire) >= depth_) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return WriteResult::kDropped;
        }
        Slot& s = slots_[w & mask_];
        s.msg.assign(data, len);
        s.recvNs = recvNs;
        w_.store(w + 1, std::memory_order_release);
        return WriteResult::kOk;
    }

    uint64_t readable() const noexcept {
        const uint64_t w = w_.load(std::memory_order_acquire);
        const uint64_t r = r_.load(std::memory_order_relaxed);
        return w > r ? w - r : 0;
    }

    bool peek(const std::string*& out, int64_t& recvNs) const noexcept {
        const uint64_t r = r_.load(std::memory_order_relaxed);
        if (r == w_.load(std::memory_order_acquire)) return false;
        const Slot& s = slots_[r & mask_];
        out = &s.msg;
        recvNs = s.recvNs;
        return true;
    }

    void release() noexcept {
        r_.store(r_.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }

private:
    alignas(64) std::atomic<uint64_t> w_{0};
    alignas(64) std::atomic<uint64_t> r_{0};
    alignas(64) std::atomic<uint64_t> dropped_{0};
    alignas(64) std::atomic<uint64_t> overflow_{0};
    size_t depth_{0};
    size_t mask_{0};
    size_t maxMsg_{0};
    std::vector<Slot> slots_;
};

} // namespace run

// ---- A 组：现状（模板 N）----
extern "C" int  a_write(tpl::Ring<1024>& r, const char* d, size_t n, int64_t t) {
    return static_cast<int>(r.write(d, n, t));
}
extern "C" bool a_peek(const tpl::Ring<1024>& r, const std::string** o, int64_t* t) {
    return r.peek(*o, *t);
}
extern "C" void a_release(tpl::Ring<1024>& r) { r.release(); }
extern "C" uint64_t a_readable(const tpl::Ring<1024>& r) { return r.readable(); }

// ---- B 组：提案（运行时 depth）----
extern "C" int  b_write(run::Ring& r, const char* d, size_t n, int64_t t) {
    return static_cast<int>(r.write(d, n, t));
}
extern "C" bool b_peek(const run::Ring& r, const std::string** o, int64_t* t) {
    return r.peek(*o, *t);
}
extern "C" void b_release(run::Ring& r) { r.release(); }
extern "C" uint64_t b_readable(const run::Ring& r) { return r.readable(); }
