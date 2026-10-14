// md_slot_ring_probe.cpp —— 只用于 x86-64 codegen 反汇编（不运行）
//
// 目的：验证 MdSlotRing 的热路径函数里 **locked RMW = 0**
//       （这是本仓库队列家族的既有纪律；见 tests/shm_queue/codegen/rmwcount.py）
//
//   clang++ -std=c++17 -O2 -arch x86_64 -S -I<db/include> md_slot_ring_probe.cpp -o probe.s
//   python3 tests/shm_queue/codegen/rmwcount.py probe.s
//
// 用 extern "C" 是为了让 rmwcount.py 拿到不带修饰的符号名。

#include "base/MdSlotRing.h"

#include <cstddef>
#include <cstdint>
#include <string>

using Ring = md::MdSlotRing<1024>;

extern "C" int probe_write(Ring& r, const char* d, size_t n, int64_t t) {
    return static_cast<int>(r.write(d, n, t));
}

extern "C" bool probe_peek(const Ring& r, const std::string** o, int64_t* t) {
    return r.peek(*o, *t);
}

extern "C" void probe_release(Ring& r) { r.release(); }

extern "C" uint64_t probe_readable(const Ring& r) { return r.readable(); }
