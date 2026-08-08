// db_shm_perf.cpp
//
// SHM 行情消费性能测试:
//   1. live 模式: 订阅真实 topic, 统计 poll 吞吐与命中率
//   2. bench 模式: 本地 pub/sub 微基准, 测量 write/pop_last 延迟
//
// 用法:
//   ./db_shm_perf [--duration SEC] [--cpu N] TOPIC [TOPIC...]
//   ./db_shm_perf --bench [--iters N] [--types all|DEPTH1,TRADES,...]

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include <sys/mman.h>
#include <unistd.h>

#include "program_util.h"
#include "db_shm_tool_common.h"

namespace {

using clk = std::chrono::steady_clock;
using ns = std::chrono::nanoseconds;

struct Percentiles {
    long p50 = 0;
    long p95 = 0;
    long p99 = 0;
    long max = 0;
};

Percentiles calc_percentiles(std::vector<long>& lat) {
    if (lat.empty()) {
        return {};
    }
    std::sort(lat.begin(), lat.end());
    auto at = [&](double p) -> long {
        const size_t idx = std::min(lat.size() - 1, static_cast<size_t>(lat.size() * p));
        return lat[idx];
    };
    return {at(0.50), at(0.95), at(0.99), lat.back()};
}

void print_percentiles(const char* label, long total_ns, size_t iters, const Percentiles& p) {
    const double avg_ns = iters ? static_cast<double>(total_ns) / static_cast<double>(iters) : 0.0;
    const double qps = iters ? iters / (total_ns / 1e9) : 0.0;
    std::printf("  %-36s  avg=%8.1fns  p50=%6ldns  p95=%6ldns  p99=%6ldns  max=%7ldns  (%.2f M/s)\n",
                label, avg_ns, p.p50, p.p95, p.p99, p.max, qps / 1e6);
}

struct LiveConfig {
    int duration_sec = 10;
    int cpu = -1;
    int topic_start = 1;
};

struct BenchConfig {
    int iters = 100000;
    std::vector<MdKind> kinds;
};

struct ToolConfig {
    bool bench_mode = false;
    LiveConfig live;
    BenchConfig bench;
};

bool parse_kind_token(const std::string& token, MdKind& out) {
    if (token == "DEPTH1") {
        out = MdKind::DEPTH1;
        return true;
    }
    if (token == "DEPTH5") {
        out = MdKind::DEPTH5;
        return true;
    }
    if (token == "DEPTH10") {
        out = MdKind::DEPTH10;
        return true;
    }
    if (token == "DEPTH20") {
        out = MdKind::DEPTH20;
        return true;
    }
    if (token == "TRADES") {
        out = MdKind::TRADES;
        return true;
    }
    if (token == "KLINE") {
        out = MdKind::KLINE;
        return true;
    }
    if (token == "FUNDING_RATE") {
        out = MdKind::FUNDING_RATE;
        return true;
    }
    return false;
}

std::vector<MdKind> default_bench_kinds() {
    return {
        MdKind::DEPTH1,
        MdKind::DEPTH5,
        MdKind::DEPTH10,
        MdKind::DEPTH20,
        MdKind::TRADES,
        MdKind::KLINE,
        MdKind::FUNDING_RATE,
    };
}

std::vector<MdKind> parse_kind_list(const std::string& csv) {
    if (csv == "all") {
        return default_bench_kinds();
    }

    std::vector<MdKind> kinds;
    size_t start = 0;
    while (start <= csv.size()) {
        const size_t comma = csv.find(',', start);
        const std::string token = csv.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        MdKind kind = MdKind::UNKNOWN;
        if (!token.empty()) {
            if (!parse_kind_token(token, kind)) {
                std::cerr << "unknown bench type: " << token << std::endl;
                std::exit(1);
            }
            kinds.push_back(kind);
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    if (kinds.empty()) {
        std::cerr << "--types must not be empty" << std::endl;
        std::exit(1);
    }
    return kinds;
}

ToolConfig parse_args(int argc, char* argv[]) {
    ToolConfig cfg;
    cfg.bench.kinds = default_bench_kinds();

    int i = 1;
    while (i < argc) {
        const std::string arg = argv[i];
        if (arg == "--bench") {
            cfg.bench_mode = true;
            ++i;
            continue;
        }
        if (arg == "--duration" && i + 1 < argc) {
            cfg.live.duration_sec = std::atoi(argv[++i]);
            ++i;
            continue;
        }
        if (arg == "--cpu" && i + 1 < argc) {
            cfg.live.cpu = std::atoi(argv[++i]);
            ++i;
            continue;
        }
        if (arg == "--iters" && i + 1 < argc) {
            cfg.bench.iters = std::atoi(argv[++i]);
            ++i;
            continue;
        }
        if (arg == "--types" && i + 1 < argc) {
            cfg.bench.kinds = parse_kind_list(argv[++i]);
            ++i;
            continue;
        }
        if (arg == "--help" || arg == "-h") {
            std::cout
                << "live mode:\n"
                << "  db_shm_perf [--duration SEC] [--cpu N] TOPIC [TOPIC...]\n"
                << "bench mode:\n"
                << "  db_shm_perf --bench [--iters N] [--types all|DEPTH1,TRADES,...]\n";
            std::exit(0);
        }
        break;
    }

    cfg.live.topic_start = i;
    return cfg;
}

template <typename T>
void bench_one_type(MdKind kind, int iters) {
    char shm_name[64];
    std::snprintf(shm_name, sizeof(shm_name), "/bts_db_perf_%s", md_kind_name(kind));
    shm_unlink(shm_name);

    pubsub::SPMCPublisher<T> pub(shm_name);
    pubsub::SPMCSubscriber<T> sub(shm_name);

    T data{};
    for (int i = 0; i < 1000; ++i) {
        pub.push(data);
        T tmp{};
        sub.pop_last(tmp);
    }

    std::vector<long> write_lat;
    std::vector<long> read_lat;
    write_lat.reserve(static_cast<size_t>(iters));
    read_lat.reserve(static_cast<size_t>(iters));

    long write_total = 0;
    long read_total = 0;
    for (int i = 0; i < iters; ++i) {
        const auto t0 = clk::now();
        pub.push(data);
        const auto t1 = clk::now();
        T out{};
        sub.pop_last(out);
        const auto t2 = clk::now();

        const long w = std::chrono::duration_cast<ns>(t1 - t0).count();
        const long r = std::chrono::duration_cast<ns>(t2 - t1).count();
        write_lat.push_back(w);
        read_lat.push_back(r);
        write_total += w;
        read_total += r;
    }

    char label[64];
    std::snprintf(label, sizeof(label), "%s write", md_kind_name(kind));
    print_percentiles(label, write_total, static_cast<size_t>(iters), calc_percentiles(write_lat));

    std::snprintf(label, sizeof(label), "%s pop_last", md_kind_name(kind));
    print_percentiles(label, read_total, static_cast<size_t>(iters), calc_percentiles(read_lat));

    const int empty_iters = iters * 10;
    const auto e0 = clk::now();
    for (int i = 0; i < empty_iters; ++i) {
        T out{};
        sub.pop_last(out);
    }
    const auto e1 = clk::now();
    const long empty_total = std::chrono::duration_cast<ns>(e1 - e0).count();
    std::printf("  %-36s  avg=%8.1fns  (%d empty polls)\n",
                "empty pop_last",
                empty_total / static_cast<double>(empty_iters),
                empty_iters);

    shm_unlink(shm_name);
}

void run_bench_mode(const BenchConfig& cfg) {
    std::printf("=== db_shm_perf bench mode (iters=%d) ===\n", cfg.iters);
    if (cfg.iters <= 0) {
        std::cerr << "--iters must be > 0" << std::endl;
        std::exit(1);
    }

    for (MdKind kind : cfg.kinds) {
        switch (kind) {
        case MdKind::DEPTH1:
            bench_one_type<md::Depth1>(kind, cfg.iters);
            break;
        case MdKind::DEPTH5:
            bench_one_type<md::Depth5>(kind, cfg.iters);
            break;
        case MdKind::DEPTH10:
            bench_one_type<md::Depth10>(kind, cfg.iters);
            break;
        case MdKind::DEPTH20:
            bench_one_type<md::Depth20>(kind, cfg.iters);
            break;
        case MdKind::TRADES:
            bench_one_type<md::Trades>(kind, cfg.iters);
            break;
        case MdKind::KLINE:
            bench_one_type<md::Kline>(kind, cfg.iters);
            break;
        case MdKind::FUNDING_RATE:
            bench_one_type<md::FundingRate>(kind, cfg.iters);
            break;
        default:
            break;
        }
        std::printf("\n");
    }
}

void run_live_mode(const LiveConfig& cfg, int argc, char* argv[]) {
    if (cfg.duration_sec <= 0) {
        std::cerr << "--duration must be > 0" << std::endl;
        std::exit(1);
    }

    if (cfg.cpu >= 0) {
        if (crypto::set_cpu_current_thread(cfg.cpu) != 0) {
            std::cerr << "failed to bind cpu " << cfg.cpu << std::endl;
        } else {
            std::printf("pinned to cpu %d\n", cfg.cpu);
        }
    }

    auto channels = make_md_channels(argc, argv, cfg.topic_start);
    std::printf("=== db_shm_perf live mode (duration=%ds, channels=%zu) ===\n",
                cfg.duration_sec,
                channels.size());

    const auto deadline = clk::now() + std::chrono::seconds(cfg.duration_sec);
    uint64_t polls = 0;
    uint64_t msgs = 0;
    MdPollResult result;

    while (clk::now() < deadline) {
        for (auto& ch : channels) {
            ++polls;
            if (ch.poll(result)) {
                ++msgs;
            }
        }
    }

    const double elapsed = cfg.duration_sec;
    const double msg_rate = msgs / elapsed;
    const double poll_rate = polls / elapsed;
    const double hit_rate = polls ? (100.0 * msgs / polls) : 0.0;

    std::printf("messages=%llu  polls=%llu  msg/s=%.2f  poll/s=%.2f  hit_rate=%.4f%%\n",
                static_cast<unsigned long long>(msgs),
                static_cast<unsigned long long>(polls),
                msg_rate,
                poll_rate,
                hit_rate);
}

} // namespace

int main(int argc, char* argv[]) {
    const ToolConfig cfg = parse_args(argc, argv);

    {
        const auto ml = crypto::lock_process_memory_detailed();
        if (!ml.ok) {
            fprintf(stderr,
                    "[WARN] mlockall failed: %s -- hot-path page faults not suppressed "
                    "memlock_soft=%s memlock_hard=%s VmLck=%llukB->%llukB\n",
                    strerror(ml.err),
                    ml.memlock_soft.c_str(),
                    ml.memlock_hard.c_str(),
                    ml.vm_lck_before_kb,
                    ml.vm_lck_after_kb);
        }
    }

    if (cfg.bench_mode) {
        run_bench_mode(cfg.bench);
    } else {
        run_live_mode(cfg.live, argc, argv);
    }
    return 0;
}