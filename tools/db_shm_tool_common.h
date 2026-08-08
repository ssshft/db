#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "shm_spmc_queue.h"
#include "string_util.h"
#include "time_util.h"

enum class MdKind {
    DEPTH1,
    DEPTH5,
    DEPTH10,
    DEPTH20,
    TRADES,
    KLINE,
    FUNDING_RATE,
    UNKNOWN,
};

inline const char* md_kind_name(MdKind kind) {
    switch (kind) {
    case MdKind::DEPTH1: return "DEPTH1";
    case MdKind::DEPTH5: return "DEPTH5";
    case MdKind::DEPTH10: return "DEPTH10";
    case MdKind::DEPTH20: return "DEPTH20";
    case MdKind::TRADES: return "TRADES";
    case MdKind::KLINE: return "KLINE";
    case MdKind::FUNDING_RATE: return "FUNDING_RATE";
    default: return "UNKNOWN";
    }
}

inline MdKind detect_md_kind(const char* topic) {
    if (crypto::has_str(topic, "DEPTH20")) return MdKind::DEPTH20;
    if (crypto::has_str(topic, "DEPTH10")) return MdKind::DEPTH10;
    if (crypto::has_str(topic, "DEPTH5")) return MdKind::DEPTH5;
    if (crypto::has_str(topic, "DEPTH1")) return MdKind::DEPTH1;
    if (crypto::has_str(topic, "TRADES")) return MdKind::TRADES;
    if (crypto::has_str(topic, "KLINE")) return MdKind::KLINE;
    if (crypto::has_str(topic, "FUNDING_RATE")) return MdKind::FUNDING_RATE;
    return MdKind::UNKNOWN;
}

struct MdPollResult {
    bool ok = false;
    md::MDBase base{};
    std::string text;
};

struct MdChannel {
    std::string topic;
    MdKind kind = MdKind::UNKNOWN;
    std::function<bool(MdPollResult& out)> poll;
};

template <typename T>
inline MdChannel make_md_channel(const char* topic) {
    MdChannel ch;
    ch.topic = topic;
    ch.kind = detect_md_kind(topic);
    auto suber = std::make_shared<pubsub::SPMCSubscriber<T>>(topic);
    ch.poll = [suber](MdPollResult& out) {
        T cmd;
        if (!suber->pop_last(cmd)) {
            return false;
        }
        std::memcpy(&out.base, &cmd, sizeof(md::MDBase));
        out.text = cmd.getString();
        out.ok = true;
        return true;
    };
    return ch;
}

inline MdChannel make_md_channel(const char* topic) {
    const MdKind kind = detect_md_kind(topic);
    switch (kind) {
    case MdKind::DEPTH1:
        return make_md_channel<md::Depth1>(topic);
    case MdKind::DEPTH5:
        return make_md_channel<md::Depth5>(topic);
    case MdKind::DEPTH10:
        return make_md_channel<md::Depth10>(topic);
    case MdKind::DEPTH20:
        return make_md_channel<md::Depth20>(topic);
    case MdKind::TRADES:
        return make_md_channel<md::Trades>(topic);
    case MdKind::KLINE:
        return make_md_channel<md::Kline>(topic);
    case MdKind::FUNDING_RATE:
        return make_md_channel<md::FundingRate>(topic);
    default:
        return MdChannel{topic, MdKind::UNKNOWN, {}};
    }
}

inline std::vector<MdChannel> make_md_channels(int argc, char* argv[], int arg_start = 1) {
    std::vector<MdChannel> channels;
    channels.reserve(static_cast<size_t>(argc - arg_start));
    for (int i = arg_start; i < argc; ++i) {
        const char* topic = argv[i];
        MdChannel ch = make_md_channel(topic);
        if (ch.kind == MdKind::UNKNOWN) {
            std::cerr << "unsupported topic: " << topic << std::endl;
            std::exit(1);
        }
        std::cout << topic << " (" << md_kind_name(ch.kind) << ")" << std::endl;
        channels.push_back(std::move(ch));
    }
    if (channels.empty()) {
        std::cerr << "usage: tool [options] TOPIC [TOPIC...]" << std::endl;
        std::exit(1);
    }
    return channels;
}

struct LatencySample {
    double event_ms = 0;
    double internet_ms = 0;
    int parse_us = 0;
    int intranet_us = 0;
};

inline LatencySample make_latency_sample(const md::MDBase& cmd, int64_t now_us) {
    LatencySample s;
    s.event_ms = (cmd.tsEvent - cmd.tsTrans) * 0.001;
    s.internet_ms = (cmd.tsRecv - cmd.tsTrans) * 0.001;
    s.parse_us = static_cast<int>(cmd.tsParse - cmd.tsRecv);
    s.intranet_us = static_cast<int>(now_us - cmd.tsParse);
    return s;
}