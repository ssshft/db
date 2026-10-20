#include "mgbx/MgbxMarket.h"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {

// MGBX 的量/价会出现 "1.125E+4" 这种科学计数法。
// crypto::fast_atod 用 std::from_chars，**不接受指数形式**，会返回 NAN ⇒
// 这里先走快路径，NAN 再退回 strtod（strtod 认科学计数法）。
// 不做这一步的后果：该档位被当成 NaN 丢掉，簿子慢慢烂掉，而且**没有任何报错**。
inline double safe_atod(std::string_view sv) {
    const double v = crypto::fast_atod(sv);
    if (std::isfinite(v)) {
        return v;
    }
    char buf[64];
    const size_t n = sv.size() < sizeof(buf) - 1 ? sv.size() : sizeof(buf) - 1;
    std::memcpy(buf, sv.data(), n);
    buf[n] = '\0';
    return std::strtod(buf, nullptr);
}

} // namespace

md::MgbxUnit::MgbxUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                       std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port,
                       const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // 心跳是**明文** "ping"（不是 JSON），服务端回明文 "pong"；25 s 一次，30 s 没心跳就断。
    cfg.ping_mode                = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 15;
    cfg.client_ping_text         = "ping";
    cfg.idle_timeout_sec         = 60;
    cfg.data_idle_timeout_sec    = 0;
}

void md::MgbxUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        // 见头文件第 7 条：MGBX 没有公开的资金费率接口，这里明确报出来而不是静默不出数
        LOG_ERROR("MGBX has NO public funding-rate API (neither REST nor WS channel); "
                  "FUNDING_RATE for {} is NOT available on this venue",
                  InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum != USDT_SWAP) {
        LOG_ERROR("MGBX only supports USDT_SWAP in this impl, got: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    cfg.url = MGBX_WS_MARKET;

    if (marketTypeEnum != md::DEPTH1 && marketTypeEnum != md::TRADES) {
        LOG_ERROR("MGBX {} not implemented (only DEPTH1 / TRADES)", md::MarketTypeEnum2StrMap[marketTypeEnum]);
        return;
    }

    // 订阅是"一个 symbol 一条"，且**订了就推全部 7 个频道**，没法只订深度或只订成交。
    cfg.subscribe_messages.clear();
    for (auto& info : vInstInfo) {
        cfg.subscribe_messages.push_back(fmt::format(R"({{"req":"sub_symbol","symbol":"{}"}})", info.originInstId));
        if (marketTypeEnum == md::DEPTH1) {
            mBooks[std::string(info.originInstId)] = TopBook{};
        }
    }

    LOG_INFO("MGBX ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::MgbxUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // 订阅回执 {"code":0,"msg":"success"} 和心跳 "pong" 都没有 "channel"。
    // 注意：订一个 symbol 会推 7 个频道，这里按本 unit 需要的频道**再筛一遍**，
    // 否则 DEPTH1 的 ring 会被 push.ticker / push.mark.price 这些垃圾帧占满。
    if (sv.find("\"channel\":\"") == std::string_view::npos) {
        return;
    }

    if (marketTypeEnum == md::DEPTH1) {
        // "push.deep" 是 "push.deep.full" 的前缀 ⇒ 一条判断同时覆盖增量与全量
        if (sv.find("\"push.deep\"") == std::string_view::npos) {
            return;
        }
    }
    else if (marketTypeEnum == md::TRADES) {
        if (sv.find("\"push.deal\"") == std::string_view::npos) {
            return;
        }
    }
    else {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::MgbxUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    if (marketTypeEnum == md::DEPTH1) {
        parseDepth(msg, tsNet);
    }
    else if (marketTypeEnum == md::TRADES) {
        parseDeal(msg, tsNet);
    }
}

void md::MgbxUnit::parseDepth(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("MGBX simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // ⚠️ 键顺序（实测，simdjson ondemand 前向单向，**顺序不能动**）：
    //   push.deep      : {channel, data{id, s, ba, p, q, t}}
    //   push.deep.full : {channel, data{s, id, a, b}}     ⇒ **a 在 b 之前**
    // 两种帧的 data 里键顺序不一样，所以分开处理，不能共用一套访问顺序。

    std::string_view channel;
    if (doc["channel"].get(channel) != simdjson::SUCCESS) {
        return;
    }

    const bool isFull = (channel == "push.deep.full");
    if (!isFull && channel != "push.deep") {
        return;
    }

    auto data = doc["data"];

    std::string_view symbol;
    TopBook*         book = nullptr;
    int64_t          tsMs = 0;

    if (isFull) {
        if (data["s"].get(symbol) != simdjson::SUCCESS) {
            return;
        }
        std::string_view id;                       // id 在 s 之后、a 之前，读过就扔
        data["id"].get(id);

        auto itMap = mBooks.find(std::string(symbol));
        if (itMap == mBooks.end()) {
            return;
        }
        book = &itMap->second;
        book->reset();                             // 全量 ⇒ 先清，重连后不残留上一轮的档位

        for (auto lv : data["a"]) {                // a 在前
            auto             it = lv.begin();
            std::string_view p, q;
            if ((*it).get(p) != simdjson::SUCCESS) break;
            ++it;
            if ((*it).get(q) != simdjson::SUCCESS) break;
            book->applyAsk(safe_atod(p), safe_atod(q));
        }
        for (auto lv : data["b"]) {                // b 在后
            auto             it = lv.begin();
            std::string_view p, q;
            if ((*it).get(p) != simdjson::SUCCESS) break;
            ++it;
            if ((*it).get(q) != simdjson::SUCCESS) break;
            book->applyBid(safe_atod(p), safe_atod(q));
        }
        // 全量帧没有 t 字段 ⇒ tsMs 保持 0，下面用墙钟兜底
    }
    else {
        std::string_view id;                       // id → s → ba → p → q → t
        data["id"].get(id);

        if (data["s"].get(symbol) != simdjson::SUCCESS) {
            return;
        }

        int64_t ba = 0;
        if (data["ba"].get(ba) != simdjson::SUCCESS) {
            return;
        }

        std::string_view p, q;
        if (data["p"].get(p) != simdjson::SUCCESS) return;
        if (data["q"].get(q) != simdjson::SUCCESS) return;

        data["t"].get(tsMs);                       // t 在最后

        auto itMap = mBooks.find(std::string(symbol));
        if (itMap == mBooks.end()) {
            return;
        }
        book = &itMap->second;

        const double px = safe_atod(p);
        const double sz = safe_atod(q);
        if (ba == 1) {
            book->applyBid(px, sz);
        }
        else if (ba == 2) {
            book->applyAsk(px, sz);
        }
        else {
            return;
        }
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(symbol), info, key)) {
        return;
    }

    double bp = 0, bv = 0, ap = 0, av = 0;
    if (!book->best(&bp, &bv, &ap, &av)) {
        return;
    }
    if (tsMs <= 0) {
        tsMs = crypto::getCurrentTime() / 1000;
    }

    md::Depth1 d1;
    std::memset(&d1, 0, sizeof(d1));
    d1.exchangeTypeEnum = exchangeTypeEnum;
    d1.instTypeEnum     = instTypeEnum;
    d1.marketTypeEnum   = marketTypeEnum;
    std::strncpy(d1.instId, info.instId, INSTID_SIZE);

    d1.bp1 = bp * info.reduceNumber;
    d1.bv1 = bv * info.value * info.magnifyNumber;
    d1.ap1 = ap * info.reduceNumber;
    d1.av1 = av * info.value * info.magnifyNumber;

    if (d1.bp1 <= 0 || d1.ap1 <= 0) {
        return;
    }

    d1.tsTrans = tsMs * 1000;
    d1.tsEvent = tsMs * 1000;
    d1.tsRecv  = tsNet;
    d1.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
    mDepth1Publisher[key]->push(d1);
#endif
}

void md::MgbxUnit::parseDeal(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("MGBX simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // 键顺序（实测）：{channel, data{s, p, a, m, t}}
    //   a 是**数量**（不是 asks），m 是 "BID"/"ASK"，t 在最后。

    std::string_view channel;
    if (doc["channel"].get(channel) != simdjson::SUCCESS || channel != "push.deal") {
        return;
    }

    auto data = doc["data"];

    std::string_view symbol;
    if (data["s"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    std::string_view p, a, m;
    if (data["p"].get(p) != simdjson::SUCCESS) return;
    if (data["a"].get(a) != simdjson::SUCCESS) return;
    if (data["m"].get(m) != simdjson::SUCCESS) return;

    int64_t tsMs = 0;
    data["t"].get(tsMs);

    const double px = safe_atod(p);
    const double sz = safe_atod(a);
    if (!(px > 0) || !(sz > 0)) {
        return;
    }
    if (tsMs <= 0) {
        tsMs = crypto::getCurrentTime() / 1000;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(symbol), info, key)) {
        return;
    }

    md::Trades t;
    std::memset(&t, 0, sizeof(t));
    t.exchangeTypeEnum = exchangeTypeEnum;
    t.instTypeEnum     = instTypeEnum;
    t.marketTypeEnum   = marketTypeEnum;
    std::strncpy(t.instId, info.instId, INSTID_SIZE);

    // push.deal 不带成交编号 ⇒ 用毫秒时间戳兜底
    std::snprintf(t.tradeId, sizeof(t.tradeId), "%lld", static_cast<long long>(tsMs));

    // m: "BID" = 主动买 / "ASK" = 主动卖
    t.direction = (m == "ASK") ? DT_SHORT : DT_LONG;
    t.px        = px * info.reduceNumber;
    t.sz        = sz * info.value * info.magnifyNumber;

    t.tsTrans = tsMs * 1000;
    t.tsEvent = tsMs * 1000;
    t.tsRecv  = tsNet;
    t.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
    mTradesPublisher[key]->push(t);
#endif
}

md::MgbxMarket::MgbxMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                           std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot,
                           const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::MgbxUnit* unit = new md::MgbxUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum,
                                              info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        unitVec.push_back(unit);
    }
}

md::MgbxMarket::~MgbxMarket() {}

void md::MgbxMarket::start() {
    for (size_t i = 0; i < unitVec.size(); ++i) {
        // FUNDING_RATE 的 unit 在 generateSubBody 里已经报错并留空 url，
        // 这里不能再 start（否则 WsClient 会因为 url 为空抛 invalid_argument）。
        if (unitVec[i]->isRestOnly()) {
            continue;
        }
        unitVec[i]->start();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}
