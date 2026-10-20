#include "echobit/EchobitMarket.h"

#include <cstdio>
#include <cstring>

namespace {

// "BTC-SWAP-USDT" → "BTCUSDT"：把中间的 "-SWAP" 去掉。
// 为什么不直接拼 base+quote：contractinfo 已经给了 originInstId，去掉固定子串最不容易漂移。
std::string strip_swap(const char* originInstId) {
    std::string s(originInstId);
    const std::string kSwap = "-SWAP";
    const size_t      pos   = s.find(kSwap);
    if (pos != std::string::npos) {
        s.erase(pos, kSwap.size());
    }
    return s;
}

} // namespace

md::EchobitUnit::EchobitUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                             std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port,
                             const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // 心跳是 {"ping":<毫秒时间戳>}，60 s 内不发服务端断连 ⇒ 必须现取时间，不能用静态串。
    cfg.ping_mode                = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_builder      = []() {
        return fmt::format(R"({{"ping":{}}})", crypto::getCurrentTime() / 1000);
    };
    cfg.idle_timeout_sec      = 90;
    cfg.data_idle_timeout_sec = 0;
}

void md::EchobitUnit::generateSubBody() {
    if (instTypeEnum != USDT_SWAP) {
        LOG_ERROR("ECHOBIT only supports USDT_SWAP in this impl, got: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    const std::string ts = fmt::format("?timestamp={}", crypto::getCurrentTime() / 1000);

    cfg.subscribe_messages.clear();

    if (marketTypeEnum == md::FUNDING_RATE) {
        // 全市场一帧，不需要 symbol 参数
        cfg.url = std::string(ECHOBIT_WS_INFORM) + ts;
        cfg.subscribe_messages.push_back(R"({"id":"fund_rates","topic":"fund_rates","event":"sub"})");
        for (auto& info : vInstInfo) {
            mFundSyms.insert(std::string(info.originInstId));      // BTC-SWAP-USDT
        }
        LOG_INFO("ECHOBIT FUNDING_RATE uses inform ws (all-symbol push), url: {}, symbols: {}", cfg.url,
                 mFundSyms.size());
        return;
    }

    const char* topic = nullptr;
    if (marketTypeEnum == md::DEPTH1) {
        topic = "depth";
    }
    else if (marketTypeEnum == md::TRADES) {
        topic = "trade";
    }
    else {
        LOG_ERROR("ECHOBIT {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                  md::MarketTypeEnum2StrMap[marketTypeEnum]);
        return;
    }

    cfg.url = std::string(ECHOBIT_WS_QUOTE) + ts;

    for (auto& info : vInstInfo) {
        const std::string wsSym = strip_swap(info.originInstId);
        mWsToOrigin[wsSym]      = std::string(info.originInstId);
        cfg.subscribe_messages.push_back(fmt::format(
            R"({{"id":"{}.{}","topic":"{}","event":"sub","symbol":"{}","params":{{}}}})", topic, wsSym, topic,
            wsSym));
    }

    LOG_INFO("ECHOBIT ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::EchobitUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // 过滤：行情帧第一个键是 "symbol"，资金费率帧第一个键是 "code" 且带 "topic":"fund_rates"。
    // 订阅回执（{"code":"200","topic":"depth","event":"subbed"}）和 pong 都不带 data 数组，
    // 所以统一要求出现 "\"data\":" —— 回执里没有。
    if (sv.find("\"data\":") == std::string_view::npos) {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::EchobitUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    if (marketTypeEnum == md::DEPTH1) {
        parseDepth(msg, tsNet);
    }
    else if (marketTypeEnum == md::TRADES) {
        parseTrade(msg, tsNet);
    }
    else if (marketTypeEnum == md::FUNDING_RATE) {
        parseFunding(msg, tsNet);
    }
}

void md::EchobitUnit::parseDepth(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("ECHOBIT simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // ⚠️ 键顺序（实测，simdjson ondemand 前向单向，**顺序不能动**）：
    //   {symbol, symbolName, topic, params{realtimeInterval}, data[{s, t, v, b, a, o}], f, sendTime, id}
    //   ⇒ data 里 **b(bids) 在 a(asks) 之前**，t 在 b 之前，o 在最后。

    std::string_view symbol;
    if (doc["symbol"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    std::string_view topic;
    if (doc["topic"].get(topic) != simdjson::SUCCESS || topic != "depth") {
        return;
    }

    // data 是**数组**（深度只会有一条），取第 0 条。
    // ⚠️ 必须先把 doc["data"] 落成**具名变量**：对临时的 simdjson_result 调 begin()
    //    拿到的是悬垂迭代器（HTX 那边踩过，见 HtxMarket.cpp:143 的注释）。
    //    也不比较 begin()/end()：simdjson_result 不保证提供 end()，直接取首元素 + 判 error 更稳。
    auto dataArr = doc["data"];
    auto d0      = *dataArr.begin();
    if (d0.error()) {
        return;
    }

    int64_t tsMs = 0;
    d0["t"].get(tsMs);

    double bp = 0, bv = 0, ap = 0, av = 0;

    for (auto lv : d0["b"]) {               // b 在前
        auto             it = lv.begin();
        std::string_view p, q;
        if ((*it).get(p) != simdjson::SUCCESS) break;
        ++it;
        if ((*it).get(q) != simdjson::SUCCESS) break;
        bp = crypto::fast_atod(p);
        bv = crypto::fast_atod(q);
        break;
    }
    for (auto lv : d0["a"]) {               // a 在后
        auto             it = lv.begin();
        std::string_view p, q;
        if ((*it).get(p) != simdjson::SUCCESS) break;
        ++it;
        if ((*it).get(q) != simdjson::SUCCESS) break;
        ap = crypto::fast_atod(p);
        av = crypto::fast_atod(q);
        break;
    }

    if (!(bp > 0) || !(ap > 0)) {
        return;
    }

    auto itMap = mWsToOrigin.find(std::string(symbol));
    if (itMap == mWsToOrigin.end()) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(itMap->second, info, key)) {
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

void md::EchobitUnit::parseTrade(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("ECHOBIT simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // 键顺序（实测）：{symbol, symbolName, topic, params, data[{v, t, p, q, f, m}], f, sendTime, id}

    std::string_view symbol;
    if (doc["symbol"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    std::string_view topic;
    if (doc["topic"].get(topic) != simdjson::SUCCESS || topic != "trade") {
        return;
    }

    auto itMap = mWsToOrigin.find(std::string(symbol));
    if (itMap == mWsToOrigin.end()) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(itMap->second, info, key)) {
        return;
    }

    for (auto item : doc["data"]) {
        std::string_view id;
        if (item["v"].get(id) != simdjson::SUCCESS) continue;

        int64_t tsMs = 0;
        if (item["t"].get(tsMs) != simdjson::SUCCESS) continue;

        std::string_view p, q;
        if (item["p"].get(p) != simdjson::SUCCESS) continue;
        if (item["q"].get(q) != simdjson::SUCCESS) continue;

        bool isMaker = false;               // m 在最后
        item["m"].get(isMaker);

        const double px = crypto::fast_atod(p);
        const double sz = crypto::fast_atod(q);
        if (!(px > 0) || !(sz > 0)) {
            continue;
        }
        if (tsMs <= 0) {
            tsMs = crypto::getCurrentTime() / 1000;
        }

        md::Trades t;
        std::memset(&t, 0, sizeof(t));
        t.exchangeTypeEnum = exchangeTypeEnum;
        t.instTypeEnum     = instTypeEnum;
        t.marketTypeEnum   = marketTypeEnum;
        std::strncpy(t.instId, info.instId, INSTID_SIZE);
        std::strncpy(t.tradeId, std::string(id).c_str(), sizeof(t.tradeId) - 1);

        // m = maker 是买方 ⇒ 主动卖
        t.direction = isMaker ? DT_SHORT : DT_LONG;
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
}

void md::EchobitUnit::parseFunding(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("ECHOBIT funding parse error: {}, len: {}", simdjson::error_message(doc.error()), msg.size());
        return;
    }

    // 键顺序（实测）：{code, topic:"fund_rates", event, data[{symbolId, currentTime, settleTime,
    //                  settleRate, nextSettleTime, fundRate}]}
    //   注意时间字段是**字符串**（"1792487946003"），不是数字。

    std::string_view topic;
    if (doc["topic"].get(topic) != simdjson::SUCCESS || topic != "fund_rates") {
        return;
    }

    const int64_t nowUs = crypto::getCurrentTime();

    for (auto row : doc["data"]) {
        std::string_view symbolId;
        if (row["symbolId"].get(symbolId) != simdjson::SUCCESS) continue;

        // 全市场推送：只处理本 unit 的 symbol，否则会对 170+ 个无关合约刷 LOG_ERROR
        if (mFundSyms.find(std::string(symbolId)) == mFundSyms.end()) {
            continue;
        }

        // currentTime / settleTime / settleRate 用不到就不读 —— ondemand 的 operator[]
        // 是"从当前位置向前找"，跳过中间的字段是允许的，不需要为了顺序把它们都读一遍。

        std::string_view nextStr;
        const bool       hasNext = row["nextSettleTime"].get(nextStr) == simdjson::SUCCESS;

        std::string_view fundStr;
        if (row["fundRate"].get(fundStr) != simdjson::SUCCESS) {
            continue;
        }

        md::InstrumentInfo info;
        std::string        key;
        if (!resolveInst(std::string(symbolId), info, key)) {
            continue;
        }

        md::FundingRate fr;
        std::memset(&fr, 0, sizeof(fr));
        fr.exchangeTypeEnum = exchangeTypeEnum;
        fr.instTypeEnum     = instTypeEnum;
        fr.marketTypeEnum   = marketTypeEnum;
        std::strncpy(fr.instId, info.instId, INSTID_SIZE);

        fr.fundingRate     = crypto::fast_atod(fundStr);
        fr.nextFundingRate = fr.fundingRate;
        if (hasNext) {
            fr.fundingTime = crypto::fast_atol(nextStr);
        }

        fr.tsTrans = nowUs;
        fr.tsEvent = nowUs;
        fr.tsRecv  = tsNet;
        fr.tsParse = nowUs;

#ifdef NEED_SHM
        mFundingRatePublisher[key]->push(fr);
#endif
    }
}

md::EchobitMarket::EchobitMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                                 std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec,
                                 int lot, const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::EchobitUnit* unit = new md::EchobitUnit(smc, info.exchangeTypeEnum, info.instTypeEnum,
                                                    info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        unitVec.push_back(unit);
    }
}

md::EchobitMarket::~EchobitMarket() {}

void md::EchobitMarket::start() {
    for (size_t i = 0; i < unitVec.size(); ++i) {
        unitVec[i]->start();          // 三档全部走 WS（含 FUNDING_RATE），没有 REST-only
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}
