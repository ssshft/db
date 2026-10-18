#include "xt/XtMarket.h"

#include <cstdio>
#include <cstring>

#include "Net.h"

md::XtUnit::XtUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                   std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // XT 要的是**明文 "ping"**（不是 JSON），服务端回明文 "pong"；30s 内没收到 ping 就断。
    cfg.ping_mode                = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 10;
    cfg.client_ping_text         = "ping";
    cfg.idle_timeout_sec         = 60;
    cfg.data_idle_timeout_sec    = 0;
}

void md::XtUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("XT {} FUNDING_RATE uses REST polling, no ws connection", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum != USDT_SWAP) {
        LOG_ERROR("XT only supports USDT_SWAP in this impl, got: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    cfg.url = XT_WS_FUTURES;

    cfg.subscribe_messages.clear();
    for (auto& info : vInstInfo) {
        std::string channel;
        if (marketTypeEnum == md::DEPTH1) {
            // levels 只能是 5/10/20/50；interval 只能是 100/250/500/1000ms
            channel = fmt::format("depth@{},5,100ms", info.originInstId);
        }
        else if (marketTypeEnum == md::TRADES) {
            channel = fmt::format("trade@{}", info.originInstId);
        }
        else {
            LOG_ERROR("XT {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                      md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }

        cfg.subscribe_messages.push_back(
            fmt::format(R"({{"method":"SUBSCRIBE","params":["{}"],"id":"s{}"}})", channel, cfg.subscribe_messages.size() + 1));
    }

    LOG_INFO("XT ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::XtUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // 心跳回执 "pong" / 订阅回执 {"code":0,"msg":"success","id":"s1",...} 里都没有 "topic"
    if (sv.find("\"topic\"") == std::string_view::npos) {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::XtUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("XT simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    std::string_view topic;
    if (doc["topic"].get(topic) != simdjson::SUCCESS) {
        return;
    }

    auto data = doc["data"];

    std::string_view symbol;                 // 小写 btc_usdt
    if (data["s"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(symbol), info, key)) {
        return;
    }

    // ⚠️⚠️ 键顺序（实测，simdjson ondemand 前向单向，**顺序不能动**）：
    //   depth: data{s, id, a, b, t}   ⇒ **a(asks) 排在 b(bids) 之前**，t 在最后
    //   trade: data{s, p, a, m, t}    ⇒ p → a(数量) → m → t
    //   把 b 写在 a 前面、或把 t 提到最前，都会让后面的字段静默取不到（深度恒为空）。

    if (marketTypeEnum == md::DEPTH1 && topic == "depth") {
        // data.a = asks [[p,v]...]，data.b = bids [[p,v]...]
        double bp = 0, bv = 0, ap = 0, av = 0;
        bool   gotBid = false, gotAsk = false;

        for (auto lv : data["a"]) {
            auto             it = lv.begin();
            std::string_view p, v;
            if ((*it).get(p) != simdjson::SUCCESS) break;
            ++it;
            if ((*it).get(v) != simdjson::SUCCESS) break;
            ap     = crypto::fast_atod(p);
            av     = crypto::fast_atod(v);
            gotAsk = true;
            break;
        }
        for (auto lv : data["b"]) {
            auto             it = lv.begin();
            std::string_view p, v;
            if ((*it).get(p) != simdjson::SUCCESS) break;
            ++it;
            if ((*it).get(v) != simdjson::SUCCESS) break;
            bp     = crypto::fast_atod(p);
            bv     = crypto::fast_atod(v);
            gotBid = true;
            break;
        }
        if (!gotBid || !gotAsk) {
            return;
        }

        int64_t tsMs = 0;                                        // data.t，排在 a/b 之后
        data["t"].get(tsMs);
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
    else if (marketTypeEnum == md::TRADES && topic == "trade") {
        // data{s, p, a, m, t}：p → a(数量) → m → t，顺序不能动
        std::string_view priceStr;
        if (data["p"].get(priceStr) != simdjson::SUCCESS) return;

        std::string_view amountStr;
        if (data["a"].get(amountStr) != simdjson::SUCCESS) return;

        // m: "BID" = 主动买, "ASK" = 主动卖
        std::string_view sideStr;
        data["m"].get(sideStr);

        int64_t tsMs = 0;                                        // data.t 在最后
        data["t"].get(tsMs);
        if (tsMs <= 0) {
            tsMs = crypto::getCurrentTime() / 1000;
        }

        md::Trades t;
        std::memset(&t, 0, sizeof(t));
        t.exchangeTypeEnum = exchangeTypeEnum;
        t.instTypeEnum     = instTypeEnum;
        t.marketTypeEnum   = marketTypeEnum;
        std::strncpy(t.instId, info.instId, INSTID_SIZE);

        // XT 不单独给 tradeId；同一毫秒可能多笔，用时间戳兜底（只用于去重展示，不参与撮合）
        std::snprintf(t.tradeId, sizeof(t.tradeId), "%lld", static_cast<long long>(tsMs));

        t.direction = (sideStr == "ASK") ? DT_SHORT : DT_LONG;

        t.px = crypto::fast_atod(priceStr) * info.reduceNumber;
        t.sz = crypto::fast_atod(amountStr) * info.value * info.magnifyNumber;

        t.tsTrans = tsMs * 1000;
        t.tsEvent = tsMs * 1000;
        t.tsRecv  = tsNet;
        t.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
        mTradesPublisher[key]->push(t);
#endif
    }
}

bool md::XtUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    const std::string host   = "fapi.xt.com";
    const std::string target = fmt::format("/future/market/v1/public/q/funding-rate?symbol={}", info.originInstId);

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_XT_FUNDING", host, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("XT funding http status {} for {}", status, info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("XT funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    int64_t returnCode = -1;
    doc["returnCode"].get(returnCode);
    if (returnCode != 0) {
        LOG_WARN("XT funding returnCode {}, body: {}", returnCode, body);
        return false;
    }

    auto             result = doc["result"];
    std::string_view rateStr;
    if (result["fundingRate"].get(rateStr) != simdjson::SUCCESS) {
        return false;
    }
    fr.fundingRate = crypto::fast_atod(rateStr);

    int64_t nextCollectionTime = 0;
    if (result["nextCollectionTime"].get(nextCollectionTime) == simdjson::SUCCESS) {
        fr.fundingTime = nextCollectionTime * 1000;
    }
    fr.nextFundingRate = fr.fundingRate;
    fr.tsTrans         = crypto::getCurrentTime();
    return true;
}

void md::XtUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::XtMarket::XtMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                       std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot,
                       const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::XtUnit* unit = new md::XtUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum,
                                          info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        xtUnitVec.push_back(unit);
    }
}

md::XtMarket::~XtMarket() {}

void md::XtMarket::start() {
    for (size_t i = 0; i < xtUnitVec.size(); ++i) {
        if (xtUnitVec[i]->isRestOnly()) {
            xtUnitVec[i]->startRestOnly();
        }
        else {
            xtUnitVec[i]->start();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}
