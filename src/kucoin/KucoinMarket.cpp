#include "kucoin/KucoinMarket.h"

#include <cstring>

#include "Net.h"

md::KucoinUnit::KucoinUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                           std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port,
                           const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // KuCoin 的 pingInterval 是 18000ms，留 3s 余量
    cfg.ping_mode                = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 15;
    cfg.client_ping_text         = R"({"id":"1","type":"ping"})";
    cfg.idle_timeout_sec         = 60;
    cfg.data_idle_timeout_sec    = 0;
}

bool md::KucoinUnit::fetchWsToken(std::string& endpoint, std::string& token) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {
        {"User-Agent", "utrade-db/1.0"}, {"Content-Type", "application/json"}};

    std::string bodyIn  = "{}";
    std::string bodyOut;
    int         status = 0;
    if (!Net::Instance().syncPost("DB_KUCOIN_TOKEN", KUCOIN_REST_FUTURES, "/api/v1/bullet-public", bodyIn, kHeaders,
                                  {}, bodyOut, status)) {
        return false;
    }
    if (status != 200) {
        LOG_ERROR("KUCOIN bullet-public http status {}: {}", status, bodyOut);
        return false;
    }

    simdjson::padded_string padded(bodyOut);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("KUCOIN bullet-public json error: {}, body: {}", simdjson::error_message(doc.error()), bodyOut);
        return false;
    }

    auto data = doc["data"];
    std::string_view tok;
    if (data["token"].get(tok) != simdjson::SUCCESS || tok.empty()) {
        LOG_ERROR("KUCOIN bullet-public no token: {}", bodyOut);
        return false;
    }
    token.assign(tok);

    endpoint = KUCOIN_WS_FUTURES;
    for (auto srv : data["instanceServers"]) {
        std::string_view ep;
        if (srv["endpoint"].get(ep) == simdjson::SUCCESS && !ep.empty()) {
            endpoint.assign(ep);
        }
        break;
    }
    return true;
}

void md::KucoinUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("KUCOIN {} FUNDING_RATE uses REST polling, no ws connection", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum != USDT_SWAP) {
        LOG_ERROR("KUCOIN only supports USDT_SWAP in this impl, got: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    // token 换不到就没法连；重试 3 次再放弃（放弃后 url 为空，start() 里会被 catch 住并打日志）
    std::string endpoint, token;
    for (int attempt = 1; attempt <= 3; ++attempt) {
        if (fetchWsToken(endpoint, token)) {
            break;
        }
        LOG_WARN("KUCOIN bullet-public attempt {}/3 failed", attempt);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (token.empty()) {
        LOG_ERROR("KUCOIN can not get ws token, unit will not connect");
        return;
    }

    // connectId 必须唯一（同一个 token 可开多连接，但要靠 connectId 区分）
    cfg.url = fmt::format("{}?token={}&connectId={}", endpoint, token, crypto::get_int_rand(100000, 999999));

    cfg.subscribe_messages.clear();
    int subId = 0;
    for (auto& info : vInstInfo) {
        std::string topic;
        if (marketTypeEnum == md::DEPTH1) {
            topic = fmt::format("/contractMarket/tickerV2:{}", info.originInstId);   // 含 bestBid/Ask 及量
        }
        else if (marketTypeEnum == md::TRADES) {
            topic = fmt::format("/contractMarket/execution:{}", info.originInstId);
        }
        else {
            LOG_ERROR("KUCOIN {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                      md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }

        cfg.subscribe_messages.push_back(
            fmt::format(R"({{"id":"{}","type":"subscribe","topic":"{}","response":true,"privateChannel":false}})",
                        ++subId, topic));
    }

    LOG_INFO("KUCOIN ws url: {}, sub count: {}", cfg.url, cfg.subscribe_messages.size());
}

void md::KucoinUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // {"id":"c...","type":"welcome"} / {"id":"1","type":"ack"} / {"id":"9","type":"pong"}
    // 只有数据帧是 "type":"message"。
    // ⚠️ 匹配 `"message"` 这个**带引号的 token**，不要写死 `"type":"message"` ——
    //    后者把冒号后的空白也绑死了，服务端一旦改成 `"type": "message"` 就全丢。
    if (sv.find("\"message\"") == std::string_view::npos) {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::KucoinUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("KUCOIN simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    auto data = doc["data"];

    std::string_view symbol;
    if (data["symbol"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(symbol), info, key)) {
        return;
    }

    // ⚠️⚠️ 键顺序（实测，simdjson ondemand 前向单向，**顺序不能动**）：
    //   tickerV2 : symbol, sequence, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, ts
    //              ⇒ **size 排在 price 之前**，ts 在最后
    //   execution: symbol, sequence, side, size, price, takerOrderId, makerOrderId, tradeId, ts
    //              ⇒ side → size → price → tradeId → ts
    //   顺序写反不报错，字段静默为 0 ⇒ 深度/成交全丢。
    //
    // ⚠️ KuCoin 的 ts 是**纳秒**（1792223692621000000，19 位 > 2^53），必须 int64，
    //    走 double 会丢精度（末位被截断到 2^53 的粒度 ≈ 0.1ms，看似能用，实际会串帧）。

    if (marketTypeEnum == md::DEPTH1) {
        double bidSize = 0, askSize = 0;
        data["bestBidSize"].get(bidSize);          // 数字（张），parse_double 对整数也成立

        std::string_view bidPriceStr;
        if (data["bestBidPrice"].get(bidPriceStr) != simdjson::SUCCESS) return;

        std::string_view askPriceStr;
        if (data["bestAskPrice"].get(askPriceStr) != simdjson::SUCCESS) return;

        data["bestAskSize"].get(askSize);

        int64_t       tsNs = 0;
        data["ts"].get(tsNs);
        const int64_t tsUs = (tsNs > 0) ? tsNs / 1000 : crypto::getCurrentTime();

        md::Depth1 d1;
        std::memset(&d1, 0, sizeof(d1));
        d1.exchangeTypeEnum = exchangeTypeEnum;
        d1.instTypeEnum     = instTypeEnum;
        d1.marketTypeEnum   = marketTypeEnum;
        std::strncpy(d1.instId, info.instId, INSTID_SIZE);

        d1.bp1 = crypto::fast_atod(bidPriceStr) * info.reduceNumber;
        d1.ap1 = crypto::fast_atod(askPriceStr) * info.reduceNumber;
        d1.bv1 = bidSize * info.value * info.magnifyNumber;
        d1.av1 = askSize * info.value * info.magnifyNumber;

        if (d1.bp1 <= 0 || d1.ap1 <= 0) {
            return;
        }

        d1.tsTrans = tsUs;
        d1.tsEvent = tsUs;
        d1.tsRecv  = tsNet;
        d1.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
        mDepth1Publisher[key]->push(d1);
#endif
    }
    else if (marketTypeEnum == md::TRADES) {
        // execution 的 data 是**单个对象**（一帧一笔），不是数组
        std::string_view sideStr;
        data["side"].get(sideStr);

        double size = 0;
        data["size"].get(size);                 // 张数（整数）

        std::string_view priceStr;
        if (data["price"].get(priceStr) != simdjson::SUCCESS) return;

        std::string_view tradeIdStr;
        if (data["tradeId"].get(tradeIdStr) != simdjson::SUCCESS) {
            tradeIdStr = std::string_view();
        }

        int64_t       tsNs = 0;
        data["ts"].get(tsNs);
        const int64_t tsUs = (tsNs > 0) ? tsNs / 1000 : crypto::getCurrentTime();

        md::Trades t;
        std::memset(&t, 0, sizeof(t));
        t.exchangeTypeEnum = exchangeTypeEnum;
        t.instTypeEnum     = instTypeEnum;
        t.marketTypeEnum   = marketTypeEnum;
        std::strncpy(t.instId, info.instId, INSTID_SIZE);

        if (!tradeIdStr.empty()) {
            std::strncpy(t.tradeId, tradeIdStr.data(), std::min<size_t>(tradeIdStr.size(), sizeof(t.tradeId) - 1));
        }

        // side 是 taker 方向: "buy" = 主动买
        t.direction = (sideStr == "sell") ? DT_SHORT : DT_LONG;

        t.px = crypto::fast_atod(priceStr) * info.reduceNumber;
        t.sz = size * info.value * info.magnifyNumber;

        t.tsTrans = tsUs;
        t.tsEvent = tsUs;
        t.tsRecv  = tsNet;
        t.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
        mTradesPublisher[key]->push(t);
#endif
    }
}

bool md::KucoinUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    const std::string target = fmt::format("/api/v1/funding-rate/{}/current", info.originInstId);

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_KUCOIN_FUNDING", KUCOIN_REST_FUTURES, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("KUCOIN funding http status {} for {}", status, info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("KUCOIN funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    std::string_view code = "?";
    doc["code"].get(code);
    if (code != "200000") {
        LOG_WARN("KUCOIN funding code {} body: {}", std::string(code), body);
        return false;
    }

    // ⚠️ 字段名是 value（不是 fundingRate），fundingTime 是**下次**结算时间
    auto data = doc["data"];
    if (data["value"].get(fr.fundingRate) != simdjson::SUCCESS) {
        return false;
    }
    int64_t fundingTime = 0;
    if (data["fundingTime"].get(fundingTime) == simdjson::SUCCESS) {
        fr.fundingTime = fundingTime * 1000;
    }
    fr.nextFundingRate = fr.fundingRate;
    fr.tsTrans         = crypto::getCurrentTime();
    return true;
}

void md::KucoinUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::KucoinMarket::KucoinMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                               std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec,
                               int lot, const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::KucoinUnit* unit = new md::KucoinUnit(smc, info.exchangeTypeEnum, info.instTypeEnum,
                                                  info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        kucoinUnitVec.push_back(unit);
    }
}

md::KucoinMarket::~KucoinMarket() {}

void md::KucoinMarket::start() {
    for (size_t i = 0; i < kucoinUnitVec.size(); ++i) {
        if (kucoinUnitVec[i]->isRestOnly()) {
            kucoinUnitVec[i]->startRestOnly();
        }
        else {
            kucoinUnitVec[i]->start();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}
