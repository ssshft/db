#include "weex/WeexMarket.h"

#include <cstdio>
#include <cstring>

#include "Net.h"

md::WeexUnit::WeexUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                       std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port,
                       const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // 心跳是 JSON {"method":"PING"} → {"result":true}；服务端不发 WS PING。
    cfg.ping_mode                = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = R"({"method":"PING"})";
    cfg.idle_timeout_sec         = 90;
    cfg.data_idle_timeout_sec    = 0;
}

void md::WeexUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("WEEX {} FUNDING_RATE uses REST polling, no ws connection", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum != USDT_SWAP) {
        LOG_ERROR("WEEX only supports USDT_SWAP in this impl, got: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    cfg.url = WEEX_WS_PUBLIC;

    const char* suffix = nullptr;
    if (marketTypeEnum == md::DEPTH1) {
        suffix = "@depth15";        // 枚举只有 15 / 200，没有 1 档
    }
    else if (marketTypeEnum == md::TRADES) {
        suffix = "@trade";
    }
    else {
        LOG_ERROR("WEEX {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                  md::MarketTypeEnum2StrMap[marketTypeEnum]);
        return;
    }

    // 一个 SUBSCRIBE 的 params 里可以放多个 topic；按 50 个一组切，避免单帧过大。
    constexpr size_t kPerMsg = 50;
    cfg.subscribe_messages.clear();
    std::string params;
    size_t      n = 0;
    for (auto& info : vInstInfo) {
        if (!params.empty()) {
            params += ',';
        }
        params += fmt::format(R"("{}")", std::string(info.originInstId) + suffix);
        if (marketTypeEnum == md::DEPTH1) {
            mBooks[std::string(info.originInstId)] = TopBook{};
        }
        if (++n % kPerMsg == 0) {
            cfg.subscribe_messages.push_back(
                fmt::format(R"({{"method":"SUBSCRIBE","params":[{}],"id":{}}})", params, cfg.subscribe_messages.size() + 1));
            params.clear();
        }
    }
    if (!params.empty()) {
        cfg.subscribe_messages.push_back(
            fmt::format(R"({{"method":"SUBSCRIBE","params":[{}],"id":{}}})", params, cfg.subscribe_messages.size() + 1));
    }

    LOG_INFO("WEEX ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::WeexUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // 过滤：连接回执 {"cid":...,"event":"connected"}、订阅回执 {"result":true,"id":N}、
    // 心跳回执 {"result":true} 都没有业务键。业务帧的第一个键是 "e"。
    if (sv.find("\"e\":\"") == std::string_view::npos) {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::WeexUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    if (marketTypeEnum == md::DEPTH1) {
        parseDepth(msg, tsNet);
    }
    else if (marketTypeEnum == md::TRADES) {
        parseTrade(msg, tsNet);
    }
}

void md::WeexUnit::parseDepth(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("WEEX simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // ⚠️ 键顺序（实测，simdjson ondemand 前向单向，**顺序不能动**）：
    //   {e, E, s, U, u, l, d, b, a}
    //   ⇒ s 在 d/b/a 之前（这点比 Phemex 好），**b 在 a 之前**，d 在 b 之前。
    //   把 a 写在 b 前面 ⇒ 买档静默取空 ⇒ 整家所没有 DEPTH1。

    std::string_view e;
    if (doc["e"].get(e) != simdjson::SUCCESS) {
        return;
    }
    if (e != "depth" && e != "depthSnapshot") {
        return;
    }

    int64_t tsMs = 0;
    doc["E"].get(tsMs);

    std::string_view symbol;
    if (doc["s"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    std::string_view kind;                 // "SNAPSHOT" / "CHANGED"
    doc["d"].get(kind);

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(symbol), info, key)) {
        return;
    }

    auto itBook = mBooks.find(std::string(symbol));
    if (itBook == mBooks.end()) {
        return;
    }
    TopBook& book = itBook->second;

    if (kind == "SNAPSHOT") {
        book.reset();                      // 全量：先清，否则重连后会残留上一轮的档位
    }

    for (auto lv : doc["b"]) {             // b 在前
        auto             it = lv.begin();
        std::string_view p, q;
        if ((*it).get(p) != simdjson::SUCCESS) break;
        ++it;
        if ((*it).get(q) != simdjson::SUCCESS) break;
        book.applyBid(crypto::fast_atod(p), crypto::fast_atod(q));
    }
    for (auto lv : doc["a"]) {             // a 在后
        auto             it = lv.begin();
        std::string_view p, q;
        if ((*it).get(p) != simdjson::SUCCESS) break;
        ++it;
        if ((*it).get(q) != simdjson::SUCCESS) break;
        book.applyAsk(crypto::fast_atod(p), crypto::fast_atod(q));
    }

    double bp = 0, bv = 0, ap = 0, av = 0;
    if (!book.best(&bp, &bv, &ap, &av)) {
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

void md::WeexUnit::parseTrade(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("WEEX simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // 键顺序（实测）：{e, E, s, d[{T, t, p, q, v, m}]}
    //   ⇒ s 在 d 之前；d 内部 T → t → p → q → v → m。

    std::string_view e;
    if (doc["e"].get(e) != simdjson::SUCCESS) {
        return;
    }
    if (e != "trade" && e != "tradeSnapshot") {
        return;
    }

    std::string_view symbol;
    if (doc["s"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(symbol), info, key)) {
        return;
    }

    // 一帧里可能多笔，按 T 升序回放（交易所给的是新→旧）。
    // 先在栈上收，再倒序处理 —— ondemand 的 array 只能前向遍历一次。
    struct Row {
        int64_t     tsMs;
        std::string id;
        double      px;
        double      sz;
        bool        isMaker;
    };
    Row    rows[64];
    size_t n = 0;

    for (auto item : doc["d"]) {
        if (n >= 64) break;
        Row r{};
        if (item["T"].get(r.tsMs) != simdjson::SUCCESS) continue;

        std::string_view id, p, q;
        if (item["t"].get(id) != simdjson::SUCCESS) continue;
        if (item["p"].get(p) != simdjson::SUCCESS) continue;
        if (item["q"].get(q) != simdjson::SUCCESS) continue;

        bool isMaker = false;
        item["m"].get(isMaker);

        r.id      = std::string(id);
        r.px      = crypto::fast_atod(p);
        r.sz      = crypto::fast_atod(q);
        r.isMaker = isMaker;
        rows[n++] = std::move(r);
    }

    if (n == 0) {
        return;
    }

    auto& last = mLastTrade[std::string(symbol)];

    for (size_t i = n; i-- > 0;) {          // 倒序 ⇒ 时间升序
        const Row& r = rows[i];
        if (r.tsMs <= 0 || !(r.px > 0) || !(r.sz > 0)) {
            continue;
        }
        // 去重：比上一笔更旧，或同毫秒同 uuid ⇒ 已经推过了
        if (r.tsMs < last.tsMs || (r.tsMs == last.tsMs && r.id == last.id)) {
            continue;
        }
        last.tsMs = r.tsMs;
        last.id   = r.id;

        md::Trades t;
        std::memset(&t, 0, sizeof(t));
        t.exchangeTypeEnum = exchangeTypeEnum;
        t.instTypeEnum     = instTypeEnum;
        t.marketTypeEnum   = marketTypeEnum;
        std::strncpy(t.instId, info.instId, INSTID_SIZE);
        std::strncpy(t.tradeId, r.id.c_str(), sizeof(t.tradeId) - 1);

        // m = maker 是卖方 ⇒ 主动卖
        t.direction = r.isMaker ? DT_SHORT : DT_LONG;
        t.px        = r.px * info.reduceNumber;
        t.sz        = r.sz * info.value * info.magnifyNumber;

        t.tsTrans = r.tsMs * 1000;
        t.tsEvent = r.tsMs * 1000;
        t.tsRecv  = tsNet;
        t.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
        mTradesPublisher[key]->push(t);
#endif
    }
}

bool md::WeexUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    const std::string host   = "api-contract.weex.com";
    const std::string target = fmt::format("/capi/v3/market/fundingRate?symbol={}", info.originInstId);

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_WEEX_FUNDING", host, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("WEEX funding http status {} for {}", status, info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("WEEX funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    // 返回的是**数组**（新→旧），不是 {"code":..,"data":..} 信封
    int64_t curMs = 0, prevMs = 0;
    double  rate  = 0;
    int     idx   = 0;
    for (auto row : doc) {
        std::string_view rateStr;
        if (row["fundingRate"].get(rateStr) != simdjson::SUCCESS) {
            break;
        }
        int64_t ft = 0;
        row["fundingTime"].get(ft);

        if (idx == 0) {
            rate  = crypto::fast_atod(rateStr);
            curMs = ft;
        }
        else if (idx == 1) {
            prevMs = ft;
            break;
        }
        ++idx;
    }
    if (idx == 0) {
        return false;
    }

    // 没有"下次结算时间"字段 ⇒ 用相邻两条的间隔推（实测 8 h）；只有一条就退回 8 h 常量。
    int64_t intervalMs = (curMs > 0 && prevMs > 0 && curMs > prevMs) ? (curMs - prevMs) : 8 * 3600 * 1000LL;

    fr.fundingRate     = rate;
    fr.nextFundingRate = rate;
    fr.fundingTime     = curMs > 0 ? curMs + intervalMs : 0;
    fr.tsTrans         = crypto::getCurrentTime();
    return true;
}

void md::WeexUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::WeexMarket::WeexMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                           std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot,
                           const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::WeexUnit* unit = new md::WeexUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum,
                                              info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        unitVec.push_back(unit);
    }
}

md::WeexMarket::~WeexMarket() {}

void md::WeexMarket::start() {
    for (size_t i = 0; i < unitVec.size(); ++i) {
        if (unitVec[i]->isRestOnly()) {
            unitVec[i]->startRestOnly();
        }
        else {
            unitVec[i]->start();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}
