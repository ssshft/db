#include "phemex/PhemexMarket.h"

#include <cstdio>
#include <cstring>

#include "Net.h"

namespace {

// 一帧里最多收多少档。Phemex 快照固定 30 档，增量帧实测 1~5 档；
// 给 64 是留余量，超出就丢掉多余的（并告警一次），绝不越界写栈。
constexpr size_t kMaxLevels = 64;

struct Level {
    double px;
    double sz;
};

} // namespace

md::PhemexUnit::PhemexUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                           std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port,
                           const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // 服务端不发 WS PING，必须客户端自己发 server.ping；id 固定 1 即可（服务端原样回显）。
    cfg.ping_mode                = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = R"({"id":1,"method":"server.ping","params":[]})";
    cfg.idle_timeout_sec         = 90;
    cfg.data_idle_timeout_sec    = 0;
}

void md::PhemexUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("PHEMEX {} FUNDING_RATE uses REST polling, no ws connection",
                 InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum != USDT_SWAP && instTypeEnum != C_SWAP) {
        LOG_ERROR("PHEMEX only supports USDT_SWAP / C_SWAP in this impl, got: {}",
                  InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    const char* method = nullptr;
    if (marketTypeEnum == md::DEPTH1) {
        method = "orderbook_p.subscribe";
    }
    else if (marketTypeEnum == md::TRADES) {
        method = "trade_p.subscribe";
    }
    else {
        LOG_ERROR("PHEMEX {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                  md::MarketTypeEnum2StrMap[marketTypeEnum]);
        return;
    }

    cfg.url = PHEMEX_WS;

    cfg.subscribe_messages.clear();
    int id = 1;
    for (auto& info : vInstInfo) {
        cfg.subscribe_messages.push_back(
            fmt::format(R"({{"id":{},"method":"{}","params":["{}"]}})", id++, method, info.originInstId));
        if (marketTypeEnum == md::DEPTH1) {
            mBooks[std::string(info.originInstId)] = TopBook{};   // 首帧快照前是空簿子
        }
    }

    LOG_INFO("PHEMEX ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::PhemexUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // 过滤：订阅回执 {"error":null,...} 和心跳回执 {"error":null,"id":1,"result":"pong"} 都没有业务键。
    // 深度帧第一个键是 "depth"，成交帧第一个键是 "dts" —— 两者都不出现在回执里。
    if (sv.find("\"orderbook_p\"") == std::string_view::npos &&
        sv.find("\"trades_p\"") == std::string_view::npos) {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::PhemexUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    if (marketTypeEnum == md::DEPTH1) {
        parseDepth(msg, tsNet);
    }
    else if (marketTypeEnum == md::TRADES) {
        parseTrade(msg, tsNet);
    }
}

void md::PhemexUnit::parseDepth(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("PHEMEX simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // ⚠️ 键顺序（实测，simdjson ondemand 前向单向）：
    //   {depth, dts, mts, orderbook_p{asks, bids}, sequence, symbol, timestamp, type}
    //   ⇒ **asks 在 bids 之前**，**symbol 在 orderbook_p 之后**。
    //   所以这里先把档位收进栈上数组，读到 symbol 之后再回放 —— 不能"先取 symbol"。

    Level  asks[kMaxLevels];
    Level  bids[kMaxLevels];
    size_t nAsk = 0;
    size_t nBid = 0;

    auto ob = doc["orderbook_p"];

    for (auto lv : ob["asks"]) {                       // asks 在前
        if (nAsk >= kMaxLevels) break;
        auto             it = lv.begin();
        std::string_view p, q;
        if ((*it).get(p) != simdjson::SUCCESS) break;
        ++it;
        if ((*it).get(q) != simdjson::SUCCESS) break;
        asks[nAsk].px = crypto::fast_atod(p);
        asks[nAsk].sz = crypto::fast_atod(q);
        ++nAsk;
    }
    for (auto lv : ob["bids"]) {
        if (nBid >= kMaxLevels) break;
        auto             it = lv.begin();
        std::string_view p, q;
        if ((*it).get(p) != simdjson::SUCCESS) break;
        ++it;
        if ((*it).get(q) != simdjson::SUCCESS) break;
        bids[nBid].px = crypto::fast_atod(p);
        bids[nBid].sz = crypto::fast_atod(q);
        ++nBid;
    }

    std::string_view symbol;
    if (doc["symbol"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    int64_t tsMs = 0;                                  // timestamp 在 symbol 之后（纳秒）
    if (doc["timestamp"].get(tsMs) == simdjson::SUCCESS && tsMs > 0) {
        tsMs /= 1000000;
    }
    else {
        tsMs = crypto::getCurrentTime() / 1000;
    }

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

    for (size_t i = 0; i < nBid; ++i) {
        book.applyBid(bids[i].px, bids[i].sz);         // sz == 0 等价于删档
    }
    for (size_t i = 0; i < nAsk; ++i) {
        book.applyAsk(asks[i].px, asks[i].sz);
    }

    double bp = 0, bv = 0, ap = 0, av = 0;
    if (!book.best(&bp, &bv, &ap, &av)) {
        // 首帧快照之前是正常的；快照之后还取不到就是漏档了
        if (!bookWarned_ && (book.hasBid() || book.hasAsk())) {
            bookWarned_ = true;
            LOG_WARN("PHEMEX book incomplete for {}, bidLevels={} askLevels={} (只告警一次)",
                     info.originInstId, book.hasBid() ? 1 : 0, book.hasAsk() ? 1 : 0);
        }
        return;
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

void md::PhemexUnit::parseTrade(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("PHEMEX simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // 成交帧的键顺序（实测）：{dts, mts, sequence, symbol, trades_p}
    //   ⇒ symbol **在** trades_p 之前（和深度帧相反，别记混）。
    //   trades_p 元素 = [纳秒时间戳, "Buy"/"Sell", 价格, 数量]。

    std::string_view symbol;
    if (doc["symbol"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(symbol), info, key)) {
        return;
    }

    for (auto row : doc["trades_p"]) {
        auto it = row.begin();

        int64_t tsNs = 0;
        if ((*it).get(tsNs) != simdjson::SUCCESS) continue;
        ++it;

        std::string_view side;
        if ((*it).get(side) != simdjson::SUCCESS) continue;
        ++it;

        std::string_view p;
        if ((*it).get(p) != simdjson::SUCCESS) continue;
        ++it;

        std::string_view q;
        if ((*it).get(q) != simdjson::SUCCESS) continue;

        const double px = crypto::fast_atod(p);
        const double sz = crypto::fast_atod(q);
        if (!(px > 0) || !(sz > 0)) {
            continue;
        }

        const int64_t tsMs = tsNs / 1000000;

        md::Trades t;
        std::memset(&t, 0, sizeof(t));
        t.exchangeTypeEnum = exchangeTypeEnum;
        t.instTypeEnum     = instTypeEnum;
        t.marketTypeEnum   = marketTypeEnum;
        std::strncpy(t.instId, info.instId, INSTID_SIZE);

        // Phemex 不单独给 tradeId ⇒ 用纳秒时间戳，纳秒级不会撞
        std::snprintf(t.tradeId, sizeof(t.tradeId), "%lld", static_cast<long long>(tsNs));

        // side: "Buy" 主动买 / "Sell" 主动卖
        t.direction = (side == "Sell") ? DT_SHORT : DT_LONG;
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

bool md::PhemexUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    const std::string host = "api.phemex.com";
    const std::string target =
        fmt::format("/contract-biz/public/real-funding-rates?symbol={}", info.originInstId);

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_PHEMEX_FUNDING", host, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("PHEMEX funding http status {} for {}", status, info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("PHEMEX funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    int64_t code = -1;
    doc["code"].get(code);
    if (code != 0) {
        LOG_WARN("PHEMEX funding code {} for {}, body: {}", code, info.originInstId, body);
        return false;
    }

    for (auto row : doc["data"]["rows"]) {
        std::string_view rateStr;
        if (row["fundingRate"].get(rateStr) != simdjson::SUCCESS) {
            return false;
        }
        fr.fundingRate = crypto::fast_atod(rateStr);

        int64_t nextMs = 0;                              // nextfundingTime（小写 f）是数字毫秒
        if (row["nextfundingTime"].get(nextMs) == simdjson::SUCCESS) {
            fr.fundingTime = nextMs;
        }
        fr.nextFundingRate = fr.fundingRate;
        fr.tsTrans         = crypto::getCurrentTime();
        return true;
    }
    return false;
}

void md::PhemexUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::PhemexMarket::PhemexMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                               std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec,
                               int lot, const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::PhemexUnit* unit = new md::PhemexUnit(smc, info.exchangeTypeEnum, info.instTypeEnum,
                                                  info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        unitVec.push_back(unit);
    }
}

md::PhemexMarket::~PhemexMarket() {}

void md::PhemexMarket::start() {
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
