#include "hotcoin/HotcoinMarket.h"

#include <cstdio>
#include <cstring>

#include "Net.h"

md::HotcoinUnit::HotcoinUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                             std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port,
                             const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    cfg.ping_mode                = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = R"({"event":"ping"})";
    cfg.idle_timeout_sec         = 90;
    cfg.data_idle_timeout_sec    = 0;
}

void md::HotcoinUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("HOTCOIN {} FUNDING_RATE uses REST polling, no ws connection",
                 InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum != USDT_SWAP) {
        LOG_ERROR("HOTCOIN only supports USDT_SWAP in this impl, got: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    cfg.url = HOTCOIN_WS;

    const char* type = nullptr;
    if (marketTypeEnum == md::DEPTH1) {
        type = "depth";
    }
    else if (marketTypeEnum == md::TRADES) {
        type = "fills";
    }
    else {
        LOG_ERROR("HOTCOIN {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                  md::MarketTypeEnum2StrMap[marketTypeEnum]);
        return;
    }

    // params 是**对象**不是数组 ⇒ 一个主题一条消息，没法合批。
    cfg.subscribe_messages.clear();
    for (auto& info : vInstInfo) {
        cfg.subscribe_messages.push_back(fmt::format(
            R"({{"event":"subscribe","params":{{"biz":"perpetual","type":"{}","contractCode":"{}","zip":false,"serialize":false}}}})",
            type, info.originInstId));
    }

    LOG_INFO("HOTCOIN ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::HotcoinUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // 过滤：订阅回执 {"channel":"subscribe",...} 和心跳 {"event":"pong"} 都没有 "data"。
    // 业务帧第一个键就是 "data"，所以按它过滤最省。
    if (sv.find("\"data\"") == std::string_view::npos) {
        return;
    }
    // 订阅回执里也有 "data":{"result":true} —— 排除掉，否则会往 ring 里灌无用的回执帧
    if (sv.find("\"result\":true") != std::string_view::npos) {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::HotcoinUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    if (marketTypeEnum == md::DEPTH1) {
        parseDepth(msg, tsNet);
    }
    else if (marketTypeEnum == md::TRADES) {
        parseFills(msg, tsNet);
    }
}

void md::HotcoinUnit::parseDepth(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("HOTCOIN simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // ⚠️ 键顺序（实测，simdjson ondemand 前向单向，**顺序不能动**）：
    //   {data{asks, bids}, biz, type, contractCode, env, timestamp}
    //   ⇒ **data 在最前、contractCode 在最后**。所以先把最优档收进局部变量，
    //     读完 contractCode 再发；反过来写（先取 contractCode）会拿不到档位。

    double ap = 0, av = 0, bp = 0, bv = 0;

    auto data = doc["data"];

    for (auto lv : data["asks"]) {          // asks 在前
        auto             it = lv.begin();
        std::string_view p, q;
        if ((*it).get(p) != simdjson::SUCCESS) break;
        ++it;
        if ((*it).get(q) != simdjson::SUCCESS) break;
        // 第三个元素是累计量，DEPTH1 用不到
        ap = crypto::fast_atod(p);
        av = crypto::fast_atod(q);
        break;
    }
    for (auto lv : data["bids"]) {
        auto             it = lv.begin();
        std::string_view p, q;
        if ((*it).get(p) != simdjson::SUCCESS) break;
        ++it;
        if ((*it).get(q) != simdjson::SUCCESS) break;
        bp = crypto::fast_atod(p);
        bv = crypto::fast_atod(q);
        break;
    }

    std::string_view type;
    doc["type"].get(type);
    if (type != "depth") {
        return;
    }

    std::string_view contractCode;
    if (doc["contractCode"].get(contractCode) != simdjson::SUCCESS) {
        return;
    }

    int64_t tsMs = 0;                        // timestamp 在 contractCode 之后
    if (doc["timestamp"].get(tsMs) == simdjson::SUCCESS && tsMs <= 0) {
        tsMs = 0;
    }
    if (tsMs <= 0) {
        tsMs = crypto::getCurrentTime() / 1000;
    }

    if (!(ap > 0) || !(bp > 0)) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(contractCode), info, key)) {
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

void md::HotcoinUnit::parseFills(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("HOTCOIN simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // 键顺序（实测）：{data[[价格, 数量, 方向, 毫秒, id], ...], biz, type, contractCode, env, timestamp}
    //   ⇒ data 是**数组的数组**（一帧可以有多笔），contractCode 在 data 之后。
    //   所以先把记录收到栈上，读完 contractCode 再发。
    //   （2026-10-20 用真实帧回放才发现：一度按"扁平数组"写过，那样一条成交都解析不出来。）

    struct Rec {
        double  px;
        double  sz;
        int64_t tsMs;
        bool    isSell;
    };
    Rec    recs[64];
    size_t n = 0;

    for (auto rec : doc["data"]) {
        if (n >= 64) break;
        auto it = rec.begin();

        std::string_view pxStr, szStr, sideStr;
        if ((*it).get(pxStr) != simdjson::SUCCESS) break;
        ++it;
        if ((*it).get(szStr) != simdjson::SUCCESS) break;
        ++it;
        if ((*it).get(sideStr) != simdjson::SUCCESS) break;
        ++it;
        int64_t tsMs = 0;
        if ((*it).get(tsMs) != simdjson::SUCCESS) break;

        recs[n].px     = crypto::fast_atod(pxStr);
        recs[n].sz     = crypto::fast_atod(szStr);
        recs[n].tsMs   = tsMs;
        recs[n].isSell = (sideStr == "short");
        ++n;
    }

    std::string_view type;
    doc["type"].get(type);
    if (type != "fills") {
        return;
    }

    std::string_view contractCode;
    if (doc["contractCode"].get(contractCode) != simdjson::SUCCESS) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(contractCode), info, key)) {
        return;
    }

    for (size_t i = 0; i < n; ++i) {
        const double px = recs[i].px;
        const double sz = recs[i].sz;
        if (!(px > 0) || !(sz > 0)) {
            continue;
        }
        int64_t tsMs = recs[i].tsMs;
        if (tsMs <= 0) {
            tsMs = crypto::getCurrentTime() / 1000;
        }

        md::Trades t;
        std::memset(&t, 0, sizeof(t));
        t.exchangeTypeEnum = exchangeTypeEnum;
        t.instTypeEnum     = instTypeEnum;
        t.marketTypeEnum   = marketTypeEnum;
        std::strncpy(t.instId, info.instId, INSTID_SIZE);

        // 第 5 个元素 id 实测恒为 0 ⇒ 用毫秒时间戳 + 序号兜底（同毫秒多笔时不至于撞）
        std::snprintf(t.tradeId, sizeof(t.tradeId), "%lld_%zu", static_cast<long long>(tsMs), i);

        // "long" = 主动买 / "short" = 主动卖
        t.direction = recs[i].isSell ? DT_SHORT : DT_LONG;
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

bool md::HotcoinUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    const std::string host = "api-ct.hotcoin.fit";
    const std::string target =
        fmt::format("/api/v1/perpetual/public/{}/fee-rate", info.originInstId);   // 必须小写

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_HOTCOIN_FUNDING", host, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("HOTCOIN funding http status {} for {}", status, info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("HOTCOIN funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    int64_t code = -1;
    doc["code"].get(code);
    if (code != 200) {
        LOG_WARN("HOTCOIN funding code {} for {}, body: {}", code, info.originInstId, body);
        return false;
    }

    double  rate  = 0;
    int64_t curMs = 0, prevMs = 0;
    int     idx   = 0;
    for (auto row : doc["data"]["rows"]) {
        // feeRate 是 JSON 数字且可能是科学计数法（7.117e-05）⇒ 必须 get_double()
        double r = 0;
        if (row["feeRate"].get(r) != simdjson::SUCCESS) {
            break;
        }
        int64_t ti = 0;
        row["timeIndex"].get(ti);

        if (idx == 0) {
            rate  = r;
            curMs = ti;
        }
        else {
            prevMs = ti;
            break;
        }
        ++idx;
    }
    if (idx == 0) {
        return false;
    }

    // 没有"下次结算时间" ⇒ 用相邻两条的间隔推（实测 8 h）
    const int64_t intervalMs = (curMs > 0 && prevMs > 0 && curMs > prevMs) ? (curMs - prevMs) : 8 * 3600 * 1000LL;

    fr.fundingRate     = rate;
    fr.nextFundingRate = rate;
    fr.fundingTime     = curMs > 0 ? curMs + intervalMs : 0;
    fr.tsTrans         = crypto::getCurrentTime();
    return true;
}

void md::HotcoinUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::HotcoinMarket::HotcoinMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                                 std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec,
                                 int lot, const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::HotcoinUnit* unit = new md::HotcoinUnit(smc, info.exchangeTypeEnum, info.instTypeEnum,
                                                    info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        unitVec.push_back(unit);
    }
}

md::HotcoinMarket::~HotcoinMarket() {}

void md::HotcoinMarket::start() {
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
