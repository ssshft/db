#include "bitunix/BitunixMarket.h"

#include <cstdio>
#include <cstring>
#include <ctime>

#include "Net.h"

namespace {

// "2026-10-20T09:12:43Z" → epoch 毫秒。Bitunix 的成交时间只到秒，所以 ms 恒为 0。
// 用 timegm（POSIX）而不是 std::get_time + 本地时区：报文里是 UTC，本机时区会整体偏 8 小时。
int64_t iso_utc_to_ms(std::string_view s) {
    if (s.size() < 20) {
        return 0;
    }
    std::tm tm{};
    tm.tm_year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0') - 1900;
    tm.tm_mon  = (s[5] - '0') * 10 + (s[6] - '0') - 1;
    tm.tm_mday = (s[8] - '0') * 10 + (s[9] - '0');
    tm.tm_hour = (s[11] - '0') * 10 + (s[12] - '0');
    tm.tm_min  = (s[14] - '0') * 10 + (s[15] - '0');
    tm.tm_sec  = (s[17] - '0') * 10 + (s[18] - '0');
    const time_t t = ::timegm(&tm);
    return t <= 0 ? 0 : static_cast<int64_t>(t) * 1000;
}

} // namespace

md::BitunixUnit::BitunixUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy,
                             MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host,
                             int port, const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // 心跳必须带当前秒级时间戳，静态字符串做不到 ⇒ 用 builder 每次现拼。
    cfg.ping_mode                = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_builder      = []() {
        return fmt::format(R"({{"op":"ping","ping":{}}})", crypto::getCurrentTime() / 1000000);
    };
    cfg.idle_timeout_sec      = 90;   // 20 s 一 ping，连续 90 s 一个 frame 都没有才算死
    cfg.data_idle_timeout_sec = 0;    // 稀疏品种（新上币）也会几分钟没成交，不按 data 判活
}

void md::BitunixUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("BITUNIX {} FUNDING_RATE uses REST polling, no ws connection",
                 InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum != USDT_SWAP) {
        LOG_ERROR("BITUNIX only supports USDT_SWAP in this impl, got: {}",
                  InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    cfg.url = BITUNIX_WS_PUBLIC;

    const char* ch = nullptr;
    if (marketTypeEnum == md::DEPTH1) {
        ch = "depth_book1";
    }
    else if (marketTypeEnum == md::TRADES) {
        ch = "trade";
    }
    else {
        LOG_ERROR("BITUNIX {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                  md::MarketTypeEnum2StrMap[marketTypeEnum]);
        return;
    }

    // 交易所限制：一个连接 300 个频道、每秒 5 条消息 ⇒ 必须一条消息带全部 symbol。
    // 这里再按 100 个一组切，纯粹是留余量（tokenLot 默认 30，正常只有一条）。
    constexpr size_t kPerMsg = 100;
    cfg.subscribe_messages.clear();
    std::string args;
    size_t      n = 0;
    for (auto& info : vInstInfo) {
        if (!args.empty()) {
            args += ',';
        }
        args += fmt::format(R"({{"symbol":"{}","ch":"{}"}})", info.originInstId, ch);
        if (++n % kPerMsg == 0) {
            cfg.subscribe_messages.push_back(fmt::format(R"({{"op":"subscribe","args":[{}]}})", args));
            args.clear();
        }
    }
    if (!args.empty()) {
        cfg.subscribe_messages.push_back(fmt::format(R"({{"op":"subscribe","args":[{}]}})", args));
    }

    LOG_INFO("BITUNIX ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::BitunixUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // 过滤：订阅回执 {"op":"connect"...}、心跳回执 {"op":"ping","pong":...} 都没有 "ch":"。
    // 业务帧的第一个键就是 "ch"，所以这个判断对 30 个 symbol 的满负荷流也是最省的。
    if (sv.find("\"ch\":\"") == std::string_view::npos) {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::BitunixUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("BITUNIX simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // ⚠️ 键顺序（实测，simdjson ondemand 前向单向，**顺序不能动**）：
    //   depth: {ch, symbol, ts, data{b, a}}      ⇒ **b 在 a 之前**
    //   trade: {ch, symbol, ts, data[{t, p, v, s}]} ⇒ t → p → v → s
    // 把 a 写在 b 前面、或先取 data 再回头取 symbol，都会让后面的字段静默取空。

    std::string_view ch;
    if (doc["ch"].get(ch) != simdjson::SUCCESS) {
        return;
    }

    std::string_view symbol;
    if (doc["symbol"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    int64_t tsMs = 0;
    doc["ts"].get(tsMs);

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(symbol), info, key)) {
        return;
    }

    if (marketTypeEnum == md::DEPTH1 && ch == "depth_book1") {
        auto data = doc["data"];

        double bp = 0, bv = 0, ap = 0, av = 0;

        for (auto lv : data["b"]) {          // b 在前
            auto             it = lv.begin();
            std::string_view p, q;
            if ((*it).get(p) != simdjson::SUCCESS) break;
            ++it;
            if ((*it).get(q) != simdjson::SUCCESS) break;
            bp = crypto::fast_atod(p);
            bv = crypto::fast_atod(q);
            break;
        }
        for (auto lv : data["a"]) {          // a 在后
            auto             it = lv.begin();
            std::string_view p, q;
            if ((*it).get(p) != simdjson::SUCCESS) break;
            ++it;
            if ((*it).get(q) != simdjson::SUCCESS) break;
            ap = crypto::fast_atod(p);
            av = crypto::fast_atod(q);
            break;
        }

        publishDepth1(info, key, bp, bv, ap, av, tsMs, tsNet);
    }
    else if (marketTypeEnum == md::TRADES && ch == "trade") {
        for (auto item : doc["data"]) {
            std::string_view t, p, v, s;
            if (item["t"].get(t) != simdjson::SUCCESS) continue;
            if (item["p"].get(p) != simdjson::SUCCESS) continue;
            if (item["v"].get(v) != simdjson::SUCCESS) continue;
            if (item["s"].get(s) != simdjson::SUCCESS) continue;

            // s: "buy" 主动买 / "sell" 主动卖
            const Direction dir = (s == "sell") ? DT_SHORT : DT_LONG;

            // 没有 tradeId ⇒ 用 ISO 时间戳当 id（20 字符，tradeId 有 32 字节）
            char idBuf[32] = {0};
            const size_t n = t.size() < sizeof(idBuf) - 1 ? t.size() : sizeof(idBuf) - 1;
            std::memcpy(idBuf, t.data(), n);

            publishTrade(info, key, crypto::fast_atod(p), crypto::fast_atod(v), dir, idBuf,
                         iso_utc_to_ms(t), tsNet);
        }
    }
}

void md::BitunixUnit::publishDepth1(md::InstrumentInfo& info, const std::string& key, double bp, double bv,
                                    double ap, double av, int64_t tsMs, int64_t tsNet) {
    if (!(bp > 0) || !(ap > 0)) {
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

void md::BitunixUnit::publishTrade(md::InstrumentInfo& info, const std::string& key, double px, double sz,
                                   Direction dir, const char* tradeId, int64_t tsMs, int64_t tsNet) {
    if (!(px > 0) || !(sz > 0)) {
        return;
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
    std::strncpy(t.tradeId, tradeId, sizeof(t.tradeId) - 1);

    t.direction = dir;
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

bool md::BitunixUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    const std::string host   = "fapi.bitunix.com";
    const std::string target = fmt::format("/api/v1/futures/market/funding_rate?symbol={}", info.originInstId);

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_BITUNIX_FUNDING", host, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("BITUNIX funding http status {} for {}", status, info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("BITUNIX funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    int64_t code = -1;
    doc["code"].get(code);
    if (code != 0) {
        LOG_WARN("BITUNIX funding code {} for {}, body: {}", code, info.originInstId, body);
        return false;
    }

    auto             data = doc["data"];
    std::string_view rateStr;
    if (data["fundingRate"].get(rateStr) != simdjson::SUCCESS) {
        return false;
    }
    fr.fundingRate = crypto::fast_atod(rateStr);

    // nextFundingTime 是**字符串**毫秒
    std::string_view nextStr;
    if (data["nextFundingTime"].get(nextStr) == simdjson::SUCCESS) {
        fr.fundingTime = crypto::fast_atol(nextStr);
    }

    fr.nextFundingRate = fr.fundingRate;
    fr.tsTrans         = crypto::getCurrentTime();
    return true;
}

void md::BitunixUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::BitunixMarket::BitunixMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                                 std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec,
                                 int lot, const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::BitunixUnit* unit = new md::BitunixUnit(smc, info.exchangeTypeEnum, info.instTypeEnum,
                                                    info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        unitVec.push_back(unit);
    }
}

md::BitunixMarket::~BitunixMarket() {}

void md::BitunixMarket::start() {
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
