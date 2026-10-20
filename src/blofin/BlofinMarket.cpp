#include "blofin/BlofinMarket.h"

#include <cstdio>
#include <cstring>

#include "Net.h"

md::BlofinUnit::BlofinUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                           std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port,
                           const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // OKX 系的心跳是明文 "ping" → 明文 "pong"。BloFin 未实测，按同族处理。
    cfg.ping_mode                = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = "ping";
    cfg.idle_timeout_sec         = 90;
    cfg.data_idle_timeout_sec    = 0;
}

void md::BlofinUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("BLOFIN {} FUNDING_RATE uses REST polling, no ws connection",
                 InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum != USDT_SWAP) {
        LOG_ERROR("BLOFIN only supports USDT_SWAP in this impl, got: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    cfg.url = BLOFIN_WS_PUBLIC;

    const char* channel = nullptr;
    if (marketTypeEnum == md::DEPTH1) {
        channel = "books1";
    }
    else if (marketTypeEnum == md::TRADES) {
        channel = "trades";
    }
    else {
        LOG_ERROR("BLOFIN {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                  md::MarketTypeEnum2StrMap[marketTypeEnum]);
        return;
    }

    // OKX 系一个 args 里可以放多个频道；按 50 个一组切，避免单帧过大。
    constexpr size_t kPerMsg = 50;
    cfg.subscribe_messages.clear();
    std::string args;
    size_t      n = 0;
    for (auto& info : vInstInfo) {
        if (!args.empty()) {
            args += ',';
        }
        args += fmt::format(R"({{"channel":"{}","instId":"{}"}})", channel, info.originInstId);
        if (++n % kPerMsg == 0) {
            cfg.subscribe_messages.push_back(fmt::format(R"({{"op":"subscribe","args":[{}]}})", args));
            args.clear();
        }
    }
    if (!args.empty()) {
        cfg.subscribe_messages.push_back(fmt::format(R"({{"op":"subscribe","args":[{}]}})", args));
    }

    LOG_INFO("BLOFIN ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::BlofinUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // 订阅回执 {"event":"subscribe",...} 和 "pong" 都没有 "data"；业务帧第一个键是 "arg"。
    if (sv.find("\"arg\"") == std::string_view::npos) {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::BlofinUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto                    doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("BLOFIN simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    // ⚠️ 键顺序（**未实测**，按 OKX v5）：{arg{channel, instId}, data[{...}]}
    //   若部署机上这里恒为空，第一件事就是用 /tmp 的探针抓一帧真实报文核对顺序。

    auto arg = doc["arg"];

    std::string_view channel;
    if (arg["channel"].get(channel) != simdjson::SUCCESS) {
        return;
    }

    std::string_view instId;
    if (arg["instId"].get(instId) != simdjson::SUCCESS) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(instId), info, key)) {
        return;
    }

    if (marketTypeEnum == md::DEPTH1 && channel == "books1") {
        for (auto item : doc["data"]) {
            double bp = 0, bv = 0, ap = 0, av = 0;

            for (auto lv : item["asks"]) {          // OKX: asks 在 bids 之前
                auto             it = lv.begin();
                std::string_view p, q;
                if ((*it).get(p) != simdjson::SUCCESS) break;
                ++it;
                if ((*it).get(q) != simdjson::SUCCESS) break;
                ap = crypto::fast_atod(p);
                av = crypto::fast_atod(q);
                break;
            }
            for (auto lv : item["bids"]) {
                auto             it = lv.begin();
                std::string_view p, q;
                if ((*it).get(p) != simdjson::SUCCESS) break;
                ++it;
                if ((*it).get(q) != simdjson::SUCCESS) break;
                bp = crypto::fast_atod(p);
                bv = crypto::fast_atod(q);
                break;
            }

            if (!(bp > 0) || !(ap > 0)) {
                continue;
            }

            int64_t tsMs = 0;                        // ts 是**字符串**毫秒
            std::string_view tsStr;
            if (item["ts"].get(tsStr) == simdjson::SUCCESS) {
                tsMs = crypto::fast_atol(tsStr);
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
                continue;
            }

            d1.tsTrans = tsMs * 1000;
            d1.tsEvent = tsMs * 1000;
            d1.tsRecv  = tsNet;
            d1.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
            mDepth1Publisher[key]->push(d1);
#endif
            break;                                   // books1 只关心最优一档
        }
    }
    else if (marketTypeEnum == md::TRADES && channel == "trades") {
        for (auto item : doc["data"]) {
            std::string_view tradeId, pxStr, szStr, side, tsStr;
            if (item["tradeId"].get(tradeId) != simdjson::SUCCESS) continue;
            if (item["px"].get(pxStr) != simdjson::SUCCESS) continue;
            if (item["sz"].get(szStr) != simdjson::SUCCESS) continue;
            if (item["side"].get(side) != simdjson::SUCCESS) continue;
            item["ts"].get(tsStr);

            const double px = crypto::fast_atod(pxStr);
            const double sz = crypto::fast_atod(szStr);
            if (!(px > 0) || !(sz > 0)) {
                continue;
            }

            int64_t tsMs = crypto::fast_atol(tsStr);
            if (tsMs <= 0) {
                tsMs = crypto::getCurrentTime() / 1000;
            }

            md::Trades t;
            std::memset(&t, 0, sizeof(t));
            t.exchangeTypeEnum = exchangeTypeEnum;
            t.instTypeEnum     = instTypeEnum;
            t.marketTypeEnum   = marketTypeEnum;
            std::strncpy(t.instId, info.instId, INSTID_SIZE);
            std::strncpy(t.tradeId, std::string(tradeId).c_str(), sizeof(t.tradeId) - 1);

            t.direction = (side == "sell") ? DT_SHORT : DT_LONG;
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
}

bool md::BlofinUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    const std::string host   = "openapi.blofin.com";
    const std::string target = fmt::format("/api/v5/market/funding-rate?instId={}", info.originInstId);

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_BLOFIN_FUNDING", host, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("BLOFIN funding http status {} for {} (451 = 地区限制，需境外 IP)", status,
                 info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("BLOFIN funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    // OKX 系的 code 是**字符串** "0"
    std::string_view code;
    if (doc["code"].get(code) != simdjson::SUCCESS || code != "0") {
        LOG_WARN("BLOFIN funding code {} for {}, body: {}", std::string(code), info.originInstId, body);
        return false;
    }

    for (auto row : doc["data"]) {
        std::string_view rateStr;
        if (row["fundingRate"].get(rateStr) != simdjson::SUCCESS) {
            return false;
        }
        fr.fundingRate = crypto::fast_atod(rateStr);

        std::string_view nextRateStr;
        if (row["nextFundingRate"].get(nextRateStr) == simdjson::SUCCESS) {
            fr.nextFundingRate = crypto::fast_atod(nextRateStr);
        }
        else {
            fr.nextFundingRate = fr.fundingRate;
        }

        // nextFundingTime 是字符串毫秒；取不到就退回"当前 + 8 h"
        std::string_view nextTimeStr;
        if (row["nextFundingTime"].get(nextTimeStr) == simdjson::SUCCESS) {
            fr.fundingTime = crypto::fast_atol(nextTimeStr);
        }
        else {
            std::string_view curTimeStr;
            const int64_t    cur =
                (row["fundingTime"].get(curTimeStr) == simdjson::SUCCESS) ? crypto::fast_atol(curTimeStr) : 0;
            fr.fundingTime = cur > 0 ? cur + 8 * 3600 * 1000LL : 0;
        }

        fr.tsTrans = crypto::getCurrentTime();
        return true;
    }
    return false;
}

void md::BlofinUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::BlofinMarket::BlofinMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                               std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec,
                               int lot, const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::BlofinUnit* unit = new md::BlofinUnit(smc, info.exchangeTypeEnum, info.instTypeEnum,
                                                  info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        unitVec.push_back(unit);
    }
}

md::BlofinMarket::~BlofinMarket() {}

void md::BlofinMarket::start() {
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
