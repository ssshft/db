#include "bitget/BitgetMarket.h"

#include <cstdio>
#include <cstring>

#include "Net.h"

md::BitgetUnit::BitgetUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                           std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port,
                           const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // Bitget 要求客户端每 20s 发一次 {"op":"ping"}，服务端回 {"op":"pong"}
    cfg.ping_mode               = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text        = R"({"op":"ping"})";
    cfg.idle_timeout_sec        = 60;
    cfg.data_idle_timeout_sec   = 0;
}

void md::BitgetUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("BITGET {} FUNDING_RATE uses REST polling, no ws connection", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum == USDT_SWAP) {
        mProductType = "USDT-FUTURES";
    }
    else if (instTypeEnum == USDC_SWAP) {
        mProductType = "USDC-FUTURES";
    }
    else if (instTypeEnum == C_SWAP) {
        mProductType = "COIN-FUTURES";
    }
    else {
        LOG_ERROR("BITGET does not support instType: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    cfg.url = BITGET_WS_PUBLIC;

    std::string channel;
    if (marketTypeEnum == md::DEPTH1) {
        channel = "books1";        // 一档快照，每帧都是完整的 1 档
    }
    else if (marketTypeEnum == md::TRADES) {
        channel = "trade";
    }
    else {
        LOG_ERROR("BITGET {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                  md::MarketTypeEnum2StrMap[marketTypeEnum]);
        return;
    }

    std::string args;
    for (auto& info : vInstInfo) {
        if (!args.empty()) {
            args += ",";
        }
        args += fmt::format(R"({{"instType":"{}","channel":"{}","instId":"{}"}})",
                            mProductType, channel, info.originInstId);
    }

    cfg.subscribe_messages.clear();
    cfg.subscribe_messages.push_back(fmt::format(R"({{"op":"subscribe","args":[{}]}})", args));

    LOG_INFO("BITGET ws url: {}, sub: {}", cfg.url, cfg.subscribe_messages.front());
}

void md::BitgetUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // {"op":"pong"} / {"event":"subscribe",...} / {"event":"error",...} 都没有 "data" 字段
    if (sv.find("\"data\"") == std::string_view::npos) {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::BitgetUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("BITGET simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    auto             arg = doc["arg"];
    std::string_view instId;
    if (arg["instId"].get(instId) != simdjson::SUCCESS) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(instId), info, key)) {
        return;
    }

    // ⚠️⚠️ 顶层键顺序（实测）：action, arg, data, ts —— **ts 排在 data 之后**。
    //    simdjson ondemand 是前向单向迭代器，先读 ts 会把游标推过 data，data 之后再也取不到
    //    （不报错，静默零数据）。所以这里**不用顶层 ts**：depth 用元素自带的 ts，
    //    trade 用每笔成交的 ts，语义上也更准。
    auto data = doc["data"];

    if (marketTypeEnum == md::DEPTH1) {
        // data[] 元素键顺序（实测）：asks, bids, ts, seq, pseq
        // ⇒ **asks 必须写在 bids 前面**，顺序反了 bids 取不到，深度恒为空。
        double  bp = 0, bv = 0, ap = 0, av = 0;
        bool    gotBid = false, gotAsk = false;
        int64_t tsMs   = 0;

        for (auto d : data) {
            for (auto lv : d["asks"]) {
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
            for (auto lv : d["bids"]) {
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
            std::string_view tsStr;                 // 元素 ts 是**字符串** ms（顶层才是数字）
            if (d["ts"].get(tsStr) == simdjson::SUCCESS) {
                tsMs = crypto::fast_atol(tsStr);
            }
            break;                          // books1 只有一档，取第一个元素即可
        }

        if (!gotBid || !gotAsk) {
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
    else if (marketTypeEnum == md::TRADES) {
        // data[] 元素键顺序（实测）：ts, price, size, side, tradeId
        // ⚠️ ts 是**第一个**字段且是字符串 ⇒ 必须最先读，读晚了整条都拿不到。
        for (auto d : data) {
            std::string_view tsStr;
            if (d["ts"].get(tsStr) != simdjson::SUCCESS) continue;
            int64_t tradeTs = crypto::fast_atol(tsStr);

            std::string_view priceStr;
            if (d["price"].get(priceStr) != simdjson::SUCCESS) continue;

            std::string_view sizeStr;
            if (d["size"].get(sizeStr) != simdjson::SUCCESS) continue;

            // Bitget trade 的 side 是 **taker 方向**: "sell" = 主动卖
            std::string_view sideStr;
            const bool isSell = (d["side"].get(sideStr) == simdjson::SUCCESS && sideStr == "sell");

            std::string_view tradeIdStr;
            if (d["tradeId"].get(tradeIdStr) != simdjson::SUCCESS) {
                tradeIdStr = std::string_view();
            }

            if (tradeTs <= 0) {
                tradeTs = crypto::getCurrentTime() / 1000;
            }

            md::Trades t;
            std::memset(&t, 0, sizeof(t));
            t.exchangeTypeEnum = exchangeTypeEnum;
            t.instTypeEnum     = instTypeEnum;
            t.marketTypeEnum   = marketTypeEnum;
            std::strncpy(t.instId, info.instId, INSTID_SIZE);

            if (!tradeIdStr.empty()) {
                std::strncpy(t.tradeId, tradeIdStr.data(),
                             std::min<size_t>(tradeIdStr.size(), sizeof(t.tradeId) - 1));
            }
            t.direction = isSell ? DT_SHORT : DT_LONG;

            t.px = crypto::fast_atod(priceStr) * info.reduceNumber;
            t.sz = crypto::fast_atod(sizeStr) * info.value * info.magnifyNumber;

            t.tsTrans = tradeTs * 1000;
            t.tsEvent = tradeTs * 1000;
            t.tsRecv  = tsNet;
            t.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
            mTradesPublisher[key]->push(t);
#endif
        }
    }
}

bool md::BitgetUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    const std::string host   = "api.bitget.com";
    const std::string target = fmt::format("/api/v2/mix/market/current-fund-rate?symbol={}&productType={}",
                                           info.originInstId, crypto::to_lower(mProductType));

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_BITGET_FUNDING", host, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("BITGET funding http status {} for {}", status, info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("BITGET funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    // ⚠️ 别用 get() 的返回值判成功 —— 仓库里 simdjson 版本不一，按**值**判更稳
    std::string_view code = "?";
    doc["code"].get(code);
    if (code != "00000") {
        LOG_WARN("BITGET funding code {} body: {}", std::string(code), body);
        return false;
    }

    for (auto d : doc["data"]) {
        std::string_view rateStr;
        if (d["fundingRate"].get(rateStr) != simdjson::SUCCESS) continue;
        fr.fundingRate = crypto::fast_atod(rateStr);

        std::string_view nextStr;       // 这个接口里字段叫 nextUpdate（不是 nextFundingTime）
        if (d["nextUpdate"].get(nextStr) == simdjson::SUCCESS) {
            fr.fundingTime = crypto::fast_atol(nextStr) * 1000;
        }
        fr.nextFundingRate = fr.fundingRate;
        fr.tsTrans         = crypto::getCurrentTime();
        return true;
    }
    return false;
}

void md::BitgetUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::BitgetMarket::BitgetMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                               std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec,
                               int lot, const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::BitgetUnit* unit = new md::BitgetUnit(smc, info.exchangeTypeEnum, info.instTypeEnum,
                                                  info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        bitgetUnitVec.push_back(unit);
    }
}

md::BitgetMarket::~BitgetMarket() {}

void md::BitgetMarket::start() {
    for (size_t i = 0; i < bitgetUnitVec.size(); ++i) {
        if (bitgetUnitVec[i]->isRestOnly()) {
            bitgetUnitVec[i]->startRestOnly();
        }
        else {
            bitgetUnitVec[i]->start();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}
