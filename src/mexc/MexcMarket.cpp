#include "mexc/MexcMarket.h"

#include <cstdio>
#include <cstring>

#include "Net.h"

md::MexcUnit::MexcUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                       std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // MEXC 客户端每 20s 发 {"method":"ping"}，服务端回 {"channel":"pong"}
    cfg.ping_mode                = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = R"({"method":"ping"})";
    cfg.idle_timeout_sec         = 60;
    cfg.data_idle_timeout_sec    = 0;
}

void md::MexcUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("MEXC {} FUNDING_RATE uses REST polling, no ws connection", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum != USDT_SWAP) {
        LOG_ERROR("MEXC only supports USDT_SWAP, got: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    cfg.url = MEXC_WS_CONTRACT;

    cfg.subscribe_messages.clear();
    for (auto& info : vInstInfo) {
        if (marketTypeEnum == md::DEPTH1) {
            // limit=5 而不是 limit=1：MEXC 的最小档位是 5，多 4 档的带宽可以忽略，
            // 换来的是"每帧都是完整快照"，不需要维护增量簿。
            cfg.subscribe_messages.push_back(
                fmt::format(R"({{"method":"sub.depth.full","param":{{"symbol":"{}","limit":5}}}})", info.originInstId));
        }
        else if (marketTypeEnum == md::TRADES) {
            cfg.subscribe_messages.push_back(
                fmt::format(R"({{"method":"sub.deal","param":{{"symbol":"{}"}}}})", info.originInstId));
        }
        else {
            LOG_ERROR("MEXC {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                      md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }
    }

    LOG_INFO("MEXC ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::MexcUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string_view sv(reinterpret_cast<const char*>(data), len);

    // 只收 push.*；rs.* 回执与 {"channel":"pong"} 丢掉
    if (sv.find("\"push.") == std::string_view::npos) {
        return;
    }

    pushRaw(reinterpret_cast<const char*>(data), len, crypto::getCurrentTime());
}

void md::MexcUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("MEXC simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    std::string_view symbol;
    if (doc["symbol"].get(symbol) != simdjson::SUCCESS) {
        return;
    }

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(std::string(symbol), info, key)) {
        return;
    }

    // ⚠️⚠️ 顶层键顺序（实测）：symbol, data, channel, ts —— **ts 排在 data 之后**。
    //    simdjson ondemand 前向单向：先读 ts 会把游标推过 data，之后 data 取不到（静默零数据）。
    //    这里不用顶层 ts，用 data 内部的时间戳（depth 的 cts / deal 的 t），口径也更准。
    auto data = doc["data"];

    if (marketTypeEnum == md::DEPTH1) {
        // data = {cts, asks:[[p,v,cnt]], bids:[[p,v,cnt]], version}
        // ⚠️ **asks 在 bids 之前**（实测），写反了 bids 取不到 ⇒ 深度恒为空。
        int64_t cts = 0;
        data["cts"].get(cts);

        double bp = 0, bv = 0, ap = 0, av = 0;
        bool   gotBid = false, gotAsk = false;

        for (auto lv : data["asks"]) {
            auto it = lv.begin();
            if ((*it).get(ap) != simdjson::SUCCESS) break;      // 数字，不是字符串
            ++it;
            if ((*it).get(av) != simdjson::SUCCESS) break;
            gotAsk = true;                                      // 第 3 个元素是挂单笔数，不需要
            break;
        }
        for (auto lv : data["bids"]) {
            auto it = lv.begin();
            if ((*it).get(bp) != simdjson::SUCCESS) break;
            ++it;
            if ((*it).get(bv) != simdjson::SUCCESS) break;
            gotBid = true;
            break;
        }
        if (!gotBid || !gotAsk) {
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

        const int64_t evt = (cts > 0) ? cts : crypto::getCurrentTime() / 1000;
        d1.tsTrans = evt * 1000;
        d1.tsEvent = evt * 1000;
        d1.tsRecv  = tsNet;
        d1.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
        mDepth1Publisher[key]->push(d1);
#endif
    }
    else if (marketTypeEnum == md::TRADES) {
        // data = [{p, v, T, O, M, t, i, cts}]，一批可能十几笔
        // ⚠️ 元素键顺序固定为 p → v → T → t → i，**不能按"读起来顺"重排**。
        // ⚠️ 注意 cts 在 deal 里是**字符串**（"1792228181340"），在 depth 里才是数字 ——
        //    对字符串调 get(int64_t&) 会返回 INCORRECT_TYPE 且静默得 0，所以这里不用 cts，
        //    统一用数字字段 t（实测与 cts 同值）。
        for (auto d : data) {
            double px = 0;
            if (d["p"].get(px) != simdjson::SUCCESS) continue;
            double vol = 0;
            if (d["v"].get(vol) != simdjson::SUCCESS) continue;

            int64_t takerDir = 0;                   // T: 1 = 主动买, 2 = 主动卖
            d["T"].get(takerDir);

            int64_t tradeTs = 0;                    // t 是数字 ms
            d["t"].get(tradeTs);
            if (tradeTs <= 0) {
                tradeTs = crypto::getCurrentTime() / 1000;
            }

            std::string_view idStr;
            if (d["i"].get(idStr) != simdjson::SUCCESS) {
                idStr = std::string_view();
            }

            md::Trades t;
            std::memset(&t, 0, sizeof(t));
            t.exchangeTypeEnum = exchangeTypeEnum;
            t.instTypeEnum     = instTypeEnum;
            t.marketTypeEnum   = marketTypeEnum;
            std::strncpy(t.instId, info.instId, INSTID_SIZE);

            if (!idStr.empty()) {
                std::strncpy(t.tradeId, idStr.data(), std::min<size_t>(idStr.size(), sizeof(t.tradeId) - 1));
            }
            t.direction = (takerDir == 2) ? DT_SHORT : DT_LONG;

            t.px = px * info.reduceNumber;
            t.sz = vol * info.value * info.magnifyNumber;

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

bool md::MexcUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    const std::string host   = "contract.mexc.com";
    const std::string target = fmt::format("/api/v1/contract/funding_rate/{}", info.originInstId);

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_MEXC_FUNDING", host, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("MEXC funding http status {} for {}", status, info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("MEXC funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    bool ok = false;
    doc["success"].get(ok);
    if (!ok) {
        LOG_WARN("MEXC funding success=false, body: {}", body);
        return false;
    }

    auto data = doc["data"];
    if (data["fundingRate"].get(fr.fundingRate) != simdjson::SUCCESS) {
        return false;
    }
    int64_t nextSettle = 0;
    if (data["nextSettleTime"].get(nextSettle) == simdjson::SUCCESS) {
        fr.fundingTime = nextSettle * 1000;
    }
    fr.nextFundingRate = fr.fundingRate;
    fr.tsTrans         = crypto::getCurrentTime();
    return true;
}

void md::MexcUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::MexcMarket::MexcMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                           std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot,
                           const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::MexcUnit* unit = new md::MexcUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum,
                                              info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        mexcUnitVec.push_back(unit);
    }
}

md::MexcMarket::~MexcMarket() {}

void md::MexcMarket::start() {
    for (size_t i = 0; i < mexcUnitVec.size(); ++i) {
        if (mexcUnitVec[i]->isRestOnly()) {
            mexcUnitVec[i]->startRestOnly();
        }
        else {
            mexcUnitVec[i]->start();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}
