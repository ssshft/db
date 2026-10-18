#include "bingx/BingxMarket.h"

#include <cstdio>
#include <cstring>

#include "Net.h"
#include "base/GzipUtil.h"

md::BingxUnit::BingxUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                         std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port,
                         const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    // BingX 的心跳是**服务端发文本 "Ping"、客户端回文本 "Pong"**，不是客户端周期 ping。
    // 所以这里 Disabled，真正的心跳在 onWebsocketMsg 里处理。
    cfg.ping_mode             = net::WsConfig::PingMode::Disabled;
    cfg.idle_timeout_sec      = 60;
    cfg.data_idle_timeout_sec = 0;
}

void md::BingxUnit::generateSubBody() {
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("BINGX {} FUNDING_RATE uses REST polling, no ws connection", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum != USDT_SWAP) {
        LOG_ERROR("BINGX only supports USDT_SWAP in this impl, got: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    cfg.url = BINGX_WS_SWAP;

    cfg.subscribe_messages.clear();
    for (auto& info : vInstInfo) {
        std::string dataType;
        if (marketTypeEnum == md::DEPTH1) {
            dataType = fmt::format("{}@depth5", info.originInstId);   // 没有 @depth1，最小是 depth5
        }
        else if (marketTypeEnum == md::TRADES) {
            dataType = fmt::format("{}@trade", info.originInstId);
        }
        else {
            LOG_ERROR("BINGX {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                      md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }

        cfg.subscribe_messages.push_back(
            fmt::format(R"({{"id":"s{}","reqType":"sub","dataType":"{}"}})", ++subId, dataType));
    }

    LOG_INFO("BINGX ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::BingxUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    std::string raw;
    if (isBinary) {
        if (!mdutil::gunzip(data, len, raw)) {
            raw.assign(reinterpret_cast<const char*>(data), len);
        }
    }
    else {
        raw.assign(reinterpret_cast<const char*>(data), len);
    }

    // 应用层心跳: 服务端 "Ping" → 回 "Pong"（必须，否则 ~30s 断开）
    if (raw == "Ping") {
        if (pWsClient) {
            pWsClient->send_text("Pong");
        }
        return;
    }
    if (raw == "Pong") {
        return;
    }

    // 订阅回执是 {"id":"s1","code":0,"msg":"","dataType":"","data":null}
    // 真实推送的 dataType 一定形如 "BTC-USDT@depth5"（含 '@'）
    // ⚠️ 不要用 `"dataType":""` 这种**连空白都写死**的串去匹配：一旦服务端改成
    //    `"dataType" : ""`，过滤失效，回执全被塞进 ring（挤掉真实行情）。
    //    判"有没有 '@'" 与序列化格式无关。
    if (raw.find("\"dataType\"") == std::string::npos) {
        return;
    }
    if (raw.find('@') == std::string::npos) {
        return;
    }

    pushRaw(raw.data(), raw.size(), crypto::getCurrentTime());
}

void md::BingxUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("BINGX simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    std::string_view dataType;
    if (doc["dataType"].get(dataType) != simdjson::SUCCESS) {
        return;
    }
    const size_t at = dataType.find('@');
    if (at == std::string_view::npos) {
        return;
    }
    const std::string originInstId(dataType.substr(0, at));      // "BTC-USDT"

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(originInstId, info, key)) {
        return;
    }

    // ⚠️⚠️ 顶层键顺序（实测）：
    //   depth 帧 : code, dataType, ts, data     （ts 在 data **之前**）
    //   trade 帧 : code, dataType, data         （**根本没有 ts 字段**）
    //   simdjson ondemand 前向单向：trade 帧里若先探 ts，会一直扫到文档末尾并失败，
    //   游标停在末尾 ⇒ 紧接着的 data 也取不到 ⇒ 成交静默全丢（不报错）。
    //   所以两条通道的读取顺序**必须分开写**，不能共用一段"先读 ts"。

    if (marketTypeEnum == md::DEPTH1) {
        int64_t tsMs = 0;                                        // 顶层 ts 是数字 ms
        doc["ts"].get(tsMs);
        if (tsMs <= 0) {
            tsMs = crypto::getCurrentTime() / 1000;
        }

        auto data = doc["data"];
        // data = {"bids":[["p","v"]...],"asks":[[..]]}（实测 bids 在前，顺序正确）
        double bp = 0, bv = 0, ap = 0, av = 0;
        bool   gotBid = false, gotAsk = false;

        for (auto lv : data["bids"]) {
            auto it = lv.begin();
            std::string_view p, v;
            if ((*it).get(p) != simdjson::SUCCESS) break;
            ++it;
            if ((*it).get(v) != simdjson::SUCCESS) break;
            bp     = crypto::fast_atod(p);
            bv     = crypto::fast_atod(v);
            gotBid = true;
            break;
        }
        for (auto lv : data["asks"]) {
            auto it = lv.begin();
            std::string_view p, v;
            if ((*it).get(p) != simdjson::SUCCESS) break;
            ++it;
            if ((*it).get(v) != simdjson::SUCCESS) break;
            ap     = crypto::fast_atod(p);
            av     = crypto::fast_atod(v);
            gotAsk = true;
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

        d1.tsTrans = tsMs * 1000;
        d1.tsEvent = tsMs * 1000;
        d1.tsRecv  = tsNet;
        d1.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
        mDepth1Publisher[key]->push(d1);
#endif
    }
    else if (marketTypeEnum == md::TRADES) {
        auto data = doc["data"];                                 // data 是 trade 帧的**最后一个**键
        // data[] 元素键顺序（实测）：q, p, T, m, s  ⇒ **q 必须最先读**，p 在后。
        for (auto d : data) {
            std::string_view qtyStr;
            if (d["q"].get(qtyStr) != simdjson::SUCCESS) continue;

            std::string_view priceStr;
            if (d["p"].get(priceStr) != simdjson::SUCCESS) continue;

            int64_t tradeTs = 0;                                 // T 是毫秒数字
            d["T"].get(tradeTs);
            if (tradeTs <= 0) {
                tradeTs = crypto::getCurrentTime() / 1000;
            }

            // m = isBuyerMaker: true ⇒ 买方是挂单方 ⇒ 主动方是**卖方**
            bool buyerIsMaker = false;
            d["m"].get(buyerIsMaker);

            md::Trades t;
            std::memset(&t, 0, sizeof(t));
            t.exchangeTypeEnum = exchangeTypeEnum;
            t.instTypeEnum     = instTypeEnum;
            t.marketTypeEnum   = marketTypeEnum;
            std::strncpy(t.instId, info.instId, INSTID_SIZE);

            // BingX 不给 tradeId，用成交时间戳兜底（只用于去重展示，不参与撮合）
            std::snprintf(t.tradeId, sizeof(t.tradeId), "%lld", static_cast<long long>(tradeTs));

            t.direction = buyerIsMaker ? DT_SHORT : DT_LONG;

            t.px = crypto::fast_atod(priceStr) * info.reduceNumber;
            t.sz = crypto::fast_atod(qtyStr) * info.value * info.magnifyNumber;

            t.tsTrans = tradeTs * 1000;
            t.tsEvent = tradeTs * 1000;      // 没有整帧 ts，用成交自身时间
            t.tsRecv  = tsNet;
            t.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
            mTradesPublisher[key]->push(t);
#endif
        }
    }
}

bool md::BingxUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    // ⚠️ host 用 open-api.bingx.com；open-api-swap.bingx.com 对这条路径回 403 (CloudFront)
    const std::string host   = "open-api.bingx.com";
    const std::string target = fmt::format("/openApi/swap/v2/quote/premiumIndex?symbol={}", info.originInstId);

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_BINGX_FUNDING", host, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("BINGX funding http status {} for {}", status, info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("BINGX funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    int64_t code = -1;
    doc["code"].get(code);
    if (code != 0) {
        LOG_WARN("BINGX funding code {}, body: {}", code, body);
        return false;
    }

    auto             data = doc["data"];
    std::string_view rateStr;
    if (data["lastFundingRate"].get(rateStr) != simdjson::SUCCESS) {
        return false;
    }
    fr.fundingRate = crypto::fast_atod(rateStr);

    int64_t nextFundingTime = 0;
    if (data["nextFundingTime"].get(nextFundingTime) == simdjson::SUCCESS) {
        fr.fundingTime = nextFundingTime * 1000;
    }
    fr.nextFundingRate = fr.fundingRate;
    fr.tsTrans         = crypto::getCurrentTime();
    return true;
}

void md::BingxUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::BingxMarket::BingxMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                             std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec,
                             int lot, const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::BingxUnit* unit = new md::BingxUnit(smc, info.exchangeTypeEnum, info.instTypeEnum,
                                                info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        bingxUnitVec.push_back(unit);
    }
}

md::BingxMarket::~BingxMarket() {}

void md::BingxMarket::start() {
    for (size_t i = 0; i < bingxUnitVec.size(); ++i) {
        if (bingxUnitVec[i]->isRestOnly()) {
            bingxUnitVec[i]->startRestOnly();
        }
        else {
            bingxUnitVec[i]->start();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}
