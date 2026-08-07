// GateioSbeMarket.cpp
//
// Gate.io SBE (期货 + 现货) 行情客户端实现, 单一 GateioSbeUnit 内部按 instType 分流。
//
// 消息分派:
//   opcode=2 binary → parseMarketData → parseSpotData / parseFuturesData → 各自 switch(tid)
//   opcode=1 text   → 订阅 ack / spot.pong / futures.pong / 系统通知, 打 log 或过滤

#include "gateio/GateioSbeMarket.h"



// ============================================================================
// 内部工具 (匿名 namespace)
// ============================================================================

// ---- 深度快照填充: 泛型模板, spot/futures 的 OrderBookIter 接口一致 ----------

template <typename Iter>
inline void fill_depth5(md::Depth5& d, const Iter& iter, int8_t pxExp, int8_t szExp, const md::InstrumentInfo& info) {
    double* bpArr[5] = {&d.bp1, &d.bp2, &d.bp3, &d.bp4, &d.bp5};
    double* bvArr[5] = {&d.bv1, &d.bv2, &d.bv3, &d.bv4, &d.bv5};
    double* apArr[5] = {&d.ap1, &d.ap2, &d.ap3, &d.ap4, &d.ap5};
    double* avArr[5] = {&d.av1, &d.av2, &d.av3, &d.av4, &d.av5};
    uint16_t bc = std::min<uint16_t>(iter.bid_count(), 5);
    for (uint16_t i = 0; i < bc; ++i) {
        const auto* e = iter.bid(i);
        *bpArr[i] = gateiosbe::to_double(e->pxMantissa, pxExp) * info.reduceNumber;
        *bvArr[i] = gateiosbe::to_double(e->szMantissa, szExp) * info.magnifyNumber;
    }
    uint16_t ac = std::min<uint16_t>(iter.ask_count(), 5);
    for (uint16_t i = 0; i < ac; ++i) {
        const auto* e = iter.ask(i);
        *apArr[i] = gateiosbe::to_double(e->pxMantissa, pxExp) * info.reduceNumber;
        *avArr[i] = gateiosbe::to_double(e->szMantissa, szExp) * info.magnifyNumber;
    }
}

template <typename Iter>
inline void fill_depth10(md::Depth10& d, const Iter& iter, int8_t pxExp, int8_t szExp, const md::InstrumentInfo& info) {
    double* bpArr[10] = {&d.bp1, &d.bp2, &d.bp3, &d.bp4, &d.bp5, &d.bp6, &d.bp7, &d.bp8, &d.bp9, &d.bp10};
    double* bvArr[10] = {&d.bv1, &d.bv2, &d.bv3, &d.bv4, &d.bv5, &d.bv6, &d.bv7, &d.bv8, &d.bv9, &d.bv10};
    double* apArr[10] = {&d.ap1, &d.ap2, &d.ap3, &d.ap4, &d.ap5, &d.ap6, &d.ap7, &d.ap8, &d.ap9, &d.ap10};
    double* avArr[10] = {&d.av1, &d.av2, &d.av3, &d.av4, &d.av5, &d.av6, &d.av7, &d.av8, &d.av9, &d.av10};
    uint16_t bc = std::min<uint16_t>(iter.bid_count(), 10);
    for (uint16_t i = 0; i < bc; ++i) {
        const auto* e = iter.bid(i);
        *bpArr[i] = gateiosbe::to_double(e->pxMantissa, pxExp) * info.reduceNumber;
        *bvArr[i] = gateiosbe::to_double(e->szMantissa, szExp) * info.magnifyNumber;
    }
    uint16_t ac = std::min<uint16_t>(iter.ask_count(), 10);
    for (uint16_t i = 0; i < ac; ++i) {
        const auto* e = iter.ask(i);
        *apArr[i] = gateiosbe::to_double(e->pxMantissa, pxExp) * info.reduceNumber;
        *avArr[i] = gateiosbe::to_double(e->szMantissa, szExp) * info.magnifyNumber;
    }
}

template <typename Iter>
inline void fill_depth20(md::Depth20& d, const Iter& iter, int8_t pxExp, int8_t szExp, const md::InstrumentInfo& info) {
    double* bpArr[20] = {&d.bp1,&d.bp2,&d.bp3,&d.bp4,&d.bp5,&d.bp6,&d.bp7,&d.bp8,&d.bp9,&d.bp10,
                         &d.bp11,&d.bp12,&d.bp13,&d.bp14,&d.bp15,&d.bp16,&d.bp17,&d.bp18,&d.bp19,&d.bp20};
    double* bvArr[20] = {&d.bv1,&d.bv2,&d.bv3,&d.bv4,&d.bv5,&d.bv6,&d.bv7,&d.bv8,&d.bv9,&d.bv10,
                         &d.bv11,&d.bv12,&d.bv13,&d.bv14,&d.bv15,&d.bv16,&d.bv17,&d.bv18,&d.bv19,&d.bv20};
    double* apArr[20] = {&d.ap1,&d.ap2,&d.ap3,&d.ap4,&d.ap5,&d.ap6,&d.ap7,&d.ap8,&d.ap9,&d.ap10,
                         &d.ap11,&d.ap12,&d.ap13,&d.ap14,&d.ap15,&d.ap16,&d.ap17,&d.ap18,&d.ap19,&d.ap20};
    double* avArr[20] = {&d.av1,&d.av2,&d.av3,&d.av4,&d.av5,&d.av6,&d.av7,&d.av8,&d.av9,&d.av10,
                         &d.av11,&d.av12,&d.av13,&d.av14,&d.av15,&d.av16,&d.av17,&d.av18,&d.av19,&d.av20};
    uint16_t bc = std::min<uint16_t>(iter.bid_count(), 20);
    for (uint16_t i = 0; i < bc; ++i) {
        const auto* e = iter.bid(i);
        *bpArr[i] = gateiosbe::to_double(e->pxMantissa, pxExp) * info.reduceNumber;
        *bvArr[i] = gateiosbe::to_double(e->szMantissa, szExp) * info.magnifyNumber;
    }
    uint16_t ac = std::min<uint16_t>(iter.ask_count(), 20);
    for (uint16_t i = 0; i < ac; ++i) {
        const auto* e = iter.ask(i);
        *apArr[i] = gateiosbe::to_double(e->pxMantissa, pxExp) * info.reduceNumber;
        *avArr[i] = gateiosbe::to_double(e->szMantissa, szExp) * info.magnifyNumber;
    }
}

// Gate 应用层 pong 过滤 (spot.pong / futures.pong)
inline bool is_gate_pong(const uint8_t* data, size_t len) noexcept {
    std::string_view sv(reinterpret_cast<const char*>(data), len);
    return sv.find("\"pong\"") != std::string_view::npos || sv.find(".pong") != std::string_view::npos;
}


// ============================================================================
// Ctor: 根据 instType 决定 ping text
// ============================================================================
md::GateioSbeUnit::GateioSbeUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, SbeAccount sbeAcc, const char* host, int port, const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    sbeAccount = sbeAcc;

    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    if (instTypeEnum == SPOT) {
        cfg.client_ping_text = R"({"channel":"spot.ping"})";
    }
    else {
        cfg.client_ping_text = R"({"channel":"futures.ping"})";
    }
    cfg.idle_timeout_sec = 60;
    cfg.data_idle_timeout_sec = 0;
}


// ============================================================================
// generateSubBody: URL + channel 名 都按 instType 分流
// ============================================================================
void md::GateioSbeUnit::generateSubBody() {
    std::string exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    std::string instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    std::string marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];

    // 1) URL
    if (instTypeEnum == SPOT) {
        cfg.url = GATEIO_WS_SBE_SPOT;
    }       
    else if (instTypeEnum == USDT_SWAP) {
        cfg.url = GATEIO_WS_SBE_USDT_SWAP;
    }   
    else if (instTypeEnum == BTC_SWAP) {
        cfg.url = GATEIO_WS_SBE_BTC_SWAP;
    }
    else if (instTypeEnum == USDT_FUTURES) {
        cfg.url = GATEIO_WS_SBE_USDT_FUTURES;
    }
    else if (instTypeEnum == BTC_FUTURES) {
        cfg.url = GATEIO_WS_SBE_BTC_FUTURES;
    }
    else {
        LOG_ERROR("[GATEIO_SBE] unsupported instType: {}", instTypeStr);
        return;
    }

    // 2) channel 前缀 + orderBook 精度参数按 spot/futures 差异化
    const char* prefix = (instTypeEnum == SPOT) ? "spot" : "futures";
    // Gate JSON 里 spot.order_book 用 interval="100ms", futures.order_book 用 accuracy="0"
    const char* obThird = (instTypeEnum == SPOT) ? "100ms" : "0";

    cfg.subscribe_messages.clear();
    long timeSec = crypto::getCurrentTimeSeconds();

    for (auto& info : vInstInfo) {
        const std::string& originInstId = info.originInstId;

        std::string channel;
        std::string payloadJson;

        if (marketTypeEnum == md::DEPTH1) {
            // → tid=1 bbo
            channel = fmt::format("{}.book_ticker", prefix);
            payloadJson = fmt::format(R"(["{}"])", originInstId);
        }
        else if (marketTypeEnum == md::DEPTH5 || marketTypeEnum == md::DEPTH10 || marketTypeEnum == md::DEPTH20) {
            // → tid=4 orderBook (snapshot, level=N)
            int levels = (marketTypeEnum == md::DEPTH5) ? 5 : (marketTypeEnum == md::DEPTH10) ? 10 : 20;
            channel = fmt::format("{}.order_book", prefix);
            payloadJson = fmt::format(R"(["{}","{}","{}"])", originInstId, levels, obThird);
        }
        else if (marketTypeEnum == md::TRADES) {
            // → tid=2 publicTrade  (spot 无 group, futures 有 group)
            channel = fmt::format("{}.trades", prefix);
            payloadJson = fmt::format(R"(["{}"])", originInstId);
        }
        else if (marketTypeEnum == md::KLINE_1m) {
            channel = fmt::format("{}.candlesticks", prefix);
            payloadJson = fmt::format(R"(["{}"])", originInstId);
        }
        else if (marketTypeEnum == md::FUNDING_RATE) {
            if (instTypeEnum == USDT_SWAP) {
                channel = fmt::format("{}.tickers", prefix);
                payloadJson = fmt::format(R"(["{}"])", originInstId);
            }
        }
        else {
            LOG_ERROR("[GATEIO_SBE] unsupported marketType: {}", marketTypeStr);
            continue;
        }

        cfg.subscribe_messages.push_back(fmt::format(R"({{"time":{},"channel":"{}","event":"subscribe","payload":{}}})", timeSec, channel, payloadJson));
    }

    LOG_INFO("[GATEIO_SBE] {} ({}) ws url: {}, {} subscribe msgs prepared", exchIdStr, instTypeStr, cfg.url, cfg.subscribe_messages.size());
    
    for (auto& s : cfg.subscribe_messages) {
        LOG_INFO("sub: {}", s);
    }
}

// ============================================================================
// onWebsocketMsg: opcode 分流 (isBinary → SBE 入队;  text → pong 过滤 / log)
// ============================================================================
void md::GateioSbeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t /*ns*/) {
    latestDataUpdateTime = crypto::getCurrentTime();
    if (isBinary) {

        std::string msg(reinterpret_cast<const char*>(data), len);
        mQueue.push(std::move(msg));
    }
    else {
        std::string txt(reinterpret_cast<const char*>(data), len);
        LOG_INFO("[GATEIO_SBE] Received sbe text msg: {}", txt);
    }
}

// ============================================================================
// lookupInfo
// ============================================================================
bool md::GateioSbeUnit::lookupInfo(std::string_view sym, md::InstrumentInfo& out) const {
    if (sym.empty()) return false;
    std::string originInstId(sym);
    if (!smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), out)) {
        LOG_ERROR("[GATEIO_SBE] smc cannot find originInstId: {}", originInstId);
        return false;
    }
    return true;
}


// ============================================================================
// parseMarketData: 顶层按 instType 分流到 spot / futures 版本
// ============================================================================
void md::GateioSbeUnit::parseMarketData(const std::string& msg) {
    long tsNet = crypto::getCurrentTime();
    const uint8_t* data = reinterpret_cast<const uint8_t*>(msg.data());
    size_t len = msg.size();

    if (len < sizeof(gateiosbe::MessageHeader)) {
        LOG_ERROR("[GATEIO_SBE] frame too short: {} bytes", len);
        return;
    }

    if (instTypeEnum == SPOT) {
        parseSpotData(data, len, tsNet);
    } else {
        parseFuturesData(data, len, tsNet);
    }
}


// ============================================================================
// parseFuturesData: 期货 SBE 分派
//   templateId 语义: 1 bbo, 2 publicTrade(有 group), 3 obu, 4 orderBook(asks 先), 5 obu-update
// ============================================================================
void md::GateioSbeUnit::parseFuturesData(const uint8_t* data, size_t len, long tsNet) {
    namespace fx = gateiosbefutures;
    uint16_t tid = gateiosbe::peek_template_id(data, len);

    switch (tid) {

    // ---- tid=1 bbo → Depth1  (期货 wire 是 ask 在前) ----
    case fx::kTemplateBbo: {
        if (marketTypeEnum != md::DEPTH1) {
            return;
        }

        const auto* v = gateiosbe::view_of<fx::BboView>(data, len);
        if (!v) { 
            LOG_ERROR("[GATEIO_SBE][fx] Bbo view failed"); 
            return; 
        }

        md::InstrumentInfo info;
        if (!lookupInfo(fx::bbo_symbol(data, len), info)) {
            return;
        }

        const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

        md::Depth1 d1; 
        memset(&d1, 0, sizeof(d1));
        d1.exchangeTypeEnum = exchangeTypeEnum;
        d1.instTypeEnum = instTypeEnum;
        d1.marketTypeEnum = marketTypeEnum;
        strncpy(d1.instId, info.instId, INSTID_SIZE);
        long tsNs = static_cast<long>(v->t) * 1000;
        d1.tsTrans = tsNs; 
        d1.tsEvent = tsNs; 
        d1.tsRecv = tsNet;
        d1.bp1 = gateiosbe::to_double(v->bidMantissaPrice, v->pxExponent) * info.reduceNumber;
        d1.bv1 = gateiosbe::to_double(v->bidMantissaSize,  v->szExponent) * info.magnifyNumber;
        d1.ap1 = gateiosbe::to_double(v->askMantissaPrice, v->pxExponent) * info.reduceNumber;
        d1.av1 = gateiosbe::to_double(v->askMantissaSize,  v->szExponent) * info.magnifyNumber;
        d1.tsParse = crypto::getCurrentTime();

        std::cout << d1.getString() << std::endl;
#ifdef NEED_SHM
        mDepth1Publisher[key]->push(d1);;
#endif
        break;
    }

    // ---- tid=2 publicTrade → Trades*  (期货是 group, size 带符号编码方向) ----
    case fx::kTemplatePublicTrade: {
        if (marketTypeEnum != md::TRADES) {
            return;
        }

        fx::PublicTradeIter iter(data, len);
        if (!iter.ok() || !iter.root()) { 
            LOG_ERROR("[GATEIO_SBE][fx] PublicTrade iter failed"); 
            return; 
        }

        const auto* root = iter.root();

        md::InstrumentInfo info;
        if (!lookupInfo(iter.symbol(), info)) {
            return;
        }

        const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

        for (uint16_t i = 0; i < iter.count(); ++i) {
            const auto* e = iter.entry(i);

            md::Trades t; 
            memset(&t, 0, sizeof(t));
            t.exchangeTypeEnum = exchangeTypeEnum;
            t.instTypeEnum = instTypeEnum;
            t.marketTypeEnum = marketTypeEnum;
            strncpy(t.instId, info.instId, INSTID_SIZE);
            t.tsTrans = static_cast<long>(e->t) * 1000;
            t.tsEvent = static_cast<long>(root->time) * 1000;
            t.tsRecv  = tsNet;

            std::string tidStr = std::to_string(e->id);
            strncpy(t.tradeId, tidStr.c_str(), INSTID_SIZE);

            double sizeAbs = std::abs(gateiosbe::to_double(e->size,  root->szExponent));
            double px = gateiosbe::to_double(e->price, root->pxExponent);
            t.px = px * info.reduceNumber;
            t.sz = sizeAbs * info.magnifyNumber;
            t.direction = (e->size >= 0) ? DT_LONG : DT_SHORT;
            t.tsParse = crypto::getCurrentTime();

            std::cout << t.getString() << std::endl;
#ifdef NEED_SHM
            mTradesPublisher[key]->push(t);
#endif
        }
        break;
    }

    // ---- tid=4 orderBook → Depth5/10/20  (期货 asks 先, OrderBookIter 已处理) ----
    case fx::kTemplateOrderBook: {
        fx::OrderBookIter iter(data, len);
        if (!iter.ok() || !iter.root()) { 
            LOG_ERROR("[GATEIO_SBE][fx] OrderBook iter failed"); 
            return; 
        }
        const auto* root = iter.root();

        md::InstrumentInfo info;
        if (!lookupInfo(iter.symbol(), info)) {
            return;
        }

        long tsNs = static_cast<long>(root->t) * 1000;
        const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

        auto fill_common = [&](auto& d) {
            d.exchangeTypeEnum = exchangeTypeEnum;
            d.instTypeEnum = instTypeEnum;
            d.marketTypeEnum = marketTypeEnum;
            strncpy(d.instId, info.instId, INSTID_SIZE);
            d.tsTrans = tsNs; 
            d.tsEvent = tsNs; 
            d.tsRecv = tsNet;
        };

        if (marketTypeEnum == md::DEPTH5) {
            md::Depth5 d; 
            memset(&d, 0, sizeof(d)); 
            fill_common(d);
            fill_depth5(d, iter, root->pxExponent, root->szExponent, info);
            d.tsParse = crypto::getCurrentTime();

            std::cout << d.getString() << std::endl;
#ifdef NEED_SHM
            mDepth5Publisher[key]->push(d);
#endif
        }
        else if (marketTypeEnum == md::DEPTH10) {
            md::Depth10 d; 
            memset(&d, 0, sizeof(d)); 
            fill_common(d);
            fill_depth10(d, iter, root->pxExponent, root->szExponent, info);
            d.tsParse = crypto::getCurrentTime();

            std::cout << d.getString() << std::endl;
#ifdef NEED_SHM
            mDepth10Publisher[key]->push(d);
#endif
        }
        else if (marketTypeEnum == md::DEPTH20) {
            md::Depth20 d; 
            memset(&d, 0, sizeof(d)); 
            fill_common(d);
            fill_depth20(d, iter, root->pxExponent, root->szExponent, info);
            d.tsParse = crypto::getCurrentTime();

            std::cout << d.getString() << std::endl;
#ifdef NEED_SHM
            mDepth20Publisher[key]->push(d);
#endif
        }
        break;
    }

    case fx::kTemplateCandlestick: {
        if (marketTypeEnum != md::KLINE_1m) {
            return;
        }

        fx::CandlestickIter iter(data,len);
        if (!iter.ok() || !iter.root()) {
            LOG_ERROR("[GATEIO_SBE][fx] Candlestick iter failed");
            return;
        }

        md::InstrumentInfo info;
        if (!lookupInfo(iter.name(0), info)) {
            return;
        }

        const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

        const auto* root = iter.root();

        for (uint16_t i=0; i<iter.count(); ++i) {
            const auto* e = iter.entry(i);

            md::Kline k;
            memset(&k, 0, sizeof(k));
            k.exchangeTypeEnum = exchangeTypeEnum;
            k.instTypeEnum = instTypeEnum;
            k.marketTypeEnum = marketTypeEnum;
            strncpy(k.instId, info.instId, INSTID_SIZE);

            // Gate candlestick: t 是秒
            k.tsTrans = static_cast<long>(e->t)*1000000L;
            k.tsEvent = static_cast<long>(root->time);
            k.tsRecv = tsNet;
            k.openPrice = gateiosbe::to_double(e->openMantissa, root->pxExponent) * info.reduceNumber;
            k.highPrice = gateiosbe::to_double(e->highMantissa, root->pxExponent) * info.reduceNumber;
            k.lowPrice = gateiosbe::to_double(e->lowMantissa, root->pxExponent) * info.reduceNumber;
            k.closePrice = gateiosbe::to_double(e->closeMantissa, root->pxExponent) * info.reduceNumber;
            k.totalVolume = gateiosbe::to_double(e->volumeMantissa, root->szExponent) * info.magnifyNumber; 
            k.totalAmount = gateiosbe::to_double(e->amountMantissa, root->amountExponent);

            double avgPrice = 0.0;
            if(k.totalVolume > ZERO_NUM) {
                avgPrice = k.totalAmount / k.totalVolume;
            }
            k.avgPrice = avgPrice * info.reduceNumber;

            k.isFinished = e->complete;
            k.tsParse = crypto::getCurrentTime();

            std::cout << k.getString() << std::endl;

            if (!k.isFinished) {
                continue;
            }
    #ifdef NEED_SHM
            mKlinePublisher[key]->push(k);
    #endif
        }
        break;
    }
    case fx::kTemplateObu:
    case fx::kTemplateOrderBookUpdate:
        // TODO: order-book 累积器
        break;

    default:
        LOG_WARN("[GATEIO_SBE][fx] unknown templateId: {}", tid);
        break;
    }
}


// ============================================================================
// parseSpotData: 现货 SBE 分派
//   templateId 语义: 1 bbo(bid 先), 2 publicTrade(**无 group**), 3 obu, 4 orderBook(bids 先), 5 obu-update
// ============================================================================
void md::GateioSbeUnit::parseSpotData(const uint8_t* data, size_t len, long tsNet) {
    namespace sp = gateiosbespot;
    uint16_t tid = gateiosbe::peek_template_id(data, len);

    switch (tid) {

    // ---- tid=1 bbo → Depth1  (现货 wire 是 bid 在前, spot::BboView 已按此顺序) ----
    case sp::kTemplateBbo: {
        if (marketTypeEnum != md::DEPTH1) {
            return;
        }
        const auto* v = gateiosbe::view_of<sp::BboView>(data, len);
        if (!v) { 
            LOG_ERROR("[GATEIO_SBE][sp] Bbo view failed"); 
            return; 
        }

        md::InstrumentInfo info;
        if (!lookupInfo(sp::bbo_symbol(data, len), info)) {
            return;
        }

        const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

        md::Depth1 d1; 
        memset(&d1, 0, sizeof(d1));
        d1.exchangeTypeEnum = exchangeTypeEnum;
        d1.instTypeEnum = instTypeEnum;
        d1.marketTypeEnum = marketTypeEnum;
        strncpy(d1.instId, info.instId, INSTID_SIZE);
        long tsNs = static_cast<long>(v->t) * 1000;
        d1.tsTrans = tsNs; 
        d1.tsEvent = tsNs; 
        d1.tsRecv = tsNet;
        d1.bp1 = gateiosbe::to_double(v->bidMantissaPrice, v->pxExponent) * info.reduceNumber;
        d1.bv1 = gateiosbe::to_double(v->bidMantissaSize, v->szExponent) * info.magnifyNumber;
        d1.ap1 = gateiosbe::to_double(v->askMantissaPrice, v->pxExponent) * info.reduceNumber;
        d1.av1 = gateiosbe::to_double(v->askMantissaSize, v->szExponent) * info.magnifyNumber;
        d1.tsParse = crypto::getCurrentTime();

        std::cout << d1.getString() << std::endl;
#ifdef NEED_SHM
        mDepth1Publisher[key]->push(d1);
#endif
        break;
    }

    // ---- tid=2 publicTrade → 单笔 Trades  (⚠️ 现货无 group, 根字段一笔成交) ----
    case sp::kTemplatePublicTrade: {
        if (marketTypeEnum != md::TRADES) {
            return;
        }
        const auto* v = gateiosbe::view_of<sp::PublicTradeView>(data, len);
        if (!v) { 
            LOG_ERROR("[GATEIO_SBE][sp] PublicTrade view failed"); 
            return; 
        }

        md::InstrumentInfo info;
        if (!lookupInfo(sp::public_trade_symbol(data, len), info)) {
            return;
        }

        const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

        md::Trades t; 
        memset(&t, 0, sizeof(t));
        t.exchangeTypeEnum = exchangeTypeEnum;
        t.instTypeEnum = instTypeEnum;
        t.marketTypeEnum = marketTypeEnum;
        strncpy(t.instId, info.instId, INSTID_SIZE);
        t.tsTrans = static_cast<long>(v->createTimeUs) * 1000;
        t.tsEvent = static_cast<long>(v->time) * 1000;
        t.tsRecv = tsNet;

        std::string tidStr = std::to_string(v->id);
        strncpy(t.tradeId, tidStr.c_str(), INSTID_SIZE);

        double px = gateiosbe::to_double(v->priceMantissa, v->pxExponent);
        double sz = gateiosbe::to_double(v->amountMantissa, v->szExponent);
        t.px = px * info.reduceNumber;
        t.sz = sz * info.magnifyNumber;
        // 现货 side 是显式枚举 (0=SELL, 1=BUY)
        t.direction = (v->side == gateiosbe::Side_Buy) ? DT_LONG : DT_SHORT;

        t.tsParse = crypto::getCurrentTime();

        std::cout << t.getString() << std::endl;

#ifdef NEED_SHM
        mTradesPublisher[key]->push(t);
#endif
        break;
    }

    // ---- tid=4 orderBook → Depth5/10/20  (现货 bids 先, spot::OrderBookIter 已处理) ----
    case sp::kTemplateOrderBook: {
        sp::OrderBookIter iter(data, len);
        if (!iter.ok() || !iter.root()) { 
            LOG_ERROR("[GATEIO_SBE][sp] OrderBook iter failed"); 
            return; 
        }
        const auto* root = iter.root();

        md::InstrumentInfo info;
        if (!lookupInfo(iter.symbol(), info)) {
            return;
        }

        long tsNs = static_cast<long>(root->t) * 1000;
        const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

        auto fill_common = [&](auto& d) {
            d.exchangeTypeEnum = exchangeTypeEnum;
            d.instTypeEnum = instTypeEnum;
            d.marketTypeEnum = marketTypeEnum;
            strncpy(d.instId, info.instId, INSTID_SIZE);
            d.tsTrans = tsNs; d.tsEvent = tsNs; d.tsRecv = tsNet;
        };

        if (marketTypeEnum == md::DEPTH5) {
            md::Depth5 d; 
            memset(&d, 0, sizeof(d)); 
            fill_common(d);
            fill_depth5(d, iter, root->pxExponent, root->szExponent, info);
            d.tsParse = crypto::getCurrentTime();

            std::cout << d.getString() << std::endl;
#ifdef NEED_SHM
            mDepth5Publisher[key]->push(d);
#endif
        }
        else if (marketTypeEnum == md::DEPTH10) {
            md::Depth10 d; 
            memset(&d, 0, sizeof(d)); 
            fill_common(d);
            fill_depth10(d, iter, root->pxExponent, root->szExponent, info);
            d.tsParse = crypto::getCurrentTime();

            std::cout << d.getString() << std::endl;
#ifdef NEED_SHM
            mDepth10Publisher[key]->push(d);
#endif
        }
        else if (marketTypeEnum == md::DEPTH20) {
            md::Depth20 d; 
            memset(&d, 0, sizeof(d)); 
            fill_common(d);
            fill_depth20(d, iter, root->pxExponent, root->szExponent, info);
            d.tsParse = crypto::getCurrentTime();

            std::cout << d.getString() << std::endl;
#ifdef NEED_SHM
            mDepth20Publisher[key]->push(d);
#endif
        }
        break;
    }

    case sp::kTemplateCandlestick: {
        if (marketTypeEnum != md::KLINE) {
            return;
        }

        const auto* v = gateiosbe::view_of<sp::CandlestickView>(data, len);
        if (!v) {
            LOG_ERROR("[GATEIO_SBE][sp] Candlestick view failed");
            return;
        }

        auto name = sp::candlestick_name(data, len);
        md::InstrumentInfo info;

        if (!lookupInfoFromCandlestick(name, info)) {
            return;
        }

        md::Kline k;
        memset(&k, 0, sizeof(k));
        k.exchangeTypeEnum = exchangeTypeEnum;
        k.instTypeEnum = instTypeEnum;
        k.marketTypeEnum = marketTypeEnum;
        strncpy(k.instId, info.instId, INSTID_SIZE);

        // candle timestamp
        long ts = static_cast<long>(v->t);
        k.tsEvent = ts;
        k.tsTrans = ts;
        k.tsRecv = tsNet;
        k.openPrice = gateiosbe::to_double(v->openMantissa, v->pxExponent) * info.reduceNumber;
        k.highPrice = gateiosbe::to_double(v->highMantissa, v->pxExponent) * info.reduceNumber;
        k.lowPrice = gateiosbe::to_double(v->lowMantissa, v->pxExponent) * info.reduceNumber;
        k.closePrice = gateiosbe::to_double(v->closeMantissa, v->pxExponent) * info.reduceNumber;
        k.totalVolume = gateiosbe::to_double(v->volumeMantissa, v->szExponent) * info.magnifyNumber;
        k.totalAmount = gateiosbe::to_double(v->amountMantissa, v->amountExponent);
        k.isFinished = v->complete;
        k.tsParse = crypto::getCurrentTime();

        double avgPrice = 0.0;
        if(k.totalVolume > ZERO_NUM) {
            avgPrice = k.totalAmount / k.totalVolume;
        }
        kline.avgPrice = avgPrice * info.reduceNumber;

        std::cout << k.getString() << std::endl;

        if (!k.isFinished) {
            return;
        }

    #ifdef NEED_SHM
        mKlinePublisher[key]->push(k);
    #endif

        break;
    }



    case sp::kTemplateObu:
    case sp::kTemplateOrderBookUpdate:
        // TODO: order-book 累积器
        break;

    default:
        LOG_WARN("[GATEIO_SBE][sp] unknown templateId: {}", tid);
        break;
    }
}


// ============================================================================
// Market wrapper
// ============================================================================
md::GateioSbeMarket::GateioSbeMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, SbeAccount sbeAccount, int lot, const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd)
{
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        auto* unit = new md::GateioSbeUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum, info.vInstInfo, sbeAccount, _host, _port, _passwd);
        unit->generateSubBody();
        gateioSbeUnitVec.push_back(unit);
    }
}

md::GateioSbeMarket::~GateioSbeMarket() {
    for (auto* u : gateioSbeUnitVec) delete u;
    gateioSbeUnitVec.clear();
}

void md::GateioSbeMarket::start() {
    for (auto* u : gateioSbeUnitVec) {
        u->start();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}