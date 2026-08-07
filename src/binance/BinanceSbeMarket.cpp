#include "binance/BinanceSbeMarket.h"
#include "binance/BinanceSbeDecoder.h"


md::BinanceSbeUnit::BinanceSbeUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, SbeAccount sbeAcc, const char* host, int port, const char* passwd) : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    subId = crypto::get_int_rand(100,10000);
    sbeAccount = sbeAcc;

    cfg.headers.emplace_back("X-MBX-APIKEY", sbeAccount.apiKey);
    cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
    cfg.idle_timeout_sec = 60;
    cfg.data_idle_timeout_sec = 0; // 看实际情况是否开启  
}

void md::BinanceSbeUnit::generateSubBody() {
    // LOG_INFO("%s", getString().c_str());
    std::string exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    std::string instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    std::string marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
    std::string lowerMarketTypeStr = crypto::to_lower(marketTypeStr);

    if (instTypeEnum == SPOT) {
        wsUrl = BINANCE_SBE_WS_PUBLIC_SPOT;
    }
    else {
        LOG_ERROR("SBE only supports SPOT, get instType: {}", instTypeStr);
    }

    subParams.clear();

    for (auto info : vInstInfo) {
        std::string lowerOriginInstId = crypto::to_lower(info.originInstId);
        if (instTypeEnum == SPOT) {
            if (crypto::has_str(marketTypeStr, "DEPTH")) {
                if(crypto::str_cmp(marketTypeStr.c_str(), "DEPTH1")) {
                    std::string param = fmt::format("{}@bestBidAsk", lowerOriginInstId);
                    subParams.push_back(param);
                }
                else if(crypto::str_cmp(marketTypeStr.c_str(), "DEPTH20")) {
                    std::string param = fmt::format("{}@depth20", lowerOriginInstId);
                    subParams.push_back(param);
                }
                else {
                    LOG_ERROR("not support {}", marketTypeStr);
                }
            }
            else if (crypto::has_str(marketTypeStr, "TRADE")) {
                std::string param = fmt::format("{}@trade", lowerOriginInstId);
                subParams.push_back(param);
            }
            else {
                LOG_ERROR("not support {}", marketTypeStr);
            }
        }
        else {
            LOG_ERROR("not support subMarketType: {}", instTypeStr);
        }
    }

    std::string paramsCsv;
    paramsCsv.reserve(subParams.size() * 32);
    for (size_t i = 0; i < subParams.size(); ++i) {
        if (i) {
            paramsCsv += ",";
        }
        paramsCsv += "\"";
        paramsCsv += subParams[i];
        paramsCsv += "\"";
    }

    std::string subJson = fmt::format(R"({{"method":"SUBSCRIBE","params":[{}],"id":{}}})", paramsCsv, subId++);
    cfg.subscribe_messages.clear();
    cfg.subscribe_messages.push_back(subJson);

    LOG_INFO("{} sbe url: {}, sub body: {}", exchIdStr, cfg.url, subJson);   
}


void md::BinanceSbeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    latestDataUpdateTime = crypto::getCurrentTime();

    if (isBinary) {
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWebsocketMsg: " << msg << std::endl;
        mQueue.push(msg);
    }
    else {
        std::string txt(reinterpret_cast<const char*>(data), len);
        LOG_INFO("Received sbe text msg: {}", txt);
    }
}

void md::BinanceSbeUnit::parseMarketData(const std::string& msg) {
    long tsNet = crypto::getCurrentTime();

    // Binance SBE 只在 SPOT (stream-sbe.binance.com) 提供
    if (instTypeEnum != SPOT) {
        LOG_ERROR("BinanceSbeUnit only supports SPOT, got instType: {}", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    const uint8_t* data = reinterpret_cast<const uint8_t*>(msg.data());
    size_t len = msg.size();

    if (len < sizeof(binancesbe::MessageHeader)) {
        LOG_ERROR("[BINANCE_SBE] frame too short: {} bytes", len);
        return;
    }

    uint16_t tid = binancesbe::peek_template_id(data, len);
    switch (tid) {

    // ------------------------------------------------------------------
    // BestBidAskStreamEvent (10001) → Depth1
    // ------------------------------------------------------------------
    case binancesbe::kTemplateBestBidAsk: {
        if (marketTypeEnum != md::DEPTH1) {
            return;
        }

        const auto* v = binancesbe::view_of<binancesbe::BestBidAskView>(data, len);
        if (!v) {
            LOG_ERROR("[BINANCE_SBE] BestBidAsk view failed, len: {}", len);
            return;
        }
        std::string_view sym = binancesbe::bba_symbol(data, len);

        md::InstrumentInfo info;
        std::string originInstId(sym);
        if (!smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info)) {
            LOG_ERROR("[BINANCE_SBE] smc cannot find originInstId: {}", originInstId);
            return;
        }

        const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

        md::Depth1 depth1;
        memset(&depth1, 0, sizeof(md::Depth1));
        depth1.exchangeTypeEnum = exchangeTypeEnum;
        depth1.instTypeEnum     = instTypeEnum;
        depth1.marketTypeEnum   = marketTypeEnum;
        strncpy(depth1.instId, info.instId, INSTID_SIZE);

        long tsUs = static_cast<long>(v->eventTime);
        depth1.tsTrans = tsUs;
        depth1.tsEvent = tsUs;
        depth1.tsRecv  = tsNet;

        double bidPx = binancesbe::to_double(v->bidPrice, v->priceExponent);
        double bidQty = binancesbe::to_double(v->bidQty,  v->qtyExponent);
        double askPx = binancesbe::to_double(v->askPrice, v->priceExponent);
        double askQty = binancesbe::to_double(v->askQty,  v->qtyExponent);

        depth1.bp1 = bidPx  * info.reduceNumber;
        depth1.bv1 = bidQty * info.magnifyNumber;
        depth1.ap1 = askPx  * info.reduceNumber;
        depth1.av1 = askQty * info.magnifyNumber;

        depth1.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
        mDepth1Publisher[key]->push(depth1);
#endif
        break;
    }

    // ------------------------------------------------------------------
    // DepthSnapshotStreamEvent (10002) → Depth5 / Depth10 / Depth20
    // ------------------------------------------------------------------
    case binancesbe::kTemplateDepthSnapshot: {
        binancesbe::DepthSnapshotIter iter(data, len);
        if (!iter.ok() || !iter.root()) {
            LOG_ERROR("[BINANCE_SBE] DepthSnapshot iter failed, len: {}", len);
            return;
        }
        const auto* root = iter.root();
        std::string_view sym = iter.symbol();

        md::InstrumentInfo info;
        std::string originInstId(sym);
        if (!smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info)) {
            LOG_ERROR("[BINANCE_SBE] smc cannot find originInstId: {}", originInstId);
            return;
        }

        long tsUs = static_cast<long>(root->eventTime);
        long tsNs = tsUs;

        auto fill_common = [&](auto& d) {
            d.exchangeTypeEnum = exchangeTypeEnum;
            d.instTypeEnum     = instTypeEnum;
            d.marketTypeEnum   = marketTypeEnum;
            strncpy(d.instId, info.instId, INSTID_SIZE);
            d.tsTrans = tsNs;
            d.tsEvent = tsNs;
            d.tsRecv  = tsNet;
        };

        std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

        if (marketTypeEnum == md::DEPTH5) {
            md::Depth5 d; memset(&d, 0, sizeof(d)); fill_common(d);
            double* bpArr[5] = {&d.bp1,&d.bp2,&d.bp3,&d.bp4,&d.bp5};
            double* bvArr[5] = {&d.bv1,&d.bv2,&d.bv3,&d.bv4,&d.bv5};
            double* apArr[5] = {&d.ap1,&d.ap2,&d.ap3,&d.ap4,&d.ap5};
            double* avArr[5] = {&d.av1,&d.av2,&d.av3,&d.av4,&d.av5};
            uint16_t bc = std::min<uint16_t>(iter.bid_count(), 5);
            for (uint16_t i = 0; i < bc; ++i) {
                const auto* e = iter.bid(i);
                *bpArr[i] = binancesbe::to_double(e->price, root->priceExponent) * info.reduceNumber;
                *bvArr[i] = binancesbe::to_double(e->qty,   root->qtyExponent)   * info.magnifyNumber;
            }
            uint16_t ac = std::min<uint16_t>(iter.ask_count(), 5);
            for (uint16_t i = 0; i < ac; ++i) {
                const auto* e = iter.ask(i);
                *apArr[i] = binancesbe::to_double(e->price, root->priceExponent) * info.reduceNumber;
                *avArr[i] = binancesbe::to_double(e->qty,   root->qtyExponent)   * info.magnifyNumber;
            }
            d.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
            mDepth5Publisher[key]->push(d);
#endif
        }
        else if (marketTypeEnum == md::DEPTH10) {
            md::Depth10 d; memset(&d, 0, sizeof(d)); fill_common(d);
            double* bpArr[10] = {&d.bp1,&d.bp2,&d.bp3,&d.bp4,&d.bp5,&d.bp6,&d.bp7,&d.bp8,&d.bp9,&d.bp10};
            double* bvArr[10] = {&d.bv1,&d.bv2,&d.bv3,&d.bv4,&d.bv5,&d.bv6,&d.bv7,&d.bv8,&d.bv9,&d.bv10};
            double* apArr[10] = {&d.ap1,&d.ap2,&d.ap3,&d.ap4,&d.ap5,&d.ap6,&d.ap7,&d.ap8,&d.ap9,&d.ap10};
            double* avArr[10] = {&d.av1,&d.av2,&d.av3,&d.av4,&d.av5,&d.av6,&d.av7,&d.av8,&d.av9,&d.av10};
            uint16_t bc = std::min<uint16_t>(iter.bid_count(), 10);
            for (uint16_t i = 0; i < bc; ++i) {
                const auto* e = iter.bid(i);
                *bpArr[i] = binancesbe::to_double(e->price, root->priceExponent) * info.reduceNumber;
                *bvArr[i] = binancesbe::to_double(e->qty,   root->qtyExponent)   * info.magnifyNumber;
            }
            uint16_t ac = std::min<uint16_t>(iter.ask_count(), 10);
            for (uint16_t i = 0; i < ac; ++i) {
                const auto* e = iter.ask(i);
                *apArr[i] = binancesbe::to_double(e->price, root->priceExponent) * info.reduceNumber;
                *avArr[i] = binancesbe::to_double(e->qty,   root->qtyExponent)   * info.magnifyNumber;
            }
            d.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM

            mDepth10Publisher[key]->push(d);
#endif
        }
        else if (marketTypeEnum == md::DEPTH20) {
            md::Depth20 d; memset(&d, 0, sizeof(d)); fill_common(d);
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
                *bpArr[i] = binancesbe::to_double(e->price, root->priceExponent) * info.reduceNumber;
                *bvArr[i] = binancesbe::to_double(e->qty,   root->qtyExponent)   * info.magnifyNumber;
            }
            uint16_t ac = std::min<uint16_t>(iter.ask_count(), 20);
            for (uint16_t i = 0; i < ac; ++i) {
                const auto* e = iter.ask(i);
                *apArr[i] = binancesbe::to_double(e->price, root->priceExponent) * info.reduceNumber;
                *avArr[i] = binancesbe::to_double(e->qty,   root->qtyExponent)   * info.magnifyNumber;
            }
            d.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
            mDepth20Publisher[key]->push(d);
#endif
        }
        // else 该 unit 订的不是 DEPTH5/10/20, 忽略
        break;
    }

    // ------------------------------------------------------------------
    // TradesStreamEvent (10000) → Trades (每条 entry 生成一条 Trades)
    // ------------------------------------------------------------------
    case binancesbe::kTemplateTrades: {
        if (marketTypeEnum != md::TRADES) return;

        binancesbe::TradesIter iter(data, len);
        if (!iter.ok() || !iter.root()) {
            LOG_ERROR("[BINANCE_SBE] Trades iter failed, len: {}", len);
            return;
        }
        const auto* root = iter.root();
        std::string_view sym = iter.symbol();

        md::InstrumentInfo info;
        std::string originInstId(sym);
        if (!smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info)) {
            LOG_ERROR("[BINANCE_SBE] smc cannot find originInstId: {}", originInstId);
            return;
        }

        std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

        long tsUs = static_cast<long>(root->transactTime);

        uint32_t n = iter.count();
        for (uint32_t i = 0; i < n; ++i) {
            const auto* e = iter.entry(i);

            md::Trades trades;
            memset(&trades, 0, sizeof(trades));
            trades.exchangeTypeEnum = exchangeTypeEnum;
            trades.instTypeEnum     = instTypeEnum;
            trades.marketTypeEnum   = marketTypeEnum;
            strncpy(trades.instId, info.instId, INSTID_SIZE);
            trades.tsTrans = tsUs;
            trades.tsEvent = static_cast<long>(root->eventTime) * 1000;
            trades.tsRecv  = tsNet;

            std::string tidStr = std::to_string(e->id);
            strncpy(trades.tradeId, tidStr.c_str(), INSTID_SIZE);

            double px = binancesbe::to_double(e->price, root->priceExponent);
            double sz = binancesbe::to_double(e->qty,   root->qtyExponent);
            trades.px = px * info.reduceNumber;
            trades.sz = sz * info.magnifyNumber;

            // isBuyerMaker=True 意味着买方是 maker → 主动方是卖方 → DT_SHORT
            trades.direction = (e->isBuyerMaker == binancesbe::Bool_True) ? DT_SHORT : DT_LONG;

            trades.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
            mTradesPublisher[key]->push(trades);
#endif
        }
        break;
    }

    // ------------------------------------------------------------------
    // DepthDiffStreamEvent (10003): 目前不使用 (Depth5/10/20 走 snapshot 通道)。
    // 保留分支便于未来接 order-book 累积器。
    // ------------------------------------------------------------------
    case binancesbe::kTemplateDepthDiff:
        break;

    default:
        LOG_WARN("[BINANCE_SBE] unknown templateId: {}", tid);
        break;
    }
}

md::BinanceSbeMarket::BinanceSbeMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, SbeAccount sbeAccount, int lot, const char* host, const int port, const char* passwd) : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    std::cout << "============ " << exchId << std::endl;
    std::cout << "--=-=-=-=" << unitInfoVec.size() << std::endl;

    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        std::cout << "start create binance unit" << std::endl;
        auto& info = unitInfoVec[i];
        md::BinanceSbeUnit* unit = new md::BinanceSbeUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum, info.vInstInfo, sbeAccount, _host, _port, _passwd);
        std::cout << "start generate sub body" << std::endl;
        unit->generateSubBody();
        binanceSbeUnitVec.push_back(unit);
    }

}

md::BinanceSbeMarket::~BinanceSbeMarket() {
    for (size_t i = 0; i < binanceSbeUnitVec.size(); ++i) {
        if (binanceSbeUnitVec[i]) {
            delete binanceSbeUnitVec[i];
            binanceSbeUnitVec[i] = nullptr;
        }
    }
}


void md::BinanceSbeMarket::start() {
    for (size_t i = 0; i < binanceSbeUnitVec.size(); ++i) {
        binanceSbeUnitVec[i]->start();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}
