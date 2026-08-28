#include "gateio/GateioMarket.h"


md::GateioUnit::GateioUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* passwd) : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    if (instTypeEnum == SPOT) {
        cfg.client_ping_text = R"({"channel":"spot.ping"})";
    }
    else {
        cfg.client_ping_text = R"({"channel":"futures.ping"})";
    }
    cfg.idle_timeout_sec = 60;
    cfg.data_idle_timeout_sec = 0; // 看实际情况是否开启
}

void md::GateioUnit::generateSubBody() {
    std::string exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    std::string instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    std::string marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
    std::string lowerMarketTypeStr = crypto::to_lower(marketTypeStr);

    if (instTypeEnum == SPOT) {
        cfg.url = GATEIO_WS_PUBLIC_SPOT;
    }
    else if (instTypeEnum == USDT_SWAP) {
        cfg.url = GATEIO_WS_PUBLIC_USDT_SWAP;
    }
    else if (instTypeEnum == USDT_FUTURES) {
        cfg.url = GATEIO_WS_PUBLIC_USDT_FUTURES;
    }
    else if (instTypeEnum == BTC_SWAP) {
        cfg.url = GATEIO_WS_PUBLIC_BTC_SWAP;
    }
    else if (instTypeEnum == BTC_FUTURES) {
        cfg.url = GATEIO_WS_PUBLIC_BTC_FUTURES;
    }

    cfg.subscribe_messages.clear();
    int64_t timeSec = crypto::getCurrentTimeSeconds();

    for (auto info : vInstInfo) {
        const std::string& originInstId = info.originInstId;
        std::string channel = "";
        std::string payloadJson = "";
        if (instTypeEnum == SPOT) {
            if (marketTypeEnum == md::DEPTH1) {
                channel = "spot.book_ticker";
                payloadJson = fmt::format(R"(["{}"])", originInstId);
            }
            else if (marketTypeEnum == md::DEPTH5 || marketTypeEnum == md::DEPTH10 || marketTypeEnum == md::DEPTH20) {
                int levels = (marketTypeEnum == md::DEPTH5) ? 5 : (marketTypeEnum == md::DEPTH10) ? 10 : 20;
                channel = "spot.order_book";
                payloadJson = fmt::format(R"(["{}","{}","100ms"])", originInstId, levels);
            }
            else if (marketTypeEnum == md::TRADES) {
                channel = "spot.trades";
                payloadJson = fmt::format(R"(["{}"])", originInstId); 
            }
            else if (marketTypeEnum == md::KLINE_1m) {                
                channel = "spot.candlesticks";
                payloadJson = fmt::format(R"(["1m","{}"])", originInstId);
            }
            else {
                LOG_ERROR("not support {}", marketTypeStr);
                continue;
            }
        }
        else if (instTypeEnum == USDT_SWAP || instTypeEnum == BTC_SWAP || instTypeEnum == USDT_FUTURES || instTypeEnum == BTC_FUTURES) {
            if (marketTypeEnum == md::DEPTH1) {
                channel = "futures.book_ticker";
                payloadJson = fmt::format(R"(["{}"])", originInstId);
            }
            else if (marketTypeEnum == md::DEPTH5 || marketTypeEnum == md::DEPTH10 || marketTypeEnum == md::DEPTH20) {
                int levels = (marketTypeEnum == md::DEPTH5) ? 5 : (marketTypeEnum == md::DEPTH10) ? 10 : 20;
                channel = "futures.order_book";
                payloadJson = fmt::format(R"(["{}","{}","0"])", originInstId, levels);
            }
            else if (marketTypeEnum == md::TRADES) {
                channel = "futures.trades";
                payloadJson = fmt::format(R"(["{}"])", originInstId);  
            }
            else if (marketTypeEnum == md::KLINE_1m) {
                channel = "futures.candlesticks";
                payloadJson = fmt::format(R"(["1m","{}"])", originInstId);  
            }
            else if (marketTypeEnum == md::FUNDING_RATE) {
                channel = "futures.tickers";
                payloadJson = fmt::format(R"(["{}"])", originInstId);  
            }
            else {
                LOG_ERROR("not support {}", marketTypeStr);
                continue;
            }     
        }
        else {
            LOG_ERROR("not support subMarketType: {}", instTypeStr);
            continue;
        }

        std::string subJson = fmt::format(R"({{"time":{},"channel":"{}","event":"subscribe","payload":{}}})", timeSec, channel, payloadJson);
        cfg.subscribe_messages.push_back(subJson);
    }

    LOG_INFO("{} ws url: {}, {} subscribe msgs prepared", exchIdStr, cfg.url, cfg.subscribe_messages.size());
    for (auto& s : cfg.subscribe_messages) {
        LOG_INFO(" sub: {}", s);
    }

}

// subWebsocekt / ping / pong 由 BaseUnit + BeastWsClient 处理

void md::GateioUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool /*isBinary*/, int64_t /*ns*/) {
    latestDataUpdateTime = crypto::getCurrentTime();

    // Gateio pong 回执: {"time":XXXX,"channel":"spot.pong",...} 或 futures.pong
    // 简单方式: 看 channel 是否含 "pong" 就 skip
    std::string_view sv(reinterpret_cast<const char*>(data), len);
    if (sv.find("\".ping\"") != std::string_view::npos || sv.find("\".pong\"") != std::string_view::npos
        || sv.find("\"spot.pong\"") != std::string_view::npos
        || sv.find("\"futures.pong\"") != std::string_view::npos) {
        return;
    }

    std::string msg(sv);
    std::cout << "onWebsocketMsg: " << msg << std::endl;
    mQueue.push(std::move(msg));
}

//处理消息 解析json并发送给redis或共享内存
void md::GateioUnit::parseMarketData(const std::string& msg) {
    switch (instTypeEnum) {
        case SPOT:
            parseSpotData(msg);
            return;
        case USDT_SWAP:
            parseSwapData(msg);
            return;
        default:
            return;
    }
}

void md::GateioUnit::parseSpotData(const std::string& msg) {
    int64_t tsNet = crypto::getCurrentTime();

    simdjson::padded_string paddedMsg(msg);
    auto doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("simdjson parse msg: {} error: {}", msg, simdjson::error_message(doc.error()));
        return;
    }

    int64_t tsM = 0;
    if (doc["time_ms"].get(tsM) == simdjson::SUCCESS) {
        tsM *= 1000;
    }

    std::string_view eventStr;
    if (doc["event"].get(eventStr)) {
        LOG_ERROR("msg has no event field or not update msg: {}", msg);
        return;   
    }

    if (eventStr != "update") {
        LOG_ERROR("not update msg: {}", msg);
        return;
    }

    auto data = doc["result"];
    if (data.error()) {
        LOG_ERROR("msg has no data field! msg: {}", msg);
        return;
    }

    md::InstrumentInfo info;
    switch (marketTypeEnum) {
        case md::DEPTH1: {
            md::Depth1 depth1;
            memset(&depth1, 0, sizeof(md::Depth1));
            depth1.exchangeTypeEnum = exchangeTypeEnum;
            depth1.instTypeEnum = instTypeEnum;
            depth1.marketTypeEnum = marketTypeEnum;
            strncpy(depth1.instId, info.instId, INSTID_SIZE);

            int64_t tsT = 0;
            data["t"].get(tsT);
            tsT *= 1000;

            depth1.tsTrans = tsT;
            depth1.tsEvent = tsT;
            depth1.tsRecv = tsNet;

            std::string originInstId = "";
            std::string_view sVal;
            if (data["s"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }
            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false){
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

            auto bp1 = data["b"];
            auto bv1 = data["B"];
            auto ap1 = data["a"];
            auto av1 = data["A"];
            
            std::string_view bidPriceStr;
            std::string_view bidVolStr;
            std::string_view askPriceStr;
            std::string_view askVolStr;
            if (bp1.get(bidPriceStr) || bv1.get(bidVolStr) || ap1.get(askPriceStr) || av1.get(askVolStr)) {
                LOG_ERROR("Failed to get string values, msg: {}", msg);
                return;
            }

            depth1.bp1 = crypto::fast_atod(bidPriceStr) * info.reduceNumber;
            depth1.bv1 = crypto::fast_atod(bidVolStr) * info.magnifyNumber;
            depth1.ap1 = crypto::fast_atod(askPriceStr) * info.reduceNumber;
            depth1.av1 = crypto::fast_atod(askVolStr) * info.magnifyNumber;

            depth1.tsParse = crypto::getCurrentTime();

            std::cout << depth1.getString() << std::endl;
#ifdef NEED_SHM
            mDepth1Publisher[key]->push(depth1);                
#endif
            return;
        }

        case md::DEPTH5: {
            md::Depth5 depth5;
            memset(&depth5, 0, sizeof(md::Depth5));
            depth5.exchangeTypeEnum = exchangeTypeEnum;
            depth5.instTypeEnum = instTypeEnum;
            depth5.marketTypeEnum = marketTypeEnum;
            strncpy(depth5.instId, info.instId, INSTID_SIZE);

            int64_t tsT = 0;
            data["t"].get(tsT);
            int64_t ts = tsT * 1000;

            depth5.tsTrans = ts;
            depth5.tsEvent = ts;
            depth5.tsRecv = tsNet;

            std::string originInstId = "";
            std::string_view sVal;
            if (data["s"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }
            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false){
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

            auto bidsArray = data["bids"];
            std::string_view bidPrice[5];
            std::string_view bidVol[5];

            size_t bidsCount = 0;
            for (auto bidLevel : bidsArray) {
                if (bidsCount >= 5) {
                    break;
                }

                auto it = bidLevel.begin();
                if ((*it).get(bidPrice[bidsCount])) {
                    break;
                } 
                ++it;
                
                if ((*it).get(bidVol[bidsCount])) {
                    break;
                } 

                bidsCount++;
            }

            if (bidsCount == 5) {
                depth5.bp1 = crypto::fast_atod(bidPrice[0]) * info.reduceNumber;
                depth5.bv1 = crypto::fast_atod(bidVol[0]) * info.magnifyNumber;
                depth5.bp2 = crypto::fast_atod(bidPrice[1]) * info.reduceNumber;
                depth5.bv2 = crypto::fast_atod(bidVol[1]) * info.magnifyNumber;
                depth5.bp3 = crypto::fast_atod(bidPrice[2]) * info.reduceNumber;
                depth5.bv3 = crypto::fast_atod(bidVol[2]) * info.magnifyNumber;
                depth5.bp4 = crypto::fast_atod(bidPrice[3]) * info.reduceNumber;
                depth5.bv4 = crypto::fast_atod(bidVol[3]) * info.magnifyNumber;
                depth5.bp5 = crypto::fast_atod(bidPrice[4]) * info.reduceNumber;
                depth5.bv5 = crypto::fast_atod(bidVol[4]) * info.magnifyNumber;
            }

            auto asksArray = data["asks"];
            std::string_view askPrice[5];
            std::string_view askVol[5];

            size_t asksCount = 0;
            for (auto askLevel : asksArray) {
                if (asksCount >= 5) {
                    break;
                }

                auto it = askLevel.begin();
                if ((*it).get(askPrice[asksCount])) {
                    break;
                } 
                ++it;
                
                if ((*it).get(askVol[asksCount])) {
                    break;
                } 

                asksCount++;
            }

            if (asksCount == 5) {
                depth5.ap1 = crypto::fast_atod(askPrice[0]) * info.reduceNumber;
                depth5.av1 = crypto::fast_atod(askVol[0]) * info.magnifyNumber;
                depth5.ap2 = crypto::fast_atod(askPrice[1]) * info.reduceNumber;
                depth5.av2 = crypto::fast_atod(askVol[1]) * info.magnifyNumber;
                depth5.ap3 = crypto::fast_atod(askPrice[2]) * info.reduceNumber;
                depth5.av3 = crypto::fast_atod(askVol[2]) * info.magnifyNumber;
                depth5.ap4 = crypto::fast_atod(askPrice[3]) * info.reduceNumber;
                depth5.av4 = crypto::fast_atod(askVol[3]) * info.magnifyNumber;
                depth5.ap5 = crypto::fast_atod(askPrice[4]) * info.reduceNumber;
                depth5.av5 = crypto::fast_atod(askVol[4]) * info.magnifyNumber;
            }

            depth5.tsParse = crypto::getCurrentTime();

            std::cout << depth5.getString() << std::endl;

#ifdef NEED_SHM
            mDepth5Publisher[key]->push(depth5);                
#endif
            return;
        }  
        case md::DEPTH10: {
            md::Depth10 depth10;
            memset(&depth10, 0, sizeof(md::Depth10));
            depth10.exchangeTypeEnum = exchangeTypeEnum;
            depth10.instTypeEnum = instTypeEnum;
            depth10.marketTypeEnum = marketTypeEnum;
            strncpy(depth10.instId, info.instId, INSTID_SIZE);

            int64_t tsT = 0;
            data["t"].get(tsT);
            int64_t ts = tsT * 1000;

            depth10.tsTrans = ts;
            depth10.tsEvent = ts;
            depth10.tsRecv = tsNet;

            std::string originInstId = "";
            std::string_view sVal;
            if (data["s"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }
            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false){
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

            auto bidsArray = data["bids"];
            std::string_view bidPrice[10];
            std::string_view bidVol[10];

            size_t bidsCount = 0;
            for (auto bidLevel : bidsArray) {
                if (bidsCount >= 10) {
                    break;
                }

                auto it = bidLevel.begin();
                if ((*it).get(bidPrice[bidsCount])) {
                    break;
                } 
                ++it;
                
                if ((*it).get(bidVol[bidsCount])) {
                    break;
                } 

                bidsCount++;
            }

            if (bidsCount == 10) {
                depth10.bp1 = crypto::fast_atod(bidPrice[0]) * info.reduceNumber;
                depth10.bv1 = crypto::fast_atod(bidVol[0]) * info.magnifyNumber;
                depth10.bp2 = crypto::fast_atod(bidPrice[1]) * info.reduceNumber;
                depth10.bv2 = crypto::fast_atod(bidVol[1]) * info.magnifyNumber;
                depth10.bp3 = crypto::fast_atod(bidPrice[2]) * info.reduceNumber;
                depth10.bv3 = crypto::fast_atod(bidVol[2]) * info.magnifyNumber;
                depth10.bp4 = crypto::fast_atod(bidPrice[3]) * info.reduceNumber;
                depth10.bv4 = crypto::fast_atod(bidVol[3]) * info.magnifyNumber;
                depth10.bp5 = crypto::fast_atod(bidPrice[4]) * info.reduceNumber;
                depth10.bv5 = crypto::fast_atod(bidVol[4]) * info.magnifyNumber;
                depth10.bp6 = crypto::fast_atod(bidPrice[5]) * info.reduceNumber;
                depth10.bv6 = crypto::fast_atod(bidVol[5]) * info.magnifyNumber;
                depth10.bp7 = crypto::fast_atod(bidPrice[6]) * info.reduceNumber;
                depth10.bv7 = crypto::fast_atod(bidVol[6]) * info.magnifyNumber;
                depth10.bp8 = crypto::fast_atod(bidPrice[7]) * info.reduceNumber;
                depth10.bv8 = crypto::fast_atod(bidVol[7]) * info.magnifyNumber;
                depth10.bp9 = crypto::fast_atod(bidPrice[8]) * info.reduceNumber;
                depth10.bv9 = crypto::fast_atod(bidVol[8]) * info.magnifyNumber;
                depth10.bp10 = crypto::fast_atod(bidPrice[9]) * info.reduceNumber;
                depth10.bv10 = crypto::fast_atod(bidVol[9]) * info.magnifyNumber;
            }

            auto asksArray = data["asks"];
            std::string_view askPrice[10];
            std::string_view askVol[10];

            size_t asksCount = 0;
            for (auto askLevel : asksArray) {
                if (asksCount >= 10) {
                    break;
                }

                auto it = askLevel.begin();
                if ((*it).get(askPrice[asksCount])) {
                    break;
                } 
                ++it;
                
                if ((*it).get(askVol[asksCount])) {
                    break;
                } 

                asksCount++;
            }

            if (asksCount == 10) {
                depth10.ap1 = crypto::fast_atod(askPrice[0]) * info.reduceNumber;
                depth10.av1 = crypto::fast_atod(askVol[0]) * info.magnifyNumber;
                depth10.ap2 = crypto::fast_atod(askPrice[1]) * info.reduceNumber;
                depth10.av2 = crypto::fast_atod(askVol[1]) * info.magnifyNumber;
                depth10.ap3 = crypto::fast_atod(askPrice[2]) * info.reduceNumber;
                depth10.av3 = crypto::fast_atod(askVol[2]) * info.magnifyNumber;
                depth10.ap4 = crypto::fast_atod(askPrice[3]) * info.reduceNumber;
                depth10.av4 = crypto::fast_atod(askVol[3]) * info.magnifyNumber;
                depth10.ap5 = crypto::fast_atod(askPrice[4]) * info.reduceNumber;
                depth10.av5 = crypto::fast_atod(askVol[4]) * info.magnifyNumber;
                depth10.ap6 = crypto::fast_atod(askPrice[5]) * info.reduceNumber;
                depth10.av6 = crypto::fast_atod(askVol[5]) * info.magnifyNumber;
                depth10.ap7 = crypto::fast_atod(askPrice[6]) * info.reduceNumber;
                depth10.av7 = crypto::fast_atod(askVol[6]) * info.magnifyNumber;
                depth10.ap8 = crypto::fast_atod(askPrice[7]) * info.reduceNumber;
                depth10.av8 = crypto::fast_atod(askVol[7]) * info.magnifyNumber;
                depth10.ap9 = crypto::fast_atod(askPrice[8]) * info.reduceNumber;
                depth10.av9 = crypto::fast_atod(askVol[8]) * info.magnifyNumber;
                depth10.ap10 = crypto::fast_atod(askPrice[9]) * info.reduceNumber;
                depth10.av10 = crypto::fast_atod(askVol[9]) * info.magnifyNumber;
            }

            depth10.tsParse = crypto::getCurrentTime();

            std::cout << depth10.getString() << std::endl;

#ifdef NEED_SHM
            mDepth10Publisher[key]->push(depth10);                
#endif
            return;    
        }
        case md::DEPTH20: {
            md::Depth20 depth20;
            memset(&depth20, 0, sizeof(md::Depth20));
            depth20.exchangeTypeEnum = exchangeTypeEnum;
            depth20.instTypeEnum = instTypeEnum;
            depth20.marketTypeEnum = marketTypeEnum;
            strncpy(depth20.instId, info.instId, INSTID_SIZE);

            int64_t tsT = 0;
            data["t"].get(tsT);
            int64_t ts = tsT * 1000;

            depth20.tsTrans = ts;
            depth20.tsEvent = ts;
            depth20.tsRecv = tsNet;

            std::string originInstId = "";
            std::string_view sVal;
            if (data["s"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }
            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false){
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

            auto bidsArray = data["bids"];
            std::string_view bidPrice[20];
            std::string_view bidVol[20];

            size_t bidsCount = 0;
            for (auto bidLevel : bidsArray) {
                if (bidsCount >= 20) {
                    break;
                }

                auto it = bidLevel.begin();
                if ((*it).get(bidPrice[bidsCount])) {
                    break;
                } 
                ++it;
                
                if ((*it).get(bidVol[bidsCount])) {
                    break;
                } 

                bidsCount++;
            }

            if (bidsCount == 20) {
                depth20.bp1 = crypto::fast_atod(bidPrice[0]) * info.reduceNumber;
                depth20.bv1 = crypto::fast_atod(bidVol[0]) * info.magnifyNumber;
                depth20.bp2 = crypto::fast_atod(bidPrice[1]) * info.reduceNumber;
                depth20.bv2 = crypto::fast_atod(bidVol[1]) * info.magnifyNumber;
                depth20.bp3 = crypto::fast_atod(bidPrice[2]) * info.reduceNumber;
                depth20.bv3 = crypto::fast_atod(bidVol[2]) * info.magnifyNumber;
                depth20.bp4 = crypto::fast_atod(bidPrice[3]) * info.reduceNumber;
                depth20.bv4 = crypto::fast_atod(bidVol[3]) * info.magnifyNumber;
                depth20.bp5 = crypto::fast_atod(bidPrice[4]) * info.reduceNumber;
                depth20.bv5 = crypto::fast_atod(bidVol[4]) * info.magnifyNumber;
                depth20.bp6 = crypto::fast_atod(bidPrice[5]) * info.reduceNumber;
                depth20.bv6 = crypto::fast_atod(bidVol[5]) * info.magnifyNumber;
                depth20.bp7 = crypto::fast_atod(bidPrice[6]) * info.reduceNumber;
                depth20.bv7 = crypto::fast_atod(bidVol[6]) * info.magnifyNumber;
                depth20.bp8 = crypto::fast_atod(bidPrice[7]) * info.reduceNumber;
                depth20.bv8 = crypto::fast_atod(bidVol[7]) * info.magnifyNumber;
                depth20.bp9 = crypto::fast_atod(bidPrice[8]) * info.reduceNumber;
                depth20.bv9 = crypto::fast_atod(bidVol[8]) * info.magnifyNumber;
                depth20.bp10 = crypto::fast_atod(bidPrice[9]) * info.reduceNumber;
                depth20.bv10 = crypto::fast_atod(bidVol[9]) * info.magnifyNumber;
                depth20.bp11 = crypto::fast_atod(bidPrice[10]) * info.reduceNumber;
                depth20.bv11 = crypto::fast_atod(bidVol[10]) * info.magnifyNumber;
                depth20.bp12 = crypto::fast_atod(bidPrice[11]) * info.reduceNumber;
                depth20.bv12 = crypto::fast_atod(bidVol[11]) * info.magnifyNumber;
                depth20.bp13 = crypto::fast_atod(bidPrice[12]) * info.reduceNumber;
                depth20.bv13 = crypto::fast_atod(bidVol[12]) * info.magnifyNumber;
                depth20.bp14 = crypto::fast_atod(bidPrice[13]) * info.reduceNumber;
                depth20.bv14 = crypto::fast_atod(bidVol[13]) * info.magnifyNumber;
                depth20.bp15 = crypto::fast_atod(bidPrice[14]) * info.reduceNumber;
                depth20.bv15 = crypto::fast_atod(bidVol[14]) * info.magnifyNumber;
                depth20.bp16 = crypto::fast_atod(bidPrice[15]) * info.reduceNumber;
                depth20.bv16 = crypto::fast_atod(bidVol[15]) * info.magnifyNumber;
                depth20.bp17 = crypto::fast_atod(bidPrice[16]) * info.reduceNumber;
                depth20.bv17 = crypto::fast_atod(bidVol[16]) * info.magnifyNumber;
                depth20.bp18 = crypto::fast_atod(bidPrice[17]) * info.reduceNumber;
                depth20.bv18 = crypto::fast_atod(bidVol[17]) * info.magnifyNumber;
                depth20.bp19 = crypto::fast_atod(bidPrice[18]) * info.reduceNumber;
                depth20.bv19 = crypto::fast_atod(bidVol[18]) * info.magnifyNumber;
                depth20.bp20 = crypto::fast_atod(bidPrice[19]) * info.reduceNumber;
                depth20.bv20 = crypto::fast_atod(bidVol[19]) * info.magnifyNumber;
            }

            auto asksArray = data["asks"];
            std::string_view askPrice[20];
            std::string_view askVol[20];

            size_t asksCount = 0;
            for (auto askLevel : asksArray) {
                if (asksCount >= 20) {
                    break;
                }

                auto it = askLevel.begin();
                if ((*it).get(askPrice[asksCount])) {
                    break;
                } 
                ++it;
                
                if ((*it).get(askVol[asksCount])) {
                    break;
                } 

                asksCount++;
            }

            if (asksCount == 20) {
                depth20.ap1 = crypto::fast_atod(askPrice[0]) * info.reduceNumber;
                depth20.av1 = crypto::fast_atod(askVol[0]) * info.magnifyNumber;
                depth20.ap2 = crypto::fast_atod(askPrice[1]) * info.reduceNumber;
                depth20.av2 = crypto::fast_atod(askVol[1]) * info.magnifyNumber;
                depth20.ap3 = crypto::fast_atod(askPrice[2]) * info.reduceNumber;
                depth20.av3 = crypto::fast_atod(askVol[2]) * info.magnifyNumber;
                depth20.ap4 = crypto::fast_atod(askPrice[3]) * info.reduceNumber;
                depth20.av4 = crypto::fast_atod(askVol[3]) * info.magnifyNumber;
                depth20.ap5 = crypto::fast_atod(askPrice[4]) * info.reduceNumber;
                depth20.av5 = crypto::fast_atod(askVol[4]) * info.magnifyNumber;
                depth20.ap6 = crypto::fast_atod(askPrice[5]) * info.reduceNumber;
                depth20.av6 = crypto::fast_atod(askVol[5]) * info.magnifyNumber;
                depth20.ap7 = crypto::fast_atod(askPrice[6]) * info.reduceNumber;
                depth20.av7 = crypto::fast_atod(askVol[6]) * info.magnifyNumber;
                depth20.ap8 = crypto::fast_atod(askPrice[7]) * info.reduceNumber;
                depth20.av8 = crypto::fast_atod(askVol[7]) * info.magnifyNumber;
                depth20.ap9 = crypto::fast_atod(askPrice[8]) * info.reduceNumber;
                depth20.av9 = crypto::fast_atod(askVol[8]) * info.magnifyNumber;
                depth20.ap10 = crypto::fast_atod(askPrice[9]) * info.reduceNumber;
                depth20.av10 = crypto::fast_atod(askVol[9]) * info.magnifyNumber;
                depth20.ap11 = crypto::fast_atod(askPrice[10]) * info.reduceNumber;
                depth20.av11 = crypto::fast_atod(askVol[10]) * info.magnifyNumber;
                depth20.ap12 = crypto::fast_atod(askPrice[11]) * info.reduceNumber;
                depth20.av12 = crypto::fast_atod(askVol[11]) * info.magnifyNumber;
                depth20.ap13 = crypto::fast_atod(askPrice[12]) * info.reduceNumber;
                depth20.av13 = crypto::fast_atod(askVol[12]) * info.magnifyNumber;
                depth20.ap14 = crypto::fast_atod(askPrice[13]) * info.reduceNumber;
                depth20.av14 = crypto::fast_atod(askVol[13]) * info.magnifyNumber;
                depth20.ap15 = crypto::fast_atod(askPrice[14]) * info.reduceNumber;
                depth20.av15 = crypto::fast_atod(askVol[14]) * info.magnifyNumber;
                depth20.ap16 = crypto::fast_atod(askPrice[15]) * info.reduceNumber;
                depth20.av16 = crypto::fast_atod(askVol[15]) * info.magnifyNumber;
                depth20.ap17 = crypto::fast_atod(askPrice[16]) * info.reduceNumber;
                depth20.av17 = crypto::fast_atod(askVol[16]) * info.magnifyNumber;
                depth20.ap18 = crypto::fast_atod(askPrice[17]) * info.reduceNumber;
                depth20.av18 = crypto::fast_atod(askVol[17]) * info.magnifyNumber;
                depth20.ap19 = crypto::fast_atod(askPrice[18]) * info.reduceNumber;
                depth20.av19 = crypto::fast_atod(askVol[18]) * info.magnifyNumber;
                depth20.ap20 = crypto::fast_atod(askPrice[19]) * info.reduceNumber;
                depth20.av20 = crypto::fast_atod(askVol[19]) * info.magnifyNumber;
            }

            depth20.tsParse = crypto::getCurrentTime();

            std::cout << depth20.getString() << std::endl;

#ifdef NEED_SHM
            mDepth20Publisher[key]->push(depth20);                
#endif
            return;    
        }
        case md::TRADES: {
            md::Trades trades;
            memset(&trades, 0, sizeof(md::Trades));
            trades.exchangeTypeEnum = exchangeTypeEnum;
            trades.instTypeEnum = instTypeEnum;
            trades.marketTypeEnum = marketTypeEnum;
            strncpy(trades.instId, info.instId, INSTID_SIZE);

            int64_t tradeId;
            data["id"].get(tradeId);
            fmt::format_to(trades.tradeId, "{}", tradeId);

            int64_t tsT = 0;
            data["create_time_ms"].get(tsT);
            int64_t ts = tsT * 1000;

            trades.tsTrans = ts;
            trades.tsEvent = ts;
            trades.tsRecv = tsNet;

            std::string_view sideStr;
            data["side"].get(sideStr);
            if (sideStr == "sell") {
                trades.direction = DT_SHORT;
            }
            else if (sideStr == "buy") {
                trades.direction = DT_LONG;
            }

            std::string originInstId = "";
            std::string_view sVal;
            if (data["currency_pair"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }

            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false){
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

            std::string_view tradeVolStr;
            std::string_view tradePriceStr;
            data["amount"].get(tradeVolStr);
            data["price"].get(tradePriceStr);
            
            trades.px = crypto::fast_atod(tradePriceStr) * info.reduceNumber;
            trades.sz = crypto::fast_atod(tradeVolStr) * info.magnifyNumber;

            trades.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
            mTradesPublisher[key]->push(trades); 
#endif
            return;
        }
        case md::KLINE_1m: {
            md::Kline kline;
            memset(&kline, 0, sizeof(md::Kline));
            kline.exchangeTypeEnum = exchangeTypeEnum;
            kline.instTypeEnum = instTypeEnum;
            kline.marketTypeEnum = marketTypeEnum;
            strncpy(kline.instId, info.instId, INSTID_SIZE);

            kline.tsTrans = tsM;
            kline.tsEvent = tsM;
            kline.tsRecv = tsNet;

            std::string_view barTimeStr;
            std::string_view highPriceStr;
            std::string_view lowPriceStr;
            std::string_view openPriceStr;
            std::string_view closePriceStr;
            std::string_view amountStr;
            std::string_view volStr;

            data["t"].get(barTimeStr);
            data["v"].get(volStr);
            data["c"].get(closePriceStr);
            data["h"].get(highPriceStr);
            data["l"].get(lowPriceStr);
            data["o"].get(openPriceStr);

            std::string originInstId = "";
            std::string_view nVal;
            if (data["n"].get(nVal) == simdjson::SUCCESS) {
                std::vector<std::string> v = crypto::split(std::string(nVal), "_");
                if (v.size() >= 3) {
                    originInstId = v[1] + "_" + v[2];
                }
            }

            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false){
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);


            data["a"].get(amountStr);
            
            kline.barTime = crypto::fast_atol(barTimeStr) * 1000000;
            kline.highPrice = crypto::fast_atod(highPriceStr) * info.reduceNumber;
            kline.lowPrice = crypto::fast_atod(lowPriceStr) * info.reduceNumber;
            kline.openPrice = crypto::fast_atod(openPriceStr) * info.reduceNumber;
            kline.closePrice = crypto::fast_atod(closePriceStr) * info.reduceNumber;

            double avgPrice = 0;
            double amount = crypto::fast_atod(amountStr);
            double volume = crypto::fast_atod(volStr);
            if(amount > ZERO_NUM) {
                avgPrice = amount / volume;
            }
            kline.avgPrice = avgPrice * info.reduceNumber;
            kline.totalVolume = volume * info.magnifyNumber;
            kline.totalAmount = amount;

            bool isFinished = false;
            data["w"].get(isFinished);
            kline.isFinished = isFinished;

            if (!kline.isFinished) {
                return;
            }

            kline.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
            mKlinePublisher[key]->push(kline); 
#endif
            return;
        }
        default: {
            LOG_ERROR("not support marketType: {}", md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }
    }
}

void md::GateioUnit::parseSwapData(const std::string& msg) {
    int64_t tsNet = crypto::getCurrentTime();

    simdjson::padded_string paddedMsg(msg);
    auto doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("simdjson parse msg: {} error: {}", msg, simdjson::error_message(doc.error()));
        return;
    }

    int64_t tsM = 0;
    if (doc["time_ms"].get(tsM) == simdjson::SUCCESS) {
        tsM *= 1000;
    }
    
    std::string_view eventStr;
    if (doc["event"].get(eventStr)) {
        LOG_ERROR("msg has no event field or not update msg: {}", msg);
        return;   
    }

    if (eventStr != "update" && eventStr != "all") {
        LOG_ERROR("not update or all msg: {}", msg);
        return;
    }

    auto data = doc["result"];
    if (data.error()) {
        LOG_ERROR("msg has no data field! msg: {}", msg);
        return;
    }

    std::string originInstId = "";

    switch (marketTypeEnum) {
        case md::DEPTH1: {
            std::string_view sVal;
            if (data["s"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }
            break;
        }
        case md::DEPTH5:
        case md::DEPTH10:
        case md::DEPTH20: {
            std::string_view sVal;
            if (data["contract"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }
            break;
        }
        case md::TRADES:
        case md::FUNDING_RATE: {
            auto it = data.begin();
            auto firstElement = *it;
            std::string_view sVal;
            if (firstElement["contract"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }
            break;        
        }
        case md::KLINE_1m: {
            for (auto element : data) {
                std::string_view nStr;
                if (element["n"].get(nStr) != simdjson::SUCCESS) {
                    continue;
                }

                std::vector<std::string> v = crypto::split(std::string(nStr), "_");
                if (v.size() >= 3) {
                    originInstId = v[1] + "_" + v[2];
                    break;
                }
            }
            break;
        }
        default: {
            LOG_ERROR("not support marketType: {}", md::MarketTypeEnum2StrMap[marketTypeEnum]);
        }
    }

    md::InstrumentInfo info;
    if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
        LOG_ERROR("smc cannot find originInstId: {}", originInstId);
        return;
    }

    std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

    switch (marketTypeEnum) {
        case md::DEPTH1: {
            md::Depth1 depth1;
            memset(&depth1, 0, sizeof(md::Depth1));
            depth1.exchangeTypeEnum = exchangeTypeEnum;
            depth1.instTypeEnum = instTypeEnum;
            depth1.marketTypeEnum = marketTypeEnum;
            strncpy(depth1.instId, info.instId, INSTID_SIZE);

            int64_t tsT = 0;
            data["t"].get(tsT);
            int64_t ts = tsT * 1000;
            depth1.tsTrans = ts;
            depth1.tsEvent = ts;
            depth1.tsRecv = tsNet;

            std::string originInstId = "";
            std::string_view sVal;
            if (data["s"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }

            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

            auto bp1 = data["b"];
            auto bv1 = data["B"];
            auto ap1 = data["a"];
            auto av1 = data["A"];
            
            std::string_view bidPriceStr;
            std::string_view bidVolStr;
            std::string_view askPriceStr;
            std::string_view askVolStr;
            if (bp1.get(bidPriceStr) || bv1.get(bidVolStr) || ap1.get(askPriceStr) || av1.get(askVolStr)) {
                LOG_ERROR("Failed to get string values, msg: {}", msg);
                return;
            }

            depth1.bp1 = crypto::fast_atod(bidPriceStr) * info.reduceNumber;
            depth1.bv1 = crypto::fast_atod(bidVolStr) * info.magnifyNumber;
            depth1.ap1 = crypto::fast_atod(askPriceStr) * info.reduceNumber;
            depth1.av1 = crypto::fast_atod(askVolStr) * info.magnifyNumber;

            depth1.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
            mDepth1Publisher[key]->push(depth1);                
#endif
            return;
        }

        case md::DEPTH5: {
            md::Depth5 depth5;
            memset(&depth5, 0, sizeof(md::Depth5));
            depth5.exchangeTypeEnum = exchangeTypeEnum;
            depth5.instTypeEnum = instTypeEnum;
            depth5.marketTypeEnum = marketTypeEnum;
            strncpy(depth5.instId, info.instId, INSTID_SIZE);

            int64_t tsT = 0;
            data["t"].get(tsT);
            int64_t ts = tsT * 1000;

            depth5.tsTrans = ts;
            depth5.tsEvent = ts;
            depth5.tsRecv = tsNet;

            std::string originInstId = "";
            std::string_view sVal;
            if (data["contract"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }

            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

            auto bidsArray = data["bids"];
            std::string_view bidPrice[5];
            std::string_view bidVol[5];

            size_t bidsCount = 0;
            for (auto bidLevel : bidsArray) {
                if (bidsCount >= 5) {
                    break;
                }

                bidLevel["p"].get(bidPrice[bidsCount]);
                bidLevel["s"].get(bidVol[bidsCount]);

                bidsCount++;
            }

            if (bidsCount == 5) {
                depth5.bp1 = crypto::fast_atod(bidPrice[0]) * info.reduceNumber;
                depth5.bv1 = crypto::fast_atod(bidVol[0]) * info.magnifyNumber;
                depth5.bp2 = crypto::fast_atod(bidPrice[1]) * info.reduceNumber;
                depth5.bv2 = crypto::fast_atod(bidVol[1]) * info.magnifyNumber;
                depth5.bp3 = crypto::fast_atod(bidPrice[2]) * info.reduceNumber;
                depth5.bv3 = crypto::fast_atod(bidVol[2]) * info.magnifyNumber;
                depth5.bp4 = crypto::fast_atod(bidPrice[3]) * info.reduceNumber;
                depth5.bv4 = crypto::fast_atod(bidVol[3]) * info.magnifyNumber;
                depth5.bp5 = crypto::fast_atod(bidPrice[4]) * info.reduceNumber;
                depth5.bv5 = crypto::fast_atod(bidVol[4]) * info.magnifyNumber;
            }

            auto asksArray = data["asks"];
            std::string_view askPrice[5];
            std::string_view askVol[5];

            size_t asksCount = 0;
            for (auto askLevel : asksArray) {
                if (asksCount >= 5) {
                    break;
                }

                askLevel["p"].get(askPrice[asksCount]);
                askLevel["s"].get(askVol[asksCount]);

                asksCount++;
            }

            if (asksCount == 5) {
                depth5.ap1 = crypto::fast_atod(askPrice[0]) * info.reduceNumber;
                depth5.av1 = crypto::fast_atod(askVol[0]) * info.magnifyNumber;
                depth5.ap2 = crypto::fast_atod(askPrice[1]) * info.reduceNumber;
                depth5.av2 = crypto::fast_atod(askVol[1]) * info.magnifyNumber;
                depth5.ap3 = crypto::fast_atod(askPrice[2]) * info.reduceNumber;
                depth5.av3 = crypto::fast_atod(askVol[2]) * info.magnifyNumber;
                depth5.ap4 = crypto::fast_atod(askPrice[3]) * info.reduceNumber;
                depth5.av4 = crypto::fast_atod(askVol[3]) * info.magnifyNumber;
                depth5.ap5 = crypto::fast_atod(askPrice[4]) * info.reduceNumber;
                depth5.av5 = crypto::fast_atod(askVol[4]) * info.magnifyNumber;
            }

            depth5.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
            mDepth5Publisher[key]->push(depth5);                
#endif
            return;
        }  
        case md::DEPTH10: {
            md::Depth10 depth10;
            memset(&depth10, 0, sizeof(md::Depth10));
            depth10.exchangeTypeEnum = exchangeTypeEnum;
            depth10.instTypeEnum = instTypeEnum;
            depth10.marketTypeEnum = marketTypeEnum;
            strncpy(depth10.instId, info.instId, INSTID_SIZE);

            int64_t tsT = 0;
            data["t"].get(tsT);
            int64_t ts = tsT * 1000;

            depth10.tsTrans = ts;
            depth10.tsEvent = ts;
            depth10.tsRecv = tsNet;

            std::string originInstId = "";
            std::string_view sVal;
            if (data["contract"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }

            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

            auto bidsArray = data["bids"];
            std::string_view bidPrice[10];
            std::string_view bidVol[10];

            size_t bidsCount = 0;
            for (auto bidLevel : bidsArray) {
                if (bidsCount >= 10) {
                    break;
                }
                
                bidLevel["p"].get(bidPrice[bidsCount]);
                bidLevel["s"].get(bidVol[bidsCount]);

                bidsCount++;
            }

            if (bidsCount == 10) {
                depth10.bp1 = crypto::fast_atod(bidPrice[0]) * info.reduceNumber;
                depth10.bv1 = crypto::fast_atod(bidVol[0]) * info.magnifyNumber;
                depth10.bp2 = crypto::fast_atod(bidPrice[1]) * info.reduceNumber;
                depth10.bv2 = crypto::fast_atod(bidVol[1]) * info.magnifyNumber;
                depth10.bp3 = crypto::fast_atod(bidPrice[2]) * info.reduceNumber;
                depth10.bv3 = crypto::fast_atod(bidVol[2]) * info.magnifyNumber;
                depth10.bp4 = crypto::fast_atod(bidPrice[3]) * info.reduceNumber;
                depth10.bv4 = crypto::fast_atod(bidVol[3]) * info.magnifyNumber;
                depth10.bp5 = crypto::fast_atod(bidPrice[4]) * info.reduceNumber;
                depth10.bv5 = crypto::fast_atod(bidVol[4]) * info.magnifyNumber;
                depth10.bp6 = crypto::fast_atod(bidPrice[5]) * info.reduceNumber;
                depth10.bv6 = crypto::fast_atod(bidVol[5]) * info.magnifyNumber;
                depth10.bp7 = crypto::fast_atod(bidPrice[6]) * info.reduceNumber;
                depth10.bv7 = crypto::fast_atod(bidVol[6]) * info.magnifyNumber;
                depth10.bp8 = crypto::fast_atod(bidPrice[7]) * info.reduceNumber;
                depth10.bv8 = crypto::fast_atod(bidVol[7]) * info.magnifyNumber;
                depth10.bp9 = crypto::fast_atod(bidPrice[8]) * info.reduceNumber;
                depth10.bv9 = crypto::fast_atod(bidVol[8]) * info.magnifyNumber;
                depth10.bp10 = crypto::fast_atod(bidPrice[9]) * info.reduceNumber;
                depth10.bv10 = crypto::fast_atod(bidVol[9]) * info.magnifyNumber;
            }

            auto asksArray = data["asks"];
            std::string_view askPrice[10];
            std::string_view askVol[10];

            size_t asksCount = 0;
            for (auto askLevel : asksArray) {
                if (asksCount >= 10) {
                    break;
                }
                
                askLevel["p"].get(askPrice[asksCount]);
                askLevel["s"].get(askVol[asksCount]);

                asksCount++;
            }

            if (asksCount == 10) {
                depth10.ap1 = crypto::fast_atod(askPrice[0]) * info.reduceNumber;
                depth10.av1 = crypto::fast_atod(askVol[0]) * info.magnifyNumber;
                depth10.ap2 = crypto::fast_atod(askPrice[1]) * info.reduceNumber;
                depth10.av2 = crypto::fast_atod(askVol[1]) * info.magnifyNumber;
                depth10.ap3 = crypto::fast_atod(askPrice[2]) * info.reduceNumber;
                depth10.av3 = crypto::fast_atod(askVol[2]) * info.magnifyNumber;
                depth10.ap4 = crypto::fast_atod(askPrice[3]) * info.reduceNumber;
                depth10.av4 = crypto::fast_atod(askVol[3]) * info.magnifyNumber;
                depth10.ap5 = crypto::fast_atod(askPrice[4]) * info.reduceNumber;
                depth10.av5 = crypto::fast_atod(askVol[4]) * info.magnifyNumber;
                depth10.ap6 = crypto::fast_atod(askPrice[5]) * info.reduceNumber;
                depth10.av6 = crypto::fast_atod(askVol[5]) * info.magnifyNumber;
                depth10.ap7 = crypto::fast_atod(askPrice[6]) * info.reduceNumber;
                depth10.av7 = crypto::fast_atod(askVol[6]) * info.magnifyNumber;
                depth10.ap8 = crypto::fast_atod(askPrice[7]) * info.reduceNumber;
                depth10.av8 = crypto::fast_atod(askVol[7]) * info.magnifyNumber;
                depth10.ap9 = crypto::fast_atod(askPrice[8]) * info.reduceNumber;
                depth10.av9 = crypto::fast_atod(askVol[8]) * info.magnifyNumber;
                depth10.ap10 = crypto::fast_atod(askPrice[9]) * info.reduceNumber;
                depth10.av10 = crypto::fast_atod(askVol[9]) * info.magnifyNumber;
            }

            depth10.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
            mDepth10Publisher[key]->push(depth10);                
#endif
            return;
        }
        case md::DEPTH20: {
            md::Depth20 depth20;
            memset(&depth20, 0, sizeof(md::Depth20));
            depth20.exchangeTypeEnum = exchangeTypeEnum;
            depth20.instTypeEnum = instTypeEnum;
            depth20.marketTypeEnum = marketTypeEnum;
            strncpy(depth20.instId, info.instId, INSTID_SIZE);

            int64_t tsT = 0;
            data["t"].get(tsT);
            int64_t ts = tsT * 1000;

            depth20.tsTrans = ts;
            depth20.tsEvent = ts;
            depth20.tsRecv = tsNet;

            std::string originInstId = "";
            std::string_view sVal;
            if (data["contract"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }

            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

            auto bidsArray = data["bids"];
            std::string_view bidPrice[20];
            std::string_view bidVol[20];

            size_t bidsCount = 0;
            for (auto bidLevel : bidsArray) {
                if (bidsCount >= 20) {
                    break;
                }
                
                bidLevel["p"].get(bidPrice[bidsCount]);
                bidLevel["s"].get(bidVol[bidsCount]);

                bidsCount++;
            }

            if (bidsCount == 20) {
                depth20.bp1 = crypto::fast_atod(bidPrice[0]) * info.reduceNumber;
                depth20.bv1 = crypto::fast_atod(bidVol[0]) * info.magnifyNumber;
                depth20.bp2 = crypto::fast_atod(bidPrice[1]) * info.reduceNumber;
                depth20.bv2 = crypto::fast_atod(bidVol[1]) * info.magnifyNumber;
                depth20.bp3 = crypto::fast_atod(bidPrice[2]) * info.reduceNumber;
                depth20.bv3 = crypto::fast_atod(bidVol[2]) * info.magnifyNumber;
                depth20.bp4 = crypto::fast_atod(bidPrice[3]) * info.reduceNumber;
                depth20.bv4 = crypto::fast_atod(bidVol[3]) * info.magnifyNumber;
                depth20.bp5 = crypto::fast_atod(bidPrice[4]) * info.reduceNumber;
                depth20.bv5 = crypto::fast_atod(bidVol[4]) * info.magnifyNumber;
                depth20.bp6 = crypto::fast_atod(bidPrice[5]) * info.reduceNumber;
                depth20.bv6 = crypto::fast_atod(bidVol[5]) * info.magnifyNumber;
                depth20.bp7 = crypto::fast_atod(bidPrice[6]) * info.reduceNumber;
                depth20.bv7 = crypto::fast_atod(bidVol[6]) * info.magnifyNumber;
                depth20.bp8 = crypto::fast_atod(bidPrice[7]) * info.reduceNumber;
                depth20.bv8 = crypto::fast_atod(bidVol[7]) * info.magnifyNumber;
                depth20.bp9 = crypto::fast_atod(bidPrice[8]) * info.reduceNumber;
                depth20.bv9 = crypto::fast_atod(bidVol[8]) * info.magnifyNumber;
                depth20.bp10 = crypto::fast_atod(bidPrice[9]) * info.reduceNumber;
                depth20.bv10 = crypto::fast_atod(bidVol[9]) * info.magnifyNumber;
                depth20.bp11 = crypto::fast_atod(bidPrice[10]) * info.reduceNumber;
                depth20.bv11 = crypto::fast_atod(bidVol[10]) * info.magnifyNumber;
                depth20.bp12 = crypto::fast_atod(bidPrice[11]) * info.reduceNumber;
                depth20.bv12 = crypto::fast_atod(bidVol[11]) * info.magnifyNumber;
                depth20.bp13 = crypto::fast_atod(bidPrice[12]) * info.reduceNumber;
                depth20.bv13 = crypto::fast_atod(bidVol[12]) * info.magnifyNumber;
                depth20.bp14 = crypto::fast_atod(bidPrice[13]) * info.reduceNumber;
                depth20.bv14 = crypto::fast_atod(bidVol[13]) * info.magnifyNumber;
                depth20.bp15 = crypto::fast_atod(bidPrice[14]) * info.reduceNumber;
                depth20.bv15 = crypto::fast_atod(bidVol[14]) * info.magnifyNumber;
                depth20.bp16 = crypto::fast_atod(bidPrice[15]) * info.reduceNumber;
                depth20.bv16 = crypto::fast_atod(bidVol[15]) * info.magnifyNumber;
                depth20.bp17 = crypto::fast_atod(bidPrice[16]) * info.reduceNumber;
                depth20.bv17 = crypto::fast_atod(bidVol[16]) * info.magnifyNumber;
                depth20.bp18 = crypto::fast_atod(bidPrice[17]) * info.reduceNumber;
                depth20.bv18 = crypto::fast_atod(bidVol[17]) * info.magnifyNumber;
                depth20.bp19 = crypto::fast_atod(bidPrice[18]) * info.reduceNumber;
                depth20.bv19 = crypto::fast_atod(bidVol[18]) * info.magnifyNumber;
                depth20.bp20 = crypto::fast_atod(bidPrice[19]) * info.reduceNumber;
                depth20.bv20 = crypto::fast_atod(bidVol[19]) * info.magnifyNumber;

            }

            auto asksArray = data["asks"];
            std::string_view askPrice[20];
            std::string_view askVol[20];

            size_t asksCount = 0;
            for (auto askLevel : asksArray) {
                if (asksCount >= 20) {
                    break;
                }
                
                askLevel["p"].get(askPrice[asksCount]);
                askLevel["s"].get(askVol[asksCount]);

                asksCount++;
            }

            if (asksCount == 20) {
                depth20.ap1 = crypto::fast_atod(askPrice[0]) * info.reduceNumber;
                depth20.av1 = crypto::fast_atod(askVol[0]) * info.magnifyNumber;
                depth20.ap2 = crypto::fast_atod(askPrice[1]) * info.reduceNumber;
                depth20.av2 = crypto::fast_atod(askVol[1]) * info.magnifyNumber;
                depth20.ap3 = crypto::fast_atod(askPrice[2]) * info.reduceNumber;
                depth20.av3 = crypto::fast_atod(askVol[2]) * info.magnifyNumber;
                depth20.ap4 = crypto::fast_atod(askPrice[3]) * info.reduceNumber;
                depth20.av4 = crypto::fast_atod(askVol[3]) * info.magnifyNumber;
                depth20.ap5 = crypto::fast_atod(askPrice[4]) * info.reduceNumber;
                depth20.av5 = crypto::fast_atod(askVol[4]) * info.magnifyNumber;
                depth20.ap6 = crypto::fast_atod(askPrice[5]) * info.reduceNumber;
                depth20.av6 = crypto::fast_atod(askVol[5]) * info.magnifyNumber;
                depth20.ap7 = crypto::fast_atod(askPrice[6]) * info.reduceNumber;
                depth20.av7 = crypto::fast_atod(askVol[6]) * info.magnifyNumber;
                depth20.ap8 = crypto::fast_atod(askPrice[7]) * info.reduceNumber;
                depth20.av8 = crypto::fast_atod(askVol[7]) * info.magnifyNumber;
                depth20.ap9 = crypto::fast_atod(askPrice[8]) * info.reduceNumber;
                depth20.av9 = crypto::fast_atod(askVol[8]) * info.magnifyNumber;
                depth20.ap10 = crypto::fast_atod(askPrice[9]) * info.reduceNumber;
                depth20.av10 = crypto::fast_atod(askVol[9]) * info.magnifyNumber;
                depth20.ap11 = crypto::fast_atod(askPrice[10]) * info.reduceNumber;
                depth20.av11 = crypto::fast_atod(askVol[10]) * info.magnifyNumber;
                depth20.ap12 = crypto::fast_atod(askPrice[11]) * info.reduceNumber;
                depth20.av12 = crypto::fast_atod(askVol[11]) * info.magnifyNumber;
                depth20.ap13 = crypto::fast_atod(askPrice[12]) * info.reduceNumber;
                depth20.av13 = crypto::fast_atod(askVol[12]) * info.magnifyNumber;
                depth20.ap14 = crypto::fast_atod(askPrice[13]) * info.reduceNumber;
                depth20.av14 = crypto::fast_atod(askVol[13]) * info.magnifyNumber;
                depth20.ap15 = crypto::fast_atod(askPrice[14]) * info.reduceNumber;
                depth20.av15 = crypto::fast_atod(askVol[14]) * info.magnifyNumber;
                depth20.ap16 = crypto::fast_atod(askPrice[15]) * info.reduceNumber;
                depth20.av16 = crypto::fast_atod(askVol[15]) * info.magnifyNumber;
                depth20.ap17 = crypto::fast_atod(askPrice[16]) * info.reduceNumber;
                depth20.av17 = crypto::fast_atod(askVol[16]) * info.magnifyNumber;
                depth20.ap18 = crypto::fast_atod(askPrice[17]) * info.reduceNumber;
                depth20.av18 = crypto::fast_atod(askVol[17]) * info.magnifyNumber;
                depth20.ap19 = crypto::fast_atod(askPrice[18]) * info.reduceNumber;
                depth20.av19 = crypto::fast_atod(askVol[18]) * info.magnifyNumber;
                depth20.ap20 = crypto::fast_atod(askPrice[19]) * info.reduceNumber;
                depth20.av20 = crypto::fast_atod(askVol[19]) * info.magnifyNumber;
            }

            depth20.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
            mDepth20Publisher[key]->push(depth20);                
#endif
            return;
        }
        case md::TRADES: {
            md::Trades trades;
            memset(&trades, 0, sizeof(md::Trades));
            trades.exchangeTypeEnum = exchangeTypeEnum;
            trades.instTypeEnum = instTypeEnum;
            trades.marketTypeEnum = marketTypeEnum;
            strncpy(trades.instId, info.instId, INSTID_SIZE);

            auto it = data.begin();
            auto d = *it;

            int64_t tradeId;
            d["id"].get(tradeId);
            fmt::format_to(trades.tradeId, "{}", tradeId);

            std::string_view tradeVolStr;
            d["size"].get(tradeVolStr);


            int64_t tsT = 0;
            d["create_time_ms"].get(tsT);
            int64_t ts = tsT * 1000;

            trades.tsTrans = ts;
            trades.tsEvent = ts;
            trades.tsRecv = tsNet;

            std::string_view tradePriceStr;
            d["price"].get(tradePriceStr);
            
            trades.px = crypto::fast_atod(tradePriceStr) * info.reduceNumber;

            double size = crypto::fast_atod(tradeVolStr);
            trades.sz = fabs(size) * info.magnifyNumber;

            if (size > 0) {
                trades.direction = DT_LONG; 
            }
            else {
                trades.direction = DT_SHORT;
            }

            trades.tsParse = crypto::getCurrentTime();


            std::string originInstId = "";
            std::string_view sVal;
            if (d["contract"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }

            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

#ifdef NEED_SHM
            mTradesPublisher[key]->push(trades); 
#endif
            return;
        }
        case md::KLINE_1m: {
            for (auto element : data) {
                md::Kline kline;
                memset(&kline, 0, sizeof(md::Kline));
                kline.exchangeTypeEnum = exchangeTypeEnum;
                kline.instTypeEnum = instTypeEnum;
                kline.marketTypeEnum = marketTypeEnum;
                strncpy(kline.instId, info.instId, INSTID_SIZE);

                kline.tsTrans = tsM;
                kline.tsEvent = tsM;
                kline.tsRecv = tsNet;

                std::string_view barTimeStr;
                std::string_view highPriceStr;
                std::string_view lowPriceStr;
                std::string_view openPriceStr;
                std::string_view closePriceStr;
                std::string_view amountStr;
                std::string_view volStr;

                element["t"].get(barTimeStr);
                element["c"].get(closePriceStr);
                element["h"].get(highPriceStr);
                element["l"].get(lowPriceStr);
                element["o"].get(openPriceStr);
                element["a"].get(amountStr);

                std::string originInstId = "";
                std::string_view nStr;
                if (element["n"].get(nStr) != simdjson::SUCCESS) {
                    continue;
                }

                std::vector<std::string> v = crypto::split(std::string(nStr), "_");
                if (v.size() >= 3) {
                    originInstId = v[1] + "_" + v[2];
                    break;
                }

                if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
                    LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                    return;
                }
                const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

                bool isFinished = false;
                element["w"].get(isFinished);
                kline.isFinished = isFinished;

                if (!kline.isFinished) {
                    continue;
                }

                element["v"].get(volStr);

                kline.barTime = crypto::fast_atol(barTimeStr) * 1000000;
                kline.highPrice = crypto::fast_atod(highPriceStr) * info.reduceNumber;
                kline.lowPrice = crypto::fast_atod(lowPriceStr) * info.reduceNumber;
                kline.openPrice = crypto::fast_atod(openPriceStr) * info.reduceNumber;
                kline.closePrice = crypto::fast_atod(closePriceStr) * info.reduceNumber;

                double avgPrice = 0;
                double amount = crypto::fast_atod(amountStr);
                double volume = crypto::fast_atod(volStr);
                if(volume > ZERO_NUM) {
                    avgPrice = amount / volume;
                }
                kline.avgPrice = avgPrice * info.reduceNumber;
                kline.totalVolume = volume * info.magnifyNumber;
                kline.totalAmount = amount;


                kline.tsParse = crypto::getCurrentTime();
    #ifdef NEED_SHM
                mKlinePublisher[key]->push(kline); 
    #endif
            }

            return;
        }
        case md::FUNDING_RATE: {
            md::FundingRate fundingRate;
            memset(&fundingRate, 0, sizeof(md::FundingRate));
            fundingRate.exchangeTypeEnum = exchangeTypeEnum;
            fundingRate.instTypeEnum = instTypeEnum;
            fundingRate.marketTypeEnum = marketTypeEnum;
            strncpy(fundingRate.instId, info.instId, INSTID_SIZE);

            fundingRate.tsTrans = tsM;
            fundingRate.tsEvent = tsM;
            fundingRate.tsRecv = tsNet;

            auto it = data.begin();
            auto d = *it;

            std::string originInstId = "";
            std::string_view sVal;
            if (d["contract"].get(sVal) == simdjson::SUCCESS) {
                originInstId = std::string(sVal);
            }

            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);


            std::string_view fundingRateStr;
            std::string_view nextFundingRateStr;
            d["funding_rate"].get(fundingRateStr);
            d["funding_rate_indicative"].get(nextFundingRateStr);
            
            fundingRate.fundingRate = crypto::fast_atod(fundingRateStr);
            fundingRate.nextFundingRate = crypto::fast_atod(nextFundingRateStr);

            int64_t fundingTime;
            d["funding_next_apply"].get(fundingTime);
            fundingRate.fundingTime = fundingTime;

            fundingRate.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
            mFundingRatePublisher[key]->push(fundingRate); 
#endif
            return;
        }
        default: {
            LOG_ERROR("not support marketType: {}", md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }
    }
}

md::GateioMarket::GateioMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot, const char* host, const int port, const char* passwd) : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        std::cout << "start create gateio unit" << std::endl;
        auto& info = unitInfoVec[i];
        md::GateioUnit* unit = new md::GateioUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        std::cout << "start generate sub body" << std::endl;
        unit->generateSubBody();
        gateioUnitVec.push_back(unit);
    }
}

md::GateioMarket::~GateioMarket() {

}


void md::GateioMarket::start() {
    for (size_t i = 0; i < gateioUnitVec.size(); ++i) {
        gateioUnitVec[i]->start();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}
