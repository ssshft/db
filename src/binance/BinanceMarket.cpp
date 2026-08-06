#include "binance/BinanceMarket.h"


md::BinanceUnit::BinanceUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* passwd) : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    subId = crypto::get_int_rand(100,10000);

    // ws config
    // 多久没收到**任何 frame**(含 PING/PONG) → 重连 (0 = 不检查)。
    // 抓"TCP/网络层死了": ping/pong 也算 frame, 所以这里超时一定意味着连接真挂了。
    cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
    cfg.idle_timeout_sec = 60;
    cfg.data_idle_timeout_sec = 0; // 看实际情况是否开启
}

void md::BinanceUnit::generateSubBody() {
    std::string exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    std::string instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    std::string marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
    std::string lowerMarketTypeStr = crypto::to_lower(marketTypeStr);

    if (instTypeEnum == SPOT) {
        cfg.url = BINANCE_WS_PUBLIC_SPOT;
    }
    else if (instTypeEnum == USDT_SWAP || instTypeEnum == USDT_FUTURES || instTypeEnum == USDC_SWAP) {
        if (marketTypeEnum == DEPTH1 || marketTypeEnum == DEPTH5 || marketTypeEnum == DEPTH10 || marketTypeEnum == DEPTH20 || marketTypeEnum == TRADES) {
            cfg.url = BINANCE_WS_PUBLIC_USDT_SWAP_FUTURES_PUBLIC;
        }
        else {
            cfg.url = BINANCE_WS_PUBLIC_USDT_SWAP_FUTURES_MARKET;
        }
        
    }
    else if (instTypeEnum == C_SWAP || instTypeEnum == C_FUTURES) {
        cfg.url = BINANCE_WS_PUBLIC_USD_SWAP_FUTURES;
    }

    subParams.clear();

    for (auto info : vInstInfo) {
        std::string lowerOriginInstId = crypto::to_lower(info.originInstId);
        if (instTypeEnum == SPOT) {
            if (crypto::has_str(marketTypeStr, "DEPTH")) {
                if(crypto::str_cmp(marketTypeStr.c_str(), "DEPTH1")) {
                    std::string param = fmt::format("{}@bookTicker", lowerOriginInstId);
                    subParams.push_back(param);
                }
                else {
                    std::string param = fmt::format("{}@{}@100ms", lowerOriginInstId, lowerMarketTypeStr);
                    subParams.push_back(param);
                }
            }
            else if (crypto::has_str(marketTypeStr, "TRADE")) {
                std::string param = fmt::format("{}@trade", lowerOriginInstId);
                subParams.push_back(param);
            }
            else if (crypto::has_str(marketTypeStr, "KLINE")) {
                std::string param = fmt::format("{}@{}", lowerOriginInstId, lowerMarketTypeStr);
                subParams.push_back(param);
            }
            else {
                LOG_ERROR("not support {}", marketTypeStr);
            }
        }//这里usdt和busd本位都在这里处理
        else if (instTypeEnum == USDT_SWAP || instTypeEnum == USDT_FUTURES || instTypeEnum == USDC_SWAP) {
            if (crypto::has_str(marketTypeStr, "DEPTH")) {
                if (crypto::str_cmp(marketTypeStr.c_str(), "DEPTH1")) {
                    std::string param = fmt::format("{}@bookTicker", lowerOriginInstId);
                    subParams.push_back(param);
                }
                else {
                    std::string param = fmt::format("{}@{}@100ms", lowerOriginInstId, lowerMarketTypeStr);
                    subParams.push_back(param);
                }
            }
            else if (crypto::has_str(marketTypeStr, "TRADE")) {
                std::string param = fmt::format("{}@trade", lowerOriginInstId);
                subParams.push_back(param);
            }
            else if (crypto::has_str(marketTypeStr, "KLINE")) {
                std::string param = fmt::format("{}@{}", lowerOriginInstId, lowerMarketTypeStr);
                subParams.push_back(param);
            }
            else if (crypto::has_str(marketTypeStr, "FUNDING")) {
                if (instTypeEnum == USDT_SWAP || instTypeEnum == USDC_SWAP) {
                    std::string param = fmt::format("{}@markPrice@1s", lowerOriginInstId);
                    subParams.push_back(param);
                }
            }
        }//币本位
        else if (instTypeEnum == C_SWAP || instTypeEnum == C_FUTURES) {
            if (crypto::has_str(marketTypeStr, "DEPTH")) {
                if (crypto::str_cmp(marketTypeStr.c_str(), "DEPTH1")) {
                    std::string param = fmt::format("{}@bookTicker", lowerOriginInstId);
                    subParams.push_back(param);
                }
                else {
                    std::string param = fmt::format("{}@{}@100ms", lowerOriginInstId, lowerMarketTypeStr);
                    subParams.push_back(param);
                }
            }
            else if (crypto::has_str(marketTypeStr, "TRADE")) {
                std::string param = fmt::format("{}@aggTrade", lowerOriginInstId);
                subParams.push_back(param);
            }
            else if (crypto::has_str(marketTypeStr, "KLINE")) {
                std::string param = fmt::format("{}@{}", lowerOriginInstId, lowerMarketTypeStr);
                subParams.push_back(param);
            }
            else if (crypto::has_str(marketTypeStr, "FUNDING")) {
                if (instTypeEnum == C_SWAP) {
                    std::string param = fmt::format("{}@markPrice@1s", lowerOriginInstId);
                    subParams.push_back(param);
                }
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

    LOG_INFO("{} ws url: {}, sub body: {}", exchIdStr, cfg.url, subJson);

}

void md::BinanceUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    latestDataUpdateTime = crypto::getCurrentTime();
    std::string msg(reinterpret_cast<const char*>(data), len);
    std::cout << "onWebsocketMsg: " << msg << std::endl;
    mQueue.push(std::move(msg));
}

//处理消息 解析json并发送给redis或共享内存
void md::BinanceUnit::parseMarketData(const std::string& msg) {
    long tsNet = crypto::getCurrentTime();

    simdjson::padded_string paddedMsg(msg);
    auto doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("simdjson parse msg: {} error: {}", msg, simdjson::error_message(doc.error()));
        return;
    }
    
    auto data = doc["data"];
    if (data.error()) {
        LOG_ERROR("msg has no data field! msg: {}", msg);
        return;
    }
    
    std::string originInstId = "";
    std::string_view streamVal;
    if (doc["stream"].get(streamVal) == simdjson::SUCCESS) {
        std::vector<std::string> v = crypto::split(std::string(streamVal), "@");
        if (!v.empty()) {
            originInstId = crypto::to_upper(v[0]);
        }
    }
    
    md::InstrumentInfo info;
    if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false){
        LOG_ERROR("smc cannot find originInstId: {}", originInstId);
        return;
    }

    std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

    if (instTypeEnum == SPOT) {
        if (marketTypeEnum == md::DEPTH1) {
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
        
            md::Depth1 depth1;
            memset(&depth1, 0, sizeof(md::Depth1));
            depth1.exchangeTypeEnum = exchangeTypeEnum;
            depth1.instTypeEnum = instTypeEnum;
            depth1.marketTypeEnum = marketTypeEnum;
            strncpy(depth1.instId, info.instId, INSTID_SIZE);
            depth1.tsTrans = tsNet;
            depth1.tsEvent = tsNet;
            depth1.tsRecv = tsNet;
            depth1.bp1 = crypto::fast_atod(bidPriceStr) * info.reduceNumber;
            depth1.bv1 = crypto::fast_atod(bidVolStr) * info.magnifyNumber;
            depth1.ap1 = crypto::fast_atod(askPriceStr) * info.reduceNumber;
            depth1.av1 = crypto::fast_atod(askVolStr) * info.magnifyNumber;
            depth1.tsParse = crypto::getCurrentTime();

            std::cout << "--- depth1-- " << depth1.getString() << std::endl;
#ifdef NEED_SHM
            mDepth1Publisher[key]->push(depth1);                
#endif
        }
        else if (marketTypeEnum == md::DEPTH5) {
            md::Depth5 depth5;
            memset(&depth5, 0, sizeof(md::Depth5));
            depth5.exchangeTypeEnum = exchangeTypeEnum;
            depth5.instTypeEnum = instTypeEnum;
            depth5.marketTypeEnum = marketTypeEnum;
            strncpy(depth5.instId, info.instId, INSTID_SIZE);
            depth5.tsTrans = tsNet;
            depth5.tsEvent = tsNet;
            depth5.tsRecv = tsNet;

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

            std::cout << "--- depth5-- " << depth5.getString() << std::endl;

#ifdef NEED_SHM
            mDepth5Publisher[key]->push(depth5);                
#endif
        }
        else if (marketTypeEnum == md::DEPTH10) {
            md::Depth10 depth10;
            memset(&depth10, 0, sizeof(md::Depth10));
            depth10.exchangeTypeEnum = exchangeTypeEnum;
            depth10.instTypeEnum = instTypeEnum;
            depth10.marketTypeEnum = marketTypeEnum;
            strncpy(depth10.instId, info.instId, INSTID_SIZE);
            depth10.tsTrans = tsNet;
            depth10.tsEvent = tsNet;
            depth10.tsRecv = tsNet;

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

            std::cout << "--- depth10-- " << depth10.getString() << std::endl;

#ifdef NEED_SHM
            mDepth10Publisher[key]->push(depth10);                
#endif
        }
        else if (marketTypeEnum == md::DEPTH20) {
            md::Depth20 depth20;
            memset(&depth20, 0, sizeof(md::Depth20));
            depth20.exchangeTypeEnum = exchangeTypeEnum;
            depth20.instTypeEnum = instTypeEnum;
            depth20.marketTypeEnum = marketTypeEnum;
            strncpy(depth20.instId, info.instId, INSTID_SIZE);
            depth20.tsTrans = tsNet;
            depth20.tsEvent = tsNet;
            depth20.tsRecv = tsNet;

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

#ifdef NEED_SHM
            mDepth20Publisher[key]->push(depth20);                
#endif
        }
        else if(marketTypeEnum == md::TRADES) {
            md::Trades trades;
            memset(&trades, 0, sizeof(md::Trades));
            trades.exchangeTypeEnum = exchangeTypeEnum;
            trades.instTypeEnum = instTypeEnum;
            trades.marketTypeEnum = marketTypeEnum;
            strncpy(trades.instId, info.instId, INSTID_SIZE);

            long tsEvent = 0;
            data["E"].get(tsEvent);

            long tradeId;
            std::string_view tradePriceStr;
            std::string_view tradeVolStr;
            data["t"].get(tradeId);
            data["p"].get(tradePriceStr);
            data["q"].get(tradeVolStr);

            long tsTrans = 0;
            data["T"].get(tsTrans);

            fmt::format_to(trades.tradeId, "{}", tradeId);
            trades.px = crypto::fast_atod(tradePriceStr);
            trades.sz = crypto::fast_atod(tradeVolStr);

            bool direction = false;
            data["m"].get(direction);
            trades.direction = direction ? DT_SHORT : DT_LONG;

            trades.tsTrans = tsTrans * 1000;
            trades.tsEvent = tsEvent * 1000;
            trades.tsRecv = tsNet;
            trades.tsParse = crypto::getCurrentTime();

            std::cout << trades.getString() << std::endl;

#ifdef NEED_SHM
            mTradesPublisher[key]->push(trades); 
#endif
        }
        else if (marketTypeEnum == md::KLINE_1m) {
            md::Kline kline;
            memset(&kline, 0, sizeof(md::Kline));
            kline.exchangeTypeEnum = exchangeTypeEnum;
            kline.instTypeEnum = instTypeEnum;
            kline.marketTypeEnum = marketTypeEnum;
            strncpy(kline.instId, info.instId, INSTID_SIZE);

            long tsEvent = 0;
            data["E"].get(tsEvent);

            kline.tsTrans = tsEvent * 1000;
            kline.tsEvent = tsEvent * 1000;
            kline.tsRecv = tsNet;

            auto k = data["k"];
            
            long barTime = 0;
            std::string_view highPriceStr;
            std::string_view lowPriceStr;
            std::string_view openPriceStr;
            std::string_view closePriceStr;
            std::string_view amountStr;
            std::string_view volStr;

            k["t"].get(barTime);
            k["o"].get(openPriceStr);
            k["c"].get(closePriceStr);
            k["h"].get(highPriceStr);
            k["l"].get(lowPriceStr);
            k["v"].get(volStr);
            
            bool isFinished = false;
            k["x"].get(isFinished);
            kline.isFinished = isFinished;
            if (!kline.isFinished) {
                return;
            }

            k["q"].get(amountStr);
            
            kline.barTime = barTime * 1000;
            kline.highPrice = crypto::fast_atod(highPriceStr) * info.reduceNumber;
            kline.lowPrice = crypto::fast_atod(lowPriceStr) * info.reduceNumber;
            kline.openPrice = crypto::fast_atod(openPriceStr) * info.reduceNumber;
            kline.closePrice = crypto::fast_atod(closePriceStr) * info.reduceNumber;

            double avgPrice = 0;
            double amount = crypto::fast_atod(amountStr);
            double volume = crypto::fast_atod(volStr);
            if(amount > ZERO_NUM){
                avgPrice = amount / volume;
            }

            kline.avgPrice = avgPrice * info.reduceNumber;
            kline.totalVolume = volume * info.magnifyNumber;
            kline.totalAmount = amount;

            kline.tsParse = crypto::getCurrentTime();

            std::cout << kline.getString() << std::endl;

#ifdef NEED_SHM
            mKlinePublisher[key]->push(kline); 
#endif
        }
        else {
            return;
        }
    }
    else if(instTypeEnum == USDT_SWAP || instTypeEnum == USDT_FUTURES || instTypeEnum == USDC_SWAP || instTypeEnum == C_SWAP || instTypeEnum == C_FUTURES) {
        if(marketTypeEnum == md::DEPTH1) {
            md::Depth1 depth1;
            memset(&depth1, 0, sizeof(md::Depth1));
            depth1.exchangeTypeEnum = exchangeTypeEnum;
            depth1.instTypeEnum = instTypeEnum;
            depth1.marketTypeEnum = marketTypeEnum;
            strncpy(depth1.instId, info.instId, INSTID_SIZE);

            auto bp1 = data["b"];
            auto bv1 = data["B"];
            auto ap1 = data["a"];
            auto av1 = data["A"];
            
            std::string_view bidPriceStr;
            std::string_view bidVolStr;
            std::string_view askPriceStr;
            std::string_view askVolStr;
            bp1.get(bidPriceStr);
            bv1.get(bidVolStr);
            ap1.get(askPriceStr);
            av1.get(askVolStr);
   
            long tsTrans = 0;
            data["T"].get(tsTrans);

            long tsEvent = 0;
            data["E"].get(tsEvent);

            depth1.tsTrans = tsTrans * 1000;
            depth1.tsEvent = tsEvent * 1000;
            depth1.tsRecv = tsNet;
            depth1.bp1 = crypto::fast_atod(bidPriceStr) * info.reduceNumber;
            depth1.bv1 = crypto::fast_atod(bidVolStr) * info.magnifyNumber;
            depth1.ap1 = crypto::fast_atod(askPriceStr) * info.reduceNumber;
            depth1.av1 = crypto::fast_atod(askVolStr) * info.magnifyNumber;

            depth1.tsParse = crypto::getCurrentTime();

            std::cout << depth1.getString() << std::endl;
#ifdef NEED_SHM
            mDepth1Publisher[key]->push(depth1);             
#endif
        }
        else if (marketTypeEnum == md::DEPTH5) {
            md::Depth5 depth5;
            memset(&depth5, 0, sizeof(md::Depth5));
            depth5.exchangeTypeEnum = exchangeTypeEnum;
            depth5.instTypeEnum = instTypeEnum;
            depth5.marketTypeEnum = marketTypeEnum;
            strncpy(depth5.instId, info.instId, INSTID_SIZE);

            long tsEvent = 0;
            data["E"].get(tsEvent);

            long tsTrans = 0;
            data["T"].get(tsTrans);

            depth5.tsTrans = tsTrans * 1000;
            depth5.tsEvent = tsEvent * 1000;
            depth5.tsRecv = tsNet;

            auto bidsArray = data["b"];
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

            auto asksArray = data["a"];
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
        }
        else if (marketTypeEnum == md::DEPTH10) {
            md::Depth10 depth10;
            memset(&depth10, 0, sizeof(md::Depth10));
            depth10.exchangeTypeEnum = exchangeTypeEnum;
            depth10.instTypeEnum = instTypeEnum;
            depth10.marketTypeEnum = marketTypeEnum;
            strncpy(depth10.instId, info.instId, INSTID_SIZE);

            long tsEvent = 0;
            data["E"].get(tsEvent);

            long tsTrans = 0;
            data["T"].get(tsTrans);

            depth10.tsTrans = tsTrans * 1000;
            depth10.tsEvent = tsEvent * 1000;
            depth10.tsRecv = tsNet;

            auto bidsArray = data["b"];
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

            auto asksArray = data["a"];
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
        }
        else if (marketTypeEnum == md::DEPTH20) {
            md::Depth20 depth20;
            memset(&depth20, 0, sizeof(md::Depth20));
            depth20.exchangeTypeEnum = exchangeTypeEnum;
            depth20.instTypeEnum = instTypeEnum;
            depth20.marketTypeEnum = marketTypeEnum;
            strncpy(depth20.instId, info.instId, INSTID_SIZE);

            long tsEvent = 0;
            data["E"].get(tsEvent);

            long tsTrans = 0;
            data["T"].get(tsTrans);

            depth20.tsTrans = tsTrans * 1000;
            depth20.tsEvent = tsEvent * 1000;
            depth20.tsRecv = tsNet;

            auto bidsArray = data["b"];
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

            auto asksArray = data["a"];
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
        }
        else if (marketTypeEnum == md::TRADES) {
            md::Trades trades;
            memset(&trades, 0, sizeof(md::Trades));
            trades.exchangeTypeEnum = exchangeTypeEnum;
            trades.instTypeEnum = instTypeEnum;
            trades.marketTypeEnum = marketTypeEnum;
            strncpy(trades.instId, info.instId, INSTID_SIZE);

            long tsEvent = 0;
            long tsTrans = 0;
            data["E"].get(tsEvent);

            if (instTypeEnum == USDT_SWAP || instTypeEnum == USDT_FUTURES || instTypeEnum == USDC_SWAP) {
                data["T"].get(tsTrans);

                long tradeId;
                data["t"].get(tradeId);
                fmt::format_to(trades.tradeId, "{}", tradeId);
            }
            else {
                long tradeId;
                data["a"].get(tradeId);
                fmt::format_to(trades.tradeId, "{}", tradeId);

            }

            std::string_view tradePriceStr;
            std::string_view tradeVolStr;
            data["p"].get(tradePriceStr);
            data["q"].get(tradeVolStr);

            trades.px = crypto::fast_atod(tradePriceStr) * info.reduceNumber;;
            trades.sz = crypto::fast_atod(tradeVolStr) * info.magnifyNumber;

            if (instTypeEnum == C_SWAP || instTypeEnum == C_FUTURES) {
                data["T"].get(tsTrans);
            }

            bool direction = false;
            data["m"].get(direction);
            trades.direction = direction ? DT_SHORT : DT_LONG;

            trades.tsTrans = tsTrans * 1000;
            trades.tsEvent = tsEvent * 1000;
            trades.tsRecv = tsNet;
            trades.tsParse = crypto::getCurrentTime();

            std::cout << trades.getString() << std::endl;

#ifdef NEED_SHM
            mTradesPublisher[key]->push(trades); 
#endif
        }
        else if (marketTypeEnum == md::KLINE_1m) {
            md::Kline kline;
            memset(&kline, 0, sizeof(md::Kline));
            kline.exchangeTypeEnum = exchangeTypeEnum;
            kline.instTypeEnum = instTypeEnum;
            kline.marketTypeEnum = marketTypeEnum;
            strncpy(kline.instId, info.instId, INSTID_SIZE);

            long tsEvent = 0;
            data["E"].get(tsEvent);

            kline.tsTrans = tsEvent * 1000;
            kline.tsEvent = tsEvent * 1000;
            kline.tsRecv = tsNet;

            auto k = data["k"];
            
            long barTime = 0;
            std::string_view highPriceStr;
            std::string_view lowPriceStr;
            std::string_view openPriceStr;
            std::string_view closePriceStr;
            std::string_view amountStr;
            std::string_view volStr;

            k["t"].get(barTime);
            k["o"].get(openPriceStr);
            k["c"].get(closePriceStr);
            k["h"].get(highPriceStr);
            k["l"].get(lowPriceStr);
            k["v"].get(volStr);

            bool isFinished = false;
            k["x"].get(isFinished);
            kline.isFinished = isFinished;

            if (!kline.isFinished) {
                return;
            }

            k["q"].get(amountStr);
            
            kline.barTime = barTime * 1000;
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

            kline.tsParse = crypto::getCurrentTime();

            std::cout << kline.getString() << std::endl;

#ifdef NEED_SHM
            mKlinePublisher[key]->push(kline); 
#endif
        }
        else if (marketTypeEnum == md::FUNDING_RATE) {
            md::FundingRate fundingRate;
            memset(&fundingRate, 0, sizeof(md::FundingRate));
            fundingRate.exchangeTypeEnum = exchangeTypeEnum;
            fundingRate.instTypeEnum = instTypeEnum;
            fundingRate.marketTypeEnum = marketTypeEnum;
            strncpy(fundingRate.instId, info.instId, INSTID_SIZE);

            long tsEvent = 0;
            data["E"].get(tsEvent);

            fundingRate.tsTrans = tsEvent * 1000;
            fundingRate.tsEvent = tsEvent * 1000;
            fundingRate.tsRecv = tsNet;

            std::string_view fundingRateStr;
            data["r"].get(fundingRateStr);

            long fundingTime = 0;
            data["T"].get(fundingTime);

            fundingRate.fundingRate = crypto::fast_atod(fundingRateStr);
            fundingRate.fundingTime = fundingTime * 1000;
            fundingRate.tsParse = crypto::getCurrentTime();

            std::cout << fundingRate.getString() << std::endl;

#ifdef NEED_SHM
            mFundingRatePublisher[key]->push(fundingRate); 
#endif
        }
        else {
            return;
        }
    }
    else {
        return;
    }
}


md::BinanceMarket::BinanceMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot, const char* host, const int port, const char* passwd) : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    LOG_INFO("Market construct exchId: {} unit size: {}", exchId, unitInfoVec.size());

    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::BinanceUnit* unit = new md::BinanceUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        binanceUnitVec.push_back(unit);
    }

}

md::BinanceMarket::~BinanceMarket() {

}


void md::BinanceMarket::start() {
    for (size_t i = 0; i < binanceUnitVec.size(); ++i) {
        binanceUnitVec[i]->start();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}
