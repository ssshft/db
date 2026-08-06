#include "bybit/BybitMarket.h"


md::BybitUnit::BybitUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* passwd) : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    subId = crypto::get_int_rand(100,10000);

    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text = R"({"op":"ping"})";
    
    cfg.idle_timeout_sec = 60;
    cfg.data_idle_timeout_sec = 0; // 看实际情况是否开启

}

void md::BybitUnit::generateSubBody() {
    // LOG_INFO("%s", getString().c_str());
    std::string exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    std::string instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    std::string marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
    std::string lowerMarketTypeStr = crypto::to_lower(marketTypeStr);

    if (instTypeEnum == SPOT) {
        cfg.url = BYBIT_WS_PUBLIC_SPOT;
    }
    else if (instTypeEnum == USDT_SWAP) {
        cfg.url = BYBIT_WS_PUBLIC_USDT_SWAP;
    }

    else if (instTypeEnum == C_SWAP) {
        cfg.url = BYBIT_WS_PUBLIC_C_SWAP;
    }

    subArgs.clear();

    for (auto info : vInstInfo) {
        const std::string& originInstId = info.originInstId;
        if (crypto::has_str(marketTypeStr, "DEPTH1")) {
            std::string param = fmt::format("orderbook.1.{}", originInstId);
            subArgs.push_back(param);
        }
        else if (crypto::has_str(marketTypeStr, "DEPTH5") || crypto::has_str(marketTypeStr, "DEPTH10") || crypto::has_str(marketTypeStr, "DEPTH20")) {
            std::string param = fmt::format("orderbook.50.{}", originInstId);
            subArgs.push_back(param);
        }
        else if (crypto::has_str(marketTypeStr, "TRADE")) {
            std::string param = fmt::format("publicTrade.{}", originInstId);
            subArgs.push_back(param);
        }
        else if (crypto::has_str(marketTypeStr, "KLINE_1m")) {
            std::string param = fmt::format("kline.1.{}", originInstId);
            subArgs.push_back(param);
        }
        else if (crypto::has_str(marketTypeStr, "FUNDING")) {
            if (instTypeEnum == USDT_SWAP) {
                std::string param = fmt::format("tickers.{}", originInstId);
                subArgs.push_back(param);
            }
        }
    }

    std::string paramsCsv;
    paramsCsv.reserve(subArgs.size() * 32);
    for (size_t i = 0; i < subArgs.size(); ++i) {
        if (i) {
            paramsCsv += ",";
        }
        paramsCsv += "\"";
        paramsCsv += subArgs[i];
        paramsCsv += "\"";
    }

    std::string subJson = fmt::format(R"({{"op":"subscribe","args":[{}],"req_id":"{}"}})", paramsCsv, subId++);
    cfg.subscribe_messages.clear();
    cfg.subscribe_messages.push_back(subJson);

    LOG_INFO("{} ws url: {}, sub body: {}", exchIdStr, cfg.url, subJson);
}

void md::BybitUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    latestDataUpdateTime = crypto::getCurrentTime();

    std::string_view sv(reinterpret_cast<const char*>(data), len);
    if (sv.find("\"pong\"") != std::string_view::npos) {
        return;
    }

    std::string msg(sv);
    std::cout << "onWebsocketMsg: " << msg << std::endl;
    mQueue.push(std::move(msg));
}

//处理消息 解析json并发送给redis或共享内存
void md::BybitUnit::parseMarketData(const std::string& msg) {
    long tsNet = crypto::getCurrentTime();

    simdjson::padded_string paddedMsg(msg);
    auto doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("simdjson parse msg: {} error: {}", msg, simdjson::error_message(doc.error()));
        return;
    }

    std::string_view topicVal;
    if (doc["topic"].get(topicVal)) {
        LOG_ERROR("msg has no topic field! msg: {}", msg);
        return;    
    }

    long ts = 0;
    if (doc["ts"].get(ts) == simdjson::SUCCESS) {
        ts *= 1000;
    }

    auto data = doc["data"];
    if (data.error()) {
        LOG_ERROR("msg has no data field! msg: {}", msg);
        return;
    }

    std::string originInstId = "";
    if (topicVal[0] == 'o') {
        std::string_view sVal;
        if (data["s"].get(sVal) == simdjson::SUCCESS) {
            originInstId = std::string(sVal);
        }
    }
    else if (topicVal[0] == 'k') {
        std::vector<std::string> v = crypto::split(std::string(topicVal), ".");
        if (!v.empty()) {
            originInstId = crypto::to_upper(v[v.size() - 1]);
        }
    }
    else if (topicVal[0] == 't') {
        std::string_view tyVal;
        if (data["type"].get(tyVal) == simdjson::SUCCESS) {
            if (tyVal[0] == 'd') {
                return;
            }
        }

        std::string_view sVal;
        if (data["symbol"].get(sVal) == simdjson::SUCCESS) {
            originInstId = std::string(sVal);
        }
    }
    else {
        LOG_ERROR("can not handle topic: {}", std::string(topicVal));
        return;
    }

    md::InstrumentInfo info;
    if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
        LOG_ERROR("smc cannot find originInstId: {}", originInstId);
        return;
    }

    const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);
    if (marketTypeEnum == md::DEPTH1) {
        md::Depth1 depth1;
        memset(&depth1, 0, sizeof(md::Depth1));
        depth1.exchangeTypeEnum = exchangeTypeEnum;
        depth1.instTypeEnum = instTypeEnum;
        depth1.marketTypeEnum = marketTypeEnum;
        strncpy(depth1.instId, info.instId, INSTID_SIZE);

        auto bidsArray = data["b"];
        std::string_view bidPrice;
        std::string_view bidVol;

        size_t bidsCount = 0;
        for (auto bidLevel : bidsArray) {
            if (bidsCount >= 1) {
                break;
            }

            auto it = bidLevel.begin();
            if ((*it).get(bidPrice)) {
                break;
            } 
            ++it;
            
            if ((*it).get(bidVol)) {
                break;
            } 

            bidsCount++;
        }

        if (bidsCount == 1) {
            depth1.bp1 = crypto::fast_atod(bidPrice) * info.reduceNumber;
            depth1.bv1 = crypto::fast_atod(bidVol) * info.magnifyNumber;
        }

        auto asksArray = data["a"];
        std::string_view askPrice;
        std::string_view askVol;

        size_t asksCount = 0;
        for (auto askLevel : asksArray) {
            if (asksCount >= 1) {
                break;
            }

            auto it = askLevel.begin();
            if ((*it).get(askPrice)) {
                break;
            } 
            ++it;
            
            if ((*it).get(askVol)) {
                break;
            } 

            asksCount++;
        }

        if (asksCount == 1) {
            depth1.ap1 = crypto::fast_atod(askPrice) * info.reduceNumber;
            depth1.av1 = crypto::fast_atod(askVol) * info.magnifyNumber;
        }

        long tsT = 0;
        if (data["cts"].get(tsT) == simdjson::SUCCESS) {
            tsT *= 1000;
        }

        depth1.tsTrans = tsT;
        depth1.tsEvent = ts;
        depth1.tsRecv = tsNet;
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

        long tsT = 0;
        if (data["cts"].get(tsT) == simdjson::SUCCESS) {
            tsT *= 1000;
        }

        depth5.tsTrans = tsT;
        depth5.tsEvent = ts;
        depth5.tsRecv = tsNet;
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

        long tsT = 0;
        if (data["cts"].get(tsT) == simdjson::SUCCESS) {
            tsT *= 1000;
        }

        depth10.tsTrans = tsT;
        depth10.tsEvent = ts;
        depth10.tsRecv = tsNet;
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

        long tsT = 0;
        if (data["cts"].get(tsT) == simdjson::SUCCESS) {
            tsT *= 1000;
        }

        depth20.tsTrans = tsT;
        depth20.tsEvent = ts;
        depth20.tsRecv = tsNet;
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

        for (auto d : data) {
            std::string_view tradeIdStr;
            d["i"].get(tradeIdStr);
            strncpy(trades.tradeId, tradeIdStr.data(), INSTID_SIZE);

            long tsT = 0;
            d["T"].get(tsT);
            tsT *= 1000;

            std::string originInstId = "";
            std::string_view sVal;

            if (instTypeEnum == USDT_SWAP || instTypeEnum == C_SWAP) {
                if (d["s"].get(sVal) == simdjson::SUCCESS) {
                    originInstId = std::string(sVal);
                } 
            }

            std::string_view tradePriceStr;
            std::string_view tradeVolStr;
            d["p"].get(tradePriceStr);
            d["v"].get(tradeVolStr);
            trades.px = crypto::fast_atod(tradePriceStr) * info.reduceNumber;
            trades.sz = crypto::fast_atod(tradeVolStr) * info.magnifyNumber;

            std::string_view sideStr;
            d["S"].get(sideStr);
            if (sideStr == "Sell") {
                trades.direction = DT_SHORT;
            }
            else if (sideStr == "Buy") {
                trades.direction = DT_LONG;
            }

            if (instTypeEnum == SPOT) {
                if (d["s"].get(sVal) == simdjson::SUCCESS) {
                    originInstId = std::string(sVal);
                } 
            }

            md::InstrumentInfo info;
            if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
                LOG_ERROR("smc cannot find originInstId: {}", originInstId);
                return;
            }
            const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

            trades.tsTrans = tsT;
            trades.tsEvent = ts;
            trades.tsRecv = tsNet;
            trades.tsParse = crypto::getCurrentTime();

            std::cout << trades.getString() << std::endl;

    #ifdef NEED_SHM
            mTradesPublisher[key]->push(trades); 
    #endif

        }
    }
    else if (marketTypeEnum == md::KLINE_1m) {
        md::Kline kline;
        memset(&kline, 0, sizeof(md::Kline));
        kline.exchangeTypeEnum = exchangeTypeEnum;
        kline.instTypeEnum = instTypeEnum;
        kline.marketTypeEnum = marketTypeEnum;
        strncpy(kline.instId, info.instId, INSTID_SIZE);

        auto it = data.begin();
        auto d = *it;

        long barTime = 0;
        d["start"].get(barTime);
        barTime *= 1000;

        std::string_view openPriceStr;
        std::string_view highPriceStr;
        std::string_view lowPriceStr;
        std::string_view closePriceStr;
        std::string_view volStr;
        std::string_view amountStr;
        
        d["open"].get(openPriceStr);
        d["close"].get(openPriceStr);
        d["high"].get(openPriceStr);
        d["low"].get(openPriceStr);
        d["volume"].get(volStr);
        d["turnover"].get(amountStr);
    
        kline.barTime = barTime;
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
        d["confirm"].get(isFinished);
        kline.isFinished = isFinished;
        if (!kline.isFinished) {
            return;
        }

        long ts = 0;
        doc["ts"].get(ts);
        ts *= 1000;

        kline.tsTrans = ts;
        kline.tsEvent = ts;
        kline.tsRecv = tsNet;
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

        std::string_view fundingTimeStr;
        if (data["nextFundingTime"].get(fundingTimeStr) == simdjson::SUCCESS) {
            fundingRate.fundingTime = crypto::fast_atol(fundingTimeStr) * 1000;
        }

        std::string_view fundingRateStr;
        if (data["fundingRate"].get(fundingRateStr) == simdjson::SUCCESS) {
            fundingRate.fundingRate = crypto::fast_atod(fundingRateStr);
        }

        long ts = 0;
        if (doc["ts"].get(ts) == simdjson::SUCCESS) {
            ts *= 1000;
        }

        fundingRate.tsTrans = ts;
        fundingRate.tsEvent = ts;
        fundingRate.tsRecv = tsNet;
        fundingRate.tsParse = crypto::getCurrentTime();
        std::cout << fundingRate.getString() << std::endl;

    #ifdef NEED_SHM
        mFundingRatePublisher[key]->push(fundingRate); 
    #endif
    }
}


md::BybitMarket::BybitMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot, const char* host, const int port, const char* passwd) : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    std::cout << "============ " << exchId << std::endl;
    std::cout << "--=-=-=-=" << unitInfoVec.size() << std::endl;

    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        std::cout << "start create bybit unit" << std::endl;
        auto& info = unitInfoVec[i];
        md::BybitUnit* unit = new md::BybitUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        std::cout << "start generate sub body" << std::endl;
        unit->generateSubBody();
        bybitUnitVec.push_back(unit);
    }

}

md::BybitMarket::~BybitMarket() {

}


void md::BybitMarket::start() {
    for (size_t i = 0; i < bybitUnitVec.size(); ++i) {
        bybitUnitVec[i]->start();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}
