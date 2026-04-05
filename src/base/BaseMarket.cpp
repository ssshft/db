#include "base/BaseMarket.h"
#include "key_util.h"


md::BaseUnit::BaseUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* password) {
    smc = s;
    exchangeTypeEnum = exchTy;
    instTypeEnum = instTy;
    marketTypeEnum = marketTy;
    vInstInfo = instInfoVec;
    std::cout << "==========================" << std::endl;
    redisClient = new RedisClient(host, port, password, false, true);
    latestDataUpdateTime = 0;

    std::cout << "start create shm" << "  vInstInfo size: " << vInstInfo.size() << std::endl;

#ifdef NEED_SHM
    for (size_t i = 0; i < vInstInfo.size(); ++i) {
        auto& info = vInstInfo[i];
        std::string key = crypto::get_md_channel_key(info.exchangeTypeEnum, info.instTypeEnum, marketTypeEnum, info.instId);
        if (marketTypeEnum == md::DEPTH1) {
            std::shared_ptr<pubsub::SPMCPublisher<md::Depth1>> shmQueue = std::make_shared<pubsub::SPMCPublisher<md::Depth1>>(key.c_str());
            mDepth1Publisher[key] = shmQueue;
        }
        else if (marketTypeEnum == md::DEPTH5) {
            std::shared_ptr<pubsub::SPMCPublisher<md::Depth5>> shmQueue = std::make_shared<pubsub::SPMCPublisher<md::Depth5>>(key.c_str());
            mDepth5Publisher[key] = shmQueue;
        }
        else if (marketTypeEnum == md::DEPTH10) {
            std::shared_ptr<pubsub::SPMCPublisher<md::Depth10>> shmQueue = std::make_shared<pubsub::SPMCPublisher<md::Depth10>>(key.c_str());
            mDepth10Publisher[key] = shmQueue;
        }
        else if (marketTypeEnum == md::DEPTH20) {
            std::shared_ptr<pubsub::SPMCPublisher<md::Depth20>> shmQueue = std::make_shared<pubsub::SPMCPublisher<md::Depth20>>(key.c_str());
            mDepth20Publisher[key] = shmQueue;
        }
        else if (marketTypeEnum == md::TRADES) {
            std::shared_ptr<pubsub::SPMCPublisher<md::Trades>> shmQueue = std::make_shared<pubsub::SPMCPublisher<md::Trades>>(key.c_str());
            mTradesPublisher[key] = shmQueue;
        }
        else if (marketTypeEnum == md::KLINE_1m) {
            std::shared_ptr<pubsub::SPMCPublisher<md::Kline>> shmQueue = std::make_shared<pubsub::SPMCPublisher<md::Kline>>(key.c_str());
            mKlinePublisher[key] = shmQueue;
        }
        else if (marketTypeEnum == md::FUNDING_RATE) {
            std::shared_ptr<pubsub::SPMCPublisher<md::FundingRate>> shmQueue = std::make_shared<pubsub::SPMCPublisher<md::FundingRate>>(key.c_str());
            mFundingRatePublisher[key] = shmQueue;
        }
    }
#endif
}

BaseUnit::~BaseUnit() {
    
}

void md::BaseUnit::start() {
    try {
        std::thread parseThread(&BaseUnit::consume, this);
        parseThread.detach();

        std::thread monitorThread(&BaseUnit::monitorWs, this);
        monitorThread.detach();
    }
    catch (const std::exception& e) {
        LOG_ERROR("unit start error: {}", e.what());
    }
}

void md::BaseUnit::consume() {
    std::string msg;
    while (1) {
        try {
            if (mQueue.pop(msg)) {
                parseMarketData(msg);
                continue;
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("{}.{}.{} exception {} happened, msg: {}", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], md::MarketTypeEnum2StrMap[marketTypeEnum], e.what(), msg);
        }

    }
    
}

void md::BaseUnit::monitorWs() {
    while (1) {
        try {
            int sleepMill = crypto::get_int_rand(100, 500);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepMill));
            subWebsocekt();

            int count = 10;
            while (isConnected) {
                std::this_thread::sleep_for(std::chrono::seconds(10));
                count++;

                if (!isConnected) {
                    LOG_WARN("{} ws address disconnected, connecting now", wsUrl);
                    break;
                }
                else {
                    long now = crypto::getCurrentTime(); // ms
                    if (exchangeTypeEnum == BINANCE) {
                        int reconnectCount = 0;
                        std::vector<std::string> keysDelayVec;
                        int thresholdIndex = 1;
                        long thresholdBase = 1e6;
                        
                        if (instTypeEnum == USDT_SWAP) {
                            switch (marketTypeEnum) {
                                case md::FUNDING_RATE: {
                                    thresholdBase = 600 * 1e6;
                                    break;
                                }
                                case md::KLINE_1m:
                                case md::KLINE_1h:
                                case md::KLINE_2h:
                                case md::KLINE_4h:
                                case md::KLINE_8h: {
                                    thresholdBase = 120 * 1e6;
                                    break;
                                }
                                case md::TRADES: {
                                    thresholdBase = 600 * 1e6;
                                    break;             
                                }
                                case md::DEPTH1:
                                case md::DEPTH5:
                                case md::DEPTH10:
                                case md::DEPTH20: {
                                    thresholdBase = 30 * 1e6;
                                    break;        
                                }
                                default: {
                                    thresholdBase = 30 * 1e6;
                                    break;             
                                }
                            }

                            long threshold = thresholdBase * thresholdIndex;
                            for (auto iter = mLatestUpdateTime.begin(); iter != mLatestUpdateTime.end(); ++iter) {
                                std::string key = iter->first;
                                long lastUpdateTime = iter->second;

                                if (lastUpdateTime == 0) {
                                    continue;
                                }

                                long diff = now - lastUpdateTime;
                                if (diff > threshold) {
                                    LOG_ERROR("{}, latestUpdateTime: {}, no data for {} seconds!", key, lastUpdateTime, diff);
                                    keysDelayVec.push_back(key);
                                    reconnectCount++;
                                }
                            }

                            if (reconnectCount >= 3 * thresholdIndex || reconnectCount >= vInstInfo.size()) {
                                std::string instIdStr = keysDelayVec.size() > 0 ? keysDelayVec[0] : "";
                                LOG_ERROR("{} ws address: {} no data for {} seconds, reconnectCount: {}, need reconnect!", instIdStr, wsUrl, threshold, reconnectCount);
                                for (size_t i = 0; i < keysDelayVec.size(); ++i) {
                                    std::string key = keysDelayVec[i];
                                    mLatestUpdateTime[key] = 0;
                                }

                                isConnected = false;
                                break;
                            }
                        }
                    }

                    long dataUpdatedDiff = now - latestDataUpdateTime;
                    if (dataUpdatedDiff > 60 * 1e6) {
                        LOG_ERROR("ws address: {} no data for {} seconds, need reconnect!", wsUrl, dataUpdatedDiff);
                        isConnected = false;
                        break;
                    }

                    switch (exchangeTypeEnum) {
                        case BINANCE: {
                            if (count % 2 == 0) {
                                LOG_INFO("{}.{}.{} ws is connected, will send pong!", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], md::MarketTypeEnum2StrMap[marketTypeEnum]);
                                pong();
                            }
                            break;
                        }
                        default: {
                            if (count % 2 == 0) {
                                LOG_INFO("{}.{}.{} ws is connected, will send pong!", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], md::MarketTypeEnum2StrMap[marketTypeEnum]);
                                ping();
                            }
                            break;    
                        }
                    }

                }
            }
        }
        catch (const std::exception& e) {
            isConnected = false;
            LOG_ERROR("ws connect exception: {}", e.what());
        }

        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

void md::BaseUnit::onCloseMsg(web::websockets::client::websocket_close_status status, const utility::string_t& reason, const std::error_code& code, std::shared_ptr<websocket_callback_client> selfWs) {
    try {
        if (selfWs != pWsClient) {
            LOG_INFO("DB old websocket callback client closed successfully!");
            return;
        }

        isConnected = false;
        LOG_ERROR("DB receive close msg, reason: {}, error: {}", reason, code.message());

        if (crypto::str_cmp(reason.c_str(), "End of File") || crypto::str_cmp(reason.c_str(), "Underlying Transport Error") || crypto::str_cmp(reason.c_str(), "Normal")) {
            return;
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("{}", e.what());
    }
}

md::BaseMarket::BaseMarket(sm::SecurityManager* s, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot, const char* host, int port, const char* passwd) {
    smc = s;
    _instTypeVec = instTypeVec;
    _marketTypeVec = marketTypeVec;
    _instIdVec = instIdVec;
    tokenLot = lot;
    strcpy(_host, host);
    _port = port;
    strcpy(_passwd, passwd);

    generateUnitInfo();
}

md::BaseMarket::~BaseMarket() {

}

void md::BaseMarket::generateUnitInfo() {
    unitInfoVec.clear();
    for (auto marketType : _marketTypeVec) {
        for (auto instType : _instTypeVec) {
            std::vector<std::string> validInstIdVec;
            for (auto instId : _instIdVec) {
                md::InstrumentInfo info;
                if (smc->get_instrument_info(exchId, instType.c_str(), instId.c_str(), info)) {
                    validInstIdVec.push_back(info.originInstId);
                }
                else {
                    LOG_ERROR("not found {}.{}.{} smc info", exchId, instType, instId);
                }
            }

            if (crypto::str_cmp(instType.c_str(), "SPOT") && !crypto::has_str(marketType.c_str(), "FUNDING")) {
                size_t validSize = validInstIdVec.size();
                size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                for (size_t us = 0; us < unitSize; us++) {
                    UnitInfo unitInfo;
                    unitInfo.exchangeTypeEnum = ExchangeTypeStr2EnumMap[exchId];
                    unitInfo.instTypeEnum = InstTypeStr2EnumMap[instType];
                    unitInfo.marketTypeEnum = MarketTypeStr2EnumMap[marketType];
                    size_t startValidNum = tokenLot * us;
                    size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                    for (size_t i = startValidNum; i < endValidNum; i++) {
                        md::InstrumentInfo info;
                        if (smc->get_instrument_info(exchId, instType.c_str(), validInstIdVec[i].c_str(), info)) {
                            unitInfo.vInstInfo.push_back(info);
                        }      
                    }
                    unitInfoVec.push_back(unitInfo);
                }
            }
            else if ((crypto::has_str(instType.c_str(), "SWAP")) || (crypto::has_str(instType.c_str(), "FUTURES") && crypto::has_str(marketType.c_str(), "FUNDING") == false)) {//交割合约没有费率信息
                std::vector<std::string> coinStrVec;
                std::vector<std::string> notCoinStrVec;
                for (std::string inst : validInstIdVec) {
                    md::InstrumentInfo info;
                    if (smc->get_instrument_info(exchId, instType.c_str(), inst.c_str(), info)) {
                        if (crypto::str_cmp(info.quote, "USD")) {
                            coinStrVec.push_back(inst);
                        }
                        else {
                            notCoinStrVec.push_back(inst);
                        }
                    }
                    else {
                        LOG_ERROR("will not execute here! {}.{}.{}", exchId, instType, inst);
                    }
                }

                if (coinStrVec.size() > 0) {
                    size_t validSize = coinStrVec.size();
                    size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                    for (size_t us = 0; us < unitSize; us++) {
                        UnitInfo unitInfo;
                        unitInfo.exchangeTypeEnum = ExchangeTypeStr2EnumMap[exchId];
                        unitInfo.instTypeEnum = InstTypeStr2EnumMap[instType];
                        unitInfo.marketTypeEnum = MarketTypeStr2EnumMap[marketType];

                        size_t startValidNum = tokenLot * us;
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        for (size_t i = startValidNum;i < endValidNum; i++) {
                            md::InstrumentInfo info;
                            if (smc->get_instrument_info(exchId, instType.c_str(), coinStrVec[i].c_str(), info)) {
                                unitInfo.vInstInfo.push_back(info);
                            }
                        }

                        unitInfoVec.push_back(unitInfo);
                    }
                }

                if (notCoinStrVec.size() > 0){
                    size_t validSize = notCoinStrVec.size();
                    size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                    for (size_t us = 0; us < unitSize; us++) {
                        UnitInfo unitInfo;
                        unitInfo.exchangeTypeEnum = ExchangeTypeStr2EnumMap[exchId];
                        unitInfo.instTypeEnum = InstTypeStr2EnumMap[instType];
                        unitInfo.marketTypeEnum = MarketTypeStr2EnumMap[marketType];

                        size_t startValidNum = tokenLot * us;
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        for (size_t i = startValidNum; i < endValidNum; i++) {
                            md::InstrumentInfo info;
                            if (smc->get_instrument_info(exchId, instType.c_str(), notCoinStrVec[i].c_str(), info)) {
                                unitInfo.vInstInfo.push_back(info);
                            }
                        }

                        unitInfoVec.push_back(unitInfo);
                    }
                }
            }
        }
    }
}
