#include "base/BaseMarket.h"
#include "key_util.h"


md::BaseUnit::BaseUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* password) {
    smc = s;
    exchangeTypeEnum = exchTy;
    instTypeEnum = instTy;
    marketTypeEnum = marketTy;
    vInstInfo = instInfoVec;
    // redisClient = new RedisClient(host, port, password, false, true);
    latestDataUpdateTime = 0;
    pWsClient = nullptr;

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

    LOG_INFO("Unit construct complete, exchId: {} instType: {} marketType: {} instInfo size: {}", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], MarketTypeEnum2StrMap[marketTypeEnum], vInstInfo.size());
}

md::BaseUnit::~BaseUnit() {
    
}

void md::BaseUnit::start() {
    try {
        std::thread parseThread(&BaseUnit::consume, this);
        parseThread.detach();

        subWebsocekt();
    }
    catch (const std::exception& e) {
        LOG_ERROR("unit start error: {}", e.what());
    }
}

void md::BaseUnit::subWebsocekt() {
    pWsClient = net::WsClient::create(cfg);

    pWsClient->on_message([this](const uint8_t* d, size_t n, bool b, int64_t t) {
        this->onWebsocketMsg(d, n, b, t);
    });

    pWsClient->on_open([this]() { 
        this->onOpen(); 
    });

    pWsClient->on_close([this](int c, const std::string& r) { 
        this->onClose(c, r); 
    });

    pWsClient->on_error([this](const std::string& m) { 
        this->onError(m); 
    });

    pWsClient->start();
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

void md::BaseUnit::onOpen() {
    LOG_INFO("Unit ws open, exchId: {} instType: {} marketType: {}", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], MarketTypeEnum2StrMap[marketTypeEnum]);
}

void md::BaseUnit::onClose(int code, const std::string& reason) {
    LOG_WARN("Unit ws closed: code={} reason={} (auto-reconnect), exchId: {} instType: {} marketType: {} instInfo size: {}", code, reason, ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], MarketTypeEnum2StrMap[marketTypeEnum], vInstInfo.size());
}

void md::BaseUnit::onError(const std::string& msg) {
    LOG_ERROR("Unit ws error: {}, exchId: {} instType: {} marketType: {} instInfo size: {}", msg, ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], MarketTypeEnum2StrMap[marketTypeEnum], vInstInfo.size());
}

md::BaseMarket::BaseMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot, const char* host, int port, const char* passwd) {
    smc = s;
    strcpy(exchId, exId);
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
