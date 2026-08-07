#pragma once

#include <set>
#include <string>
#include <vector>
#include <map>
#include <thread>
#include <chrono>
#include <algorithm>

#include "redis_client.h"
#include "securitymanager.h"
#include "log_engine.h"
#include "concurrent_queue.h"
#include "shm_spmc_queue.h"

#include "BeastWsClient.h"
#include <simdjson.h>

#include "shm_global.h"
#include "time_util.h"
#include "config.h"

#include "crypto_exception.h"
#include "precision_util.h"

                                                         
namespace md {
    constexpr int BUFF_SIZE = 1024 * 512;
    constexpr int kDelayCountThenRestart = 1024 * 16;
    constexpr int kTokenUnitSize = 2048;

    struct UnitInfo {
        ExchangeType exchangeTypeEnum;
        InstType instTypeEnum;
        md::MarketType marketTypeEnum;
        std::vector<md::InstrumentInfo> vInstInfo;
    };


    class BaseUnit { // 需要有两次跳转的功能
    public:
        BaseUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379, const char* password = "");
        virtual ~BaseUnit();
        void start();
        void consume();
        void subWebsocekt();

        virtual void generateSubBody() = 0;
        virtual void parseMarketData(const std::string& msg) = 0;
        virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) = 0;
        virtual void onOpen();
        virtual void onClose(int code, const std::string& reason);
        virtual void onError(const std::string& msg);
    

    protected:
        ExchangeType exchangeTypeEnum;
        InstType instTypeEnum;
        md::MarketType marketTypeEnum;
        std::vector<md::InstrumentInfo> vInstInfo;
        std::string wsUrl{""};
    

        long latestDataUpdateTime{0};

        net::WsConfig cfg;
        std::shared_ptr<net::WsClient> pWsClient{nullptr};

        sm::SecurityManager* smc{nullptr};
        pubsub::ConcurrentQueueZMQ<std::string, BUFF_SIZE> mQueue; // 此queue需要替换

        RedisClient* redisClient{nullptr};
        std::unordered_map<std::string, std::shared_ptr<pubsub::SPMCPublisher<md::Depth1>>> mDepth1Publisher;
        std::unordered_map<std::string, std::shared_ptr<pubsub::SPMCPublisher<md::Depth5>>> mDepth5Publisher;
        std::unordered_map<std::string, std::shared_ptr<pubsub::SPMCPublisher<md::Depth10>>> mDepth10Publisher;
        std::unordered_map<std::string, std::shared_ptr<pubsub::SPMCPublisher<md::Depth20>>> mDepth20Publisher;
        std::unordered_map<std::string, std::shared_ptr<pubsub::SPMCPublisher<md::Trades>>> mTradesPublisher;
        std::unordered_map<std::string, std::shared_ptr<pubsub::SPMCPublisher<md::Kline>>> mKlinePublisher;
        std::unordered_map<std::string, std::shared_ptr<pubsub::SPMCPublisher<md::FundingRate>>> mFundingRatePublisher;

        simdjson::ondemand::parser parser;
    };


    class BaseMarket {
    public:
        BaseMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30, const char* host = "127.0.0.1", int port = 9379, const char* passwd = "");
        virtual ~BaseMarket();
        void generateUnitInfo();
        virtual void start() = 0;

    protected:
        sm::SecurityManager* smc{nullptr};
        std::vector<std::string> _instTypeVec;
        std::vector<std::string> _marketTypeVec;
        std::vector<std::string> _instIdVec;
        std::vector<UnitInfo> unitInfoVec;
        
        int tokenLot{30};
        char exchId[16]{""};
        char _host[32]{""};
        char _passwd[32]{""};
        int _port{0};
    };
}
