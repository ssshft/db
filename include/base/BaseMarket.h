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

#include <cpprest/http_client.h>
#include <cpprest/ws_client.h>
#include <cpprest/filestream.h>
#include <simdjson.h>

#include "shm_global.h"
#include "time_util.h"

#include "crypto_exception.h"
#include "precision_util.h"


#define START_SUB_WEBSOCKET() \
    try { \
        LOG_INFO("start to sub websocket to {}", wsUrl); \
        if (pWsClient != nullptr) { \
            auto oldWsClient = pWsClient; \
            std::thread([oldWsClient]() { \
                try { \
                    oldWsClient->set_close_handler([](websocket_close_status, const utility::string_t&, const std::error_code&) { \
                        LOG_INFO("old websocket callback client closed successfully!"); \
                    }); \
                    oldWsClient->close().wait(); \
                    LOG_INFO("old websocket callback client closed successfully!"); \
                } \
                catch (...) { \
                    LOG_ERROR("Exception during old websocket cleanup!"); \
                } \
            }).detach(); \
            pWsClient = nullptr; \
        } \
        try { \
            LOG_INFO("creating new websocket callback client!"); \
            pWsClient = std::make_shared<websocket_callback_client>(); \
            LOG_INFO("new websocket callback client created!"); \
        } \
        catch (const std::exception& e) { \
            LOG_ERROR("failed to create websocket callback client: {}", e.what()); \
            return; \
        } \
        \
        web::http::uri_builder builder(wsUrl); \
        LOG_INFO("{} connecting to {}", ExchangeTypeEnum2StrMap[exchangeTypeEnum], builder.to_string()); \
        std::promise<bool> prom; \
        std::future<bool> fut = prom.get_future(); \
        \
        try { \
            pWsClient->connect(builder.to_string()) \
            .then([&]() { \
                auto selfWs = pWsClient; \
                pWsClient->set_message_handler([this](const web::websockets::client::websocket_incoming_message& msg) { \
                    this->onWebsocketMsg(msg); \
                }); \
                pWsClient->set_close_handler([this, selfWs](websocket_close_status close_status, const utility::string_t& reason, const std::error_code& error) { \
                    this->onCloseMsg(close_status, reason, error, selfWs); \
                }); \
                prom.set_value(true); \
            }); \
            \
            if (fut.wait_for(std::chrono::seconds(10)) == std::future_status::timeout) { \
                isConnected = false; \
                LOG_ERROR("connected with {} timeout!", builder.to_string()); \
                return; \
            } \
            \
            isConnected = true; \
            LOG_INFO("connected with {} successfully!", builder.to_string()); \
        } \
        catch (const std::exception& e) { \
            isConnected = false; \
            LOG_ERROR("connected with {} exception: {}", builder.to_string(), e.what()); \
        } 



#define START_SUB_SBE_WEBSOCKET(cfg) \
    try { \
        LOG_INFO("start to sub websocket to {}", wsUrl); \
        if (pWsClient != nullptr) { \
            auto oldWsClient = pWsClient; \
            std::thread([oldWsClient]() { \
                try { \
                    oldWsClient->set_close_handler([](websocket_close_status, const utility::string_t&, const std::error_code&) { \
                        LOG_INFO("old websocket callback client closed successfully!"); \
                    }); \
                    oldWsClient->close().wait(); \
                    LOG_INFO("old websocket callback client closed successfully!"); \
                } \
                catch (...) { \
                    LOG_ERROR("Exception during old websocket cleanup!"); \
                } \
            }).detach(); \
            pWsClient = nullptr; \
        } \
        try { \
            LOG_INFO("creating new websocket callback client!"); \
            pWsClient = std::make_shared<websocket_callback_client>(cfg); \
            LOG_INFO("new websocket callback client created!"); \
        } \
        catch (const std::exception& e) { \
            LOG_ERROR("failed to create websocket callback client: {}", e.what()); \
            return; \
        } \
        \
        web::http::uri_builder builder(wsUrl); \
        LOG_INFO("{} connecting to {}", ExchangeTypeEnum2StrMap[exchangeTypeEnum], builder.to_string()); \
        std::promise<bool> prom; \
        std::future<bool> fut = prom.get_future(); \
        \
        try { \
            pWsClient->connect(builder.to_string()) \
            .then([&]() { \
                auto selfWs = pWsClient; \
                pWsClient->set_message_handler([this](const web::websockets::client::websocket_incoming_message& msg) { \
                    this->onWebsocketMsg(msg); \
                }); \
                pWsClient->set_close_handler([this, selfWs](websocket_close_status close_status, const utility::string_t& reason, const std::error_code& error) { \
                    this->onCloseMsg(close_status, reason, error, selfWs); \
                }); \
                prom.set_value(true); \
            }); \
            \
            if (fut.wait_for(std::chrono::seconds(10)) == std::future_status::timeout) { \
                isConnected = false; \
                LOG_ERROR("connected with {} timeout!", builder.to_string()); \
                return; \
            } \
            \
            isConnected = true; \
            LOG_INFO("connected with {} successfully!", builder.to_string()); \
        } \
        catch (const std::exception& e) { \
            isConnected = false; \
            LOG_ERROR("connected with {} exception: {}", builder.to_string(), e.what()); \
        } 


#define END_SUB_WEBSOCKET() \
    } \
    catch (const std::exception& e) { \
        isConnected = false; \
        LOG_ERROR("connected with {} error: {}", wsUrl, e.what()); \
    }

                                                         
namespace md {
    using namespace web;
    using namespace web::websockets::client;
    using namespace concurrency::streams;
    using namespace rapidjson;

    constexpr int BUFF_SIZE = 1024 * 512;
    constexpr int kDelayCountThenRestart = 1024 * 16;
    constexpr int kTokenUnitSize = 2048;

    struct SbeAccount {
        std::string apiKey{""};
	    std::string secretKey{""};
        std::string password{""};    
    };

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
        void monitorWs();
        virtual void generateSubBody() = 0;
        virtual void subWebsocekt() = 0;
        virtual void onWebsocketMsg(const web::websockets::client::websocket_incoming_message& msg) = 0;
        virtual void parseMarketData(const std::string& msg) = 0;
        virtual void ping() = 0;
        virtual void pong() = 0;
        virtual void onCloseMsg(web::websockets::client::websocket_close_status status, const utility::string_t& reason, const std::error_code& code, std::shared_ptr<websocket_callback_client> selfWs);

    protected:
        ExchangeType exchangeTypeEnum;
        InstType instTypeEnum;
        md::MarketType marketTypeEnum;
        std::vector<md::InstrumentInfo> vInstInfo;
        std::string wsUrl{""};
        bool isConnected{false};

        tbb::concurrent_unordered_map<std::string, long> mLatestUpdateTime;
        long latestDataUpdateTime{0};

        std::shared_ptr<websocket_callback_client> pWsClient{nullptr};
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
