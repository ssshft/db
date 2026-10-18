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
#include "shm_global.h"

#include "BeastWsClient.h"
#include <simdjson.h>

#include "shm_global.h"
#include "time_util.h"
#include "config.h"

#include "crypto_exception.h"
#include "precision_util.h"
#include "MdSlotRing.h"

                                                         
namespace md {
    struct UnitInfo {
        ExchangeType exchangeTypeEnum;
        InstType instTypeEnum;
        md::MarketType marketTypeEnum;
        std::vector<md::InstrumentInfo> vInstInfo;
    };

    class BaseUnit {
    public:
        BaseUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379, const char* password = "");
        virtual ~BaseUnit();
        void start();
        void consume();
        void subWebsocekt();

        virtual void generateSubBody() = 0;
        virtual void parseMarketData(const std::string& msg, int64_t tsNet) = 0;
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
    
        int64_t latestDataUpdateTime{0};

        net::WsConfig cfg;
        std::shared_ptr<net::WsClient> pWsClient{nullptr};

        sm::SecurityManager* smc{nullptr};
        md::MdSlotRing* mdRing{nullptr};
        
        RedisClient* redisClient{nullptr};
        std::unordered_map<std::string, std::shared_ptr<Depth1Spmc>> mDepth1Publisher;
        std::unordered_map<std::string, std::shared_ptr<Depth5Spmc>> mDepth5Publisher;
        std::unordered_map<std::string, std::shared_ptr<Depth10Spmc>> mDepth10Publisher;
        std::unordered_map<std::string, std::shared_ptr<Depth20Spmc>> mDepth20Publisher;
        std::unordered_map<std::string, std::shared_ptr<TradesSpmc>> mTradesPublisher;
        std::unordered_map<std::string, std::shared_ptr<KlineSpmc>> mKlinePublisher;
        std::unordered_map<std::string, std::shared_ptr<FundingRateSpmc>> mFundingRatePublisher;

        simdjson::ondemand::parser parser;

        // ---- 新接入交易所（HTX/BITGET/MEXC/KUCOIN/BINGX/XT）共用的两个小工具 ----
        //
        // 把一帧原始报文写进 ring。比直接调 mdRing->write 多两件事：
        //   ① kOverflow（len > maxMsg）打**一次性** LOG_ERROR —— 现有各 unit 都忽略返回值，
        //      一旦单帧超长就是静默丢数据，录数据时最难查。
        //   ② 不改变 kDropped（背压）语义。
        void pushRaw(const char* data, size_t len, int64_t tsNet);

        // 按 originInstId 反查 InstrumentInfo 并算出 md channel key。
        // 六家的 parseMarketData 都要做这一步，抄六遍容易漏 smc 查不到时的日志。
        bool resolveInst(const std::string& originInstId, md::InstrumentInfo& info, std::string& key);

        bool overflowLogged_{false};   // pushRaw 的一次性告警标志（每个 unit 一个）
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
