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

        void setCpuCore(int core);
        md::MarketType getMarketType();

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

        // consume 线程的绑核号（-1 = 不绑）
        int cpuCore{-1};

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

        // 网络耗时：本地收到 − 交易所事件时间（两者都是墙钟 µs）。
        void recordNetDelay(int64_t tsRecv, int64_t tsEvent);

        bool overflowLogged_{false};   // pushRaw 的一次性告警标志（每个 unit 一个）
    };


    class BaseMarket {
    public:
        BaseMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30, const char* host = "127.0.0.1", int port = 9379, const char* passwd = "");
        virtual ~BaseMarket();
        void generateUnitInfo();

        // 唯一的 start 实现：遍历 _units 逐个起线程（见 BaseMarket.cpp）。
        void start();

        // 线程绑核：配置里每个交易所给一个**核列表**（cpucores，例如 [0,1,2]），
        // 这里按 unit 创建顺序，给每个**自旋** unit（DEPTH1/TRADES）发一个核，用完就不绑。
        void setCpuCores(const std::vector<int>& cores);

    protected:
        // 本 market 建出来的 unit —— **唯一**的持有者（各交易所在构造时 push 进来）。
        // 三件事都靠它：分配核号（setCpuCores）、启动（start）、以及析构时能不能 delete。
        // 析构仍然留在各交易所自己手里：目前只有 3 家 SBE 会 delete，其余 17 家是空析构
        // （本来就泄漏，进程活到退出，不在本次改动范围内）。
        std::vector<BaseUnit*> _units;

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
