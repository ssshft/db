#pragma once
#include "base/market_base_v2.h"


//usdt swap
#define BYBIT_WEBSOCKET_HOST_PUBLIC_USDT_SWAP "wss://stream.bybit.com/contract/usdt/public/v3"
//c swap
#define BYBIT_WEBSOCKET_HOST_PUBLIC_C_SWAP "wss://stream.bybit.com/contract/inverse/public/v3"
//spot
#define BYBIT_WEBSOCKET_HOST_PUBLIC_SPOT "wss://stream.bybit.com/spot/public/v3"


namespace md {
    struct BybitUnit : public MarketDataBaseStruct{
        BybitUnit(ExchangeType exchangeType, InstType instType, MarketType marketType,
                    const char *ip = "127.0.0.1", int port = 9379, const char *passwd = ""){
            this->exchangeTypeEnum = exchangeType;
            this->instTypeEnum = instType;
            this->marketTypeEnum = marketType;
            redisClient = new RedisClient(ip, port, passwd);
        }
        //计数用的
        int subCount = 0;
        int subId = crypto::get_int_rand(1000, 100000);
        //缓存一档行情最新快照
        robin_hood::unordered_map<string, md::CryptoMarketData> _cacheMDMap;
        //发给交易所的订阅数据
        json::value subValue;
        virtual void construct();
        virtual void sub_websocket();
        virtual void ping();
        virtual void pong();
        virtual void on_websocket_msg(const websocket_incoming_message &in_msg);
        virtual void save_md_string(const MDMsg &mdMsg);
    };

    class BybitMarketClientV2 : public MarketDataBaseClass {
    public:
        BybitMarketClientV2(vector<string> instTypeVec, vector<string> marketTypeVec, vector<string> instIdVec,
                           int tokenLot = 30, const char *ip = "127.0.0.1",
                           const int port = 9379,const char *passwd="")
                : MarketDataBaseClass(instTypeVec, marketTypeVec, instIdVec, tokenLot, ip, port, passwd){
            strcpy(exchId, "BYBIT");
        }
        ~BybitMarketClientV2(){};

    public:
        virtual void start();
    private:
        //确定好每个unit单元的有效originInstId
        virtual void construct();
        //打印监控一些队列的堆积情况
        virtual void print_stat();

        vector<BybitUnit> tokenUnitVec;
    };
}