#pragma once
#include "base/market_base_v2.h"
//spot
#define HUOBI_WEBSOCKET_HOST_PUBLIC_SPOT "wss://api-aws.huobi.pro/ws"
//spot mbp
#define HUOBI_WEBSOCKET_HOST_PUBLIC_MBP "wss://api-aws.huobi.pro/feed"

namespace md {
    struct HuobiUnit : public MarketDataBaseStruct{
        HuobiUnit(ExchangeType exchangeType, InstType instType, MarketType marketType,
                    const char *ip = "127.0.0.1", int port = 9379, const char *passwd = ""){
            this->exchangeTypeEnum = exchangeType;
            this->instTypeEnum = instType;
            this->marketTypeEnum = marketType;
            redisClient = new RedisClient(ip, port, passwd);
        }

        bool isFinished = false;
        bool hasBinded = false;
        //发给交易所的订阅数据
//        json::value subValue;
        int subCount = 0;
        vector<json::value> subValueVec;
        bool needMBP = false;
        virtual void construct();
        virtual void sub_websocket();
        virtual void ping();
        virtual void pong();
        virtual void on_websocket_msg(const websocket_incoming_message &in_msg);
        virtual void save_md_string(const MDMsg &mdMsg);
        virtual void save_spot_md(const MDMsg &mdMsg);
        void pong(const string &tick);

//        void update_mbp(const string &topic, MBP &mbp);
//        void monitor_ws();
//        void start(){
//            //启动解析线程
//            std::thread consumeThread(&MarketDataBaseStruct::consume, this);
//            consumeThread.detach();
//            //启动ws监控线程
//            std::thread monitorWSThread(&HuobiUnit::monitor_ws, this);
//            monitorWSThread.detach();
//        }
    };

    class HuobiMarketClient : public MarketDataBaseClass {
    public:
        HuobiMarketClient(vector<string> instTypeVec, vector<string> marketTypeVec, vector<string> instIdVec,
                           int tokenLot = 30, const char *ip = "127.0.0.1", const int port = 9379,const char *passwd="")
                : MarketDataBaseClass(instTypeVec, marketTypeVec, instIdVec, tokenLot, ip, port,passwd){
            strcpy(exchId, "HUOBI");
        }
        ~HuobiMarketClient(){};

    public:
        virtual void start();
    private:
        //请求全量rest地址
//        string m_spotRestBaseUrl;
        //存储需要请求全量的instId，原始格式，大写
//        vector<string> reqInstIdVec;
        //mbp全量数据只能用一个线程串行轮询全量rest接口，因为如果多线程的话会触发限频
        virtual void req_spot_mbp();
        //确定好每个unit单元的有效originInstId
        virtual void construct();
        //打印监控一些队列的堆积情况
        virtual void print_stat();

        vector<HuobiUnit> tokenUnitVec;
        long subId = 0;
    };
}