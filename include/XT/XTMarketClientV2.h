#pragma once
#include "base/market_base_v2.h"

//spot
#define XT_WEBSOCKET_HOST_PUBLIC_SPOT "wss://xtsocket.xt.com/websocket"
//usdt swap
#define XT_WEBSOCKET_HOST_PUBLIC_USDT_SWAP "wss://fstream.xt.com/ws/market"

namespace md {
    struct XTUnit : public MarketDataBaseStruct{
        XTUnit(ExchangeType exchangeType, InstType instType, MarketType marketType,
                    const char *ip = "127.0.0.1", int port = 9379, const char *passwd = ""){
            this->exchangeTypeEnum = exchangeType;
            this->instTypeEnum = instType;
            this->marketTypeEnum = marketType;
            redisClient = new RedisClient(ip, port, passwd);
        }

        //发给交易所的订阅数据
        vector<json::value> subValueVec;
        bool mbpFlag = false;
        virtual void construct();
        virtual void sub_websocket();
        virtual void ping();
        virtual void pong();
        virtual void on_websocket_msg(const websocket_incoming_message &in_msg);
        virtual void save_md_string(const MDMsg &mdMsg);
        virtual void save_spot_md(const MDMsg &mdMsg);
        virtual void save_swap_md(const MDMsg &mdMsg);
        void pong(const string &tick);
    };

    class XTMarketClientV2 : public MarketDataBaseClass {
    public:
        XTMarketClientV2(vector<string> instTypeVec, vector<string> marketTypeVec, vector<string> instIdVec,
                           int tokenLot = 30, const char *ip = "127.0.0.1", const int port = 9379,const char *passwd="")
                : MarketDataBaseClass(instTypeVec, marketTypeVec, instIdVec, tokenLot, ip, port, passwd){
            strcpy(exchId, "XT");
        }
        ~XTMarketClientV2(){};

    public:
        virtual void start();
    private:
        //请求全量rest地址
        string m_spotRestBaseUrl;
        //存储需要请求全量的instId，原始格式，大写(0 spot 1 swap 2 futures)
        unordered_map<string, XTUnit*> reqInstIdDict[3];
        //mbp全量数据只能用一个线程串行轮询全量rest接口，因为如果多线程的话会触发限频
        virtual void req_spot_mbp();
        //确定好每个unit单元的有效originInstId
        virtual void construct();
        //打印监控一些队列的堆积情况
        virtual void print_stat();

        long subId = 0;
        vector<XTUnit> tokenUnitVec;

    };
}
