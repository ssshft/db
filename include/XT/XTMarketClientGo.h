//
// Created by qyang on 2021/11/29.
//

#ifndef DB_XTWSMARKETCLIENT_H
#define DB_XTWSMARKETCLIENT_H

//#pragma once
#include <string>
#include <iostream>
#include <thread>
#include <pthread.h>
#include <stdio.h>
#include "cpprest/json.h"
#include "utils.h"
#include <cpprest/ws_client.h>
#include <cpprest/filestream.h>
#include "data_struct.h"
#include "define.h"
#include "log_engine.h"
#include "db_global.h"
#include "boost/algorithm/string.hpp"
#include "base/marketdata_base.h"
#include "key_util.h"
#include "string_util.h"
#include "cryptlite/sha256.h"
#include <cryptlite/hmac.h>
#include "coroutine.h"


#define XTExchangeId "XT"
#define XT_TIME_OUT 10000000
//#define HUOBI_MD_THREAD_LIMIT 120

//spot
#define XT_WEBSOCKET_HOST_PUBLIC_SPOT "wss://xtsocket.xt.com/websocket"
struct SubSt{
    ExchangeType exchType;
    InstType instType;
    md::MarketType marketType;
    web::json::value subValue;

};
//struct CoroutineEnvSt{
//    SubSt subSt;
////    websocket_client *client;
//    long lastRecvTime = 0;
//    co_chan<long> mdQueue(64);
//};
namespace md{
using namespace web;
using namespace web::websockets::client;
using namespace concurrency::streams;
using namespace std;
using namespace rapidjson;

#if 1
    struct CoroutineEnvSt{
//        stCoCond_t* cond;
        SubSt subSt;
        websocket_client *client;
        long lastRecvTime = 0;
//        co_chan<std::shared_ptr<MdMessage> > mdQueue;
//        moodycamel::ConcurrentQueue<MdMessageSt*> *mdQueue = new moodycamel::ConcurrentQueue<MdMessageSt*>(1024);
    };
#endif
    long xtReceive = 0;
    co_chan<md::MdMessage *> mdChan(1024*16);
class XTWSMarketClient: public MarketDataClientBase{
//    vector<json::value> spotSubValueVec;
//    vector<json::value> spotReqValueVec;
    vector<SubSt> subStVec;
//    moodycamel::ConcurrentQueue<MdMessageSt*> *mdQueue2 = new moodycamel::ConcurrentQueue<MdMessageSt*>(1024);

public:
    XTWSMarketClient(vector<string> instTypeVec, vector<string> marketTypeVec, vector<string> instIdVec,
                          const char *ip = "127.0.0.1", const int port = 9379)
                          : MarketDataClientBase(instTypeVec, marketTypeVec, instIdVec, ip, port){
//        spotSubValue["method"] = json::value::string("SUBSCRIBE");
//        spotSubValue["id"] = json::value::number(sub_id);
    }
    ~XTWSMarketClient(){
        printf("good bye %s!\n",__FUNCTION__);
    }

    void start(){
        construct_sub_str();
        for(auto value : subStVec ){
            CoroutineEnvSt *env =  new CoroutineEnvSt;
            env->client = new websocket_client();
            env->subSt = value;
            go [=]{
                monitor(env);
            };
            go [=]{
                sub_market(env);
            };
            go [=]{
                consume_marketdata(env);
            };
//            usleep(1000000);
        }

    }

    void construct_sub_str(){
        for(auto instType : _instTypeVec){
            for(auto instId : _instIdVec){
                InstrumentInfo info;
                if(smc->get_instrument_info(XTExchangeId, instType.c_str(),
                                            instId.c_str(), info) == true){
                    string lowerOriginInstId = crypto::to_lower(info.originInstId);//(info.originInstId);//
                    for(auto marketType : _marketTypeVec){
                        if(crypto::str_cmp(instType.c_str(), "SPOT") == true){
//                            string lowerMarketType = crypto::to_lower(marketType);
                            if(crypto::has_str(marketType, "TRADE") == true){
                                SubSt st;
                                st.exchType = XT;
                                st.instType = SPOT;
                                st.marketType = TRADES;
                                json::value value;
                                value["channel"] = json::value::string("ex_last_trade");
                                value["since"] = json::value::number(0);
                                value["market"] = json::value::string(lowerOriginInstId.c_str());
                                value["event"] = json::value::string("addChannel");
                                st.subValue = value;
                                subStVec.push_back(st);
                            }
                            else if(crypto::has_str(marketType, "MBP") == true){
                                SubSt st;
                                st.exchType = XT;
                                st.instType = SPOT;
                                st.marketType = MBPType;
                                json::value value;
                                value["channel"] = json::value::string("ex_depth_data");
                                value["market"] = json::value::string(lowerOriginInstId.c_str());
                                value["event"] = json::value::string("addChannel");
                                st.subValue = value;
                                subStVec.push_back(st);
                            }
                            else{
                                LOG_ERROR("not support %s now!", marketType.c_str());
                            }
                        }
                    }
                }
                else {
                    LOG_ERROR("instrument info not exist %s.%s.%s", XTExchangeId, instType.c_str(), instId.c_str());
                }
            }
        }
        for(auto value : subStVec){
            //printf("%s\n",value.subValue.serialize().c_str());
        }
    }

private:
    void monitor(CoroutineEnvSt *env){
        long now = 0;
        long timeOut = XT_TIME_OUT;

        while(1){
//            env->client = new websocket_client();
            bool needResub = false;
            int pingCycle = 0;
            while(!needResub) {
                pingCycle++;
                try{
                    now = crypto::getCurrentTime();
                    if (now - env->lastRecvTime > timeOut && env->lastRecvTime != 0) {
                        LOG_ERROR("TIMEOUT %s ,delay:%ld, lastRecvTime:%ld", XTExchangeId,
                                  now - env->lastRecvTime, env->lastRecvTime);
                        env->client->close();
                        env->lastRecvTime = 0;
                        needResub = true;
                        break;
                    }
                    if(needResub){
                        LOG_INFO("%s need to resubscribe md", XTExchangeId);
                        break;
                    }

//                    if(pingCycle % 1 == 0){
//                        LOG_INFO("%s receive:%ld, left:%ld", XTExchangeId,
//                                 xtReceive, env->mdQueue->size_approx());
//                        printf("%s receive:%ld, left:%ld\n", XTExchangeId,
//                               xtReceive, env->mdQueue->size_approx());
//                    }
                    sleep(1);
//                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                catch(std::exception &e){
                    LOG_ERROR("%s", e.what());
                }
            }
        }
    }

    void sub_market(CoroutineEnvSt *env){
        string subUrl;
        try{
            if(SPOT == env->subSt.instType){
                subUrl = string(XT_WEBSOCKET_HOST_PUBLIC_SPOT ) ;
            }
            else{
                throw runtime_error("not support now");
            }
            printf("%s\n", subUrl.c_str());
            uri wsuri(subUrl);
            env->client->connect(wsuri).wait();
            websocket_outgoing_message outMsg;
            outMsg.set_utf8_message(env->subSt.subValue.serialize().c_str());//sub.serialize());
            env->client->send(outMsg).wait();
            printf("send success: %s\n", env->subSt.subValue.serialize().c_str());
            LOG_INFO("send success: %s", env->subSt.subValue.serialize().c_str());
        }catch (std::exception &e){
            LOG_ERROR("%s", e.what());
        }
        char* sbuf = new char[MD_LENGTH];
        while(1){
            try {
                    websocket_incoming_message msg  = env->client->receive().get();

                    unsigned char buf[MD_LENGTH]= {0};
//                    unsigned char data[MD_LENGTH] = {0};
                    auto buflen = msg.body().streambuf().scopy(buf, MD_LENGTH);
//                    uLong datalen = sizeof(data);
                    env->lastRecvTime = crypto::getCurrentTime();
//                    strcpy((char *) data, (char *) buf);

//                    printf("%s,%d\n",__FUNCTION__,__LINE__);
                    long lastRecvTime = crypto::getCurrentTime();
                    env->lastRecvTime = lastRecvTime;
                    Document d;
                    Value &value = d.Parse<kParseNumbersAsStringsFlag>((char*)buf);
                    if (value.IsObject() && value.HasMember("ping")) {
                        json::value pong;
                        pong["pong"] = json::value::string(value["ping"].GetString());
//                        printf("%s\n",pong.serialize().c_str());
                        websocket_outgoing_message outMsg;
                        outMsg.set_utf8_message(pong.serialize().c_str());//sub.serialize());
                        env->client->send(outMsg).wait();
                        continue;
                    }
                    //
                    MdMessage *md = (md::MdMessage*)calloc(1, sizeof(md::MdMessage));
//                md->exchangeTypeEnum = XT;
//                md->subMarketType = huobiMarketType;
//                strcpy(md->data, msg);
                    strcpy(md->data,(char *) buf);
//                md.length = msg.length();
                    md->tsNet = lastRecvTime;
//                printf("huobiMarketType:%d\n", huobiMarketType);
                    mdChan.TryPush(md);
                    xtReceive++;
//                    printf("%d, %s\n",xtReceive, md->data);
            } catch (std::exception &e){
                LOG_INFO("exception: %s",e.what());
                env->client->close();
                break;
            }
        }
        delete[] sbuf;

    }
    void consume_marketdata(CoroutineEnvSt *env){
        md::MdMessage* mdMessage;// = (stTask_t*)calloc(1, sizeof(stTask_t));
        while (true) {
            try{
                if(mdChan.TryPop(mdMessage)){
                    long cTime = crypto::getCurrentTime();
                    cout << "delay: " << cTime - mdMessage->tsNet << endl;
//                    cout << env->subSt.subValue.serialize() << endl;
                    free(mdMessage);
                }
                else{
                    usleep(1);
                }
            }
            catch (exception & e){
                printf("%s\n", e.what());
            }
        }
    }

#if 0
    inline void save_md_string(MdMessage &mdMsg){
        rapidjson::Document d;
        rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg.data);
        if( !rawData.IsObject()){
            LOG_INFO("msg :%s is not a json object", mdMsg.data);
            return;
        }

        if(!((rawData.HasMember("ch") && rawData.HasMember("tick")) ||//sub
                (rawData.HasMember("rep") && rawData.HasMember("data")) )){//req
            LOG_INFO("%s", mdMsg.data);
            return;
        }

        char key[128]={0};
        char mdStr[2048];
        char instType[32]={0};

//        char exchId[16] = "BINANCE";
        char instId[32] = {0};
        char marketType[32];
        string ch;
        if(rawData.HasMember("ch")){//subscribe data
            ch = string(rawData["ch"].GetString());
        }
        else if(rawData.HasMember("rep")){//req data
            ch = string(rawData["rep"].GetString());
        }
        else{
            return;
        }
//        string ch(rawData["ch"].GetString());
        vector<string> s_vec = split(ch, '.');
        string originInstId = crypto::to_upper(s_vec[1]);//(boost::algorithm::to_upper_copy(s_vec[0]));
        InstrumentInfo info;
        if(mdMsg.subMarketType == SPOTMD){
            if(smc->get_instrument_info(XTExchangeId, "SPOT",
                                        originInstId.c_str(), info) == true){
                mdMsg.instTypeEnum = SPOT;
                sprintf(instType,"%s","SPOT");
            }
            else{
                LOG_ERROR("not found spot: %s info in smc", originInstId.c_str());
                return;
            }
        }
//        else if(mdMsg.subMarketType == USDTSWAPMD || mdMsg.subMarketType == USDSWAPMD) {
//            if (smc->get_instrument_info(XTExchangeId, "SWAP",
//                                         originInstId.c_str(), info) == true) {
//                mdMsg.instTypeEnum = SWAP;
//                sprintf(instType, "%s", "SWAP");
//            }
//            else{
//                LOG_ERROR("not found swap: %s info in smc", originInstId.c_str());
//                return;
//            }
//        }
//        else if(mdMsg.subMarketType == FUTURESMD){
//            if(smc->get_instrument_info(XTExchangeId, "FUTURES",
//                                        originInstId.c_str(), info) == true){
//                mdMsg.instTypeEnum = FUTURES;
//                sprintf(instType,"%s","FUTURES");
//            }
//            else{
//                LOG_ERROR("not found futures: %s info in smc", originInstId.c_str());
//                return;
//            }
//        }
        sprintf(instId,"%s", info.instId);
        string ts(rawData["ts"].GetString());
        if(mdMsg.instTypeEnum == SPOT){
//            string mType = s_vec[1];
            if(crypto::has_str(ch, "refresh") == true){
//                printf("%s,%d\n",__FUNCTION__,__LINE__);
                const rapidjson::Value &data = rawData["tick"];
                if(crypto::has_str(ch, "refresh.5") == true ){
                    sprintf(marketType,"%s","DEPTH5");
                }
                else if(crypto::has_str(ch, "refresh.10")  == true){
                    sprintf(marketType,"%s","DEPTH10");
                }
                else if(crypto::has_str(ch, "refresh.20")  == true){
                    sprintf(marketType,"%s","DEPTH20");
                }
                else{
                    return;
                }
//                printf("%s,%d\n",__FUNCTION__,__LINE__);
                sprintf(key, "%s.%s.%s.%s", XTExchangeId, instType, marketType, instId);
                string asksStr,bidsStr;
                asksStr.append("[");
                for(rapidjson::SizeType i = 0; i < data["asks"].Size(); i++) {
                    if(i != data["asks"].Size() - 1){
                        string a;
                        a.append("[").append(data["asks"][i][0].GetString()).append(",")
                                .append(data["asks"][i][1].GetString()).append("],");
                        asksStr.append(a);
                    }
                    else{
                        string a;
                        a.append("[").append(data["asks"][i][0].GetString()).append(",")
                                .append(data["asks"][i][1].GetString()).append("]");
                        asksStr.append(a);
                    }
                }
                asksStr.append("]");
                bidsStr.append("[");
                for(rapidjson::SizeType i = 0; i < data["bids"].Size(); i++) {
                    if(i != data["bids"].Size() - 1){
                        string b;
                        b.append("[").append(data["bids"][i][0].GetString()).append(",")
                                .append(data["bids"][i][1].GetString()).append("],");
                        bidsStr.append(b);
                    }
                    else{
                        string b;
                        b.append("[").append(data["bids"][i][0].GetString()).append(",")
                                .append(data["bids"][i][1].GetString()).append("]");
                        bidsStr.append(b);
                    }
                }
                bidsStr.append("]");
                sprintf(mdStr, DEPTH_Format,
                        XTExchangeId, instType, marketType, instId,
                        asksStr.c_str(),
                        bidsStr.c_str(),
                        stol(rawData["ts"].GetString())* 1000,
                        mdMsg.tsNet, crypto::getCurrentTime()
                );
#ifdef DEBUG_PRINT
                cout << "=========="<< marketType <<"=========" << endl;
                cout << "TIME:"<< crypto::getCurrentTime() << endl;
                for(int i = data["asks"].Size() - 1; i >= 0; i--){
                    cout << data["asks"][i][0].GetString() << "->" << data["asks"][i][1].GetString() << endl;
                }
                cout << "--------------------------" << endl;
                for(int i = 0; i < data["bids"].Size(); i++){
                    cout << data["bids"][i][0].GetString() << "->" << data["bids"][i][1].GetString() << endl;
                }
#endif
            }
            else if(crypto::has_str(ch, "trade") == true){
                const rapidjson::Value &data = rawData["tick"]["data"][0];
                sprintf(marketType,"%s","TRADES");
                sprintf(key, "%s.%s.%s.%s", XTExchangeId, instType, marketType, instId);
                sprintf(mdStr, Trades_Format, XTExchangeId, instType, marketType, instId,
                        data["tradeId"].GetString(),
                        stod(data["price"].GetString()),
                        stod(data["amount"].GetString()),//TODO
                        data["direction"].GetString(),
                        stol(data["ts"].GetString()) * 1000,
                        mdMsg.tsNet, crypto::getCurrentTime()//,mdMsg.data
                );
#ifdef NEED_RPUSH_REDIS
                redisClient->rpush(key, mdStr, 2000);
                return;
#endif
            }
            else if(crypto::has_str(ch, "mbp") == true){
                sprintf(marketType,"%s","MBP");
                sprintf(key, "%s.%s.%s.%s", XTExchangeId, instType, marketType, instId);
                string topic(key);
//                cout << topic << endl;
                rapidjson::Value data;
                if(rawData.HasMember("tick")){//sub
                    data = rawData["tick"];
                }
                else if(rawData.HasMember("data")){//req
                    data = rawData["data"];
                }
                else{
                    return;
                }

                MBP mbp;
                const rapidjson::Value &asks = data["asks"];
                const rapidjson::Value &bids = data["bids"];
                mbp.seqNum = atoll(data["seqNum"].GetString());
                if(data.HasMember("prevSeqNum")){
                    mbp.prevSeqNum = atoll(data["prevSeqNum"].GetString());
                }else{
                    mbp.prevSeqNum = 0;
                }
//                printf("%s,%d\n",__FUNCTION__, __LINE__);
                for (rapidjson::SizeType i = 0; i < asks.Size(); i++) {
                    DepthPair depthPair;
                    depthPair.price = asks[i][0].GetString();
                    depthPair.size = asks[i][1].GetString();
                    mbp.asks.push_back(depthPair);
                }
                for (rapidjson::SizeType i = 0; i < bids.Size(); i++) {
                    DepthPair depthPair;
                    depthPair.price = bids[i][0].GetString();
                    depthPair.size = bids[i][1].GetString();
                    mbp.bids.push_back(depthPair);
                }
                if(mbp.prevSeqNum == 0){//req
                    update_mbp(topic, mbp);
                    if(cacheMBP.count(topic) > 0){
                        for(auto m : cacheMBP[topic]){
                            if(m.seqNum > mbp.seqNum){
                                update_mbp(topic, m);
                            }
                        }
                        cacheMBP.erase(topic);
                    }
                    return;
                }
                if(lobMap.count(topic) > 0){
                    update_mbp(topic, mbp);
                    if(lobMap[topic]->isReady == true){
                        long tsParse = crypto::getCurrentTime();
                        string asksStr,bidsStr;
                        lobMap[topic]->get_asks_bids(asksStr, bidsStr);
                        char longMdStr[2048*64];//65536 is small and will cause segment fault
//                        long tsParse2 = crypto::getCurrentTime();
                        sprintf(longMdStr, DEPTH_Format,
                                XTExchangeId, instType, marketType, instId,
                                asksStr.c_str(),
                                bidsStr.c_str(),
                                stol(rawData["ts"].GetString())* 1000,
                                mdMsg.tsNet, tsParse
                        );
#ifdef  NEED_SET_REDIS
                        redisClient->set(key, longMdStr);
#endif

#ifdef NEED_PUBLISH_REDIS
                        redisClient->publish(key, longMdStr);
#endif
                    }
                }
                else{
                    if(cacheMBP.count(topic) > 0){
                        cacheMBP[topic].push_back(mbp);
                    }
                    else{
                        vector<MBP> mbpVec ;
                        mbpVec.push_back(mbp);
                        cacheMBP[topic] = mbpVec;
                    }
                }
                return;
            }

        }
//        printf("key:%s,value:%s\n",key,mdStr);
#ifdef  NEED_SET_REDIS
        redisClient->set(key, mdStr);
#endif
#ifdef NEED_STORE_REDIS
        redisClient->rpush(STORE_REDIS_CHANNEL,mdStr);
#endif
#ifdef NEED_PUBLISH_REDIS
        redisClient->publish(key, mdStr);
#endif
        return;
    }
#endif
};
}

#endif //DB_XTWSMARKETCLIENT_H
