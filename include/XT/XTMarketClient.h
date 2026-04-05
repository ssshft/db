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
//#include "db_global.h"
#include "boost/algorithm/string.hpp"
#include "base/marketdata_base.h"
#include "key_util.h"
#include "string_util.h"
#include "cryptlite/sha256.h"
#include <cryptlite/hmac.h>

#define XTExchangeId "XT"
#define XT_TIME_OUT 10000000
//spot
#define XT_WEBSOCKET_HOST_PUBLIC_SPOT "wss://xtsocket.xt.com/websocket"

namespace md{
using namespace web;
using namespace web::websockets::client;
using namespace concurrency::streams;
using namespace std;
using namespace rapidjson;
using namespace pubsub;
class XTWSMarketClient: public MarketDataClientBase{
    vector<json::value> spotSubValueVec;
    //= new moodycamel::ConcurrentQueue<MdMessage*>(1024*16);
    long xtReceive = 0;
#ifdef NEED_SHM
//        shmmqueue::CMessageQueue *mdShmQueue;
    Publisher<md::Depth5> *publisher;
#endif

public:
    XTWSMarketClient(vector<string> instTypeVec, vector<string> marketTypeVec, vector<string> instIdVec,
                          const char *ip = "127.0.0.1", const int port = 9379)
                          : MarketDataClientBase(instTypeVec, marketTypeVec, instIdVec, ip, port){
#ifdef NEED_SHM
        publisher = new Publisher<md::Depth5>("XT.DEPTH5");
#endif

    }
    ~XTWSMarketClient(){
        printf("good bye %s!\n",__FUNCTION__);
    }

    void start(){
        construct_sub_str();
//        go [=]{
//            consume_marketdata();
//        };

//        go [=]{
//            while(1){
//                try{
//                    LOG_INFO("%s receive:%ld, left:%ld", XTExchangeId, xtReceive, mdQueue->size_approx());
//                }
//                catch(exception &e){
//                    LOG_ERROR("%s", e.what());
//                }
//                sleep(3);
//            }
//        };
        std::thread consumeThread(&XTWSMarketClient::consume_marketdata, this);
        consumeThread.detach();
        if(spotSubValueVec.size() > 0){
            for(int i = 0 ; i < spotSubValueVec.size(); i++){//auto subValue : spotSubValueVec
                std::thread produceThread(&XTWSMarketClient::monitor, this, SPOTMD, true, spotSubValueVec[i]);
                produceThread.detach();
            }
        }
        std::thread logThread(&XTWSMarketClient::print_log, this);
        logThread.detach();
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
                                json::value value;
                                value["channel"] = json::value::string("ex_last_trade");
                                value["since"] = json::value::number(0);
                                value["market"] = json::value::string(lowerOriginInstId.c_str());
                                value["event"] = json::value::string("addChannel");

                                spotSubValueVec.push_back(value);
                            }
                            else if(crypto::has_str(marketType, "MBP") == true){
                                json::value value;
                                value["channel"] = json::value::string("ex_depth_data");
                                value["market"] = json::value::string(lowerOriginInstId.c_str());
                                value["event"] = json::value::string("addChannel");
                                spotSubValueVec.push_back(value);
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
//        for(auto value : subStVec){
//            //printf("%s\n",value.subValue.serialize().c_str());
//        }
    }

private:
    void print_log(){
        while(1){
            LOG_INFO("%s receive:%ld, left:%ld", XTExchangeId, xtReceive, mdQueue->size_approx());
            sleep(3);
        }
    }

    void monitor(const SubMarketType subMarketType , bool isSub, json::value subValue){
        long lastRecvTime = 0;
        long now = 0;
        long timeOut = XT_TIME_OUT;

        while (1) {
            websocket_client client;
            if(subMarketType == SPOTMD){
                if(isSub){
                    std::thread mdSubReceive(&XTWSMarketClient::sub_market, this, std::ref(client),
                                          std::ref(lastRecvTime), SPOTMD, subValue);//spotSubValueVec
                    mdSubReceive.detach();
                }
            }
            else{
//                throw runtime_error("not support now");
                cryptothrow("not support now", -1);
            }

            bool needResub = false;
            int pingCycle = 0;
            while(!needResub) {
                pingCycle++;
                try{
                    now = crypto::getCurrentTime();
                    if (now - lastRecvTime > timeOut && lastRecvTime != 0) {
                        LOG_ERROR("TIMEOUT %s ,delay:%ld, lastRecvTime:%ld", XTExchangeId,
                                  now - lastRecvTime, lastRecvTime);
                        client.close();
                        lastRecvTime = 0;
                        needResub = true;
                        break;
                    }
                    if(needResub){
                        LOG_INFO("%s need to resubscribe md", XTExchangeId);
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::seconds(3));
                }
                catch(std::exception &e){
                    LOG_ERROR("%s", e.what());
                }
            }
        }
    }
    void sub_market(websocket_client &client, long &lastRecvTime,
                    const SubMarketType subMarketType,json::value subValue ){//vector<json::value> subValueVec
        string subUrl;
        try{
            if(subMarketType == SPOTMD){
                subUrl = string(XT_WEBSOCKET_HOST_PUBLIC_SPOT ) ;
            }
            else{
                throw runtime_error("not support now");
            }
            uri wsuri(subUrl);
            client.connect(wsuri).wait();
            websocket_outgoing_message outMsg;
            outMsg.set_utf8_message(subValue.serialize().c_str());//sub.serialize());
            client.send(outMsg).wait();
            LOG_INFO("send success: %s", subValue.serialize().c_str());
        }catch (const websocket_exception& e ){//std::exception &e
            LOG_ERROR("%s", subUrl.c_str());
            LOG_ERROR("%s", e.what());
            lastRecvTime = 1;
            return;
        }
        char* sbuf = new char[MD_LENGTH];
        while (1) {
            try {
                websocket_incoming_message msg  = client.receive().get();
                lastRecvTime = crypto::getCurrentTime();
//                unsigned char buf[MD_LENGTH]= {0};
                MdMessage md  ;
                md.exchangeTypeEnum = XT;
                md.subMarketType = subMarketType;
                msg.body().streambuf().scopy((unsigned char *)md.data, MD_LENGTH);
                Document d;
                Value &value = d.Parse<kParseNumbersAsStringsFlag>((char*)md.data);
                if (value.IsObject() && value.HasMember("ping")) {
                    json::value pong;
                    pong["pong"] = json::value::string(value["ping"].GetString());
                    websocket_outgoing_message outMsg;
                    outMsg.set_utf8_message(pong.serialize().c_str());//sub.serialize());
                    client.send(outMsg).wait();
                }
                else{
                    md.tsNet = lastRecvTime;
                    mdQueue->try_enqueue(md);
                    xtReceive++;
                }
            } catch (std::exception &e){
//                string error(e.what());
                LOG_INFO("exception: %s",e.what());
                client.close();
                break;
            }
        }
        delete[] sbuf;
    }


    void consume_marketdata(){
        MdMessage mdMessage;
        while(1){
            if(mdQueue->try_dequeue(mdMessage)){
                try{
//                    printf("%s\n",mdMessage->data);
//                    LOG_INFO("%s",mdMessage->data);
                    save_md_string(&mdMessage);
                }
                catch(std::exception &e) {
                    LOG_ERROR("%s", e.what());
                }
            }
//            else{
//                std::this_thread::sleep_for(std::chrono::microseconds(sleepMicroSeconds));
//            }
        }
    }

    inline void save_md_string(MdMessage *mdMsg){
        rapidjson::Document d;
        rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg->data);
        if( !rawData.IsObject()){
            LOG_INFO("msg :%s is not a json object", mdMsg->data);
            return;
        }

        if(!(rawData.HasMember("code") && rawData.HasMember("data") ) ){//req
            LOG_INFO("%s", mdMsg->data);
            return;
        }

        char key[128]={0};
        char mdStr[2048];
        char instType[32]={0};

//        char exchId[16] = "BINANCE";
        char instId[32] = {0};
        char marketType[32];
        string ch = string(rawData["data"]["channel"].GetString());
//        string ch(rawData["ch"].GetString());
//        vector<string> s_vec = split(ch, '.');
        string originInstId = crypto::to_upper(rawData["data"]["market"].GetString());//(boost::algorithm::to_upper_copy(s_vec[0]));
        InstrumentInfo info;
        if(mdMsg->subMarketType == SPOTMD){
            if(smc->get_instrument_info(XTExchangeId, "SPOT", originInstId.c_str(), info) == true){
                mdMsg->instTypeEnum = SPOT;
                sprintf(instType,"%s","SPOT");
            }
            else{
                LOG_ERROR("not found spot: %s info in smc", originInstId.c_str());
                return;
            }
        }

        sprintf(instId,"%s", info.instId);
//        string ts(rawData["ts"].GetString());
        if(mdMsg->instTypeEnum == SPOT){
//            string mType = s_vec[1];
            if(crypto::has_str(ch, "trade") == true){
                const rapidjson::Value &data = rawData["data"]["records"][0];
                sprintf(marketType,"%s","TRADES");
                sprintf(key, "%s.%s.%s.%s", XTExchangeId, instType, marketType, instId);
                sprintf(mdStr, Trades_Format, XTExchangeId, instType, marketType, instId,
                        data[4].GetString(),
                        stod(data[1].GetString()),
                        stod(data[2].GetString()),//TODO
                        data[3].GetString(),
                        stol(data[0].GetString()) * 1000,
                        mdMsg->tsNet, crypto::getCurrentTime()//,mdMsg.data
                );
#ifdef NEED_RPUSH_REDIS
                redisClient->rpush(key, mdStr, 2000);
#endif
                return;
            }
            else if(crypto::has_str(ch, "ex_depth_data") == true){
                sprintf(marketType,"%s","MBP");
                sprintf(key, "%s.%s.%s.%s", XTExchangeId, instType, marketType, instId);
                string topic(key);
//                cout << topic << endl;
                const rapidjson::Value &data = rawData["data"];
                MBP mbp;
                if(data.HasMember("isFull")){//full depth 50
                    mbp.prevSeqNum = 0;//假设是全量
                }
                else{
                    mbp.prevSeqNum = 1;//假设是增量
                }
                const rapidjson::Value &asks = data["asks"];
                const rapidjson::Value &bids = data["bids"];
                mbp.seqNum = crypto::getCurrentTimeSeconds();//atoll(data["seqNum"].GetString());

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
#ifdef NEED_SHM
                md::Depth5 depth;
                depth.exchangeTypeEnum = XT;
                strcpy(depth.instId, instId);
                depth.instTypeEnum = mdMsg->instTypeEnum;
                depth.marketTypeEnum = DEPTH5;
                for(auto iter = lobMap[topic]->asksMap.begin();
                iter != lobMap[topic]->asksMap.end(); iter++){
                    depth.ap1 = stod(iter->first.c_str());
                    depth.av1 = stod(iter->second.c_str());
                    break;
                }
                for(auto iter = lobMap[topic]->bidsMap.rbegin();
                iter != lobMap[topic]->bidsMap.rend(); iter++){
                    depth.bp1 = stod(iter->first.c_str());
                    depth.bv1 = stod(iter->second.c_str());
                    break;
                }
                depth.tsNet = mdMsg->tsNet;
                depth.tsParse = crypto::getCurrentTime();
                publisher->publish(depth);
//                return;
#endif
                        long tsParse = crypto::getCurrentTime();
                        string asksStr,bidsStr;
                        lobMap[topic]->get_asks_bids(asksStr, bidsStr);
                        char longMdStr[2048*8];//65536 is small and will cause segment fault
//                        long tsParse2 = crypto::getCurrentTime();
                        sprintf(longMdStr, DEPTH_Format,
                                XTExchangeId, instType, marketType, instId,
                                asksStr.c_str(),
                                bidsStr.c_str(),
                                0LL,
                                mdMsg->tsNet, tsParse
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
#ifdef DEBUG_PRINT
        printf("key:%s,value:%s\n",key,mdStr);
#endif
//#ifdef  NEED_SET_REDIS
//        redisClient->set(key, mdStr);
//#endif
//#ifdef NEED_STORE_REDIS
//        redisClient->rpush(STORE_REDIS_CHANNEL,mdStr);
//#endif
//#ifdef NEED_PUBLISH_REDIS
//        redisClient->publish(key, mdStr);
//#endif
        return;
    }
};
}

#endif //DB_XTWSMARKETCLIENT_H
