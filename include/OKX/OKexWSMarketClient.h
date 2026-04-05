//
// Created by qyang on 2021/12/3.
//

#ifndef DB_OKEXWSMARKETCLIENT_H
#define DB_OKEXWSMARKETCLIENT_H

#include <string>
#include <iostream>
#include <thread>
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

#define OKexExchangeId "OKEX"
#define OKEX_TIME_OUT 100000000
#define OKEX_WEBSOCKET_HOST_V5_PUBLIC "wss://wsaws.okex.com:8443/ws/v5/public"
/*
 {
    "op": "subscribe",
    "args": [{
        "channel": "books",
        "instId": "BTC-USDT"
    }]
}
 */
namespace md{
    using namespace web;
    using namespace web::websockets::client;
    using namespace concurrency::streams;
    using namespace std;
    using namespace rapidjson;
    using namespace pubsub;
    class OKexWSMarketClient: public MarketDataClientBase{
        json::value subValue;
        int subCount = 0;
//        json::value swapSubValue;
//        json::value futuresSubValue;
//        int spotSubCount = 0;
//        int usdtSubCount = 0;
//        int usdSubCount = 0;
//        int sub_id = 0;
        long okexReceive = 0;
#ifdef NEED_SHM
//        shmmqueue::CMessageQueue *mdShmQueue;
        Publisher<md::Depth5> *publisher;
#endif
    public:
        OKexWSMarketClient(vector<string> instTypeVec, vector<string> marketTypeVec, vector<string> instIdVec,
                              const char *ip = "127.0.0.1", const int port = 9379)
                : MarketDataClientBase(instTypeVec, marketTypeVec, instIdVec, ip, port){
            subValue["op"] = json::value::string("subscribe");
//            usdtSubValue["method"] = json::value::string("SUBSCRIBE");
//            usdSubValue["method"]  = json::value::string("SUBSCRIBE");
//        spotSubValue["id"] = json::value::number(sub_id);
#ifdef NEED_SHM
            publisher = new Publisher<md::Depth5>("OKEX.DEPTH5");
#endif
        }
        ~OKexWSMarketClient(){
            printf("good bye %s!\n",__FUNCTION__);
        }

        void start(){
            construct_sub_str();
            std::thread consumeThread(&OKexWSMarketClient::consume_marketdata, this);
//            cpu_set_t cpuset;
//            CPU_ZERO(&cpuset);
//            CPU_SET(6,&cpuset);
//            int rc = pthread_setaffinity_np( consumeThread.native_handle(),sizeof(cpu_set_t), &cpuset);
//            if (rc != 0) {
//                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
//            }
            consumeThread.detach();

            if(subCount > 0){
                std::thread produceThread(&OKexWSMarketClient::monitor, this);
                produceThread.detach();
            }

            std::thread logThread(&OKexWSMarketClient::print_log, this);
            logThread.detach();
        }

    private:
        void construct_sub_str(){
            json::value subArgs;
            for(auto instType : _instTypeVec){
                for(auto instId : _instIdVec){
                    InstrumentInfo info;
                    if(smc->get_instrument_info(OKexExchangeId, instType.c_str(),
                                                instId.c_str(), info) == true){
                        string upperOriginInstId = crypto::to_upper(info.originInstId);
                        for(auto marketType : _marketTypeVec) {
//                            string lowerMarketType = crypto::to_lower(marketType);
                            if (crypto::has_str(marketType, "DEPTH5") == true) {
                                json::value arg;
                                arg["channel"] = json::value::string("books5");
                                arg["instId"] = json::value::string(upperOriginInstId);
                                subValue["args"][subCount++] = arg;
                            } else if (crypto::has_str(marketType, "TRADE") == true) {
                                json::value arg;
                                arg["channel"] = json::value::string("trades");
                                arg["instId"] = json::value::string(upperOriginInstId);
                                subValue["args"][subCount++] = arg;
                            } else if (crypto::has_str(marketType, "KLINE_1m") == true) {
                                json::value arg;
                                arg["channel"] = json::value::string("candle1m");
                                arg["instId"] = json::value::string(upperOriginInstId);
                                subValue["args"][subCount++] = arg;
                            }
                            if (crypto::str_cmp(instType.c_str(), "SWAP") == true ) {
                                if (crypto::has_str(marketType, "FUNDING") == true) {
                                    json::value arg;
                                    arg["channel"] = json::value::string("funding-rate");
                                    arg["instId"] = json::value::string(upperOriginInstId);
                                    subValue["args"][subCount++] = arg;
                                }
                            }
                        }
                    }
                }
            }
//            printf("okex subscribe str:%s\n",subValue.serialize().c_str());
        }
        void print_log(){
            while(1){
                LOG_INFO("%s receive:%ld, left:%ld", OKexExchangeId,
                         okexReceive, mdQueue->size_approx());
                sleep(3);
            }
        }
        void monitor( ){
            long lastRecvTime = 0;
            long now = 0;
            std::vector<websocket_client> clientVec;
            long timeOut = OKEX_TIME_OUT;
            while (1) {
                websocket_client client;
                std::thread mdReceive(&OKexWSMarketClient::sub_market, this, std::ref(client),
                                      std::ref(lastRecvTime), subValue);
//                cpu_set_t cpuset;
//                CPU_ZERO(&cpuset);
//                CPU_SET(5,&cpuset);
//                int rc = pthread_setaffinity_np( mdReceive.native_handle(),sizeof(cpu_set_t), &cpuset);
//                if (rc != 0) {
//                    std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
//                }
                mdReceive.detach();
                clientVec.push_back(client);
                bool needResub = false;
                int pingCycle = 0;
                while(!needResub) {
                    pingCycle++;
                    try{
                        now = crypto::getCurrentTime();
                        for(auto client : clientVec){
                            if (now - lastRecvTime > timeOut && lastRecvTime != 0) {
                                LOG_ERROR("TIMEOUT %s ,delay:%ld, lastRecvTime:%ld", OKexExchangeId,
                                          now - lastRecvTime, lastRecvTime);
                                client.close();
                                lastRecvTime = 0;
                                needResub = true;
                                break;
                            }
                            if(pingCycle % 60 == 0){
                                websocket_outgoing_message outMsg;
                                outMsg.set_utf8_message("ping");//sub.serialize());
                                client.send(outMsg).wait();
                                LOG_INFO("send ping to okex");
                            }
                        }
                        if(needResub){
                            clientVec.clear();
                            LOG_INFO("%s need to resubscribe md", OKexExchangeId);
                            break;
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                    }
                    catch(std::exception &e){
                        string error(e.what());
                        LOG_ERROR("%s", error.c_str());
                    }
                }
            }
        }

        void sub_market(websocket_client &client, long &lastRecvTime, json::value subValue){
            string subUrl = string(OKEX_WEBSOCKET_HOST_V5_PUBLIC);
            try{
                uri wsuri(subUrl);//streams=!markPrice@arr@3s/ethusdt@depth5@100ms
                client.connect(wsuri).wait();
                websocket_outgoing_message outMsg;
                outMsg.set_utf8_message(subValue.serialize().c_str());//sub.serialize());
                client.send(outMsg).wait();
                LOG_INFO("%s send success: %s",OKexExchangeId, subValue.serialize().c_str());
            }catch (std::exception &e){
                LOG_ERROR("%s", e.what());
                lastRecvTime = 1;
            }
            while (1) {
                try {
//                printf("%s\n",subUrl.c_str());
                    websocket_incoming_message msg  = client.receive().get();
                    lastRecvTime = crypto::getCurrentTime();
                    MdMessage md;
                    md.exchangeTypeEnum = OKEX;
                    msg.body().streambuf().scopy((unsigned char*)md.data, MD_LENGTH);
                    md.tsNet = lastRecvTime;
                    mdQueue->try_enqueue(md);
                    okexReceive++;
                } catch (std::exception &e){
                    string error(e.what());
                    LOG_INFO("exception: %s",error.c_str());
                    client.close();
                    break;
                }
            }
        }

        void consume_marketdata(){
            MdMessage mdMessage;
            while(1){
                if(mdQueue->try_dequeue(mdMessage)){
                    try{
//                        printf("%s\n",mdMessage.data);
//                    LOG_INFO("%s",mdMessage.data);
                        save_md_string(&mdMessage);
                    }
                    catch(std::exception &e) {
                        LOG_ERROR("%s", e.what());
                    }
                }
//                else{
//                    std::this_thread::sleep_for(std::chrono::microseconds(sleepMicroSeconds));
//                }
            }
        }

        inline void save_md_string(MdMessage *mdMsg){
            rapidjson::Document d;
            rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg->data);
            if( !rawData.IsObject()){
                LOG_INFO("msg :%s is not a json object", mdMsg->data);
                return;
            }

            if(!rawData.HasMember("data") ){
                LOG_INFO("%s", mdMsg->data);
                return;
            }
            char key[64]={0};
            char mdStr[2048];
            char instType[16]={0};

            const rapidjson::Value &data = rawData["data"][0];
//            char exchId[16]="OKEX";
            char instId[16]={0};
            char originInstId[16]={0};
            char marketType[16];

            sprintf(originInstId,"%s",rawData["arg"]["instId"].GetString());
            string channel(rawData["arg"]["channel"].GetString());

            InstrumentInfo info;
            bool foundInSmc = false;
            if(crypto::has_str(originInstId,"SWAP") == true){//swap
                if(smc->get_instrument_info(OKexExchangeId, "SWAP",
                                            originInstId, info) == true){
                    mdMsg->instTypeEnum = SWAP;
                    sprintf(instType,"%s","SWAP");
                    foundInSmc = true;
                }
            }
            else if(crypto::has_str(originInstId,"21") == true
            || crypto::has_str(originInstId,"22") == true
            || crypto::has_str(originInstId,"23") == true ){//futures
                if(smc->get_instrument_info(OKexExchangeId, "FUTURES",
                                            originInstId, info) == true){
                    mdMsg->instTypeEnum = FUTURES;
                    sprintf(instType,"%s","FUTURES");
                    foundInSmc = true;
                }
            }
            else {//spot
                if(smc->get_instrument_info(OKexExchangeId, "SPOT",
                                            originInstId, info) == true){
                    mdMsg->instTypeEnum = SPOT;
                    sprintf(instType,"%s","SPOT");
                    foundInSmc = true;
                }
            }
            if(foundInSmc == true){
                sprintf(instId,"%s", info.instId);
            }
            else{
                LOG_ERROR("not found %s info in smc", originInstId);
                return;
            }

            if(channel.compare("books5") == 0){
//            marketType="DEPTH";
                sprintf(marketType,"%s","DEPTH5");
                sprintf(key,"%s.%s.%s.%s",OKexExchangeId,instType,marketType,instId);
#ifdef NEED_SHM
                md::Depth5 depth;
                depth.exchangeTypeEnum = OKEX;
                strcpy(depth.instId, instId);
                depth.instTypeEnum = mdMsg->instTypeEnum;
                depth.marketTypeEnum = DEPTH5;
                depth.bp1 = stod(data["bids"][0][0].GetString());
                depth.bv1 = stod(data["bids"][0][1].GetString());
                depth.ap1 = stod(data["asks"][0][0].GetString());
                depth.av1 = stod(data["asks"][0][1].GetString());
                depth.tsNet = mdMsg->tsNet;
                depth.tsParse = crypto::getCurrentTime();
//                mdShmQueue->SendMessage((shmmqueue::BYTE *) &depth, sizeof(depth));
                publisher->publish(depth);
//                printf("%ld\n", depth.tsParse);
#endif

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
                        OKexExchangeId, instType, marketType, instId,
                        asksStr.c_str(),
                        bidsStr.c_str(),
                        stol(data["ts"].GetString()) * 1000,
                        mdMsg->tsNet, crypto::getCurrentTime()
                );

            }
            else if(channel.compare("trades") == 0){
                sprintf(marketType,"%s","TRADES");
                sprintf(key,"%s.%s.%s.%s",OKexExchangeId,instType,marketType,instId);
                sprintf(mdStr,Trades_Format,OKexExchangeId,instType,marketType,instId,
                        data["tradeId"].GetString(),
                        stod(data["px"].GetString()),
                        stod(data["sz"].GetString()),
                        data["side"].GetString(),
                        stol(data["ts"].GetString()) *1000,
                        mdMsg->tsNet,crypto::getCurrentTime()//,md_msg.data
                );
#ifdef NEED_RPUSH_REDIS
                redisClient->rpush(key, mdStr, 2000);
#endif
                return;
            }
            else if(channel.compare("funding-rate") == 0){
//                printf("%s,%ld\n",__FUNCTION__ ,__LINE__);
                sprintf(marketType,"%s","FUNDING_RATE");
                sprintf(key,"%s.%s.%s.%s",OKexExchangeId,instType,marketType,instId);
                string nf(data["nextFundingRate"].GetString());
                sprintf(mdStr,Funding_Rate_Format,OKexExchangeId,instType,marketType,instId,
                        stod(data["fundingRate"].GetString()),
                        stod(nf.compare("") == 0 ? "0.0" : data["nextFundingRate"].GetString()),
                        stol(data["fundingTime"].GetString() ) * 1000,
                        0LL,
                        mdMsg->tsNet,crypto::getCurrentTime()//,md_msg.data
                );
            }
            else if(channel.compare("candle1m") == 0){
                sprintf(marketType,"%s","KLINE_1m");
                sprintf(key,"%s.%s.%s.%s",OKexExchangeId,instType,marketType,instId);
                sprintf(mdStr, Bar_Format, OKexExchangeId, instType, marketType, instId,
                        stol(data[0].GetString()) * 1000,
                        stod(data[2].GetString()),
                        stod(data[3].GetString()),
                        stod(data[1].GetString()),
                        stod(data[4].GetString()),
                        0.0,
                        stod(data[5].GetString()),
                        stod(data[6].GetString()),
                        0.0,
                        0.0,
                        0.0 ,
                        0.0 ,
                        0LL,
                        mdMsg->tsNet, crypto::getCurrentTime()//,mdMsg.data
                );

            }
            else{
                printf("%s\n",mdMsg->data);
                return;
            }

#ifdef DEBUG_PRINT
            fprintf(stdout, "key:%s,value:%s\n",key,mdStr);
#endif
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
    };

}
#endif //DB_OKEXWSMARKETCLIENT_H
