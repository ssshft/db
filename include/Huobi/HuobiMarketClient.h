//
// Created by qyang on 2021/11/29.
//

#ifndef DB_HUOBIWSMARKETCLIENT_H
#define DB_HUOBIWSMARKETCLIENT_H

//#pragma once
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

#define HuobiExchangeId "HUOBI"
#define HUOBI_TIME_OUT 10000000
//#define HUOBI_MD_THREAD_LIMIT 120

//spot
#define HUOBI_WEBSOCKET_HOST_PUBLIC_SPOT "wss://api.huobi.pro/ws"
//    {
//        "sub": "market.btcusdt.mbp.refresh.20",
//        "id": "id1"
//    }
//
//  "ch": "market.btcusdt.mbp.refresh.20",
//  "ts": 1573199608679, //system update time
//  "tick": {
#define HUOBI_SPOT_MD_DEPTH_FORMAT "market.%s.mbp.refresh.%d"

//    {
//      "sub": "market.btcusdt.trade.detail",
//      "id": "id1"
//    }
#define HUOBI_SPOT_MD_TRADES_FORMAT "market.%s.trade.detail"

//{
//      "sub": "market.zecusdt.mbp.400",
//      "id": "id1"
//}
//{
//    "ch":"market.zecusdt.mbp.400",
//    "ts":1585074391470,
//    "tick":{
//        "seqNum":100772868478,
//        "prevSeqNum":100772868476,
//        "bids":[  ],
//        "asks":[  ]
//    }
//}
#define HUOBI_SPOT_MD_MBP_SUB_REQ_FORMAT "market.%s.mbp.150"

//#define HUOBI_SPOT_MD_MBP_REQ_FORMAT "market.%s.trade.detail"

//usdt swap futures
//#define BINANCE_WEBSOCKET_HOST_PUBLIC_USDT_SWAP_FUTURES "wss://fstream.binance.com"
//spot
//#define BINANCE_WEBSOCKET_HOST_PUBLIC_SPOT "wss://stream.binance.com:9443"
//usd swap futures
//#define BINANCE_WEBSOCKET_HOST_PUBLIC_USD_SWAP_FUTURES "wss://dstream.binance.com"

//1inchusdt@kline_1m
//#define BINANCE_SPOT_MD_KLINE_FORMAT "%s@%s"

//Global站行情请求地址（除MBP增量推送及MBP全量REQ以外Websocket行情频道）
//wss://api.huobi.pro/ws
//wss://api-aws.huobi.pro/ws
//
//MBP增量推送及MBP全量REQ请求地址
//wss://api.huobi.pro/feed
//wss://api-aws.huobi.pro/feed
/*
    {"ping": 1492420473027}
    {"pong": 1492420473027}
    {
      "sub": "market.btcusdt.depth.step0",
      "id": "id1"
    }
 */

namespace md{
using namespace web;
using namespace web::websockets::client;
using namespace concurrency::streams;
using namespace std;
using namespace rapidjson;


class HuobiWSMarketClient: public MarketDataClientBase{
    vector<json::value> spotSubValueVec;
    vector<json::value> spotReqValueVec;

    int subId = 0;
    long huobiReceive = 0;
public:
    HuobiWSMarketClient(vector<string> instTypeVec, vector<string> marketTypeVec, vector<string> instIdVec,
                          const char *ip = "127.0.0.1", const int port = 9379)
                          : MarketDataClientBase(instTypeVec, marketTypeVec, instIdVec, ip, port){
//        spotSubValue["method"] = json::value::string("SUBSCRIBE");
//        spotSubValue["id"] = json::value::number(sub_id);
    }
    ~HuobiWSMarketClient(){
        printf("good bye %s!\n",__FUNCTION__);
    }

    void start(){
        construct_sub_str();
        std::thread consumeThread(&HuobiWSMarketClient::consume_marketdata, this);
        consumeThread.detach();
        if(spotSubValueVec.size() > 0){
            std::thread produceThread(&HuobiWSMarketClient::monitor, this, SPOTMD, true);
            produceThread.detach();
        }
        if(spotReqValueVec.size() > 0){
            std::thread produceThread(&HuobiWSMarketClient::monitor, this, SPOTMD, false);
            produceThread.detach();
        }

        std::thread logThread(&HuobiWSMarketClient::print_log, this);
        logThread.detach();
    }

    void construct_sub_str(){
        for(auto instType : _instTypeVec){
            for(auto instId : _instIdVec){
                InstrumentInfo info;
                if(smc->get_instrument_info(HuobiExchangeId, instType.c_str(),
                                            instId.c_str(), info) == true){
                    string lowerOriginInstId = crypto::to_lower(info.originInstId);//(info.originInstId);//
                    for(auto marketType : _marketTypeVec){
                        if(crypto::str_cmp(instType.c_str(), "SPOT") == true){
//                            string lowerMarketType = crypto::to_lower(marketType);
                            if(crypto::has_str(marketType, "DEPTH5") == true){
                                char param[32];
                                sprintf(param, HUOBI_SPOT_MD_DEPTH_FORMAT, lowerOriginInstId.c_str(), 5);
                                json::value value;
                                value["sub"] = json::value::string(param);
                                spotSubValueVec.push_back(value);
//                                spotSubValue["params"][spotSubCount++] = json::value::string(param);
                            }
                            else if(crypto::has_str(marketType, "DEPTH10") == true){
                                char param[32];
                                sprintf(param, HUOBI_SPOT_MD_DEPTH_FORMAT, lowerOriginInstId.c_str(), 10);
                                json::value value;
                                value["sub"] = json::value::string(param);
                                spotSubValueVec.push_back(value);
//                                spotSubValue["params"][spotSubCount++] = json::value::string(param);
                            }
                            else if(crypto::has_str(marketType, "DEPTH20") == true){
                                char param[32];
                                sprintf(param, HUOBI_SPOT_MD_DEPTH_FORMAT, lowerOriginInstId.c_str(), 20);
                                json::value value;
                                value["sub"] = json::value::string(param);
                                spotSubValueVec.push_back(value);
//                                spotSubValue["params"][spotSubCount++] = json::value::string(param);
                            }
                            else if(crypto::has_str(marketType, "TRADE") == true){
                                char param[32];
                                sprintf(param, HUOBI_SPOT_MD_TRADES_FORMAT, lowerOriginInstId.c_str() );
                                json::value value;
                                value["sub"] = json::value::string(param);
                                spotSubValueVec.push_back(value);
                            }
                            else if(crypto::has_str(marketType, "MBP") == true){
                                char param[32];
                                sprintf(param, HUOBI_SPOT_MD_MBP_SUB_REQ_FORMAT, lowerOriginInstId.c_str() );
                                json::value value;
                                value["sub"] = json::value::string(param);
                                spotSubValueVec.push_back(value);

                                json::value reqValue;
                                reqValue["req"] = json::value::string(param);
                                spotReqValueVec.push_back(reqValue);
                            }
                        }
                    }
                }
                else {
                    LOG_ERROR("instrument info not exist %s.%s.%s", HuobiExchangeId, instType.c_str(), instId.c_str());
                }
            }
        }
//        for(auto value : spotSubValueVec){
//            printf("%s\n",value.serialize().c_str());
//        }
    }


private:
    void print_log(){
        while(1){
            LOG_INFO("%s receive:%ld, left:%ld", HuobiExchangeId, huobiReceive, mdQueue->size_approx());
            sleep(3);
        }
    }
    void monitor(const SubMarketType huobiMarketType, bool isSub){
        long lastRecvTime = 0;
        long now = 0;
        long timeOut = HUOBI_TIME_OUT;

        while (1) {
            websocket_client client;
            if(huobiMarketType == SPOTMD){
                if(isSub){
                    std::thread mdSubReceive(&HuobiWSMarketClient::sub_market, this, std::ref(client),
                                          std::ref(lastRecvTime), SPOTMD, spotSubValueVec);
                    mdSubReceive.detach();
                }
                else{
                    string subUrl  = string(HUOBI_WEBSOCKET_HOST_PUBLIC_SPOT ) ;
                    uri wsuri(subUrl);
                    client.connect(wsuri).wait();

                    std::thread mdReqReceive(&HuobiWSMarketClient::req_market, this,std::ref(client),
                                             std::ref(lastRecvTime), SPOTMD);//, spotReqValueVec
                    mdReqReceive.detach();
                }
            }
            else{
                throw runtime_error("not support now");
            }

            bool needResub = false;
            int pingCycle = 0;
            while(!needResub) {
                pingCycle++;
                try{
                    now = crypto::getCurrentTime();
                    if (now - lastRecvTime > timeOut && lastRecvTime != 0) {
                        LOG_ERROR("TIMEOUT %s ,delay:%ld, lastRecvTime:%ld", HuobiExchangeId,
                                  now - lastRecvTime, lastRecvTime);
                        client.close();
                        lastRecvTime = 0;
                        needResub = true;
                        break;
                    }
                    if(needResub){
                        LOG_INFO("%s need to resubscribe md", HuobiExchangeId);
                        break;
                    }
                    if(!isSub){
                        string reqStr;
                        for(auto value : spotReqValueVec){
                            value["id"] = json::value::string("id" + std::to_string(subId++));
                            websocket_outgoing_message outMsg;
                            outMsg.set_utf8_message(value.serialize().c_str());//sub.serialize());
                            client.send(outMsg).wait();
//
                            reqStr.append(value.serialize().c_str()).append(",");
                            std::this_thread::sleep_for(std::chrono::milliseconds(200));
                        }
                        LOG_INFO("send success: %s", reqStr.c_str());
                        std::this_thread::sleep_for(std::chrono::seconds(20));
                    }
//                    if(pingCycle % 1 == 0){
//                        LOG_INFO("%s receive:%ld, left:%ld", HuobiExchangeId,
//                                 huobiReceive, mdQueue->size_approx());
//                    }
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                catch(std::exception &e){
                    LOG_ERROR("%s", e.what());
                }
            }
        }
    }

    void req_market(websocket_client &client, long &lastRecvTime,
                    const SubMarketType subMarketType){//, vector<json::value> reqValueVec, bool needReSub=false
        string reqUrl;
        if(subMarketType == SPOTMD){
            reqUrl = string(HUOBI_WEBSOCKET_HOST_PUBLIC_SPOT ) ;
        }
        else{
            throw runtime_error("not support now");
        }
        char* sbuf = new char[MD_LENGTH];
        while(1){
            try{
                char* msg = client.receive().then([sbuf](websocket_incoming_message in_msg) {
                    char buf[MD_LENGTH] = {0};
                    unsigned int l = MD_LENGTH;
                    in_msg.body().streambuf().getn((unsigned char *) buf, l);
                    memset(sbuf, 0, MD_LENGTH);
                    gzDecompress(buf, in_msg.length(), sbuf, MD_LENGTH);
                    return sbuf;
                }).get();

                lastRecvTime = crypto::getCurrentTime();
                Document d;
                Value &value = d.Parse<kParseNumbersAsStringsFlag>(msg);
                if(!value.IsObject()){
                    printf("%s,%d\n",__FUNCTION__,__LINE__);
//                    printf("%s\n",msg);
                    continue;
                }
                if (value.HasMember("ping")) {
                    json::value pong;
                    pong["pong"] = json::value::string(value["ping"].GetString());
//                    LOG_INFO("%s\n",msg);
                    websocket_outgoing_message outMsg;
                    outMsg.set_utf8_message(pong.serialize().c_str());//sub.serialize());
                    client.send(outMsg).wait();
                    continue;
                }
//                printf("%s,%d\n",__FUNCTION__,__LINE__);
//{"id":"id1","status":"ok","ts":1639621463978,"rep":"market.btcusdt.mbp.150",
// "data":{"seqNum":144405016453,"bids":[[48943.]]}
//                MdMessage *md = (md::MdMessage*)calloc(1, sizeof(md::MdMessage));
//                md->exchangeTypeEnum = HUOBI;
//                md->subMarketType = subMarketType;
//                strcpy(md->data, msg);
//                md->tsNet = lastRecvTime;
                MdMessage md;
                md.exchangeTypeEnum = HUOBI;
                md.subMarketType = subMarketType;
                strcpy(md.data, msg);
                md.tsNet = lastRecvTime;
                mdQueue->try_enqueue(md);
            }
            catch (std::exception &e) {
                LOG_INFO("exception: %s",e.what());
                client.close();
                break;
            }
        }
        delete[] sbuf;
    }

    void sub_market(websocket_client &client, long &lastRecvTime,
                    const SubMarketType huobiMarketType, vector<json::value> subValueVec){
        string subUrl;
        try{
            if(huobiMarketType == SPOTMD){
                subUrl = string(HUOBI_WEBSOCKET_HOST_PUBLIC_SPOT ) ;
            }
            else{
                throw runtime_error("not support now");
            }
            uri wsuri(subUrl);
            client.connect(wsuri).wait();
            for(auto subValue: subValueVec){
                subValue["id"] = json::value::string("id" + std::to_string(subId++));
                websocket_outgoing_message outMsg;
                outMsg.set_utf8_message(subValue.serialize().c_str());//sub.serialize());
                client.send(outMsg).wait();
                LOG_INFO("send success: %s", subValue.serialize().c_str());

                std::this_thread::sleep_for(std::chrono::milliseconds(110));//数据请求（req）限频规则 单个连接每两次请求不能小于100ms。
            }

        }catch (std::exception &e){
            LOG_ERROR("%s", e.what());
        }
        char* sbuf = new char[MD_LENGTH];
        while (1) {
            try {
                char* msg = client.receive().then([sbuf](websocket_incoming_message in_msg) {
                    char buf[MD_LENGTH] = {0};
                    unsigned int l = MD_LENGTH;
                    in_msg.body().streambuf().getn((unsigned char *) buf, l);
                    memset(sbuf, 0, MD_LENGTH);
                    gzDecompress(buf, in_msg.length(), sbuf, MD_LENGTH);
                    return sbuf;
                }).get();
//                printf("%s\n",msg);
//                printf("%s,%d\n",__FUNCTION__,__LINE__);
//                printf("%s\n",msg);
                lastRecvTime = crypto::getCurrentTime();
                Document d;
                Value &value = d.Parse<kParseNumbersAsStringsFlag>(msg);
                if (value.HasMember("ping")) {
//                    client.send(WebsocketHelper::pong(msg));
                    json::value pong;
                    pong["pong"] = json::value::string(value["ping"].GetString());
//                    LOG_INFO("%s\n",msg);
                    websocket_outgoing_message outMsg;
                    outMsg.set_utf8_message(pong.serialize().c_str());//sub.serialize());
                    client.send(outMsg).wait();
                }
                else{
                    MdMessage md;
                    md.exchangeTypeEnum = HUOBI;
                    md.subMarketType = huobiMarketType;
                    strcpy(md.data, msg);
                    md.tsNet = lastRecvTime;
                    mdQueue->try_enqueue(md);
                    huobiReceive++;
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
//                    printf("%s\n",mdMessage.data);
//                    LOG_INFO("%s",mdMessage.data);
                    save_md_string(&mdMessage);
                }
                catch(std::exception &e) {
                    LOG_ERROR("%s", e.what());
                }
            }
            else{
                std::this_thread::sleep_for(std::chrono::microseconds(sleepMicroSeconds));
            }
        }
    }

    inline void save_md_string(MdMessage *mdMsg){
        rapidjson::Document d;
        rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg->data);
        if( !rawData.IsObject()){
            LOG_INFO("msg :%s is not a json object", mdMsg->data);
            return;
        }

        if(!((rawData.HasMember("ch") && rawData.HasMember("tick")) ||//sub
                (rawData.HasMember("rep") && rawData.HasMember("data")) )){//req
            LOG_INFO("%s", mdMsg->data);
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
        if(mdMsg->subMarketType == SPOTMD){
            if(smc->get_instrument_info(HuobiExchangeId, "SPOT",
                                        originInstId.c_str(), info) == true){
                mdMsg->instTypeEnum = SPOT;
                sprintf(instType,"%s","SPOT");
            }
            else{
                LOG_ERROR("not found spot: %s info in smc", originInstId.c_str());
                return;
            }
        }
//        else if(mdMsg.subMarketType == USDTSWAPMD || mdMsg.subMarketType == USDSWAPMD) {
//            if (smc->get_instrument_info(HuobiExchangeId, "SWAP",
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
//            if(smc->get_instrument_info(HuobiExchangeId, "FUTURES",
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
        if(mdMsg->instTypeEnum == SPOT){
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
                sprintf(key, "%s.%s.%s.%s", HuobiExchangeId, instType, marketType, instId);
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
                        HuobiExchangeId, instType, marketType, instId,
                        asksStr.c_str(),
                        bidsStr.c_str(),
                        stol(rawData["ts"].GetString())* 1000,
                        mdMsg->tsNet, crypto::getCurrentTime()
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
                sprintf(key, "%s.%s.%s.%s", HuobiExchangeId, instType, marketType, instId);
                sprintf(mdStr, Trades_Format, HuobiExchangeId, instType, marketType, instId,
                        data["tradeId"].GetString(),
                        stod(data["price"].GetString()),
                        stod(data["amount"].GetString()),//TODO
                        data["direction"].GetString(),
                        stol(data["ts"].GetString()) * 1000,
                        mdMsg->tsNet, crypto::getCurrentTime()//,mdMsg.data
                );
#ifdef NEED_RPUSH_REDIS
                redisClient->rpush(key, mdStr, 2000);
#endif
                return;
            }
            else if(crypto::has_str(ch, "mbp") == true){
                sprintf(marketType,"%s","MBP");
                sprintf(key, "%s.%s.%s.%s", HuobiExchangeId, instType, marketType, instId);
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
                                HuobiExchangeId, instType, marketType, instId,
                                asksStr.c_str(),
                                bidsStr.c_str(),
                                stol(rawData["ts"].GetString())* 1000,
                                mdMsg->tsNet, tsParse
                        );
#ifdef  NEED_SET_REDIS
                        redisClient->set(key, longMdStr);
#endif

#ifdef NEED_PUBLISH_REDIS
                        redisClient->publish(key, longMdStr);
#endif
                        return;
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

#endif //DB_HUOBIWSMARKETCLIENT_H
