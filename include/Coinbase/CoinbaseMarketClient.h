//
// Created by qyang on 2021/11/29.
//

#ifndef DB_COINBAEWSMARKETCLIENT_H
#define DB_COINBASEWSMARKETCLIENT_H

#include <string>
#include <iostream>
#include <thread>
#include <stdio.h>
#include <algorithm>
#include <bitset>
#include "cpprest/json.h"
//#include "utils.h"
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

#define CoinbaseExchangeId "COINBASE"
#define COIN_TIME_OUT 10000000
#define COIN_RESUB_TIME 300000000

//spot
#define COIN_WEBSOCKET_HOST_PUBLIC_SPOT "wss://ws-feed.exchange.coinbase.com/"

#define COIN_REST_HOST_PUBLIC_SPOT "https://api.gateio.ws/api/v4/"

namespace md{
using namespace web;
using namespace web::websockets::client;
using namespace concurrency::streams;
using namespace std;
using namespace rapidjson;


class CoinbaseWSMarketClient: public MarketDataClientBase{
    vector<string> spotReqVec;
    vector<json::value> spotSubValueVec;
    int spotSubCount = 0;
    std::bitset<4> depthSub = 0;

    int subId = 0;
    long packRecv = 0;
    long pingRecv = 0;
    long lastSubTime = 0;
public:
    CoinbaseWSMarketClient(vector<string> instTypeVec, vector<string> marketTypeVec, vector<string> instIdVec,
                         const char *ip = "127.0.0.1", const int port = 9379)
                          : MarketDataClientBase(instTypeVec, marketTypeVec, instIdVec, ip, port){
    }
    ~CoinbaseWSMarketClient(){
        printf("good bye %s!\n",__FUNCTION__);
    }

    void start(){
        construct_sub_str();
        std::thread consumeThread(&CoinbaseWSMarketClient::consume_marketdata, this);
        consumeThread.detach();
        if(spotSubValueVec.size() > 0){
            std::thread produceThread(&CoinbaseWSMarketClient::monitor, this, SPOTMD);
            produceThread.detach();
        }
        if (spotReqVec.size() > 0) {
            std::thread mdReqReceive(&CoinbaseWSMarketClient::req_market, this, SPOTMD, spotReqVec);
            mdReqReceive.detach();
        }
//        if(usdtSubCount > 0){
//            std::thread produceThread(&CoinbaseWSMarketClient::monitor, this, USDTMD);
//            produceThread.detach();
//        }
//        if(usdSubCount > 0){
//            std::thread produceThread(&CoinbaseWSMarketClient::monitor, this, USDMD);
//            produceThread.detach();
//        }
    }

    void construct_sub_str(){
        for (auto instType: _instTypeVec) {
            json::value value;
            for (auto instId: _instIdVec) {
                InstrumentInfo info;
                if (smc->get_instrument_info(CoinbaseExchangeId, instType.c_str(), instId.c_str(), info)) {
                    value["product_ids"][spotSubCount++] = json::value::string(info.originInstId);
                }
                else {
                    LOG_ERROR("instrument info not exist %s.%s.%s", CoinbaseExchangeId, instType.c_str(), instId.c_str());
                }
            }
            if (!spotSubCount)
                return;
            value["type"] = json::value::string("subscribe");
            for (auto marketType: _marketTypeVec) {
                if (!strcmp(instType.c_str(), "SPOT")) {
                    if (!strcmp(marketType.c_str(), "DEPTH5") || !strcmp(marketType.c_str(), "DEPTH10") ||
                        !strcmp(marketType.c_str(), "DEPTH20") || !strcmp(marketType.c_str(), "MBP")) {
                        if (depthSub.none()) {
                            value["channels"][0] = json::value::string("level2");
                            //value["channels"][1] = json::value::string("heartbeat");
                            spotSubValueVec.push_back(value);
                        }
                       depthSub[0] = !strcmp(marketType.c_str(), "DEPTH5")  ? 1 : depthSub[0];
                       depthSub[1] = !strcmp(marketType.c_str(), "DEPTH10") ? 1 : depthSub[1];
                       depthSub[2] = !strcmp(marketType.c_str(), "DEPTH20") ? 1 : depthSub[2];
                       depthSub[3] = !strcmp(marketType.c_str(), "MBP")     ? 1 : depthSub[3];
                    }
                    else if (!strcmp(marketType.c_str(), "TRADES")) {
                        value["channels"][0] = json::value::string("matches");
                        value["channels"][1] = json::value::string("heartbeat");
                        spotSubValueVec.push_back(value);
                    }
                    /*else if (!strcmp(marketType.c_str(), "MBP")) {
                        value["channel"] = json::value::string("spot.order_book_update");
                        spotSubValueVec.push_back(value);
                        spotReqVec.push_back(strOriginInstId);
                    }*/
                }
            }
        }
    }

private:
    void monitor(const SubMarketType marketType){
        long lastRecvTime = 0;
        long now = 0;
        std::vector<websocket_client> clientVec;
        long timeOut = COIN_TIME_OUT;
        while (1) {
            if(marketType == SPOTMD){
                lastSubTime = crypto::getCurrentTime();
                websocket_client client;
                std::thread mdReceive(&CoinbaseWSMarketClient::sub_market, this, std::ref(client),
                                      std::ref(lastRecvTime), SPOTMD, spotSubValueVec);
                mdReceive.detach();
                clientVec.push_back(client);
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
                    for(auto client : clientVec){
                        if (now - lastRecvTime > timeOut && lastRecvTime != 0) {
                            LOG_ERROR("TIMEOUT %s ,delay:%ld, lastRecvTime:%ld", CoinbaseExchangeId,
                                      now - lastRecvTime, lastRecvTime);
                            client.close();
                            lastRecvTime = 0;
                            needResub = true;
                            break;
                        }
                        if (now - lastSubTime > COIN_RESUB_TIME) {
                            client.close();
                            needResub = true;
                            break;
                        }
                        if(pingCycle % 3 == 0){
                            LOG_INFO("%s receive:%ld, left:%ld, ping:%ld", CoinbaseExchangeId,
                                     packRecv, mdQueue->size_approx(), pingRecv);
                        }
                    }
                    if(needResub){
                        clientVec.clear();
                        LOG_INFO("%s need to resubscribe md", CoinbaseExchangeId);
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

    void req_market(const SubMarketType subMarketType, const vector<string>& reqVec) {
        int sleepSeconds = 30;
        while (true) {
            try {
                if (subMarketType == SPOTMD) {
                    for (string symbol : reqVec) {
                        string reqUrl = COIN_REST_HOST_PUBLIC_SPOT +
                            string("spot/order_book?currency_pair=")+symbol+string("&limit=1000");
                        json::value res = RestClient::perform_get(reqUrl.c_str());
                        string recvStr = res.serialize();
                        recvStr = recvStr.substr(0, 100);
                        if(res.is_object() && res.has_field("update")){
                            json::value reqData;
                            reqData["time"] = res["current"];
                            reqData["channel"] = json::value::string("spot.order_book_req");
                            reqData["event"] = json::value::string("update");
                            res["s"] = json::value::string(symbol);
                            reqData["result"] = res;
                            MdMessage md;
                            md.exchangeTypeEnum = COINBASE;
                            md.subMarketType = subMarketType;
                            strcpy(md.data, reqData.serialize().c_str());
                            //LOG_INFO("url:%s, %s", reqUrl.c_str(), reqData.serialize().c_str());
                            md.tsNet = crypto::getCurrentTime();
                            mdQueue->try_enqueue(md);
                        }
                        else{
                            LOG_ERROR("%s", res.serialize().c_str());
                        }

                        std::this_thread::sleep_for(std::chrono::milliseconds(300));
                    }
                }
            }
            catch (std::exception &ex)
            {
                LOG_ERROR("Exception: %s", ex.what());
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            std::this_thread::sleep_for(std::chrono::seconds(sleepSeconds));
        }
    }

    void sub_market(websocket_client &client, long &lastRecvTime,
                    const SubMarketType marketType, vector<json::value> subValueVec){
        string subUrl;
        try{
            if(marketType == SPOTMD){
                subUrl = string(COIN_WEBSOCKET_HOST_PUBLIC_SPOT ) ;
            }
            else{
                throw runtime_error("not support now");
            }
            uri wsuri(subUrl);
            LOG_INFO("subscribe thread working, carry %d", spotSubCount);
            client.connect(wsuri).wait();
            for(auto subValue: subValueVec){
                websocket_outgoing_message outMsg;
                string str1 = subValue.serialize();
                outMsg.set_utf8_message(subValue.serialize().c_str());
                client.send(outMsg).wait();
                LOG_INFO("send success: %s", subValue.serialize().c_str());
                std::this_thread::sleep_for(std::chrono::milliseconds(110));//数据请求（req）限频规则 单个连接每两次请求不能小于100ms。
            }
        }
        catch (std::exception &e){
            LOG_ERROR("%s", e.what());
        }
        char* sbuf = new char[MD_LENGTH];
        while (1) {
            try {
                char* msg = client.receive().then([sbuf, &client](websocket_incoming_message in_msg) {
                    unsigned int l = MD_LENGTH;
                    in_msg.body().streambuf().getn((unsigned char *)sbuf, l);
                    if (in_msg.message_type() == websocket_message_type::ping){
                        LOG_INFO("ping received");
                        websocket_outgoing_message outMsg;
                        outMsg.set_pong_message();
                        client.send(outMsg).wait();
                    }
                    else if (in_msg.length() < 10 or in_msg.length() > (size_t)(MD_LENGTH - 1)) {
                        LOG_INFO("abnormal msg length %lu", in_msg.length());
                    }
                    size_t lgLastPos = std::min(in_msg.length(), (size_t)(MD_LENGTH - 1));
                    sbuf[lgLastPos] = '\0';
                    return sbuf;
                }
                ).get();
                lastRecvTime = crypto::getCurrentTime();
                if (msg[0] == '\0') {
                    continue;
                }
                MdMessage md;
                md.exchangeTypeEnum = COINBASE;
                md.subMarketType = marketType;
                strcpy(md.data, msg);
                md.tsNet = lastRecvTime;
                mdQueue->try_enqueue(md);
                packRecv++;
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
                    //LOG_INFO("%s",mdMessage.data);
                    save_md_string(mdMessage);
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

    inline void save_md_string(MdMessage &mdMsg){
        rapidjson::Document d;
        rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg.data);
        if(!rawData.IsObject()){
            LOG_INFO("msg :%s is not a json object", mdMsg.data);
            return;
        }
        if(!rawData.HasMember("type")) {
            LOG_INFO("msg: %s lack key field", mdMsg.data);
            return;
        }
        string channel = rawData["type"].GetString();
        if (!strcmp(channel.c_str(), "heartbeat")) {
            pingRecv++;
            return;
        }
        else if (strcmp(channel.c_str(), "match") && strcmp(channel.c_str(), "last_match") &&
            strcmp(channel.c_str(), "snapshot") && strcmp(channel.c_str(), "l2update")) {
            LOG_INFO("%s", mdMsg.data);
            return;
        }
        const rapidjson::Value &data = rawData;

        char key[64]={0};
        char mdStr[2048];
        char instType[16]={0};

        char instId[16] = {0};
        char marketType[16];

        string originInstId = rawData["product_id"].GetString();
        InstrumentInfo info;
        if(mdMsg.subMarketType == SPOTMD){
            if(smc->get_instrument_info(CoinbaseExchangeId, "SPOT",
                                        originInstId.c_str(), info) == true){
                mdMsg.instTypeEnum = SPOT;
                sprintf(instType,"%s","SPOT");
            }
            else{
                LOG_ERROR("not found %s info in smc", originInstId.c_str());
                return;
            }
        }
        /*else if(mdMsg.subMarketType == USDTSWAPMD || mdMsg.subMarketType == USDSWAPMD) {
            if (smc->get_instrument_info(CoinbaseExchangeId, "SWAP",
                                         originInstId.c_str(), info) == true) {
                mdMsg.instTypeEnum = SWAP;
                sprintf(instType, "%s", "SWAP");
            }
            else{
                LOG_ERROR("not found swap: %s info in smc", originInstId.c_str());
                return;
            }
        }
        else if(mdMsg.subMarketType == FUTURESMD){
            if(smc->get_instrument_info(CoinbaseExchangeId, "FUTURES",
                                        originInstId.c_str(), info) == true){
                mdMsg.instTypeEnum = FUTURES;
                sprintf(instType,"%s","FUTURES");
            }
            else{
                LOG_ERROR("not found futures: %s info in smc", originInstId.c_str());
                return;
            }
        }*/
        sprintf(instId,"%s", originInstId.c_str());
        bitset<4> depthBit = 0;
        if(mdMsg.instTypeEnum == SPOT){
            if (!strcmp(channel.c_str(), "snapshot")) {
                string topic;
                topic.append(CoinbaseExchangeId).append(".").append(instType).append(".MBP.").append(instId);
                MBP mbp;
                const rapidjson::Value &asks = data["asks"];
                const rapidjson::Value &bids = data["bids"];
                LOG_INFO("%s snapshot received, asks %d bids %d", instId, (int)asks.Size(), (int)bids.Size());
                for (size_t i = 0; i < asks.Size(); i++) {
                    DepthPair depth;
                    depth.price = asks[i][0].GetString();
                    depth.size = asks[i][1].GetString();
                    mbp.asks.push_back(depth);
                }
                for (size_t i = 0; i < bids.Size(); i++) {
                    DepthPair depth;
                    depth.price = bids[i][0].GetString();
                    depth.size = bids[i][1].GetString();
                    mbp.bids.push_back(depth);
                }
                update_level2(topic, mbp, depthBit);
                return;
            }
            else if (!strcmp(channel.c_str(), "l2update")) {
                sprintf(marketType, "%s", "MBP");
                sprintf(key, "%s.%s.%s.%s", CoinbaseExchangeId, instType, marketType, instId);
                string topic = key;
                MBP mbp;
                const rapidjson::Value &change = data["changes"];
                long ts = EL2Time_u(data["time"].GetString());
                mbp.seqNum = mbp.prevSeqNum = 1;
                for (size_t i = 0; i < change.Size(); i++) {
                    DepthPair depth;
                    string pos = change[i][0].GetString();
                    depth.price = change[i][1].GetString();
                    depth.size = change[i][2].GetString();
                    if (!strcmp(pos.c_str(), "sell")) {
                        mbp.asks.push_back(depth);
                    }
                    else if (!strcmp(pos.c_str(), "buy")) {
                        mbp.bids.push_back(depth);
                    }
                }
                update_level2(topic, mbp, depthBit);
                if (depthSub[2] && depthBit[2]) {
                    updateDepth(topic, instType, instId, ts, mdMsg.tsNet, 20);
                }
                if (lobMap.count(topic) > 0) {
                    if (lobMap[topic]->isReady) {
                        long tsParse = crypto::getCurrentTime();
                        string strAsks, strBids;
                        lobMap[topic]->get_asks_bids(strAsks, strBids);
                        char longMdStr[2048 * 64];
                        sprintf(longMdStr, DEPTH_Format,
                                CoinbaseExchangeId, instType, marketType, instId,
                                strAsks.c_str(),
                                strBids.c_str(),
                                ts,
                                mdMsg.tsNet, tsParse
                                );
                        //LOG_INFO("%s,%s", key, longMdStr);
#ifdef NEED_SET_REDIS
                        redisClient->set(key, longMdStr);
#endif
#ifdef NEED_PUBLISH_REDIS
                        redisClient->publish(key, longMdStr);
#endif
                    }
                }
                return;
            }
            else if (!strcmp(channel.c_str(), "match") || !strcmp(channel.c_str(), "last_match")) {
                sprintf(marketType,"%s","TRADES");
                sprintf(key, "%s.%s.%s.%s", CoinbaseExchangeId, instType, marketType, instId);
                sprintf(mdStr, Trades_Format, CoinbaseExchangeId, instType, marketType, instId,
                        rawData["trade_id"].GetString(),
                        stod(rawData["price"].GetString()),
                        stod(rawData["size"].GetString()),//TODO
                        rawData["side"].GetString(),
                        EL2Time_u(rawData["time"].GetString()),
                        mdMsg.tsNet, crypto::getCurrentTime()
                );
                //LOG_INFO("%s", mdStr);
#ifdef NEED_RPUSH_REDIS
                redisClient->rpush(key, mdStr, 2000);
                return;
#endif
            }
        }
        //LOG_INFO("key:%s,value:%s", key, mdStr);
        redisClient->set(key, mdStr);
#ifdef NEED_STORE_REDIS
        //        printf("%s,%s\n",STORE_REDIS_CHANNEL,mdStr);
        redisClient->rpush(STORE_REDIS_CHANNEL,mdStr);
#endif
#ifdef NEED_PUBLISH_REDIS
        redisClient->publish(key, mdStr);
#endif
        return;
    }

    void update_level2(string &topic, MBP &mbp, bitset<4> &bit){
        if (!mbp.prevSeqNum) {
            shared_ptr<LOB> newLob(new LOB);
            for(DepthPair a : mbp.asks){
                newLob->asksMap[a.price] = a.size;
            }
            for(DepthPair b : mbp.bids){
                newLob->bidsMap[b.price] = b.size;
            }
            //printf("%s,%d\n",__FUNCTION__ ,__LINE__);
            newLob->isReady = true;
            lobMap[topic] = newLob;
        }
        else if (lobMap.count(topic)) {
            shared_ptr<LOB> lob = lobMap[topic];
            char buffer[128];
            bit[2] = checkLevel(lob, mbp, 20);
            for(DepthPair a : mbp.asks){
                string price = a.price;
                string size = a.size;
                if(crypto::str_equal_zero(size)){
                    lob->asksMap.erase(price);
                    sprintf(buffer, "d ask %s", price.c_str());
                }
                else{
                    lob->asksMap[price] = size;
                    sprintf(buffer, "u ask %s", price.c_str());
                }
            }
            for(DepthPair b : mbp.bids){
                string price = b.price;
                string size = b.size;
                if(crypto::str_equal_zero(size)){
                    lob->bidsMap.erase(price);
                    sprintf(buffer, "d bid %s", price.c_str());
                }
               else{
                    lob->bidsMap[price] = size;
                    sprintf(buffer, "u bid %s", price.c_str());
                }
            }
            //LOG_INFO("%s", buffer);
        }
    }

    bool checkLevel(shared_ptr<LOB> lob, const MBP &mbp, int level) {
        auto itr1 = lob->asksMap.begin();
        char buffer[128];
        string lastPrice;
        for (int i = 0; mbp.asks.size() && itr1 != lob->asksMap.end() && i < level; itr1++, i++) {
            for (DepthPair a : mbp.asks) {
                if (itr1->first == a.price) {
                    return true;
                }
            }
            lastPrice = itr1->first;
        }
        for (DepthPair a : mbp.asks) {
            if (lastPrice.compare(a.price) > 0) {
                return true;
            }
        }
        auto itr2 = lob->bidsMap.rbegin();
        for (int i = 0; mbp.bids.size() && itr2 != lob->bidsMap.rend() && i < level; itr2++, i++) {
            for (DepthPair b  : mbp.bids) {
                if (itr2->first == b.price) {
                    return true;
                }
            }
            lastPrice = itr2->first;
        }
        for (DepthPair b : mbp.bids) {
            if (lastPrice.compare(b.price) < 0) {
                return true;
            }
        }
        return false;
    }

    void updateDepth(string &topic, const char *instType, const char *instId, long ts, long tsNet, int level) {
       char key[64];
       char buffer[32];
       char mdStr[2048];
       char marketType[16];
       sprintf(buffer, "DEPTH%d", level);
       string mType = buffer;
       sprintf(marketType,"%s", mType.c_str());
       sprintf(key, "%s.%s.%s.%s", CoinbaseExchangeId, instType, marketType, instId);
       shared_ptr<LOB> lob = lobMap[topic];
       string asksStr,bidsStr;
       asksStr.append("[");
       auto itr1 = lob->asksMap.begin();
       for(int i = 0; i < level && i < lob->asksMap.size(); i++, itr1++) {
           if(i != level - 1 && i != lob->asksMap.size() - 1){
               string a;
               a.append("[").append(itr1->first).append(",").append(itr1->second).append("],");
               asksStr.append(a);
           }
           else{
               string a;
               a.append("[").append(itr1->first).append(",").append(itr1->second).append("]");
               asksStr.append(a);
           }
       }
       asksStr.append("]");
       bidsStr.append("[");
       auto itr2 = lob->bidsMap.rbegin();
       for(int i = 0; i < level && i < lob->bidsMap.size(); i++, itr2++) {
           if(i != level -1 && i != lob->bidsMap.size() - 1){
               string b;
               b.append("[").append(itr2->first).append(",").append(itr2->second).append("],");
               bidsStr.append(b);
           }
           else{
                string b;
                b.append("[").append(itr2->first).append(",").append(itr2->second).append("]");
                bidsStr.append(b);
            }
        }
        bidsStr.append("]");
        sprintf(mdStr, DEPTH_Format,
                CoinbaseExchangeId, instType, marketType, instId,
                asksStr.c_str(),
                bidsStr.c_str(),
                ts,
                tsNet, crypto::getCurrentTime()
        );
        redisClient->set(key, mdStr);
#ifdef NEED_STORE_REDIS
        redisClient->rpush(STORE_REDIS_CHANNEL,mdStr);
#endif
#ifdef NEED_PUBLISH_REDIS
        redisClient->publish(key, mdStr);
#endif
    }

    long EL2Time_u(string timestr) {
        struct std::tm time1;
        std::istringstream ss(timestr);
        ss >> std::get_time(&time1, "%Y-%m-%dT%H:%M:%SZ");
        std::time_t time_u = mktime(&time1);
        int usec = 0;
        if (timestr.find('.') != string::npos) {
            string usecStr = timestr.substr(timestr.find('.') + 1);
            if (usecStr.length() == 7) {
                usecStr[usecStr.length() - 1] = '\0';
                usec = stol(usecStr);
            }
        }
        return time_u * 1000000 + usec;
    }
};
}

#endif //DB_HUOBIWSMARKETCLIENT_H
