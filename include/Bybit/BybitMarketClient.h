#pragma once

#include <unordered_map>
#include <set>
#include <cpprest/ws_client.h>
#include <cpprest/filestream.h>
#include <cpprest/json.h>
#include "base/MarketBase.h"

#define BYBIT_WS_PUBLIC_SPOT   "wss://wbs.mexc.com/raw/ws"
#define BYBIT_WS_PUBLIC_SWAP   "wss://stream.bybit.com/realtime_public"

#define MEXC_REST_PUBLIC_SPOT  "https://www.mexc.com/open/api/v2/"
#define MEXC_REST_PUBLIC_SWAP  "https://contract.mexc.com/api/v1/"

#define MEXC_TIME_OUT  20000000

namespace md {
using namespace web;
using namespace web::websockets::client;
using namespace concurrency::streams;
using namespace rapidjson;

class BybitMarketClient : public MarketBase {

public:
    BybitMarketClient(vector<string> instTypeVec, vector<string> marketTypeVec, vector<string> instIdVec,
                         int tokenLot = 30, const char *ip = "127.0.0.1", const int port = 9379)
                          : MarketBase(instTypeVec, marketTypeVec, instIdVec, tokenLot, ip, port){
        sprintf(exchangeId, "%s", "BYBIT");
    }
    ~BybitMarketClient() {
    }

public:
    virtual void construct(){
        for(auto instType : _instTypeVec){
            for(auto instId : _instIdVec){
                InstrumentInfo info;
                if(smc->get_instrument_info(exchangeId, instType.c_str(), instId.c_str(), info)){
                    string strOriginInstId = info.originInstId;
                    for(auto marketType : _marketTypeVec){
                        string token = MarketBase::getToken(instType, marketType, instId);
                        vector<json::value> &subValueVec = tokenDict[token].values;
                        vector<json::value> &reqValueVec = tokenDict[token].request;
                        json::value value1, value2;
                        if(!strcmp(instType.c_str(), "SPOT")){
                        }
                        else if(!strcmp(instType.c_str(), "SWAP")){
                            if (!strcmp(marketType.c_str(), "DEPTH5") || !strcmp(marketType.c_str(), "DEPTH10") ||
                                !strcmp(marketType.c_str(), "DEPTH20")) {
                                value1["op"] = json::value::string("subscribe");
                                value1["args"][0] = json::value::string(string("orderBookL2_25.") + strOriginInstId);
                                subValueVec.push_back(value1);
                                tokenDict[token].payload++;
                            }
                            else if (!strcmp(marketType.c_str(), "TRADES")) {
                                value1["op"] = json::value::string("subscribe");
                                value1["args"][0] = json::value::string(string("trade.") + strOriginInstId);
                                subValueVec.push_back(value1);
                                tokenDict[token].payload++;
                            }
                            else if (!strcmp(marketType.c_str(), "MBP")) {
                                value1["op"] = json::value::string("req.mbp");
                                value1["symbol"] = json::value::string(strOriginInstId.c_str());
                                //reqValueVec.push_back(value1);
                                //tokenDict[token].payload++;
                            }
                        }
                    }
                }
                else {
                    LOG_ERROR("instrument info not exist %s.%s.%s", exchangeId, instType.c_str(), instId.c_str());
                }
            }
        }
    }

    virtual void request() {
        std::thread thread1(&BybitMarketClient::monitor1, this);
        thread1.detach();

        std::thread thread2(&BybitMarketClient::monitor2, this);
        thread2.detach();
    }

    virtual void consume(MdMessage *mdMsg) {
        rapidjson::Document d;
        rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg->data);
        if(!rawData.IsObject()){
            LOG_INFO("msg :%s is not a json object", mdMsg->data);
            return;
        }

        if(!rawData.HasMember("topic") || !rawData.HasMember("data")) {
            LOG_INFO("%s", mdMsg->data);
            return;
        }

        string strChannel = rawData["topic"].GetString();
        if (strChannel.find(".") == string::npos) {
            LOG_INFO("%s", mdMsg->data);
            return;
        }
        char key[64]={0};
        char mdStr[2048];
        char instType[16]={0};

        char instId[16] = {0};
        char marketType[16];

        const rapidjson::Value &data = rawData["data"];
        string originInstId = strChannel.substr(strChannel.find('.') + 1);
        InstrumentInfo info;
        if(mdMsg->subMarketType == SPOTMD){
            if(smc->get_instrument_info(exchangeId, "SPOT",
                                        originInstId.c_str(), info) == true){
                mdMsg->instTypeEnum = SPOT;
                sprintf(instType,"%s","SPOT");
            }
            else{
                LOG_ERROR("not found %s spot info in smc", originInstId.c_str());
                return;
            }
        }
        else if(mdMsg->subMarketType == USDTSWAPMD){
            if(smc->get_instrument_info(exchangeId, "SWAP", originInstId.c_str(), info)){
                mdMsg->instTypeEnum = SWAP;
                sprintf(instType,"%s","SWAP");
            }
            else{
                LOG_ERROR("not found %s swap info in smc", originInstId.c_str());
                return;
            }
        }
        sprintf(instId,"%s", info.instId);
        if(mdMsg->instTypeEnum == SPOT){
        }
        else if(mdMsg->instTypeEnum == SWAP){
            if (strstr(strChannel.c_str(), "orderBookL2_25.")) {
                sprintf(marketType,"%s","DEPTH20");
                sprintf(key, "%s.%s.%s.%s", exchangeId, instType, marketType, instId);
                if(!rawData.HasMember("type")) {
                    LOG_INFO("bad swap orderbook %s", mdMsg->data);
                    return;
                }
                string topic;
                topic.append(exchangeId).append(".").append(instType).append(".MBP.").append(instId);
                long ts = atoll(rawData["timestamp_e6"].GetString());
                string type = rawData["type"].GetString();
                MBP mbp;
                if (!strcmp(type.c_str(), "snapshot")) {
                    mbp.prevSeqNum = 0;
                    mbp.seqNum = 1;
                    int size = data["order_book"].Size();
                    for (int i = 0; i < size; i++) {
                        DepthPair depth;
                        depth.price = data["order_book"][i]["price"].GetString();
                        depth.size = data["order_book"][i]["size"].GetString();
                        string side = data["order_book"][i]["side"].GetString();
                        if (!strcmp(side.c_str(), "Buy")) {
                            mbp.bids.push_back(depth);
                        }
                        else if (!strcmp(side.c_str(), "Sell")) {
                            mbp.asks.push_back(depth);
                        }
                    }
                    update_mbp(topic, mbp);
                }
                else if (!strcmp(type.c_str(), "delta")) {
                    mbp.prevSeqNum = 1;
                    mbp.seqNum = 1;
                    DepthPair depth;
                    const rapidjson::Value &value1 = data["delete"];
                    for (int i = 0; i < value1.Size(); i++) {
                        depth.price = value1[i]["price"].GetString();
                        depth.size = "0";
                        string side = value1[i]["side"].GetString();
                        if (!strcmp(side.c_str(), "Buy")) {
                            mbp.bids.push_back(depth);
                        }
                        else if (!strcmp(side.c_str(), "Sell")) {
                            mbp.asks.push_back(depth);
                        }
                    }
                    const rapidjson::Value &update = data["update"];
                    for (int i = 0; i < update.Size(); i++) {
                        depth.price = update[i]["price"].GetString();
                        depth.size = update[i]["size"].GetString();
                        string side = update[i]["side"].GetString();
                        if (!strcmp(side.c_str(), "Buy")) {
                            mbp.bids.push_back(depth);
                        }
                        else if (!strcmp(side.c_str(), "Sell")) {
                            mbp.asks.push_back(depth);
                        }
                    }
                    const rapidjson::Value &insert = data["insert"];
                    for (int i = 0; i < insert.Size(); i++) {
                        depth.price = insert[i]["price"].GetString();
                        depth.size = insert[i]["size"].GetString();
                        string side = insert[i]["side"].GetString();
                        if (!strcmp(side.c_str(), "Buy")) {
                            mbp.bids.push_back(depth);
                        }
                        else if (!strcmp(side.c_str(), "Sell")) {
                            mbp.asks.push_back(depth);
                        }
                    }
                    if (lobDict.count(topic)) {
                        update_mbp(topic, mbp);
                        if (lobDict[topic]->isReady) {
                            long tsParse = crypto::getCurrentTime();
                            string strAsks, strBids;
                            lobDict[topic]->get_asks_bids(strAsks, strBids, 20);
                            char longMdStr[2048 * 64];
                            sprintf(longMdStr, DEPTH_Format,
                                    exchangeId, instType, marketType, instId,
                                    strAsks.c_str(),
                                    strBids.c_str(),
                                    ts,
                                    mdMsg->tsNet, tsParse
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
                }
            }
            else if (strstr(strChannel.c_str(), "trade.")) {
                sprintf(marketType,"%s","TRADES");
                sprintf(key, "%s.%s.%s.%s", exchangeId, instType, marketType, instId);
                for (int i = 0; i < data.Size(); i++) {
                    const char *direct = !strcmp(data[i]["side"].GetString(), "Buy") ? "buy" : "sell";
                    sprintf(mdStr, Trades_Format, exchangeId, instType, marketType, instId,
                            data[i]["trade_id"].GetString(),
                            stod(data[i]["price"].GetString()),
                            stod(data[i]["size"].GetString()),
                            direct,
                            stol(data[i]["trade_time_ms"].GetString()) * 1000,
                            mdMsg->tsNet, crypto::getCurrentTime()
                    );
                    //LOG_INFO("%s, %s", key, mdStr);
#ifdef NEED_RPUSH_REDIS
                    redisClient->rpush(key, mdStr, 2000);
#endif
                }
                return;
            }
            else {
                LOG_INFO("%s", mdMsg->data);
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

private:
    void monitor1(){
        long now = 0;
        long timeOut = MEXC_TIME_OUT;
        while (1) {
            for (auto &token : tokenDict) {
                if (!token.second.status && token.second.values.size()) {
                    std::thread thread(&BybitMarketClient::thrd_subscribe, this, std::ref(token.second.client),
                        std::ref(token.second));
                    thread.detach();
                    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
                }
            }

            bool needResub = false;
            int pingCycle = 0;
            while(!needResub) {
                pingCycle++;
                try{
                    now = crypto::getCurrentTime();
                    for(auto &token : tokenDict){
                        token_unit &unit = token.second;
                        if (!(pingCycle % 30) && token.first.find("swap_") != string::npos) {
                            websocket_outgoing_message outMsg;
                            outMsg.set_utf8_message("{\"method\":\"ping\"}");
                            //unit.client.send(outMsg).wait();
                            //LOG_INFO("%s ping", token.first.c_str());
                        }
                    }
                    for(auto &token : tokenDict){
                        token_unit &unit = token.second;
                        if (now - unit.time > timeOut && unit.time != 0) {
                            LOG_ERROR("%s timeout, delay:%ld, lasttime:%ld", unit.token.c_str(),
                                      now - unit.time, unit.time);
                            unit.reset();
                            needResub = true;
                            break;
                        }
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }
                catch(std::exception &e){
                    LOG_ERROR("monitor exception: %s", e.what());
                }
            }
        }
    }

    void thrd_subscribe(websocket_client &client, token_unit &unit){
        SubMarketType marketType = SPOTMD;
        string subUrl;
        try{
            if(unit.token.find("spot_") != string::npos) {
                subUrl = string(BYBIT_WS_PUBLIC_SPOT);
            }
            else if (unit.token.find("swap_") != string::npos) {
                subUrl = string(BYBIT_WS_PUBLIC_SWAP);
                marketType = USDTSWAPMD;
            }
            else{
                throw runtime_error("not support now");
            }
            uri wsuri(subUrl);
            client.connect(wsuri).wait();
            for(auto &value : unit.values){
                websocket_outgoing_message outMsg;
                outMsg.set_utf8_message(value.serialize().c_str());
                client.send(outMsg).wait();
                LOG_INFO("send success: %s", value.serialize().c_str());
                std::this_thread::sleep_for(std::chrono::milliseconds(110));
            }
        }
        catch (std::exception &e){
            LOG_ERROR("%s send exception: %s", unit.token.c_str(), e.what());
            client.close();
            return;
        }
        unit.status = 1;
        char* sbuf = new char[MD_LENGTH];
        while (1) {
            try {
                char* msg = client.receive().then([sbuf, &client](websocket_incoming_message in_msg) {
                    unsigned int l = MD_LENGTH;
                    in_msg.body().streambuf().getn((unsigned char *)sbuf, l);
                    if (in_msg.message_type() == websocket_message_type::ping){
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
                unit.time = crypto::getCurrentTime();
                if (msg[0] == '\0') {
                    continue;
                }
                enqueue(marketType, msg, unit.token);
            } catch (std::exception &e){
                LOG_INFO("%s recv exception: %s", unit.token.c_str(), e.what());
                client.close();
                break;
            }
        }
    }

    void monitor2(){
        long now = 0;
        long timeOut = MEXC_TIME_OUT;
        while (1) {
            for (auto &token : tokenDict) {
                if (token.second.request.size()) {
                    std::thread thread(&BybitMarketClient::thrd_request, this, std::ref(token.second));
                    thread.detach();
                    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
                }
            }
            return;
        }
    }

    void thrd_request(token_unit &unit) {
        int sleepMilSeconds = 500;
        while (true) {
            try {
                for (auto &value : unit.request) {
                    SubMarketType marketType = SPOTMD;
                    string op = value["op"].as_string();
                    string symbol = value["symbol"].as_string();
                    string reqUrl = MEXC_REST_PUBLIC_SPOT;
                    json::value reqData;
                    if (unit.token.find("spot_") != string::npos) {
                        if (op.find("req.depth20") != string::npos) {
                            reqUrl += string("market/depth?symbol=")+symbol+string("&depth=20");
                            reqData["channel"] = json::value::string("push.req.depth");
                        }
                        else if (op.find("req.mbp") != string::npos) {
                            reqUrl += string("market/depth?symbol=")+symbol+string("&depth=1000");
                            reqData["channel"] = json::value::string("push.req.mbp");
                        }
                        else if (op.find("req.trades") != string::npos) {
                            reqUrl += string("market/deals?symbol=")+symbol+string("&limit=20");
                            reqData["channel"] = json::value::string("push.req.trades");
                        }
                        else {
                            LOG_INFO("%s not support op %s symbol %s", unit.token.c_str(), op.c_str(), symbol.c_str());
                            continue;
                        }
                    }
                    else if (unit.token.find("swap_") != string::npos) {
                        marketType = USDTSWAPMD;
                        reqUrl = MEXC_REST_PUBLIC_SWAP;
                        if (op.find("req.mbp") != string::npos) {
                            reqUrl += string("contract/depth_commits/")+symbol+string("/1000");
                            reqData["channel"] = json::value::string("push.req.mbp");
                        }
                        else {
                            LOG_INFO("%s not support op %s symbol %s", unit.token.c_str(), op.c_str(), symbol.c_str());
                            continue;
                        }
                    }
                    else {
                        LOG_INFO("%s not support", unit.token.c_str());
                        continue;
                    }
                    json::value res = RestClient::perform_get(reqUrl.c_str());
                    if(res.is_object() && res.has_field("data")){
                        reqData["symbol"] = json::value::string(symbol);
                        reqData["data"] = res["data"];
                        enqueue(marketType, reqData.serialize().c_str(), unit.token);
                    }
                    else{
                        LOG_ERROR("%s", res.serialize().c_str());
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
            catch (std::exception &ex)
            {
                LOG_ERROR("Exception: %s", ex.what());
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepMilSeconds));
        }
    }

};
}
