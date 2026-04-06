#pragma once

#include <unordered_map>
#include <set>
#include <cpprest/ws_client.h>
#include <cpprest/filestream.h>
#include <cpprest/json.h>
#include "base/MarketBase.h"

#define MEXC_WS_PUBLIC_SPOT   "wss://wbs.mexc.com/raw/ws"
#define MEXC_WS_PUBLIC_SWAP   "wss://contract.mexc.com/ws"

#define MEXC_REST_PUBLIC_SPOT "https://www.mexc.com/open/api/v2/"
#define MEXC_REST_PUBLIC_SWAP "https://contract.mexc.com/api/v1/"

#define MEXC_TIME_OUT  20000000

namespace md {
using namespace web;
using namespace web::websockets::client;
using namespace concurrency::streams;
using namespace rapidjson;

class MexcMarketClient : public MarketBase {

public:
    MexcMarketClient(vector<string> instTypeVec, vector<string> marketTypeVec, vector<string> instIdVec,
                         int tokenLot = 30, const char *ip = "127.0.0.1", const int port = 9379)
                          : MarketBase(instTypeVec, marketTypeVec, instIdVec, tokenLot, ip, port){
        sprintf(exchangeId, "%s", "MEXC");    
    }
    ~MexcMarketClient() {
    }

public:
    virtual void construct(){
        for(auto instType : _instTypeVec){
            for(auto instId : _instIdVec){
                InstrumentInfo info;
                if(smc->get_instrument_info(exchangeId, instType.c_str(), instId.c_str(), info)){
                    string strOriginInstId = info.originInstId;
                    std::replace(strOriginInstId.begin(), strOriginInstId.end(), '-', '_');
                    for(auto marketType : _marketTypeVec){
                        string token = MarketBase::getToken(instType, marketType, instId);
                        vector<json::value> &subValueVec = tokenDict[token].values;
                        vector<json::value> &reqValueVec = tokenDict[token].request;
                        json::value value, value1;
                        if(!strcmp(instType.c_str(), "SPOT")){
                            if (!strcmp(marketType.c_str(), "DEPTH5") || !strcmp(marketType.c_str(), "DEPTH10") || 
                                !strcmp(marketType.c_str(), "DEPTH20")) {
                                value["op"] = json::value::string("sub.depth.limit");
                                value["symbol"] = json::value::string(strOriginInstId.c_str());
                                value["limit"] = json::value::number(20);
                                //subValueVec.push_back(value);
                                value1["op"] = json::value::string("req.depth20");                          
                                value1["symbol"] = json::value::string(strOriginInstId.c_str());
                                reqValueVec.push_back(value1);
                                tokenDict[token].payload++;
                            }
                            else if (!strcmp(marketType.c_str(), "TRADES")) {
                                value["op"] = json::value::string("sub.deal.aggregate");
                                value["symbol"] = json::value::string(strOriginInstId.c_str()); 
                                subValueVec.push_back(value);
                                value1["op"] = json::value::string("req.trades");
                                value1["symbol"] = json::value::string(strOriginInstId.c_str()); 
                                //reqValueVec.push_back(value1); 
                                tokenDict[token].payload++;
                            }
                            else if (!strcmp(marketType.c_str(), "MBP")) {
                                value1["op"] = json::value::string("req.mbp");
                                value1["symbol"] = json::value::string(strOriginInstId.c_str());
                                reqValueVec.push_back(value1);
                                tokenDict[token].payload++;
                            }
                        }
                        else if(!strcmp(instType.c_str(), "SWAP")){
                            if (!strcmp(marketType.c_str(), "DEPTH5") || !strcmp(marketType.c_str(), "DEPTH10") || 
                                !strcmp(marketType.c_str(), "DEPTH20")) {
                                value["method"] = json::value::string("sub.depth.full");
                                value["param"]["symbol"] = json::value::string(strOriginInstId.c_str());
                                value["param"]["limit"] = json::value::number(20);
                                subValueVec.push_back(value);
                                tokenDict[token].payload++;
                            }
                            else if (!strcmp(marketType.c_str(), "TRADES")) {
                                value["method"] = json::value::string("sub.deal");
                                value["param"]["symbol"] = json::value::string(strOriginInstId.c_str()); 
                                subValueVec.push_back(value);
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
        std::thread thread1(&MexcMarketClient::monitor1, this);
        thread1.detach();
   
        std::thread thread2(&MexcMarketClient::monitor2, this);
        thread2.detach();
    }    

    virtual void consume(MdMessage *mdMsg) {
        rapidjson::Document d;
        rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg->data);
        if(!rawData.IsObject()){
            LOG_INFO("msg :%s is not a json object", mdMsg->data);
            return;
        }

        if(!rawData.HasMember("channel") || !rawData.HasMember("data") ||
           !rawData.HasMember("symbol")) {
            LOG_INFO("%s", mdMsg->data);
            return;
        }
        
        string strChannel = rawData["channel"].GetString();
        if (strChannel.find("push.") == string::npos) {
            LOG_INFO("%s", mdMsg->data);
            return;
        }
        char key[64]={0};
        char mdStr[2048];
        char instType[16]={0};

        char instId[16] = {0};
        char marketType[16];

        const rapidjson::Value &data = rawData["data"];
        string originInstId = rawData["symbol"].GetString();
        std::replace(originInstId.begin(), originInstId.end(), '_', '-');
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
        sprintf(instId,"%s", originInstId.c_str());
        if(mdMsg->instTypeEnum == SPOT){
            if (!strcmp(strChannel.c_str(), "push.depth.full")) {
                int nBids = data["bids"].Size();
                int nAsks = data["asks"].Size();
                if(nBids == 5 && nAsks == 5){
                    sprintf(marketType, "%s", "DEPTH5");
                }
                else if (nBids == 10 && nAsks == 10) {
                    sprintf(marketType, "%s", "DEPTH10");
                } 
                else if (nBids == 20 && nAsks == 20) {
                    sprintf(marketType, "%s", "DEPTH20");
                }
                else {
                    LOG_INFO("%s Depth level mismatch, bids %d asks %d", originInstId.c_str(), nBids, nAsks);
                    return;
                }
                sprintf(key, "%s.%s.%s.%s", exchangeId, instType, marketType, instId);
                string asksStr,bidsStr;
                asksStr.append("[");
                for(int i = 0; i < nAsks; i++) {
                    if(i != nAsks - 1){
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
                for(int i = 0; i < nBids; i++) {
                    if(i != nBids - 1){
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
                        exchangeId, instType, marketType, instId,
                        asksStr.c_str(),
                        bidsStr.c_str(),
                        stol(rawData["ts"].GetString()) * 1000,
                        mdMsg->tsNet, crypto::getCurrentTime()
                );
            }
            else if (!strcmp(strChannel.c_str(), "push.req.depth") || !strcmp(strChannel.c_str(), "push.req.mbp")) {
                int nAsks = data["asks"].Size();
                int nBids = data["bids"].Size();
                if(nAsks == 20 && nAsks == nBids){
                    sprintf(marketType, "%s", "DEPTH20");
                }
                else if (!strcmp(strChannel.c_str(), "push.req.mbp")) {
                    sprintf(marketType, "%s", "MBP");
                }
                else {
                    LOG_INFO("%s Depth level mismatch, bids %d asks %d", originInstId.c_str(), nBids, nAsks);
                    return;
                }
                sprintf(key, "%s.%s.%s.%s", exchangeId, instType, marketType, instId);
                string asksStr,bidsStr;
                asksStr.append("[");
                for(int i = 0; i < nAsks; i++) {
                    if(i != nAsks - 1){
                        string a;
                        a.append("[").append(data["asks"][i]["price"].GetString()).append(",")
                                .append(data["asks"][i]["quantity"].GetString()).append("],");
                        asksStr.append(a);
                    }
                    else{
                        string a;
                        a.append("[").append(data["asks"][i]["price"].GetString()).append(",")
                                .append(data["asks"][i]["quantity"].GetString()).append("]");
                        asksStr.append(a);
                    }
                }
                asksStr.append("]");
                bidsStr.append("[");
                for(int i = 0; i < nBids; i++) {
                    if(i != nBids - 1){
                        string b;
                        b.append("[").append(data["bids"][i]["price"].GetString()).append(",")
                                .append(data["bids"][i]["quantity"].GetString()).append("],");
                        bidsStr.append(b);
                    }
                    else{
                        string b;
                        b.append("[").append(data["bids"][i]["price"].GetString()).append(",")
                                .append(data["bids"][i]["quantity"].GetString()).append("]");
                        bidsStr.append(b);
                    }
                }
                bidsStr.append("]");
                if (strcmp(strChannel.c_str(), "push.req.mbp")) {
                    sprintf(mdStr, DEPTH_Format,
                            exchangeId, instType, marketType, instId,
                            asksStr.c_str(),
                            bidsStr.c_str(),
                            0,
                            mdMsg->tsNet, crypto::getCurrentTime()
                    );
                }
                else {
                    char longMdStr[2048*64];//65536 is small and will cause segment fault
                    sprintf(longMdStr, DEPTH_Format,
                            exchangeId, instType, marketType, instId,
                            asksStr.c_str(),
                            bidsStr.c_str(),
                            0,
                            mdMsg->tsNet, crypto::getCurrentTime()
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
            else if (!strcmp(strChannel.c_str(), "push.deal.aggregate")) {
                sprintf(marketType,"%s","TRADES");
                sprintf(key, "%s.%s.%s.%s", exchangeId, instType, marketType, instId);
                int count = data["deals"].Size();
                for (int i = count - 1; i >= 0; i--) {
                    const char *direct = !strcmp(data["deals"][i]["T"].GetString(), "1") ? "buy" : "sell";
                    sprintf(mdStr, Trades_Format, exchangeId, instType, marketType, instId,
                            0,
                            stod(data["deals"][i]["p"].GetString()),
                            stod(data["deals"][i]["q"].GetString()),//TODO
                            direct,
                            stol(data["deals"][i]["t"].GetString()) * 1000,
                            mdMsg->tsNet, crypto::getCurrentTime()
                    );
#ifdef NEED_RPUSH_REDIS
                    redisClient->rpush(key, mdStr, 2000);
                    return;
#endif
                }
            }
            else if (!strcmp(strChannel.c_str(), "push.req.trades")) {
                sprintf(marketType,"%s","TRADES");
                sprintf(key, "%s.%s.%s.%s", exchangeId, instType, marketType, instId);
                int count = data.Size();
                for (int i = count - 1; i >= 0; i--) {
                    string tradeTime = data[i]["trade_time"].GetString(); 
                    string tradePrice = data[i]["trade_price"].GetString();
                    string tradeVol = data[i]["trade_quantity"].GetString();
                    string direction = data[i]["trade_type"].GetString();
                    if (tradeRecordMap[instId].Less(tradeTime, tradePrice, tradeVol, direction)) {
                        const char *direct = !strcmp(direction.c_str(), "ASK") ? "sell" : "buy";
                        sprintf(mdStr, Trades_Format, exchangeId, instType, marketType, instId,
                                0,
                                stod(tradePrice),
                                stod(tradeVol),//TODO
                                direct,
                                stol(tradeTime) * 1000,
                                mdMsg->tsNet, crypto::getCurrentTime()
                        );
                        tradeRecordMap[instId] = TradeRecord(tradeTime, tradePrice, tradeVol, direction);
                        //LOG_INFO("%s,%s", key, mdStr);
#ifdef NEED_RPUSH_REDIS
                        redisClient->rpush(key, mdStr, 2000);
                        return;
#endif
                    }
                }
            }
        }
        else if(mdMsg->instTypeEnum == SWAP){
            if (!strcmp(strChannel.c_str(), "push.depth.full") || !strcmp(strChannel.c_str(), "push.req.mbp")) {
                int nBids = data["bids"].Size();
                int nAsks = data["asks"].Size();
                if (!strcmp(strChannel.c_str(), "push.req.mbp")) {
                    sprintf(marketType, "%s", "MBP");
                }
                else {
                    if(nBids == 5 && nAsks == 5){
                        sprintf(marketType, "%s", "DEPTH5");
                    }
                    else if (nBids == 10 && nAsks == 10) {
                        sprintf(marketType, "%s", "DEPTH10");
                    } 
                    else if (nBids == 20 && nAsks == 20) {
                        sprintf(marketType, "%s", "DEPTH20");
                    }
                    else {
                        LOG_INFO("%s Depth level mismatch, bids %d asks %d", originInstId.c_str(), nBids, nAsks);
                        return;
                    }
                }
                sprintf(key, "%s.%s.%s.%s", exchangeId, instType, marketType, instId);
                string asksStr,bidsStr;
                asksStr.append("[");
                for(int i = 0; i < nAsks; i++) {
                    if(i != nAsks - 1){
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
                for(int i = 0; i < nBids; i++) {
                    if(i != nBids - 1){
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
                if (strcmp(strChannel.c_str(), "push.req.mbp")) {
                    sprintf(mdStr, DEPTH_Format,
                            exchangeId, instType, marketType, instId,
                            asksStr.c_str(),
                            bidsStr.c_str(),
                            stol(rawData["ts"].GetString()) * 1000,
                            mdMsg->tsNet, crypto::getCurrentTime()
                    );
                }
                else {
                    char longMdStr[2048*64];//65536 is small and will cause segment fault
                    sprintf(longMdStr, DEPTH_Format,
                            exchangeId, instType, marketType, instId,
                            asksStr.c_str(),
                            bidsStr.c_str(),
                            0,
                            mdMsg->tsNet, crypto::getCurrentTime()
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
            else if (!strcmp(strChannel.c_str(), "push.req.depth")) {
                int nAsks = data["asks"].Size();
                int nBids = data["bids"].Size();
                if(nAsks == 20 && nAsks == nBids){
                    sprintf(marketType, "%s", "DEPTH20");
                }
                else {
                    LOG_INFO("%s Depth level mismatch, bids %d asks %d", originInstId.c_str(), nBids, nAsks);
                    return;
                }
            }
            else if (!strcmp(strChannel.c_str(), "push.deal")) {
                sprintf(marketType,"%s","TRADES");
                sprintf(key, "%s.%s.%s.%s", exchangeId, instType, marketType, instId);
                const char *direct = !strcmp(data["T"].GetString(), "1") ? "buy" : "sell";
                sprintf(mdStr, Trades_Format, exchangeId, instType, marketType, instId,
                        0,
                        stod(data["p"].GetString()),
                        stod(data["v"].GetString()),//TODO
                        direct,
                        stol(data["t"].GetString()) * 1000,
                        mdMsg->tsNet, crypto::getCurrentTime()
                );
                //LOG_INFO("%s, %s", key, mdStr);
#ifdef NEED_RPUSH_REDIS
                redisClient->rpush(key, mdStr, 2000);
                return;
#endif
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
                    std::thread thread(&MexcMarketClient::thrd_subscribe, this, std::ref(token.second.client),
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
                            unit.client.send(outMsg).wait();
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
                subUrl = string(MEXC_WS_PUBLIC_SPOT);
            }
            else if (unit.token.find("swap_") != string::npos) {
                subUrl = string(MEXC_WS_PUBLIC_SWAP);
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
                    std::thread thread(&MexcMarketClient::thrd_request, this, std::ref(token.second));
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

private:
    struct TradeRecord {
        string tradeTime;
        string tradePrice;
        string tradeVol;
        string direction;
        TradeRecord() {}
        TradeRecord(string &time, string &price, string &vol, string &direct) {
            tradeTime = time; tradePrice = price; tradeVol = vol; direction = direct;
        }
        bool Less(string &time, string &price, string &vol, string &direct) {
            if (strcmp(time.c_str(), tradeTime.c_str())) {
                return true;
            }
            else if (!strcmp(time.c_str(), tradeTime.c_str())) {
                return strcmp(tradePrice.c_str(), price.c_str()) || strcmp(tradeVol.c_str(), vol.c_str()) ||
                       strcmp(direction.c_str(), direct.c_str());
            }
            return false;
         }
    };
    unordered_map<string, TradeRecord> tradeRecordMap;

};
}
