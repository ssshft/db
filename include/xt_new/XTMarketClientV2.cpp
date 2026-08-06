#include "XTMarketClientV2.h"
#include "utils.h"
#include "string_util.h"

void md::XTUnit::sub_websocket(){
    try{
        LOG_INFO("start to sub_websocket to %s",m_wsBaseUrl.c_str() );
        wsClient.close();
        uri_builder builder(m_wsBaseUrl);
        wsClient.connect(builder.to_string())
        .then([&]() {
            std::function<void (const websocket_incoming_message &msg)> f;
            f = std::bind(&MarketDataBaseStruct::on_websocket_msg, this, placeholders::_1);
            wsClient.set_message_handler(f);
            std::function<void (websocket_close_status close_status,
                                const utility::string_t& reason, const std::error_code& error)> c;
            c =  std::bind(&MarketDataBaseStruct::on_close_msg,this
                    ,placeholders::_1,placeholders::_2,placeholders::_3);
            wsClient.set_close_handler(c);
        }).wait();
        for(auto &subValue: subValueVec){
            //time放这里是为了防止出现掉线，重新订阅
            //subValue["time"] = crypto::getCurrentTimeSeconds();
            websocket_outgoing_message outMsg;
            outMsg.set_utf8_message(subValue.serialize().c_str());
            wsClient.send(outMsg).wait();
            //LOG_INFO("%s send success: %s",exchIdStr.c_str(), subValue.serialize().c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(110));
        }
        LOG_INFO("%s send %lu sub values to %s",exchIdStr.c_str(),subValueVec.size(), m_wsBaseUrl.c_str());
        m_IsConnected = true;
    }
    catch(std::exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s,%s,%s,%s,%d,%s",exchIdStr.c_str(),instTypeStr.c_str(),
                  marketTypeStr.c_str(), __FUNCTION__ , __LINE__, e.what());
    }
}

void md::XTUnit::ping(){
    if(m_IsConnected){
        websocket_outgoing_message outMsg;
        outMsg.set_utf8_message("ping");
        wsClient.send(outMsg);
    }
}

void md::XTUnit::pong(){
    if(m_IsConnected){
//        websocket_outgoing_message outMsg;
//        outMsg.set_pong_message();
//        wsClient.send(outMsg).wait();
    }
}

void md::XTUnit::pong(const string &tick) {
    if (m_IsConnected) {
        json::value pong;
        pong["pong"] = json::value::number(stol(tick));
        websocket_outgoing_message outMsg;
        outMsg.set_utf8_message(pong.serialize().c_str());
        wsClient.send(outMsg);
    }
}

//https://microsoft.github.io/cpprestsdk/classweb_1_1websockets_1_1client_1_1websocket__incoming__message.html
void md::XTUnit::on_websocket_msg(const websocket_incoming_message &in_msg){
    try{
        if (in_msg.message_type() == websocket_message_type::text_message){
            const string &s = in_msg.extract_string().get();
            HANDLE_TEXT_MSG(s)
            // in_msg.extract_string().then([&](const string &s){
            //     MDMsg msg;
            //     msg.header.exchangeTypeEnum = this->exchangeTypeEnum;
            //     msg.header.instTypeEnum = this->instTypeEnum;
            //     msg.header.marketTypeEnum = this->marketTypeEnum;
            //     msg.header.subMarketTypeEnum = this->subMarketTypeEnum;

            //     msg.tsNet = crypto::getCurrentTime();
            //     msg.body.data = s;
            //     m_queue->push(msg);
            //     tickCount++;
            // });
        }
    }
    catch (exception &e){
        LOG_ERROR("%s", e.what());
    }
}

void md::XTUnit::construct(){
    LOG_INFO("%s", getString().c_str());
    exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
    //先构造 ws地址
    if(subMarketTypeEnum == SMT_SPOT_MD){
        m_wsBaseUrl = XT_WEBSOCKET_HOST_PUBLIC_SPOT;
    }
    else if (subMarketTypeEnum == SMT_USDT_SWAP || subMarketTypeEnum == SMT_USD_SWAP) {
        m_wsBaseUrl = XT_WEBSOCKET_HOST_PUBLIC_USDT_SWAP;
    }
    else{
        cryptothrow(exchIdStr + " not support your sub market type!",-1);
    }
    //开始构造订阅格式
    for(auto instId : subStrVec){
        string strOriginInstId = crypto::to_lower(instId);
        json::value value;
        if(subMarketTypeEnum == SMT_SPOT_MD){
            if (marketTypeEnum == TRADES) {
                value["channel"] = json::value::string("ex_last_trade");
                value["since"] = json::value::number(0);
                value["market" ] = json::value::string(strOriginInstId.c_str());
                value["event"] = json::value::string("addChannel");
            }
            else if (marketTypeEnum == MBPType) {
                value["channel"] = json::value::string("ex_depth_data");
                value["market" ] = json::value::string(strOriginInstId.c_str());
                value["event"] = json::value::string("addChannel");
            }
            else{
                cryptothrow("not support markettype: "+marketTypeStr, -1);
            }
        }
        else if(subMarketTypeEnum == SMT_USDT_SWAP || subMarketTypeEnum == SMT_USD_SWAP) {
            if (marketTypeEnum == TRADES || marketTypeEnum == MBPType) {
                //订阅一个sub_symbol获得全套行情推送
                value["req"] = json::value::string("sub_symbol");
                value["symbol"] = json::value::string(strOriginInstId.c_str());
            }
            else{
                cryptothrow("not support markettype: "+marketTypeStr, -1);
            }

        }
        subValueVec.push_back(value);
    }
}

//处理消息 解析json并发送给redis或共享内存
void md::XTUnit::save_md_string(const MDMsg &mdMsg){
    switch (mdMsg.header.subMarketTypeEnum) {
        case SMT_SPOT_MD:
            save_spot_md(mdMsg);
            break;
        case SMT_USDT_SWAP:
        case SMT_USD_SWAP:
            save_swap_md(mdMsg);
            break;
        default:
            break;
    }
}

void md::XTUnit::save_spot_md(const MDMsg &mdMsg){
    rapidjson::Document d;
    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg.body.data.c_str());
    if(!rawData.IsObject()){
//        LOG_ERROR("msg :%s is not a json object", mdMsg.body.data.c_str());
        return;
    }
    string ch;
    if(!(rawData.HasMember("code") && rawData.HasMember("data"))) {
        if (rawData.HasMember("ping")) {
            pong(rawData["ping"].GetString());
            return;
        }
        LOG_ERROR("%s.%s.%s useless data: %s",exchIdStr.c_str(), instTypeStr.c_str(),
                      marketTypeStr.c_str(), mdMsg.body.data.c_str());
        return;
    }
    char key[64]={0};
    char mdStr[2048];
    string originInstId = crypto::to_upper(rawData["data"]["market"].GetString());
    InstrumentInfo info;
    if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                originInstId.c_str(), info) == false){
        LOG_ERROR("not found %s info in smc", originInstId.c_str());
        return;
    }
    sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
            marketTypeStr.c_str(), info.instId);
    if (mdMsg.header.marketTypeEnum == MBPType) {
        string topic = key;
        MBP mbp;
        const rapidjson::Value &data = rawData["data"];
        const rapidjson::Value &asks = data["asks"];
        const rapidjson::Value &bids = data["bids"];
        mbp.seqNum = crypto::getCurrentTimeSeconds();
        if(data.HasMember("isFull")){//full depth 50
            mbp.prevSeqNum = 0;//假设是全量
        }
        else{
            mbp.prevSeqNum = 1;//假设是增量
        }
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
        if(mbp.prevSeqNum == 0){//req
            update_mbp(topic, mbp);
            if(mbpCache.count(topic) > 0){
                for(auto &m : mbpCache[topic]){
                    if(m.seqNum > mbp.seqNum){
                        update_mbp(topic, m);
                    }
                }
                mbpCache.erase(topic);
            }
            return;
        }
        if (lobDict.count(topic) > 0) {
            update_mbp(topic, mbp);
            if (lobDict[topic]->isReady) {
                long tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
                md::CryptoMarketData cmd;
                cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
                cmd.header.marketTypeEnum = md::DEPTH5;
                cmd.header.instTypeEnum = InstType_SPOT;
                strcpy(cmd.header.instId, info.instId);
//                if(mdMsg.header.subMarketTypeEnum == SMT_SPOT_MD){
//                    cmd.header.instTypeEnum = InstType_SPOT;
//                }
//                else if(mdMsg.header.subMarketTypeEnum == SMT_USDT_SWAP){
//                    cmd.header.instTypeEnum = InstType_USDT_SWAP;
//                }
//                else if(mdMsg.header.subMarketTypeEnum == SMT_USD_SWAP){
//                    cmd.header.instTypeEnum = InstType_C_SWAP;
//                }
//                else if(mdMsg.header.subMarketTypeEnum == SMT_USDT_FUTURES){
//                    cmd.header.instTypeEnum = InstType_USDT_FUTURES;
//                }
//                else if(mdMsg.header.subMarketTypeEnum == SMT_USD_FUTURES){
//                    cmd.header.instTypeEnum = InstType_C_FUTURES;
//                }
//                else{
//                    return;
//                }
                cmd.body.depth5.exchangeTypeEnum = cmd.header.exchangeTypeEnum ;
                cmd.body.depth5.instTypeEnum = cmd.header.instTypeEnum ;
                cmd.body.depth5.marketTypeEnum = cmd.header.marketTypeEnum ;
                strcpy(cmd.body.depth5.instId, info.instId);
                strcpy(cmd.body.depth5.base, info.base);
                strcpy(cmd.body.depth5.quote, info.quote);
//                strcpy(cmd.body.depth5.margin, info.margin);

                shared_ptr<LOB> lob = lobDict[topic];
                if (lob->asksMap.size() >= 5 && lob->bidsMap.size() >= 5) {
                    auto bidIter = lob->bidsMap.rbegin();
                    cmd.body.depth5.bp1 = stod(bidIter->first);
                    cmd.body.depth5.bv1 = stod(bidIter->second);
                    bidIter++;
                    cmd.body.depth5.bp2 = stod(bidIter->first);
                    cmd.body.depth5.bv2 = stod(bidIter->second);
                    bidIter++;
                    cmd.body.depth5.bp3 = stod(bidIter->first);
                    cmd.body.depth5.bv3 = stod(bidIter->second);
                    bidIter++;
                    cmd.body.depth5.bp4 = stod(bidIter->first);
                    cmd.body.depth5.bv4 = stod(bidIter->second);
                    bidIter++;
                    cmd.body.depth5.bp5 = stod(bidIter->first);
                    cmd.body.depth5.bv5 = stod(bidIter->second);
                    auto askIter = lob->asksMap.begin();
                    cmd.body.depth5.ap1 = stod(askIter->first);
                    cmd.body.depth5.av1 = stod(askIter->second);
                    askIter++;
                    cmd.body.depth5.ap2 = stod(askIter->first);
                    cmd.body.depth5.av2 = stod(askIter->second);
                    askIter++;
                    cmd.body.depth5.ap3 = stod(askIter->first);
                    cmd.body.depth5.av3 = stod(askIter->second);
                    askIter++;
                    cmd.body.depth5.ap4 = stod(askIter->first);
                    cmd.body.depth5.av4 = stod(askIter->second);
                    askIter++;
                    cmd.body.depth5.ap5 = stod(askIter->first);
                    cmd.body.depth5.av5 = stod(askIter->second);

                    cmd.body.depth5.tsNet = mdMsg.tsNet;
                    cmd.body.depth5.tsParse = crypto::getCurrentTime();
                    g_cmdQueue.push(cmd);
                }
#endif
                string strAsks, strBids;
                lobDict[topic]->get_asks_bids(strAsks, strBids);
                char longMdStr[2048 * 64];
                sprintf(longMdStr, DEPTH_Format,
                        exchIdStr.c_str(), instTypeStr.c_str(),
                        marketTypeStr.c_str(), info.instId,
                        strAsks.c_str(),
                        strBids.c_str(),
                        0l,
                        mdMsg.tsNet, tsParse
                );
                redis_cmd(mdMsg, key, longMdStr);
            }
        }
        else {
            mbpCache[topic].push_back(mbp);
        }
        return;
    }
    else if (mdMsg.header.marketTypeEnum == TRADES) {
        const rapidjson::Value &data = rawData["data"]["records"][0];
        const char *direction = strcmp(data[3].GetString(), "bid") ? "sell" : "buy";
        sprintf(mdStr, Trades_Format,
                exchIdStr.c_str(), instTypeStr.c_str(),
                marketTypeStr.c_str(), info.instId,
                data[4].GetString(),
                stod(data[1].GetString()),
                stod(data[2].GetString()),//TODO
                direction,
                stol(data[0].GetString()) * 1000,
                mdMsg.tsNet, crypto::getCurrentTime()
        );
    }
    else{
        LOG_ERROR("not support marketType:%s", marketTypeStr.c_str());
        return;
    }
    redis_cmd(mdMsg,key,mdStr);
}

void md::XTUnit::save_swap_md(const MDMsg &mdMsg){
    rapidjson::Document d;
    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg.body.data.c_str());
    if(!rawData.IsObject()){
//        LOG_ERROR("msg :%s is not a json object", mdMsg.body.data.c_str());
        return;
    }
    string ch;
    if(!(rawData.HasMember("channel") && rawData.HasMember("data"))) {
        LOG_ERROR("%s.%s.%s useless data: %s",exchIdStr.c_str(), instTypeStr.c_str(),
                  marketTypeStr.c_str(), mdMsg.body.data.c_str());
        return;
    }
    char key[64]={0};
    char mdStr[2048];
    string channel = rawData["channel"].GetString();
    if (!(strstr(channel.c_str(), "push.deal") || strstr(channel.c_str(), "push.deep"))) {
        return;
    }
    string originInstId = crypto::to_upper(rawData["data"]["s"].GetString());
    InstrumentInfo info;
    if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                originInstId.c_str(), info) == false){
        LOG_ERROR("not found %s info in smc", originInstId.c_str());
        return;
    }
    MDMsg &msgLocal = const_cast<MDMsg &>(mdMsg);
    if (!strcmp(channel.c_str(), "push.deal")) {
        msgLocal.header.marketTypeEnum = TRADES;
        sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
            "TRADES", info.instId);
        const rapidjson::Value &data = rawData["data"];
        const char *direction = strcmp(data["m"].GetString(), "BID") ? "sell" : "buy";
        sprintf(mdStr, Trades_Format,
                exchIdStr.c_str(), instTypeStr.c_str(),
                marketTypeStr.c_str(), info.instId,
                "0",
                stod(data["p"].GetString()),
                stod(data["a"].GetString()),//TODO
                direction,
                stol(data["t"].GetString()) * 1000,
                mdMsg.tsNet, crypto::getCurrentTime()
        );
    }
    else if (!strcmp(channel.c_str(), "push.deep.full")) {
        sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
            "MBP", info.instId);
        string topic = key;
        MBP mbp;
        const rapidjson::Value &data = rawData["data"];
        const rapidjson::Value &asks = data["a"];
        const rapidjson::Value &bids = data["b"];
        mbp.seqNum = crypto::getCurrentTimeSeconds();
        mbp.prevSeqNum = 0;//假设是全量
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
        if(mbp.prevSeqNum == 0){//req
            update_mbp(topic, mbp);
            if(mbpCache.count(topic) > 0){
                for(auto &m : mbpCache[topic]){
                    if(m.seqNum > mbp.seqNum){
                        update_mbp(topic, m);
                    }
                }
                mbpCache.erase(topic);
            }
            return;
        }
    }
    else if (!strcmp(channel.c_str(), "push.deep")) {
        msgLocal.header.marketTypeEnum = MBPType;
        sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
            "MBP", info.instId);
        string topic = key;
        MBP mbp;
        const rapidjson::Value &data = rawData["data"];
        mbp.seqNum = stol(data["id"].GetString());
        mbp.prevSeqNum = 1;
        DepthPair depth;
        depth.price = data["p"].GetString();
        depth.size = data["q"].GetString();
        //买
        if (stod(data["ba"].GetString()) == 1) {
            mbp.bids.push_back(depth);
        }
        else {
            mbp.asks.push_back(depth);
        }
        if (lobDict.count(topic) > 0) {
            update_mbp(topic, mbp);
            if (lobDict[topic]->isReady) {
                long tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
                md::CryptoMarketData cmd;
                cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
                cmd.header.marketTypeEnum = md::DEPTH5;
                strcpy(cmd.header.instId, info.instId);
//                cmd.header.instTypeEnum = InstType_SPOT;
                if(mdMsg.header.subMarketTypeEnum == SMT_SPOT_MD){
                    cmd.header.instTypeEnum = InstType_SPOT;
                }
                else if(mdMsg.header.subMarketTypeEnum == SMT_USDT_SWAP){
                    cmd.header.instTypeEnum = InstType_USDT_SWAP;
                }
                else if(mdMsg.header.subMarketTypeEnum == SMT_USD_SWAP){
                    cmd.header.instTypeEnum = InstType_C_SWAP;
                }
                else if(mdMsg.header.subMarketTypeEnum == SMT_USDT_FUTURES){
                    cmd.header.instTypeEnum = InstType_USDT_FUTURES;
                }
                else if(mdMsg.header.subMarketTypeEnum == SMT_USD_FUTURES){
                    cmd.header.instTypeEnum = InstType_C_FUTURES;
                }
                else{
                    return;
                }
                cmd.body.depth5.exchangeTypeEnum = cmd.header.exchangeTypeEnum ;
                cmd.body.depth5.instTypeEnum = cmd.header.instTypeEnum ;
                cmd.body.depth5.marketTypeEnum = cmd.header.marketTypeEnum ;
                strcpy(cmd.body.depth5.instId, info.instId);
                strcpy(cmd.body.depth5.base, info.base);
                strcpy(cmd.body.depth5.quote, info.quote);
                strcpy(cmd.body.depth5.margin, info.margin);

                shared_ptr<LOB> lob = lobDict[topic];
                if (lob->asksMap.size() >= 5 && lob->bidsMap.size() >= 5) {
                    auto bidIter = lob->bidsMap.rbegin();
                    cmd.body.depth5.bp1 = stod(bidIter->first);
                    cmd.body.depth5.bv1 = stod(bidIter->second);
                    bidIter++;
                    cmd.body.depth5.bp2 = stod(bidIter->first);
                    cmd.body.depth5.bv2 = stod(bidIter->second);
                    bidIter++;
                    cmd.body.depth5.bp3 = stod(bidIter->first);
                    cmd.body.depth5.bv3 = stod(bidIter->second);
                    bidIter++;
                    cmd.body.depth5.bp4 = stod(bidIter->first);
                    cmd.body.depth5.bv4 = stod(bidIter->second);
                    bidIter++;
                    cmd.body.depth5.bp5 = stod(bidIter->first);
                    cmd.body.depth5.bv5 = stod(bidIter->second);
                    auto askIter = lob->asksMap.begin();
                    cmd.body.depth5.ap1 = stod(askIter->first);
                    cmd.body.depth5.av1 = stod(askIter->second);
                    askIter++;
                    cmd.body.depth5.ap2 = stod(askIter->first);
                    cmd.body.depth5.av2 = stod(askIter->second);
                    askIter++;
                    cmd.body.depth5.ap3 = stod(askIter->first);
                    cmd.body.depth5.av3 = stod(askIter->second);
                    askIter++;
                    cmd.body.depth5.ap4 = stod(askIter->first);
                    cmd.body.depth5.av4 = stod(askIter->second);
                    askIter++;
                    cmd.body.depth5.ap5 = stod(askIter->first);
                    cmd.body.depth5.av5 = stod(askIter->second);
                    cmd.body.depth5.ts = stol(data["t"].GetString()) * 1000;
                    cmd.body.depth5.tsNet = mdMsg.tsNet;
                    cmd.body.depth5.tsParse = crypto::getCurrentTime();
                    g_cmdQueue.push(cmd);
                }
#endif
                string strAsks, strBids;
                lobDict[topic]->get_asks_bids(strAsks, strBids);
                char longMdStr[2048 * 64];
                sprintf(longMdStr, DEPTH_Format,
                        exchIdStr.c_str(), instTypeStr.c_str(),
                        "MBP", info.instId,
                        strAsks.c_str(),
                        strBids.c_str(),
                        stol(data["t"].GetString()) * 1000,
                        mdMsg.tsNet, tsParse
                );
                redis_cmd(msgLocal, key, longMdStr);
            }
        }
        else {
            mbpCache[topic].push_back(mbp);
        }
        return;
    }
    else{
        LOG_ERROR("not support channel:%s", channel.c_str());
        return;
    }
    redis_cmd(msgLocal,key,mdStr);
}

void md::XTMarketClientV2::print_stat() {
    while(1){
        long received = 0, left = 0;
        for(auto &unit : tokenUnitVec){
            received += unit.tickCount;
            left     += unit.m_queue->get_left();
        }
        LOG_INFO("%s received:%ld, left:%ld",exchId, received, left);
        sleep(5);
    }
}

void md::XTMarketClientV2::construct() {
    m_spotRestBaseUrl = XT_WEBSOCKET_HOST_PUBLIC_SPOT;
    for(auto marketType : _marketTypeVec){
        for(auto instType : _instTypeVec){
            //存储有效的交易对
            vector<string> validInstIdVec;
            for(auto instId : _instIdVec){
                //先过滤一遍，筛选掉smc中不存在的交易对
                InstrumentInfo info;
                if(smc->get_instrument_info(exchId, instType.c_str(),
                                            instId.c_str(), info) == true){
                    string lowerOriginInstId = crypto::to_lower(info.originInstId);
                    validInstIdVec.push_back(info.originInstId);
                }
                else {
                    LOG_INFO("%s %s not support", instType.c_str(), instId.c_str());
                }
            }
            //再根据instType确定订阅类型
            //如果是spot，不需要区分，永续和交割需要进一步区分
            if(crypto::str_cmp(instType.c_str(), "SPOT") == true ){
                //现货没有费率信息
                //有效的交易对数量
                size_t validSize = validInstIdVec.size();
                //需要的token unit单元，若不能取整则单元需要多加一个
                size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                for(size_t us = 0; us < unitSize; us++){
                    XTUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
                                MarketTypeStr2EnumMap[marketType], _ip, _port, _passwd};
                    unit.mbpFlag = crypto::has_str(marketType, "MBP") ? true : false;
                    size_t startValidNum = tokenLot * (us);
                    size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                    for(size_t i = startValidNum;i < endValidNum; i++){
                        //将需要订阅的交易对，塞到struct
                        unit.subStrVec.push_back(validInstIdVec[i]);
                    }
                    unit.subMarketTypeEnum = SMT_SPOT_MD;
                    tokenUnitVec.push_back(unit);
                }
            }
            else if(crypto::str_cmp(instType.c_str(), "SWAP") ){

                vector<string> coinStrVec, notCoinStrVec;
                for(string inst : validInstIdVec){
                    InstrumentInfo info;
                    if(smc->get_instrument_info(exchId, instType.c_str(),
                                                inst.c_str(), info) == true){
                        if(crypto::str_cmp(info.quote, "USD")){
                            //币本位
                            coinStrVec.push_back(inst);
                        }
                        else{
                            //usdt或者busd本位
                            notCoinStrVec.push_back(inst);
                        }
                    }
                    else{
                        //这里不会执行到,因为上面已经筛选过了
                        string msg("not found ");
                        msg.append(exchId).append(".") .append(instType).append(".")
                        .append(inst).append(" smc info");
                        cryptothrow(msg ,-1);
                    }
                }
                if(coinStrVec.size() > 0){
                    //有效的交易对数量
                    size_t validSize = coinStrVec.size();
                    //需要的token unit单元数量，若不能取整则单元需要多加一个
                    size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                    for(size_t us = 0; us < unitSize; us++){
                        XTUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
                                            MarketTypeStr2EnumMap[marketType], _ip, _port, _passwd};
                        size_t startValidNum = tokenLot * (us);
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        unit.mbpFlag = crypto::has_str(marketType, "MBP") ? true : false;
                        for(size_t i = startValidNum;i < endValidNum; i++){
                            //将需要订阅的交易对，塞到struct
                            unit.subStrVec.push_back(coinStrVec[i]);
                        }
                        if(crypto::str_cmp(instType.c_str(), "SWAP")){
                            unit.subMarketTypeEnum = SMT_USD_SWAP;
                        }
                        else{
                            unit.subMarketTypeEnum = SMT_USD_FUTURES;
                        }
                        tokenUnitVec.push_back(unit);
                    }
                }
                if(notCoinStrVec.size() > 0){
                    //有效的交易对数量
                    size_t validSize = notCoinStrVec.size();
                    //需要的token unit单元数量，若不能取整则单元需要多加一个
                    size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                    for(size_t us = 0; us < unitSize; us++){
                        XTUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
                                            MarketTypeStr2EnumMap[marketType], _ip, _port, _passwd};
                        size_t startValidNum = tokenLot * (us);
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        unit.mbpFlag = crypto::has_str(marketType, "MBP") ? true : false;
                        for(size_t i = startValidNum;i < endValidNum; i++){
                            //将需要订阅的交易对，塞到struct
                            unit.subStrVec.push_back(notCoinStrVec[i]);
                        }
                        //
                        if(crypto::str_cmp(instType.c_str(), "SWAP")){
                            unit.subMarketTypeEnum = SMT_USDT_SWAP;
                        }
                        else{
                            unit.subMarketTypeEnum = SMT_USDT_FUTURES;
                        }
                        tokenUnitVec.push_back(unit);
                    }
                }
            }
        }
    }
}

void md::XTMarketClientV2::start(){
    bool mbpInst = false;
    construct();
    for(XTUnit &unit: tokenUnitVec){
        if (unit.mbpFlag) {
            for (auto symbol : unit.subStrVec) {
                if (unit.instTypeEnum == SPOT) {
                    reqInstIdDict[0][symbol] = &unit;
                }
                else if (unit.instTypeEnum == SWAP) {
                    reqInstIdDict[1][symbol] = &unit;
                }
                else if (unit.instTypeEnum == FUTURES) {
                    reqInstIdDict[2][symbol] = &unit;
                }
                else
                    continue;
                mbpInst = true;
            }
        }
        //共用一个smc即可
        unit.smc = smc;
        unit.construct();
        if(unit.subValueVec.size() > 0){
            unit.start();
            usleep(10000);
        }
        else{
            LOG_INFO("no need to start: %s",unit.getString().c_str());
        }
    }

    if(mbpInst){
        std::thread spotRestThread(&md::XTMarketClientV2::req_spot_mbp, this);
        spotRestThread.detach();
    }

    std::thread printStatThread(&md::XTMarketClientV2::print_stat, this);
    printStatThread.detach();
}

void md::XTMarketClientV2::req_spot_mbp() {
    int sleepSeconds = 120;
    //防止websocket还未建立连接
    std::this_thread::sleep_for(std::chrono::seconds(20));
    while(1){
        try{
	    LOG_INFO("mbp req circle");
            //索引0 spot 1 swap 2 futures
            for (int i = 0; i < 3; i++) {
                for(auto pair : reqInstIdDict[i]){
                    if (pair.second->m_IsConnected) {
                        string symbol = crypto::to_lower(pair.first);
                        json::value value;
                        if (i == 0) {
                            value["channel"] = json::value::string("ex_depth_data");
                            value["market"] = json::value::string(symbol.c_str());
                            value["event"] = json::value::string("addChannel");
                        }
                        //futures每三十次增量服务器主推一次全量无需处理
                        else if (i == 1 || i == 2) {
                            continue;
                        }
                        websocket_outgoing_message outMsg;
                        outMsg.set_utf8_message(value.serialize().c_str());
                        //LOG_INFO("send success: %s", value.serialize().c_str());
                        pair.second->wsClient.send(outMsg);
                    }
                    else {
                        LOG_INFO("ws client not connected");
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds(150));//数据请求（req）限频规则 单个连接每两次请求不能小于100ms。
                }
            }
        }
        catch (std::exception& ex)
        {
            LOG_ERROR("Exception: %s",ex.what());
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
	LOG_INFO("mbp req end");
        std::this_thread::sleep_for(std::chrono::seconds(sleepSeconds));
    }
}
