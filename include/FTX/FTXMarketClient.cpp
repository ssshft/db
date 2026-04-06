#include "FTXMarketClient.h"

void md::FTXUnit::sub_websocket(){
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
        std::this_thread::sleep_for(std::chrono::milliseconds(350));
        LOG_INFO("DB will subscrible data to ftx now");
        for(auto &subValue: subValueVec){
            websocket_outgoing_message outMsg;
            outMsg.set_utf8_message(subValue.serialize().c_str());
            wsClient.send(outMsg).wait();
            LOG_INFO("%s send success: %s",exchIdStr.c_str(), subValue.serialize().c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
        }
        LOG_INFO("%s send %lu sub values to %s",exchIdStr.c_str(),subValueVec.size(), m_wsBaseUrl.c_str());

        m_IsConnected = true;
        isFinished = true;
    }
    catch(std::exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s,%s,%s,%s,%d,%s",exchIdStr.c_str(),instTypeStr.c_str(),
                  marketTypeStr.c_str(), __FUNCTION__ , __LINE__, e.what());
    }
}

void md::FTXUnit::ping(){
    if(m_IsConnected){
        websocket_outgoing_message outMsg;
        json::value pingSubValue;
        pingSubValue["op"] = json::value::string("ping");
        outMsg.set_utf8_message(pingSubValue.serialize().c_str());
        wsClient.send(outMsg).wait();
    }
}

#if 0
void md::FTXUnit::sub_websocket(){
    try{
        wsClient.close();
        uri_builder builder(m_wsBaseUrl);
        wsClient.connect(builder.to_string())
        .then([&]() {
            if(hasBinded == false){
                std::function<void (const websocket_incoming_message &msg)> f;
                f = std::bind(&MarketDataBaseStruct::on_websocket_msg, this, placeholders::_1);
                wsClient.set_message_handler(f);
                std::function<void (websocket_close_status close_status,
                                    const utility::string_t& reason, const std::error_code& error)> c;
                c =  std::bind(&MarketDataBaseStruct::on_close_msg,this
                        ,placeholders::_1,placeholders::_2,placeholders::_3);
                wsClient.set_close_handler(c);
            }
            hasBinded = true;
        }).wait();
        std::this_thread::sleep_for(std::chrono::milliseconds(350));
        for(auto &subValue: subValueVec){
            websocket_outgoing_message outMsg;
            outMsg.set_utf8_message(subValue.serialize().c_str());
            wsClient.send(outMsg).wait();
            LOG_INFO("%s send success: %s",exchIdStr.c_str(), subValue.serialize().c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
        }
        LOG_INFO("%s send %lu sub values to %s",exchIdStr.c_str(),subValueVec.size(), m_wsBaseUrl.c_str());
        m_IsConnected = true;
        isFinished = true;
    }
    catch(std::exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s,%s,%s,%s,%d,%s",exchIdStr.c_str(),instTypeStr.c_str(),
                marketTypeStr.c_str(), __FUNCTION__ , __LINE__, e.what());
    }
}

void md::FTXUnit::ping(){
    if(m_IsConnected){
        websocket_outgoing_message outMsg;
        json::value pingSubValue;
        pingSubValue["op"] = json::value::string("ping");
        outMsg.set_utf8_message(pingSubValue.serialize().c_str());
        wsClient.send(outMsg).wait();
    }
}
#endif
void md::FTXUnit::pong(){

}

//https://microsoft.github.io/cpprestsdk/classweb_1_1websockets_1_1client_1_1websocket__incoming__message.html
void md::FTXUnit::on_websocket_msg(const websocket_incoming_message &in_msg){
    try{
        if (in_msg.message_type() == websocket_message_type::text_message){
            const string &s = in_msg.extract_string().get();
            HANDLE_TEXT_MSG(s)
            // in_msg.extract_string().then([&](const string &s){
            //     HANDLE_TEXT_MSG(s)
            // });
        }
//        else if(in_msg.message_type() == websocket_message_type::ping){
//            LOG_DEBUG("%s ping got will reply pong",exchIdStr.c_str());
//            pong();
//        }
//        else if(in_msg.message_type() == websocket_message_type::pong){
//            LOG_DEBUG("%s pong got will reply ping",exchIdStr.c_str());
////        ping();
//        }
        else if(in_msg.message_type() == websocket_message_type::close){
            LOG_DEBUG("%s close got ",exchIdStr.c_str());
            m_IsConnected = false;
        }
    }
    catch(std::exception &e){
        LOG_ERROR("%s",e.what());
    }
}

void md::FTXUnit::construct(){
//            cout << getString() << endl;
    LOG_INFO("%s", getString().c_str());
    exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
    //先构造 ws地址
    m_wsBaseUrl = FTX_WEBSOCKET_HOST_PUBLIC_SPOT;

    //开始构造订阅格式
    for(auto instId : subStrVec){
        string strOriginInstId = instId;
        json::value value;
        value["op"] = json::value::string("subscribe");
        if (marketTypeEnum == TRADES ) {
            value["channel"] = json::value::string("trades");
            value["market"] = json::value::string(strOriginInstId.c_str());
        }
        else if (marketTypeEnum == MBPType) {
            value["channel"] = json::value::string("orderbook");
            value["market"] = json::value::string(strOriginInstId.c_str());
        }
        else{
            LOG_ERROR("not implemented this markettype: %s", marketTypeStr.c_str());
        }
        subValueVec.push_back(value);
    }
//            cout << subValue.serialize() << endl;
}

void md::FTXMarketClientV2::print_stat() {
    while(1){
        long received = 0, left = 0;
        for(auto unit : tokenUnitVec){
            received += unit.tickCount;
            left     += unit.m_queue->get_left();
        }
        LOG_INFO("%s received:%ld, left:%ld",exchId, received, left);
        sleep(5);
    }
}

void md::FTXMarketClientV2::construct() {
//    m_spotRestBaseUrl = FTX_REST_HOST_PUBLIC_SPOT;
    for(auto marketType : _marketTypeVec){
        for(auto instType : _instTypeVec){
//            cout << instType << endl;
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
            }

            if(crypto::has_str(instType.c_str(), "SPOT") == true ){
                //有效的交易对数量
                size_t validSize = validInstIdVec.size();
                //需要的token unit单元，若不能取整则单元需要多加一个
                size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                for(size_t us = 0; us < unitSize; us++){
                    FTXUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
                                    MarketTypeStr2EnumMap[marketType], _ip, _port, _passwd};
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
            else if(crypto::has_str(instType.c_str(), "SWAP")
            || crypto::has_str(instType.c_str(), "FUTURES") ){
                //这里需要区分币本位和其他本位（目前主要是USDT和BUSD本位）
                vector<string> coinStrVec, notCoinStrVec;
                for(string inst : validInstIdVec){
                    InstrumentInfo info;
                    if(smc->get_instrument_info(exchId, instType.c_str(), inst.c_str(), info) == true){
                        if(crypto::str_cmp(info.quote, "USD")){
                            //币本位
                            coinStrVec.push_back(inst);
                        }
                        else{
                            //usdt
                            notCoinStrVec.push_back(inst);
                        }
                    }
                    else{
                        //这里不会执行到,因为上面已经筛选过了
                        LOG_ERROR("not found %s.%s.%s's smc info", exchId, instType.c_str(), inst.c_str());
                    }
                }
                if(coinStrVec.size() > 0){
                    //有效的交易对数量
                    size_t validSize = coinStrVec.size();
                    //需要的token unit单元数量，若不能取整则单元需要多加一个
                    size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
//                    cout << unitSize << endl;
                    for(size_t us = 0; us < unitSize; us++){
                        FTXUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
                                            MarketTypeStr2EnumMap[marketType], _ip, _port, _passwd};
                        size_t startValidNum = tokenLot * (us);
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        for(size_t i = startValidNum;i < endValidNum; i++){
                            //将需要订阅的交易对，塞到struct
                            unit.subStrVec.push_back(coinStrVec[i]);
                        }
                        if(crypto::has_str(instType.c_str(), "SWAP")){
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
                        FTXUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
                                            MarketTypeStr2EnumMap[marketType], _ip, _port, _passwd};
                        size_t startValidNum = tokenLot * (us);
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        for(size_t i = startValidNum;i < endValidNum; i++){
                            //将需要订阅的交易对，塞到struct
                            unit.subStrVec.push_back(notCoinStrVec[i]);
                        }
                        //
                        if(crypto::has_str(instType.c_str(), "SWAP")){
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

void md::FTXMarketClientV2::start(){
    //这里启动一个线程以便不阻塞主线程
    std::thread subThread([&](){
        construct();
        for(auto &unit: tokenUnitVec){
            //共用一个smc即可
            unit.smc = smc;
            unit.construct();
            if(unit.subValueVec.size() > 0){
                unit.start();
                while(!unit.isFinished){
                    std::this_thread::sleep_for(std::chrono::milliseconds(110));
                }
            }
            else{
                LOG_INFO("no need to start: %s",unit.getString().c_str());
            }
        }
//        if(reqInstIdVec.size() > 0){
//            std::thread spotRestThread(&md::FTXMarketClientV2::req_spot_mbp, this);
//            spotRestThread.detach();
//        }
    });
    subThread.detach();

    std::thread printStatThread(&md::FTXMarketClientV2::print_stat, this);
    printStatThread.detach();
}

#if 0
void md::FTXMarketClientV2::req_spot_mbp() {
    int sleepSeconds = 45;
    while(1){
        try{

                std::this_thread::sleep_for(std::chrono::milliseconds(350));
            }
        }
        catch (std::exception& ex)
        {
            LOG_ERROR("Exception: %s",ex.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        std::this_thread::sleep_for(std::chrono::seconds(sleepSeconds));
    }
}
#endif

//处理消息 解析json并发送给redis或共享内存
void md::FTXUnit::save_md_string(const MDMsg &mdMsg) {
    rapidjson::Document d;
    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg.body.data.c_str());
    if (d.HasParseError() || !rawData.IsObject() || !(rawData.HasMember("market") && rawData.HasMember("data"))) {
        return;
    }

    string originInstId = rawData["market"].GetString();
    InstrumentInfo info;
    if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                originInstId.c_str(), info) == false){
        LOG_ERROR("not found %s info in smc", originInstId.c_str());
        return;
    }
    char key[64] = {0};
    char mdStr[2048] = {0};
    sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
            marketTypeStr.c_str(), info.instId);
    string channel = rawData["channel"].GetString();

    const rapidjson::Value &data = rawData["data"];
    if(crypto::str_cmp(channel.c_str(), "trades")){
        sprintf(mdStr, Trades_Format,
                exchIdStr.c_str(), instTypeStr.c_str(),
                marketTypeStr.c_str(), info.instId,
                data[0]["id"].GetString(),
                stod(data[0]["price"].GetString()),
                stod(data[0]["size"].GetString()),
                data[0]["side"].GetString(),
//                crypto::utcstr_2_long_long(data[0]["time"].GetString()),
                mdMsg.tsNet - 1000,
//                stol(data[0]["time"].GetString()) * 1000,
                mdMsg.tsNet, crypto::getCurrentTime()
        );
//        cout << mdStr << endl;
    }
    else if(crypto::str_cmp(channel.c_str(), "orderbook")){
        string type    = rawData["type"].GetString();
        if(crypto::str_cmp(type.c_str(), "partial")){//初始orderbook
            string topic = key;
//        topic.append(GateioExchangeId).append(".").append(instType).append(".MBP.").append(instId);
            MBP mbp;
            const rapidjson::Value &asks = data["asks"];
            const rapidjson::Value &bids = data["bids"];
            mbp.prevSeqNum = 0;//stol(data["update"].GetString());
            //TODO
//        mbp.seqNum = (long)(stod(data["time"].GetString())*1000*1000);
//        mbp.seqNum = stol(data["current"].GetString());
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
            update_mbp(topic, mbp);
            return;
        }
        else if(crypto::str_cmp(type.c_str(), "update")){//增量orderbook
            if(m_IsConnected == true){
                string topic = key;
                MBP mbp;
                const rapidjson::Value &asks = data["asks"];
                const rapidjson::Value &bids = data["bids"];
                long ts = (long)(stod(data["time"].GetString())*1000*1000);
                mbp.prevSeqNum = ts;//atoll(data["U"].GetString());
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
                if (lobDict.count(topic) > 0) {
                    update_mbp(topic, mbp);
                    if (lobDict[topic]->isReady) {
                        long tsParse = crypto::getCurrentTime();
                        string strAsks, strBids;
                        lobDict[topic]->get_asks_bids(strAsks, strBids);
                        char longMdStr[2048 * 64];
                        sprintf(longMdStr, DEPTH_Format,
                                exchIdStr.c_str(), instTypeStr.c_str(),
                                marketTypeStr.c_str(), info.instId,
                                strAsks.c_str(),
                                strBids.c_str(),
                                ts,
                                mdMsg.tsNet, tsParse
                        );
//                        lobDict[topic]->print_depth(5);
                        if(lobDict[topic]->check_data() == true){
//                            m_IsConnected = false;
                            //先抹掉所有数据，再重新订阅
                            lobDict.erase(topic);
                            LOG_ERROR("%s, orderbook check failed, will resubscribe",key);
                            subscribe(originInstId);
                            return;
                        }
                        else{
                            redis_cmd(mdMsg, key, longMdStr);
                        }
                    }
                }
                return;
            }
        }
        else{

        }
    }
    else{
        LOG_ERROR("not implemented now");
        return;
    }
    redis_cmd(mdMsg, key, mdStr);
}

void md::FTXUnit::update_mbp(const string &topic, MBP &mbp){
    if(mbp.prevSeqNum == 0){
        shared_ptr<LOB> newLob(new LOB);
        newLob->seqNum = mbp.seqNum;
        for(DepthPair a : mbp.asks){
            newLob->asksMap[a.price] = a.size;
        }
        for(DepthPair b : mbp.bids){
            newLob->bidsMap[b.price] = b.size;
        }
        newLob->isReady = true;
        lobDict[topic] = newLob;
    }
    else{
        if(lobDict.count(topic)){
            shared_ptr<LOB> lob = lobDict[topic];
            lob->seqNum = mbp.seqNum;
            for(DepthPair a : mbp.asks){
                string price = a.price;
                string size = a.size;
                if(crypto::str_equal_zero(size)){
                    lob->asksMap.erase(price);
                }
                else{
                    lob->asksMap[price] = size;
                }
            }
            for(DepthPair b : mbp.bids){
                string price = b.price;
                string size = b.size;
                if(crypto::str_equal_zero(size) == true){
                    lob->bidsMap.erase(price);
                }
                else{
                    lob->bidsMap[price] = size;
                }
            }
        }
        else{
//            DEBUGLINE
        }
    }
}

void md::FTXUnit::monitor_ws(){
    while(1){
        try{
            //防止所有线程在同一时刻发送请求
            sub_websocket();
            int count = 1;
            while(m_IsConnected){
                if(!m_IsConnected){
                    LOG_ERROR("%s.%s.%s ws disconnected, will reconnect now",exchIdStr.c_str(),
                              instTypeStr.c_str(), marketTypeStr.c_str());
                    break;
                }
                else{
                    if(count % 15 == 0){
                        LOG_DEBUG("%s.%s.%s ws is conntected now, will send ping to it!", exchIdStr.c_str(),
                                  instTypeStr.c_str(), marketTypeStr.c_str());
                        ping();
                    }
//                    if(count % 30 == 0){
//                        subscribe();
//                    }
                }
                sleep(1);
                count++;
            }
        }
        catch (exception &e) {
            m_IsConnected = false;
            LOG_ERROR("%s.%s.%s exception %s happened",exchIdStr.c_str(),
                      instTypeStr.c_str(), marketTypeStr.c_str(), e.what());

        }
        sleep(5);
    }
}

void md::FTXUnit::subscribe(string &originInstId){
    if(m_IsConnected){
        LOG_ERROR("DB will resubscrible %s data to ftx now", originInstId.c_str());
        for(auto &subValue: subValueVec){
            string subStr = subValue.serialize();
            if(crypto::has_str(subStr.c_str(), originInstId.c_str())){
                json::value unsub;
                unsub["op"] = json::value::string("unsubscribe");
                unsub["channel"] = subValue["channel"];
                unsub["market"] = subValue["market"];
                websocket_outgoing_message outUnSubMsg;
                outUnSubMsg.set_utf8_message(unsub.serialize().c_str());
                wsClient.send(outUnSubMsg).wait();
                LOG_INFO("%s send unsubscribe %s success",exchIdStr.c_str(), unsub.serialize().c_str());
                websocket_outgoing_message outMsg;
                outMsg.set_utf8_message(subValue.serialize().c_str());
                wsClient.send(outMsg).wait();
                LOG_INFO("%s send subscribe %s success",exchIdStr.c_str(), subValue.serialize().c_str());
            }
        }
//        LOG_INFO("%s send %lu sub values to %s",exchIdStr.c_str(),subValueVec.size(), m_wsBaseUrl.c_str());
    }
}