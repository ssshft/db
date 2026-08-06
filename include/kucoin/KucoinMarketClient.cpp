#include "KucoinMarketClient.h"

void md::KucoinUnit::sub_websocket(){
    try{
        http_client restclient(m_restBaseUrl);
        http_request request(methods::POST);
        request.set_request_uri(m_wsTokenUrl);
        restclient.request(request)
        .then([](http_response response) -> pplx::task<json::value> {
            if(response.status_code() == status_codes::OK) {
                return response.extract_json();
            }
            LOG_DEBUG("Retrieve Oauth access token response: '%s' ", response.to_string().c_str());

            return pplx::task_from_result(json::value());
        })
        .then([&](pplx::task<json::value> previousTask) {
            // get the JSON value from the task and display content from it
            try {
                json::value const &v = previousTask.get();
                if (v.has_field("code")) {
                    string code = v.at("code").as_string();
                    if (code == "200000") {
                        if (v.has_field("data")) {
                            json::value const & result = v.at("data");
                            if (result.has_field("token")) {
                                m_token = result.at("token").as_string();
                            }
                            if (result.has_field("instanceServers")) {
                                json::value const & instanceServers = result.at("instanceServers");
                                if (instanceServers.is_array()) {
                                    auto vc = instanceServers.as_array();
                                    for(auto& a : vc) {
                                        m_wsBaseUrl = a.at("endpoint").as_string();
                                        pingInterval = a.at("pingInterval").as_integer()* 0.001;
                                    }
                                }
                            }
                        }
                    }
                    else {
                        LOG_DEBUG("Get websocket endpoint error, code: %s", code.c_str());
                    }
                }
            }
            catch (http_exception const & e) {
                printf("Error exception:%s\n", e.what());
            }
        })
        .wait();

//        cout << "ws endpoint: " << m_wsBaseUrl << ", token:" << m_token << endl;
        LOG_INFO("start to sub_websocket to %s",m_wsBaseUrl.c_str() );
        wsClient.close();
        uri_builder builder(m_wsBaseUrl);
        builder.append_query("token", m_token.c_str());
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
        LOG_INFO("DB will subscrible data to %s now", exchIdStr.c_str());

//        for(auto subValue : subValueVec){
        subValue["id"] = json::value::string(to_string(subscribeIndex++));
        websocket_outgoing_message outMsg;
        outMsg.set_utf8_message(subValue.serialize().c_str());
        wsClient.send(outMsg).wait();
        LOG_INFO("%s send success: %s",exchIdStr.c_str(), subValue.serialize().c_str());
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
//        }

        m_IsConnected = true;
    }
    catch(std::exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s.%s.%s,%s",exchIdStr.c_str(),instTypeStr.c_str(),
                  marketTypeStr.c_str(), e.what());
    }
}

void md::KucoinUnit::ping(){
    if(m_IsConnected){
        websocket_outgoing_message outMsg;
        outMsg.set_ping_message();
        wsClient.send(outMsg).wait();
    }
}

void md::KucoinUnit::pong(){

}

//https://microsoft.github.io/cpprestsdk/classweb_1_1websockets_1_1client_1_1websocket__incoming__message.html
void md::KucoinUnit::on_websocket_msg(const websocket_incoming_message &in_msg){
    try{
        if (in_msg.message_type() == websocket_message_type::text_message){
            const string &s = in_msg.extract_string().get();
            HANDLE_TEXT_MSG(s)
            // in_msg.extract_string().then([&](const string &s){
            //     HANDLE_TEXT_MSG(s)
            // });
        }
        else if(in_msg.message_type() == websocket_message_type::close){
            LOG_DEBUG("%s close got ",exchIdStr.c_str());
            m_IsConnected = false;
        }
        else{

        }
    }
    catch(std::exception &e){
        LOG_ERROR("%s",e.what());
    }
}

void md::KucoinUnit::construct(){
//            cout << getString() << endl;
    LOG_INFO("%s", getString().c_str());
    exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];

    //开始构造订阅格式
    if (marketTypeEnum == TRADES ) {
        string topic = "/market/match:";
        if(instTypeEnum == SWAP || instTypeEnum == FUTURES){
            topic = "/contractMarket/execution:";
        }
        for(size_t i = 0; i < subStrVec.size(); i++){
            if(i == subStrVec.size() - 1){
                topic.append(subStrVec[i]);
            }
            else{
                topic.append(subStrVec[i]).append(",");
            }
        }
        subValue["topic"] = json::value::string(topic);
    }
    else if (marketTypeEnum == MBPType) {
        string topic = "/spotMarket/level2Depth50:";
        if(instTypeEnum == SWAP || instTypeEnum == FUTURES){
            topic = "/contractMarket/level2Depth50:";
        }
        for(size_t i = 0; i < subStrVec.size(); i++){
            if(i == subStrVec.size() - 1){
                topic.append(subStrVec[i]);
            }
            else{
                topic.append(subStrVec[i]).append(",");
            }
        }
        subValue["topic"] = json::value::string(topic);
    }
    else if(marketTypeEnum == DEPTH1){
        string topic = "/market/ticker:";
        if(instTypeEnum == SWAP || instTypeEnum == FUTURES){
            topic = "/contractMarket/ticker:";
        }
        for(size_t i = 0; i < subStrVec.size(); i++){
            if(i == subStrVec.size() - 1){
                topic.append(subStrVec[i]);
            }
            else{
                topic.append(subStrVec[i]).append(",");
            }
        }
        subValue["topic"] = json::value::string(topic);
    }
    else if(marketTypeEnum == DEPTH5){
        string topic = "/spotMarket/level2Depth5:";
        if(instTypeEnum == SWAP || instTypeEnum == FUTURES){
            topic = "/contractMarket/level2Depth5:";
        }
        for(size_t i = 0; i < subStrVec.size(); i++){
            if(i == subStrVec.size() - 1){
                topic.append(subStrVec[i]);
            }
            else{
                topic.append(subStrVec[i]).append(",");
            }
        }
        subValue["topic"] = json::value::string(topic);
    }
    else if(marketTypeEnum == KLINE_1m){
        if(instTypeEnum == SPOT){
            string topic = "/market/candles:";
            for(size_t i = 0; i < subStrVec.size(); i++){
                if(i == subStrVec.size() - 1){
                    topic.append(subStrVec[i]).append("_1min");
                }
                else{
                    topic.append(subStrVec[i]).append("_1min").append(",");
                }
            }
            subValue["topic"] = json::value::string(topic);
        }
        else{// TODO 期货没有推送K线
            // DEBUGLINE
        }
    }
    else{
        LOG_ERROR("not implemented this markettype: %s", marketTypeStr.c_str());
    }

    if(instTypeEnum == SPOT){
        m_restBaseUrl = KUCOIN_REST_HOST_PUBLIC_SPOT;
    }
    else if(instTypeEnum == SWAP || instTypeEnum == FUTURES){
        m_restBaseUrl = KUCOIN_REST_HOST_PUBLIC_FUTURES;
    }
    subValue["type"] = json::value::string("subscribe");

    subValue["privateChannel"] = json::value::boolean(false);
    subValue["response"] = json::value::boolean(false);

//    cout << subValue.serialize() << endl;
}

void md::KucoinMarketClient::print_stat() {
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

void md::KucoinMarketClient::construct() {
//    m_spotRestBaseUrl = Kucoin_REST_HOST_PUBLIC_SPOT;
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
                    KucoinUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
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
                        KucoinUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
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
                        KucoinUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
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

void md::KucoinMarketClient::start(){
    //这里启动一个线程以便不阻塞主线程
    std::thread subThread([&](){
        construct();
        for(auto &unit: tokenUnitVec){
            //共用一个smc即可
            unit.smc = smc;
            unit.construct();
//            if(unit.subValueVec.size() > 0){
            if(unit.subValue.has_field("topic")){
                unit.start();
            }
            else{
                LOG_INFO("no need to start: %s",unit.getString().c_str());
            }
        }
    });
    subThread.detach();

    std::thread printStatThread(&md::KucoinMarketClient::print_stat, this);
    printStatThread.detach();
}

//处理消息 解析json并发送给redis或共享内存
void md::KucoinUnit::save_md_string(const MDMsg &mdMsg) {
    rapidjson::Document d;
    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg.body.data.c_str());
    if (d.HasParseError() || !(rawData.HasMember("topic") && rawData.HasMember("data"))) {
        return;
    }
    char key[64] = {0};
    char mdStr[2048] = {0};
    const rapidjson::Value &data = rawData["data"];
    // cout << mdMsg.body.data << endl;
    if(mdMsg.header.instTypeEnum == SPOT){
        if(marketTypeEnum == TRADES){
            string originInstId = rawData["data"]["symbol"].GetString();
            InstrumentInfo info;
            if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                        originInstId.c_str(), info) == false){
                LOG_ERROR("not found %s info in smc", originInstId.c_str());
                return;
            }
            sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
                    marketTypeStr.c_str(), info.instId);
            double px = stod(data["price"].GetString());
            double sz = stod(data["size"].GetString());
            string side = data["side"].GetString();
            string tradeId = data["sequence"].GetString();
            long ts = (long)(stold(data["time"].GetString()) * 0.001);
#ifdef NEED_SHM
            md::CryptoMarketData cmd;
            memset(&cmd, 0, sizeof(md::CryptoMarketData));
            cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
            cmd.header.marketTypeEnum = md::TRADES;
            HANDLE_SUBTYPE(cmd)

            strcpy(cmd.header.instId, info.instId);
            cmd.body.trades.exchangeTypeEnum = cmd.header.exchangeTypeEnum ;
            cmd.body.trades.instTypeEnum = cmd.header.instTypeEnum ;
            cmd.body.trades.marketTypeEnum = cmd.header.marketTypeEnum ;

            strcpy(cmd.body.trades.instId, info.instId);
            strcpy(cmd.body.trades.base, info.base);
            strcpy(cmd.body.trades.quote, info.quote);
            strcpy(cmd.body.trades.margin, info.margin);
            strcpy(cmd.body.trades.tradeId, tradeId.c_str());
            cmd.body.trades.px = px;
            cmd.body.trades.sz = sz;
            strcpy(cmd.body.trades.side, side.c_str());
            cmd.body.trades.ts = ts;
            cmd.body.trades.tsNet = mdMsg.tsNet;
            cmd.body.trades.tsParse = crypto::getCurrentTime();
            g_cmdQueue.push(cmd);
#endif
            sprintf(mdStr, Trades_Format,
                exchIdStr.c_str(), instTypeStr.c_str(),
                marketTypeStr.c_str(), info.instId,
                tradeId.c_str() ,
                px,
                sz,
                side.c_str(),
                ts,
                mdMsg.tsNet, crypto::getCurrentTime()
            );
        }
        else if(marketTypeEnum == DEPTH1){
            // cout << mdMsg.body.data << endl;
            string topic = rawData["topic"].GetString();
            vector<string> topicSplit = crypto::split(topic.c_str(),':');
            // vector<string> topicSplit2 = crypto::split(topicSplit[1].c_str(),'_');
            string originInstId = topicSplit[1];
            InstrumentInfo info;
            if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                        originInstId.c_str(), info) == false){
                LOG_ERROR("not found %s info in smc", originInstId.c_str());
                return;
            }
            sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
                    marketTypeStr.c_str(), info.instId);

            long ts = (long)(stold(data["time"].GetString()) * 1000);
#ifdef NEED_SHM
            double bp1 = stod(data["bestBid"].GetString());
            double bv1 = stod(data["bestBidSize"].GetString());
            double ap1 = stod(data["bestAsk"].GetString());
            double av1 = stod(data["bestAskSize"].GetString());
            md::CryptoMarketData cmd;
            cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
            cmd.header.marketTypeEnum = md::DEPTH1;
            HANDLE_SUBTYPE(cmd)

            strcpy(cmd.header.instId, info.instId);
            cmd.body.depth1.exchangeTypeEnum = cmd.header.exchangeTypeEnum ;
            cmd.body.depth1.instTypeEnum = cmd.header.instTypeEnum ;
            cmd.body.depth1.marketTypeEnum = cmd.header.marketTypeEnum ;
            strcpy(cmd.body.depth1.instId, info.instId);
            strcpy(cmd.body.depth1.base, info.base);
            strcpy(cmd.body.depth1.quote, info.quote);
            strcpy(cmd.body.depth1.margin, info.margin);

            cmd.body.depth1.bp1 = bp1;
            cmd.body.depth1.bv1 = bv1;

            cmd.body.depth1.ap1 = ap1;
            cmd.body.depth1.av1 = av1;
            cmd.body.depth1.ts = ts;
            cmd.body.depth1.tsNet = mdMsg.tsNet;
            cmd.body.depth1.tsParse = crypto::getCurrentTime();
            g_cmdQueue.push(cmd);
#endif
            string asksStr,bidsStr;
            asksStr.append("[[").append(data["bestAsk"].GetString()).append(",")
                .append(data["bestAskSize"].GetString()).append("]]");

            bidsStr.append("[[").append(data["bestBid"].GetString()).append(",")
                .append(data["bestBidSize"].GetString()).append("]]");

            sprintf(mdStr, DEPTH_Format,
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                asksStr.c_str(),
                bidsStr.c_str(),
                ts,
                mdMsg.tsNet, crypto::getCurrentTime()
            );
        }
        else if(marketTypeEnum == MBPType || marketTypeEnum == DEPTH5){
            string topic = rawData["topic"].GetString();
            vector<string> topicSplit = crypto::split(topic.c_str(),':');
            // vector<string> topicSplit2 = crypto::split(topicSplit[1].c_str(),'_');
            string originInstId = topicSplit[1];
            InstrumentInfo info;
            if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                        originInstId.c_str(), info) == false){
                LOG_ERROR("not found %s info in smc", originInstId.c_str());
                return;
            }
            sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
                    marketTypeStr.c_str(), info.instId);
            string asksStr, bidsStr;
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
            long ts = stol(data["timestamp"].GetString()) * 1000;
            char longMdStr[2048];
            sprintf(longMdStr, DEPTH_Format,
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                asksStr.c_str(),
                bidsStr.c_str(),
                ts,
                mdMsg.tsNet, crypto::getCurrentTime()
            );
            redis_cmd(mdMsg, key, longMdStr);
            return;
        }
        else if(marketTypeEnum == KLINE_1m){
            long barTime = stol(data["candles"][0].GetString());
            auto now = crypto::getCurrentTime();
            bool isFinished = now * 0.000001 - barTime > 58.5 && now * 0.000001 - barTime < 61.5 ? true : false;
            if(!isFinished){
                return;
            }
            string originInstId = rawData["data"]["symbol"].GetString();
            InstrumentInfo info;
            if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                        originInstId.c_str(), info) == false){
                LOG_ERROR("not found %s info in smc", originInstId.c_str());
                return;
            }
            sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
                marketTypeStr.c_str(), info.instId);

            sprintf(mdStr, Bar_Format,
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                barTime * 1000 * 1000,
                stod(data["candles"][3].GetString()),//high
                stod(data["candles"][4].GetString()),//low
                stod(data["candles"][1].GetString()),//open
                stod(data["candles"][2].GetString()),//close
                0.0,
                stod(data["candles"][5].GetString()),
                stod(data["candles"][6].GetString()),
                0.0,
                0.0,
                0.0,
                0.0,
                0,
                isFinished == false ? "false" : "true",
                (long)(stod(data["time"].GetString()) * 0.001) ,
                mdMsg.tsNet, now
            );
        }
        else{
            return;
        }
    }
    else if(mdMsg.header.instTypeEnum == SWAP || mdMsg.header.instTypeEnum == FUTURES){
        if(marketTypeEnum == TRADES){
            string originInstId = rawData["data"]["symbol"].GetString();
            InstrumentInfo info;
            if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                        originInstId.c_str(), info) == false){
                LOG_ERROR("not found %s info in smc", originInstId.c_str());
                return;
            }
            sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
                    marketTypeStr.c_str(), info.instId);
            double px = stod(data["price"].GetString());
            double sz = stod(data["size"].GetString());
            string side = data["side"].GetString();
            string tradeId = data["sequence"].GetString();
            long ts = (long)(stold(data["ts"].GetString()) * 0.001);
#ifdef NEED_SHM
            md::CryptoMarketData cmd;
            memset(&cmd, 0, sizeof(md::CryptoMarketData));
            cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
            cmd.header.marketTypeEnum = md::TRADES;
            HANDLE_SUBTYPE(cmd)

            strcpy(cmd.header.instId, info.instId);
            cmd.body.trades.exchangeTypeEnum = cmd.header.exchangeTypeEnum ;
            cmd.body.trades.instTypeEnum = cmd.header.instTypeEnum ;
            cmd.body.trades.marketTypeEnum = cmd.header.marketTypeEnum ;

            strcpy(cmd.body.trades.instId, info.instId);
            strcpy(cmd.body.trades.base, info.base);
            strcpy(cmd.body.trades.quote, info.quote);
            strcpy(cmd.body.trades.margin, info.margin);
            strcpy(cmd.body.trades.tradeId, tradeId.c_str());
            cmd.body.trades.px = px;
            cmd.body.trades.sz = sz;
            strcpy(cmd.body.trades.side, side.c_str());
            cmd.body.trades.ts = ts;
            cmd.body.trades.tsNet = mdMsg.tsNet;
            cmd.body.trades.tsParse = crypto::getCurrentTime();
            g_cmdQueue.push(cmd);
#endif
            sprintf(mdStr, Trades_Format,
                exchIdStr.c_str(), instTypeStr.c_str(),
                marketTypeStr.c_str(), info.instId,
                tradeId.c_str(),
                px,
                sz,
                side.c_str(),
                ts,
                mdMsg.tsNet, crypto::getCurrentTime()
            );
        }
        else if(marketTypeEnum == DEPTH1){
            // cout << mdMsg.body.data << endl;
            string topic = rawData["topic"].GetString();
            vector<string> topicSplit = crypto::split(topic.c_str(),':');
            // vector<string> topicSplit2 = crypto::split(topicSplit[1].c_str(),'_');
            string originInstId = topicSplit[1];
            InstrumentInfo info;
            if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                        originInstId.c_str(), info) == false){
                LOG_ERROR("not found %s info in smc", originInstId.c_str());
                return;
            }
            sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
                    marketTypeStr.c_str(), info.instId);

            long ts = (long)(stold(data["ts"].GetString()) * 0.001);
#ifdef NEED_SHM
            double bp1 = stod(data["bestBidPrice"].GetString());
            double bv1 = stod(data["bestBidSize"].GetString());
            double ap1 = stod(data["bestAskPrice"].GetString());
            double av1 = stod(data["bestAskSize"].GetString());
            md::CryptoMarketData cmd;
            cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
            cmd.header.marketTypeEnum = md::DEPTH1;
            HANDLE_SUBTYPE(cmd)

            strcpy(cmd.header.instId, info.instId);
            cmd.body.depth1.exchangeTypeEnum = cmd.header.exchangeTypeEnum ;
            cmd.body.depth1.instTypeEnum = cmd.header.instTypeEnum ;
            cmd.body.depth1.marketTypeEnum = cmd.header.marketTypeEnum ;
            strcpy(cmd.body.depth1.instId, info.instId);
            strcpy(cmd.body.depth1.base, info.base);
            strcpy(cmd.body.depth1.quote, info.quote);
            strcpy(cmd.body.depth1.margin, info.margin);

            cmd.body.depth1.bp1 = bp1;
            cmd.body.depth1.bv1 = bv1;

            cmd.body.depth1.ap1 = ap1;
            cmd.body.depth1.av1 = av1;
            cmd.body.depth1.ts = ts;
            cmd.body.depth1.tsNet = mdMsg.tsNet;
            cmd.body.depth1.tsParse = crypto::getCurrentTime();
            g_cmdQueue.push(cmd);
#endif
            string asksStr,bidsStr;
            asksStr.append("[[").append(data["bestAskPrice"].GetString()).append(",")
                    .append(data["bestAskSize"].GetString()).append("]]");

            bidsStr.append("[[").append(data["bestBidPrice"].GetString()).append(",")
                            .append(data["bestBidSize"].GetString()).append("]]");

            sprintf(mdStr, DEPTH_Format,
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                asksStr.c_str(),
                bidsStr.c_str(),
                ts,
                mdMsg.tsNet, crypto::getCurrentTime()
            );
        }
        else if(marketTypeEnum == MBPType || marketTypeEnum == DEPTH5){
            string topic = rawData["topic"].GetString();
            vector<string> topicSplit = crypto::split(topic.c_str(),':');
            string originInstId = topicSplit[1];
            InstrumentInfo info;
            if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                        originInstId.c_str(), info) == false){
                LOG_ERROR("not found %s info in smc", originInstId.c_str());
                return;
            }
            sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
                    marketTypeStr.c_str(), info.instId);
            string asksStr, bidsStr;
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
            long ts = stol(data["ts"].GetString()) * 1000;
            char longMdStr[2048 ];
            sprintf(longMdStr, DEPTH_Format,
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                asksStr.c_str(),
                bidsStr.c_str(),
                ts,
                mdMsg.tsNet, crypto::getCurrentTime()
            );
            redis_cmd(mdMsg, key, longMdStr);
            return;
        }
        else{
            return;
        }
    }
    else{
        return;
    }
    redis_cmd(mdMsg, key, mdStr);
    return;
    # if 0
    if(marketTypeEnum == TRADES){
        long ts = (long)(stold(data["ts"].GetString()) * 0.001);
#ifdef NEED_SHM
        md::CryptoMarketData cmd;
        memset(&cmd, 0, sizeof(md::CryptoMarketData));
        cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
        cmd.header.marketTypeEnum = md::TRADES;
        HANDLE_SUBTYPE(cmd)

        strcpy(cmd.header.instId, info.instId);
        cmd.body.trades.exchangeTypeEnum = cmd.header.exchangeTypeEnum ;
        cmd.body.trades.instTypeEnum = cmd.header.instTypeEnum ;
        cmd.body.trades.marketTypeEnum = cmd.header.marketTypeEnum ;

        strcpy(cmd.body.trades.instId, info.instId);
        strcpy(cmd.body.trades.base, info.base);
        strcpy(cmd.body.trades.quote, info.quote);
        strcpy(cmd.body.trades.margin, info.margin);
        strcpy(cmd.body.trades.tradeId, data["tradeId"].GetString());
        cmd.body.trades.px = stod(data["price"].GetString());
        cmd.body.trades.sz = stod(data["size"].GetString());
        strcpy(cmd.body.trades.side, data["side"].GetString());
        cmd.body.trades.ts = ts;
        cmd.body.trades.tsNet = mdMsg.tsNet;
        cmd.body.trades.tsParse = crypto::getCurrentTime();
        g_cmdQueue.push(cmd);
#endif
        sprintf(mdStr, Trades_Format,
            exchIdStr.c_str(), instTypeStr.c_str(),
            marketTypeStr.c_str(), info.instId,
            data["tradeId"].GetString(),
            stod(data["price"].GetString()),
            stod(data["size"].GetString()),
            data["side"].GetString(),
            ts,
            mdMsg.tsNet, crypto::getCurrentTime()
        );
    }
    else if(marketTypeEnum == DEPTH1){
#ifdef NEED_SHM
        md::CryptoMarketData cmd;
        cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
        cmd.header.marketTypeEnum = md::DEPTH1;
        HANDLE_SUBTYPE(cmd)

        strcpy(cmd.header.instId, info.instId);
        cmd.body.depth1.exchangeTypeEnum = cmd.header.exchangeTypeEnum ;
        cmd.body.depth1.instTypeEnum = cmd.header.instTypeEnum ;
        cmd.body.depth1.marketTypeEnum = cmd.header.marketTypeEnum ;
        strcpy(cmd.body.depth1.instId, info.instId);
        strcpy(cmd.body.depth1.base, info.base);
        strcpy(cmd.body.depth1.quote, info.quote);
        strcpy(cmd.body.depth1.margin, info.margin);

        cmd.body.depth1.bp1 = stod(data["bids"][0][0].GetString());
        cmd.body.depth1.bv1 = stod(data["bids"][0][1].GetString());

        cmd.body.depth1.ap1 = stod(data["asks"][0][0].GetString());
        cmd.body.depth1.av1 = stod(data["asks"][0][1].GetString());
        cmd.body.depth1.ts = stol(data["timestamp"].GetString()) * 1000;
        cmd.body.depth1.tsNet = mdMsg.tsNet;
        cmd.body.depth1.tsParse = crypto::getCurrentTime();
        g_cmdQueue.push(cmd);
#endif
        string asksStr,bidsStr;
        asksStr.append("[[").append(data["asks"][0][0].GetString()).append(",")
                .append(data["asks"][0][1].GetString()).append("]]");

        bidsStr.append("[[").append(data["bids"][0][0].GetString()).append(",")
                        .append(data["bids"][0][1].GetString()).append("]");

        sprintf(mdStr, DEPTH_Format,
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                asksStr.c_str(),
                bidsStr.c_str(),
                stol(data["timestamp"].GetString()) * 1000,
                mdMsg.tsNet, crypto::getCurrentTime()
        );
    }
    else if(marketTypeEnum == MBPType || marketTypeEnum == DEPTH5){
        string asksStr, bidsStr;
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
        long ts = stol(data["timestamp"].GetString()) * 1000;
        char longMdStr[2048 * 64];
        sprintf(longMdStr, DEPTH_Format,
            exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
            asksStr.c_str(),
            bidsStr.c_str(),
            ts,
            mdMsg.tsNet, crypto::getCurrentTime()
        );
        redis_cmd(mdMsg, key, longMdStr);
        return;
    }
    else if(marketTypeEnum == KLINE_1m){
//        cout << mdMsg.body.data << endl;
        long barTime = stol(data["candles"][0].GetString());
        auto now = crypto::getCurrentTime();
        bool isFinished = now * 0.000001 - barTime > 58.5 && now * 0.000001 - barTime < 60.5 ? true : false;
        if(!isFinished){
            return;
        }
        sprintf(mdStr, Bar_Format,
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                barTime * 1000 * 1000,
                stod(data["candles"][3].GetString()),//high
                stod(data["candles"][4].GetString()),//low
                stod(data["candles"][1].GetString()),//open
                stod(data["candles"][2].GetString()),//close
                0.0,
                stod(data["candles"][5].GetString()),
                stod(data["candles"][6].GetString()),
                0.0,
                0.0,
                0.0,
                0.0,
                0,
                isFinished == false ? "false" : "true",
                (long)(stod(data["time"].GetString()) * 0.001) ,
                mdMsg.tsNet, now//,mdMsg->data
        );
    }
    else{
//        LOG_ERROR("not implemented now");
        return;
    }
    redis_cmd(mdMsg, key, mdStr);
    #endif
}

void md::KucoinUnit::monitor_ws(){
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
                    if(count % pingInterval == 0){
                        LOG_DEBUG("%s.%s.%s ws is conntected now, will send ping to it!", exchIdStr.c_str(),
                                  instTypeStr.c_str(), marketTypeStr.c_str());
                        ping();
                    }
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
