#include "OKXMarketClient.h"


void md::OKXUnit::sub_websocket(){
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
//        std::this_thread::sleep_for(std::chrono::milliseconds(350));
        websocket_outgoing_message outMsg;
        outMsg.set_utf8_message(subValue.serialize().c_str());
        wsClient.send(outMsg).wait();
        LOG_INFO("%s send success: %s",exchIdStr.c_str(), subValue.serialize().c_str());

        m_IsConnected = true;
        isFinished = true;
    }
    catch(std::exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s,%s,%s,%s,%d,%s",exchIdStr.c_str(),instTypeStr.c_str(),
                  marketTypeStr.c_str(), __FUNCTION__ , __LINE__, e.what());
    }
}

void md::OKXUnit::ping(){
    if(m_IsConnected){
        websocket_outgoing_message outMsg;
        outMsg.set_utf8_message("ping");
        wsClient.send(outMsg).wait();
    }
}

void md::OKXUnit::pong(){

}

//https://microsoft.github.io/cpprestsdk/classweb_1_1websockets_1_1client_1_1websocket__incoming__message.html
void md::OKXUnit::on_websocket_msg(const websocket_incoming_message &in_msg){
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
    }
    catch(std::exception &e){
        LOG_ERROR("%s",e.what());
    }
}

void md::OKXUnit::construct(){
//            cout << getString() << endl;
    LOG_INFO("%s", getString().c_str());
    exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    // cout << instTypeStr << endl;
    marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
    //先构造 ws地址
    m_wsBaseUrl = OKX_WEBSOCKET_HOST_PUBLIC_DEPTH;

    if (crypto::has_str(marketTypeStr.c_str(), "KLINE") == true) {
        m_wsBaseUrl = OKX_WEBSOCKET_HOST_PUBLIC_KLINE;
    }

    //开始构造订阅格式
    subValue["op"] = json::value::string("subscribe");
    // subValue["args"]["instType"] = json::value::string(instTypeStr);
    for(auto instId : subStrVec){
        string strOriginInstId = instId;
        if (crypto::has_str(marketTypeStr.c_str(), "DEPTH1") == true) {
            json::value arg;
            arg["channel"] = json::value::string("bbo-tbt");
            arg["instId"] = json::value::string(strOriginInstId);
            subValue["args"][subCount++] = arg;
        }
        else if (crypto::has_str(marketTypeStr.c_str(), "DEPTH5") == true) {
            json::value arg;
            arg["channel"] = json::value::string("books5");
            arg["instId"] = json::value::string(strOriginInstId);
            subValue["args"][subCount++] = arg;
        }
        else if (crypto::has_str(marketTypeStr.c_str(), "TRADE") == true) {
            json::value arg;
            arg["channel"] = json::value::string("trades");
            arg["instId"] = json::value::string(strOriginInstId);
            subValue["args"][subCount++] = arg;
        }
        else if (crypto::has_str(marketTypeStr.c_str(), "KLINE_1m") == true) {
            json::value arg;
            arg["channel"] = json::value::string("candle1m");
            arg["instId"] = json::value::string(strOriginInstId);
            subValue["args"][subCount++] = arg;
        }
        if (crypto::str_cmp(instTypeStr.c_str(), "SWAP") == true ) {
            if (crypto::has_str(marketTypeStr.c_str(), "FUNDING") == true) {
                json::value arg;
                arg["channel"] = json::value::string("funding-rate");
                arg["instId"] = json::value::string(strOriginInstId);
                subValue["args"][subCount++] = arg;
            }
        }
    }
}

void md::OKXMarketClient::print_stat() {
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

void md::OKXMarketClient::construct() {
//    m_spotRestBaseUrl = OKX_REST_HOST_PUBLIC_SPOT;
    for(auto marketType : _marketTypeVec){
        for(auto instType : _instTypeVec){
            //存储有效的交易对
            vector<string> validInstIdVec;
            for(auto instId : _instIdVec){
                //先过滤一遍，筛选掉smc中不存在的交易对
                InstrumentInfo info;
                if(smc->get_instrument_info(exchId, instType.c_str(),
                                            instId.c_str(), info) == true){
//                    string lowerOriginInstId = crypto::to_lower(info.originInstId);
                    validInstIdVec.push_back(info.originInstId);
                }
            }

            if(crypto::has_str(instType.c_str(), "SPOT") == true ){
                //有效的交易对数量
                size_t validSize = validInstIdVec.size();
                //需要的token unit单元，若不能取整则单元需要多加一个
                size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                for(size_t us = 0; us < unitSize; us++){
                    OKXUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
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
                        OKXUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
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
                        OKXUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
                                            MarketTypeStr2EnumMap[marketType], _ip, _port, _passwd};
                        size_t startValidNum = tokenLot * (us);
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        for(size_t i = startValidNum;i < endValidNum; i++){
                            //将需要订阅的交易对，塞到struct
                            unit.subStrVec.push_back(notCoinStrVec[i]);
                        }
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

void md::OKXMarketClient::start(){
    //这里启动一个线程以便不阻塞主线程
    std::thread subThread([&](){
        construct();
        for(auto &unit: tokenUnitVec){
            //共用一个smc即可
            unit.smc = smc;
            unit.construct();
            if(unit.subValue.has_field("args")) {
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
//            std::thread spotRestThread(&md::OKXMarketClient::req_spot_mbp, this);
//            spotRestThread.detach();
//        }
    });
    subThread.detach();

    std::thread printStatThread(&md::OKXMarketClient::print_stat, this);
    printStatThread.detach();
}

//处理消息 解析json并发送给redis或共享内存
void md::OKXUnit::save_md_string(const MDMsg &mdMsg) {
    rapidjson::Document d;
    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg.body.data.c_str());
    if (d.HasParseError() || !(rawData.HasMember("data"))) {
        return;
    }
    // cout << mdMsg.body.data << endl;
    string originInstId = rawData["arg"]["instId"].GetString();
    InstrumentInfo info;
    if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                originInstId.c_str(), info) == false){
        LOG_ERROR("not found %s info in smc", originInstId.c_str());
        return;
    }
    // DEBUGLINE
    char key[64] = {0};
    char mdStr[2048] = {0};
    sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
            marketTypeStr.c_str(), info.instId);
    string channel = rawData["arg"]["channel"].GetString();

    const rapidjson::Value &data = rawData["data"][0];
    if(marketTypeEnum == TRADES){
        double px = stod(data["px"].GetString());
        double sz = stod(data["sz"].GetString());
        string side = data["side"].GetString();
        string tradeId = data["tradeId"].GetString();
        long ts = stol(data["ts"].GetString()) * 1000;
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
        long ts = stol(data["ts"].GetString()) * 1000;
#ifdef NEED_SHM
        double bp1 = stod(data["bids"][0][0].GetString());
        double bv1 = stod(data["bids"][0][1].GetString());
        double ap1 = stod(data["asks"][0][0].GetString());
        double av1 = stod(data["asks"][0][1].GetString());
        md::CryptoMarketData cmd;
        memset(&cmd, 0, sizeof(md::CryptoMarketData));
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
        asksStr.append("[[").append(data["asks"][0][0].GetString()).append(",")
                .append(data["asks"][0][1].GetString()).append("]]");

        bidsStr.append("[[").append(data["bids"][0][0].GetString()).append(",")
                        .append(data["bids"][0][1].GetString()).append("]]");

        sprintf(mdStr, DEPTH_Format,
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                asksStr.c_str(),
                bidsStr.c_str(),
                ts,
                mdMsg.tsNet, crypto::getCurrentTime()
        );
    }
    else if(marketTypeEnum == DEPTH5){
#ifdef NEED_SHM
        md::CryptoMarketData cmd;
        memset(&cmd, 0, sizeof(md::CryptoMarketData));
        cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
        cmd.header.marketTypeEnum = md::DEPTH5;

        HANDLE_SUBTYPE(cmd)

        strcpy(cmd.header.instId, info.instId);
        cmd.body.depth5.exchangeTypeEnum = cmd.header.exchangeTypeEnum ;
        cmd.body.depth5.instTypeEnum = cmd.header.instTypeEnum ;
        cmd.body.depth5.marketTypeEnum = cmd.header.marketTypeEnum ;
        strcpy(cmd.body.depth5.instId, info.instId);
        strcpy(cmd.body.depth5.base, info.base);
        strcpy(cmd.body.depth5.quote, info.quote);
        strcpy(cmd.body.depth5.margin, info.margin);

        cmd.body.depth5.bp1 = stod(data["bids"][0][0].GetString());
        cmd.body.depth5.bv1 = stod(data["bids"][0][1].GetString());
        cmd.body.depth5.bp2 = stod(data["bids"][1][0].GetString());
        cmd.body.depth5.bv2 = stod(data["bids"][1][1].GetString());
        cmd.body.depth5.bp3 = stod(data["bids"][2][0].GetString());
        cmd.body.depth5.bv3 = stod(data["bids"][2][1].GetString());
        cmd.body.depth5.bp4 = stod(data["bids"][3][0].GetString());
        cmd.body.depth5.bv4 = stod(data["bids"][3][1].GetString());
        cmd.body.depth5.bp5 = stod(data["bids"][4][0].GetString());
        cmd.body.depth5.bv5 = stod(data["bids"][4][1].GetString());

        cmd.body.depth5.ap1 = stod(data["asks"][0][0].GetString());
        cmd.body.depth5.av1 = stod(data["asks"][0][1].GetString());
        cmd.body.depth5.ap2 = stod(data["asks"][1][0].GetString());
        cmd.body.depth5.av2 = stod(data["asks"][1][1].GetString());
        cmd.body.depth5.ap3 = stod(data["asks"][2][0].GetString());
        cmd.body.depth5.av3 = stod(data["asks"][2][1].GetString());
        cmd.body.depth5.ap4 = stod(data["asks"][3][0].GetString());
        cmd.body.depth5.av4 = stod(data["asks"][3][1].GetString());
        cmd.body.depth5.ap5 = stod(data["asks"][4][0].GetString());
        cmd.body.depth5.av5 = stod(data["asks"][4][1].GetString());

        cmd.body.depth5.ts = stol(data["ts"].GetString()) * 1000;
        cmd.body.depth5.tsNet = mdMsg.tsNet;
        cmd.body.depth5.tsParse = crypto::getCurrentTime();
        g_cmdQueue.push(cmd);
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
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                asksStr.c_str(),
                bidsStr.c_str(),
                stol(data["ts"].GetString()) * 1000,
                mdMsg.tsNet, crypto::getCurrentTime()
        );
    }
    else if(marketTypeEnum == KLINE_1m){
        //cout << mdMsg.body.data << endl;
        auto barTime = stol(data[0].GetString()) ;//毫秒
        // auto now = crypto::getCurrentTime();
//        bool isFinished = true;
        const string &confirm = data[8].GetString();
        bool isFinished = confirm[0] == '1' ? true : false;
        if(!isFinished){
            return;
        }
        double avgPrice = 0;
        double amount = stod(data[7].GetString());
        double volume = stod(data[5].GetString());
        if(amount > ZERO_NUM){
            avgPrice = amount / volume;
        }
        sprintf(mdStr, Bar_Format,
            exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
            barTime * 1000 ,
            stod(data[2].GetString()),//high
            stod(data[3].GetString()),//low
            stod(data[1].GetString()),//open
            stod(data[4].GetString()),//close
            avgPrice,
            volume,
            amount,
            0.0,
            0.0,
            0.0,
            0.0,
            0,
            isFinished == false ? "false" : "true",
            0L,
            mdMsg.tsNet, crypto::getCurrentTime()//,mdMsg->data
        );
    }
    else if(mdMsg.header.marketTypeEnum == FUNDING_RATE){
            sprintf(mdStr, Funding_Rate_Format,
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                stod(data["fundingRate"].GetString()),
                stod(data["nextFundingRate"].GetString()),
                stol(data["fundingTime"].GetString()) * 1000 ,
                mdMsg.tsNet,
                mdMsg.tsNet, crypto::getCurrentTime()//,mdMsg->data
            );
        }
    else{
        LOG_ERROR("not implemented now");
        return;
    }
    redis_cmd(mdMsg, key, mdStr);
}


void md::OKXUnit::monitor_ws(){
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
                    if(count % 20 == 0){
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

//void md::OKXUnit::subscribe(string &originInstId){
//    if(m_IsConnected){
//        LOG_ERROR("DB will resubscrible %s data to ftx now", originInstId.c_str());
//        for(auto &subValue: subValueVec){
//            string subStr = subValue.serialize();
//            if(crypto::has_str(subStr.c_str(), originInstId.c_str())){
//                json::value unsub;
//                unsub["op"] = json::value::string("unsubscribe");
//                unsub["channel"] = subValue["channel"];
//                unsub["market"] = subValue["market"];
//                websocket_outgoing_message outUnSubMsg;
//                outUnSubMsg.set_utf8_message(unsub.serialize().c_str());
//                wsClient.send(outUnSubMsg).wait();
//                LOG_INFO("%s send unsubscribe %s success",exchIdStr.c_str(), unsub.serialize().c_str());
//                websocket_outgoing_message outMsg;
//                outMsg.set_utf8_message(subValue.serialize().c_str());
//                wsClient.send(outMsg).wait();
//                LOG_INFO("%s send subscribe %s success",exchIdStr.c_str(), subValue.serialize().c_str());
//            }
//        }
//        LOG_INFO("%s send %lu sub values to %s",exchIdStr.c_str(),subValueVec.size(), m_wsBaseUrl.c_str());
//    }
//}
