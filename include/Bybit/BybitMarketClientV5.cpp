#include "BybitMarketClientV5.h"


void md::BybitUnit::sub_websocket(){
    try{
        wsClient.close();
        subValue["req_id"] = json::value::number(subId++);
        LOG_INFO("%s send %s to %s",exchIdStr.c_str(),subValue.serialize().c_str(), m_wsBaseUrl.c_str());
        uri_builder builder(m_wsBaseUrl);

        wsClient.connect(builder.to_string())
        .then([&]() {
            //绑定消息回调函数
            std::function<void (const websocket_incoming_message &msg)> f;
            f = std::bind(&MarketDataBaseStruct::on_websocket_msg, this, placeholders::_1);
            wsClient.set_message_handler(f);
            //绑定ws断开函数
            std::function<void (websocket_close_status close_status,
                                const utility::string_t& reason, const std::error_code& error)> c;
            c =  std::bind(&MarketDataBaseStruct::on_close_msg,this
                    ,placeholders::_1,placeholders::_2,placeholders::_3);
            wsClient.set_close_handler(c);
        }).wait();
        //发送订阅数据
        websocket_outgoing_message outMsg;
        outMsg.set_utf8_message(subValue.serialize().c_str());
        wsClient.send(outMsg).wait();

        m_IsConnected = true;
    }
    catch(std::exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s,%s,%s,%s,%d,%s",exchIdStr.c_str(),instTypeStr.c_str(),
                  marketTypeStr.c_str(), __FUNCTION__ , __LINE__, e.what());
    }
}

void md::BybitUnit::ping(){
    if(m_IsConnected){
        websocket_outgoing_message outMsg;
        json::value pingSubValue;
        pingSubValue["op"] = json::value::string("ping");
        outMsg.set_utf8_message(pingSubValue.serialize().c_str());
        wsClient.send(outMsg).wait();
    }
}

void md::BybitUnit::pong(){
    if(m_IsConnected){
        // websocket_outgoing_message outMsg;
        // outMsg.set_pong_message();
        // wsClient.send(outMsg).wait();
    }
}

//https://microsoft.github.io/cpprestsdk/classweb_1_1websockets_1_1client_1_1websocket__incoming__message.html
void md::BybitUnit::on_websocket_msg(const websocket_incoming_message &in_msg){
    if (in_msg.message_type() == websocket_message_type::text_message){
        const string &s = in_msg.extract_string().get();
        HANDLE_TEXT_MSG(s)
    }
    else if(in_msg.message_type() == websocket_message_type::close){
        LOG_DEBUG("%s close got ", exchIdStr.c_str());
        m_IsConnected = false;
    }
}

void md::BybitUnit::construct(){
//            cout << getString() << endl;
    LOG_INFO("%s", getString().c_str());
    exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
//  cout << exchIdStr << instTypeStr << marketTypeStr<< endl;
    //先构造 ws地址
    if(subMarketTypeEnum == SMT_SPOT_MD){
        m_wsBaseUrl = BYBIT_WEBSOCKET_HOST_PUBLIC_SPOT;
    }
    else if(subMarketTypeEnum == SMT_USDT_SWAP){
        m_wsBaseUrl = BYBIT_WEBSOCKET_HOST_PUBLIC_USDT_SWAP;
    }
    else if(subMarketTypeEnum == SMT_USD_SWAP){
        m_wsBaseUrl = BYBIT_WEBSOCKET_HOST_PUBLIC_C_SWAP;
    }
    else{
        LOG_ERROR("not support your sub market type!");
        // cryptothrow("binance not support your sub market type!",-1);
    }
    //开始构造订阅格式,subStrVec存的是原始格式
    subValue["op"] = json::value::string("subscribe");
    for(auto instId : subStrVec){
        // string lowerOriginInstId = crypto::to_lower(instId);
        string lowerMarketType = crypto::to_lower(marketTypeStr);
        if(subMarketTypeEnum == SMT_SPOT_MD){

        }
        else if(subMarketTypeEnum == SMT_USDT_SWAP || subMarketTypeEnum == SMT_USD_SWAP){
            if(crypto::has_str(marketTypeStr, "DEPTH") == true){
                if(crypto::str_cmp(marketTypeStr.c_str(), "DEPTH1") == true){
                    string param = fmt::format("orderbook.1.{}", instId);
                    subValue["args"][subCount++] = json::value::string(param);
                }
                else{
                    string param = fmt::format("orderbook.50.{}", instId);
                    subValue["args"][subCount++] = json::value::string(param);
                }
            }
            else if(crypto::has_str(marketTypeStr, "TRADE") == true){
                string param = fmt::format("publicTrade.{}", instId);
                subValue["args"][subCount++] = json::value::string(param);
            }
            else if(crypto::has_str(marketTypeStr, "KLINE_1m") == true){
                string param = fmt::format("kline.1.{}", instId);
                subValue["args"][subCount++] = json::value::string(param);
            }
            else if(crypto::has_str(marketTypeStr, "FUNDING") == true){
                if(subMarketTypeEnum == SMT_USDT_SWAP){
                    string param = fmt::format("tickers.{}", instId);
                    subValue["args"][subCount++] = json::value::string(param);
                }
            }
            else{
                string msg = "not implemented MarketType: " + marketTypeStr;
                LOG_ERROR("%s", msg.c_str());
            }
        }
        else{
            string msg = "not support subMarketType: " + SubMarketTypeEnum2StrMap[subMarketTypeEnum];
            LOG_ERROR("%s", msg.c_str());
            // cryptothrow("not support subMarketType: "+SubMarketTypeEnum2StrMap[subMarketTypeEnum], -1);
        }
    }
//            cout << subValue.serialize() << endl;
}

//处理消息 解析json并发送给redis或共享内存
void md::BybitUnit::save_md_string(const MDMsg &mdMsg){
    rapidjson::Document d;
    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg.body.data.c_str());

    // return;
    if(d.HasParseError() || !rawData.HasMember("data") || !rawData.HasMember("topic")){
        return;
    }
    const string &topic = rawData["topic"].GetString();

    string originInstId;
    if(crypto::has_str(topic.c_str(), "orderbook")){//DEPTH1
        originInstId = rawData["data"]["s"].GetString();
    }
    else if(crypto::has_str(topic.c_str(), "publicTrade")){//TRADES
        originInstId = rawData["data"][0]["s"].GetString();
    }
    else if(crypto::has_str(topic.c_str(), "kline")){//TRADES
        // originInstId = rawData["data"][0]["s"].GetString();
        vector<string> s_vec = crypto::split(topic, '.');
        originInstId = crypto::to_upper(s_vec[s_vec.size()-1]).c_str();
    }
    else if(crypto::has_str(topic.c_str(), "tickers")){//TRADES
        originInstId = rawData["data"]["symbol"].GetString();
    }
    else {
        // cout << mdMsg.body.data << endl;
        return;
    }
    // cout << mdMsg.body.data << endl;
    InstrumentInfo info;
    if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(), originInstId.c_str(), info) == false){
        LOG_ERROR("msg: %s",mdMsg.body.data.c_str());
        return;
    }
    string mdStr;
    string key = fmt::format("{}.{}.{}.{}", exchIdStr, instTypeStr, marketTypeStr, info.instId);

    if(mdMsg.header.instTypeEnum == SPOT){
        return;
    }
    else if(mdMsg.header.instTypeEnum == SWAP){
        if(mdMsg.header.marketTypeEnum == DEPTH1){
            const rapidjson::Value &data = rawData["data"];
            auto ts = stol(rawData["ts"].GetString()) * 1000;
            // string topic = rawData["topic"].GetString();
            // cout << key << endl;
            // cout << mdMsg.body.data << endl;
            auto found = _cacheMDMap.find(key);
            if(found != _cacheMDMap.end()){
                md::CryptoMarketData &cmd = found->second;
                if(data["b"].Size() > 0){
                    cmd.body.depth1.bp1 = stod(data["b"][0][0].GetString());
                    cmd.body.depth1.bv1 = stod(data["b"][0][1].GetString());
                }
                if(data["a"].Size() > 0){
                    cmd.body.depth1.ap1 = stod(data["a"][0][0].GetString());
                    cmd.body.depth1.av1 = stod(data["a"][0][1].GetString());
                }
                cmd.body.depth1.ts = ts;
                cmd.body.depth1.tsNet = mdMsg.tsNet;
                cmd.body.depth1.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
                g_cmdQueue.push(cmd);
#endif
                const md::CryptoMarketData &rcmd = found->second;
                string asksStr = fmt::format("[[{:.12f},{:.2f}]]", rcmd.body.depth1.ap1*info.reduceNumber, rcmd.body.depth1.av1*info.magnifyNumber);
                string bidsStr = fmt::format("[[{:.12f},{:.2f}]]", rcmd.body.depth1.bp1*info.reduceNumber, rcmd.body.depth1.bv1*info.magnifyNumber);
                mdStr = fmt::format(DEPTH_Format_FMT, exchIdStr , instTypeStr , marketTypeStr , info.instId,
                    asksStr,
                    bidsStr,
                    rcmd.body.depth1.ts,
                    rcmd.body.depth1.tsNet,
                    crypto::getCurrentTime());
                // cout << cmd.getString() << endl;
                // cout << mdStr << endl;
            }
            else{
                md::CryptoMarketData cmd;
                memset(&cmd, 0, sizeof(md::CryptoMarketData));
                cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
                cmd.header.marketTypeEnum = md::DEPTH1;
                HANDLE_SUBTYPE(cmd)
                strcpy(cmd.header.instId, info.instId);
                cmd.body.depth1.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
                cmd.body.depth1.instTypeEnum = cmd.header.instTypeEnum;
                cmd.body.depth1.marketTypeEnum = cmd.header.marketTypeEnum;
                strcpy(cmd.body.depth1.instId, info.instId);
                strcpy(cmd.body.depth1.base, info.base);
                strcpy(cmd.body.depth1.quote, info.quote);
                strcpy(cmd.body.depth1.margin, info.margin);
                if(data["b"].Size() > 0){
                    cmd.body.depth1.bp1 = stod(data["b"][0][0].GetString())*info.reduceNumber;
                    cmd.body.depth1.bv1 = stod(data["b"][0][1].GetString())*info.magnifyNumber;
                }
                if(data["a"].Size() > 0){
                    cmd.body.depth1.ap1 = stod(data["a"][0][0].GetString())*info.reduceNumber;
                    cmd.body.depth1.av1 = stod(data["a"][0][1].GetString())*info.magnifyNumber;
                }
                cmd.body.depth1.ts = ts;
                cmd.body.depth1.tsNet = mdMsg.tsNet;
                cmd.body.depth1.tsParse = crypto::getCurrentTime();
                _cacheMDMap[key] = cmd;
                return;
            }
        }
        else if(mdMsg.header.marketTypeEnum == DEPTH5 || mdMsg.header.marketTypeEnum == DEPTH10
           || mdMsg.header.marketTypeEnum == DEPTH20 ){
            return;
        }
        else if (mdMsg.header.marketTypeEnum == TRADES){
            const rapidjson::Value &data = rawData["data"];
            auto ts = stol(rawData["ts"].GetString()) * 1000;
            for(rapidjson::size_t i = 0; i < data.Size(); i++){
                string side = data[i]["S"].GetString()[0] == 'S' ? "sell" : "buy";
#ifdef NEED_SHM
                md::CryptoMarketData cmd;
                memset(&cmd, 0, sizeof(md::CryptoMarketData));
                cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
                cmd.header.marketTypeEnum = md::TRADES;
                HANDLE_SUBTYPE(cmd)

                strcpy(cmd.header.instId, info.instId);
                cmd.body.trades.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
                cmd.body.trades.instTypeEnum = cmd.header.instTypeEnum;
                cmd.body.trades.marketTypeEnum = cmd.header.marketTypeEnum;

                strcpy(cmd.body.trades.instId, info.instId);
                strcpy(cmd.body.trades.base, info.base);
                strcpy(cmd.body.trades.quote, info.quote);
                strcpy(cmd.body.trades.margin, info.margin);
                strcpy(cmd.body.trades.tradeId, data[i]["i"].GetString());
                cmd.body.trades.px = stod(data[i]["p"].GetString())*info.reduceNumber;
                cmd.body.trades.sz = stod(data[i]["v"].GetString())*info.magnifyNumber;
                strcpy(cmd.body.trades.side, side.c_str());
                cmd.body.trades.ts = ts;
                cmd.body.trades.tsNet = mdMsg.tsNet;
                cmd.body.trades.tsParse = crypto::getCurrentTime();
                g_cmdQueue.push(cmd);
#endif
                mdStr = fmt::format(Trades_Format_FMT,
                    exchIdStr, instTypeStr, marketTypeStr, info.instId,
                    data[i]["i"].GetString(),
                    stod(data[i]["p"].GetString())*info.reduceNumber,
                    stod(data[i]["v"].GetString())*info.magnifyNumber,
                    side,
                    ts,
                    mdMsg.tsNet,
                    crypto::getCurrentTime());
                // cout << mdStr << endl;
            }
        }
        else if(mdMsg.header.marketTypeEnum == KLINE_1m){
            const rapidjson::Value &data = rawData["data"][0];
            bool isFinished = data["confirm"].GetBool();
            if(!isFinished){
                return;
            }
            double avgPrice = 0;
            double amount = stod(data["turnover"].GetString());
            double volume = stod(data["volume"].GetString());
            if(amount > ZERO_NUM){
                avgPrice = amount / volume;
            }
            mdStr = fmt::format(Bar_Format_FMT,
                exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                stol(data["start"].GetString()) * 1000,
                stod(data["high"].GetString())*info.reduceNumber,
                stod(data["low"].GetString())*info.reduceNumber,
                stod(data["open"].GetString())*info.reduceNumber,
                stod(data["close"].GetString())*info.reduceNumber,
                avgPrice*info.reduceNumber,
                volume*info.magnifyNumber,
                amount,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                isFinished == false ? "false" : "true",
                stol(data["timestamp"].GetString()) * 1000 ,
                mdMsg.tsNet, crypto::getCurrentTime()
            );
        }
        else if(mdMsg.header.marketTypeEnum == FUNDING_RATE){
            const rapidjson::Value &data = rawData["data"];
            auto ts = stol(rawData["ts"].GetString()) * 1000;
            auto found = _cacheMDMap.find(key);
            if(found != _cacheMDMap.end()){
                md::CryptoMarketData &cmd = found->second;
                if(rawData["data"].HasMember("nextFundingTime")){
                    cmd.body.fundingRate.fundingTime = stol(data["nextFundingTime"].GetString())*1000;
                }
                if(rawData["data"].HasMember("fundingRate")){
                    cmd.body.fundingRate.fundingRate = stod(data["fundingRate"].GetString());
                }
                if(rawData["data"].HasMember("nextFundingTime") || rawData["data"].HasMember("fundingRate")){
                    double fundingRate = cmd.body.fundingRate.fundingRate;
                    long fundingTime = cmd.body.fundingRate.fundingTime;
                    mdStr = fmt::format(Funding_Rate_Format_FMT,
                        exchIdStr, instTypeStr, marketTypeStr, info.instId,
                        fundingRate,
                        0.0,
                        fundingTime ,
                        stol(rawData["ts"].GetString()) * 1000 ,
                        mdMsg.tsNet,
                        crypto::getCurrentTime());
                    // cout << mdStr << endl;
                }
                else{
                    return;
                }
            }
            else{
                md::CryptoMarketData cmd;
                memset(&cmd, 0, sizeof(md::CryptoMarketData));
                cmd.header.exchangeTypeEnum = mdMsg.header.exchangeTypeEnum ;
                cmd.header.marketTypeEnum = md::FUNDING_RATE;
                HANDLE_SUBTYPE(cmd)
                strcpy(cmd.header.instId, info.instId);
                cmd.body.depth1.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
                cmd.body.depth1.instTypeEnum = cmd.header.instTypeEnum;
                cmd.body.depth1.marketTypeEnum = cmd.header.marketTypeEnum;
                strcpy(cmd.body.fundingRate.instId, info.instId);
                strcpy(cmd.body.fundingRate.base, info.base);
                strcpy(cmd.body.fundingRate.quote, info.quote);
                strcpy(cmd.body.fundingRate.margin, info.margin);

                cmd.body.fundingRate.fundingRate = stod(data["fundingRate"].GetString());
                cmd.body.fundingRate.fundingTime = stol(data["nextFundingTime"].GetString()) * 1000;
                cmd.body.depth1.ts = ts;
                cmd.body.depth1.tsNet = mdMsg.tsNet;
                cmd.body.depth1.tsParse = crypto::getCurrentTime();
                _cacheMDMap[key] = cmd;
                double fundingRate = cmd.body.fundingRate.fundingRate;
                long fundingTime = cmd.body.fundingRate.fundingTime;
                mdStr = fmt::format(Funding_Rate_Format_FMT,
                    exchIdStr, instTypeStr, marketTypeStr, info.instId,
                    fundingRate,
                    0.0,
                    fundingTime ,
                    stol(rawData["ts"].GetString()) * 1000 ,
                    mdMsg.tsNet,
                    crypto::getCurrentTime());
            }
//            sprintf(key, "%s.%s.%s.%s", BinanceExchangeId, instType, marketType, instId);

            // sprintf(mdStr, Funding_Rate_Format,
            //         exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
            //         stod(data["r"].GetString()),
            //         0.0,
            //         stol(data["T"].GetString()) * 1000 ,
            //         stol(data["E"].GetString()) * 1000 ,
            //         mdMsg.tsNet, crypto::getCurrentTime()//,mdMsg->data
            // );

        }
        else{
            return;
        }
    }
    else{
        return;
    }
    redis_cmd(mdMsg, key.c_str(), mdStr.c_str());
}

void md::BybitMarketClientV5::print_stat() {
    while(1){
        try{
            long received = 0, left = 0;
            for(auto unit : tokenUnitVec){
                received += unit.tickCount;
                left     += unit.m_queue->get_left();
            }
            LOG_INFO("%s received:%ld, left:%ld",exchId, received, left);
            sleep(5);
        }
        catch(std::exception& e){

        }
    }
}


void md::BybitMarketClientV5::construct() {
    for(auto marketType : _marketTypeVec){
        for(auto instType : _instTypeVec){
            //存储有效的交易对
            vector<string> validInstIdVec;
            for(auto instId : _instIdVec){
                //先过滤一遍，筛选掉smc中不存在的交易对
                InstrumentInfo info;
                if(smc->get_instrument_info(exchId, instType.c_str(), instId.c_str(), info) == true){
                    // string lowerOriginInstId = crypto::to_lower(info.originInstId);
                    validInstIdVec.push_back(info.originInstId);
//                    cout << exchId  << "." << instType  << "." << marketType << "." <<lowerOriginInstId << endl;
                }
                else{
                    LOG_ERROR("not found %s.%s.%s smc info", exchId, instType.c_str(), instId.c_str());
                }
            }
            //再根据instType确定订阅类型
            //如果是spot，不需要区分，永续和交割需要进一步区分
            if(crypto::str_cmp(instType.c_str(), "SPOT") == true &&
                    crypto::has_str(marketType.c_str(), "FUNDING") == false){
                //现货没有费率信息
                //有效的交易对数量
                size_t validSize = validInstIdVec.size();
                //需要的token unit单元，若不能取整则单元需要多加一个
                size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
//                cout << unitSize << endl;
                for(size_t us = 0; us < unitSize; us++){
                    BybitUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
                                    MarketTypeStr2EnumMap[marketType], _ip, _port, _passwd};
                    size_t startValidNum = tokenLot * (us);
                    size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                    for(size_t i = startValidNum;i < endValidNum; i++){
//                        cout << validInstIdVec[i] << endl;
                        //将需要订阅的交易对，塞到struct
                        unit.subStrVec.push_back(validInstIdVec[i]);
                    }
                    unit.subMarketTypeEnum = SMT_SPOT_MD;
                    tokenUnitVec.push_back(unit);
                }
            }
            else if(crypto::str_cmp(instType.c_str(), "SWAP")){
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
                            //usdt或者usdc本位
                            notCoinStrVec.push_back(inst);
                        }
                    }
                    else{
                        //这里不会执行到,因为上面已经筛选过了
                        string msg = fmt::format("not found {}.{}.{} smc info", exchId, instType, inst);
                        LOG_ERROR("%s", msg.c_str());
                        // cryptothrow(msg ,-1);
                    }
                }
                if(coinStrVec.size() > 0){
                    //有效的交易对数量
                    size_t validSize = coinStrVec.size();
                    //需要的token unit单元数量，若不能取整则单元需要多加一个
                    size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
//                    cout << unitSize << endl;
                    for(size_t us = 0; us < unitSize; us++){
                        BybitUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
                                            MarketTypeStr2EnumMap[marketType], _ip, _port, _passwd};
                        size_t startValidNum = tokenLot * (us);
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        for(size_t i = startValidNum;i < endValidNum; i++){
//                            cout << coinStrVec[i] << endl;
                            //将需要订阅的交易对，塞到struct
                            unit.subStrVec.push_back(coinStrVec[i]);
                        }
                        //
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
//                    cout << unitSize << endl;
                    for(size_t us = 0; us < unitSize; us++){
                        BybitUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
                                            MarketTypeStr2EnumMap[marketType], _ip, _port, _passwd};
                        size_t startValidNum = tokenLot * (us);
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        for(size_t i = startValidNum;i < endValidNum; i++){
//                            cout << notCoinStrVec[i] << endl;
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
            else{

            }
        }
    }
}


void md::BybitMarketClientV5::start(){
    construct();
    for(auto &unit: tokenUnitVec){
        //共用一个smc即可
        unit.smc = smc;
        unit.construct();
        if(unit.subValue.has_field("args")){
            unit.start();
            usleep(10000);
        }
        else{
            LOG_INFO("no need to start: %s",unit.getString().c_str());
        }
    }

    std::thread printStatThread(&md::BybitMarketClientV5::print_stat, this);
    printStatThread.detach();
}
