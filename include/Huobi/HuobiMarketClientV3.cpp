#include "HuobiMarketClientV3.h"
#include "utils.h"

void md::HuobiUnit::sub_websocket(){
    try{
        LOG_INFO("start to sub_websocket to %s",m_wsBaseUrl.c_str() );
        uri_builder builder(m_wsBaseUrl);
        wsClient.close();
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
            subValue["id"] = json::value::string("id" + to_string(subCount++));
            websocket_outgoing_message outMsg;
            outMsg.set_utf8_message(subValue.serialize().c_str());
            wsClient.send(outMsg).wait();
            LOG_INFO("%s send success: %s",exchIdStr.c_str(), subValue.serialize().c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(110));
        }
//        LOG_INFO("%s send %lu sub values to %s",exchIdStr.c_str(),subValueVec.size(), m_wsBaseUrl.c_str());
        m_IsConnected = true;
    }
    catch(std::exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s,%s,%s,%s,%d,%s",exchIdStr.c_str(),instTypeStr.c_str(),
                  marketTypeStr.c_str(), __FUNCTION__ , __LINE__, e.what());
    }
}

void md::HuobiUnit::ping(){

}

void md::HuobiUnit::pong(){

}

void md::HuobiUnit::pong(const string &tick) {
    try{
        if (m_IsConnected) {
            json::value pong;
            pong["pong"] = json::value::number(stol(tick));
            websocket_outgoing_message outMsg;
            outMsg.set_utf8_message(pong.serialize().c_str());//sub.serialize());
            wsClient.send(outMsg);
        }
    }
    catch(std::exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s,%s,%s,%s,%d,%s",exchIdStr.c_str(),instTypeStr.c_str(),
                  marketTypeStr.c_str(), __FUNCTION__ , __LINE__, e.what());
    }
}

//https://microsoft.github.io/cpprestsdk/classweb_1_1websockets_1_1client_1_1websocket__incoming__message.html
void md::HuobiUnit::on_websocket_msg(const websocket_incoming_message &in_msg){
    try{
        if (in_msg.message_type() == websocket_message_type::binary_message){
            char buf[MD_LENGTH] = {0};
            char sbuf[MD_LENGTH] = {0};
            unsigned int l = MD_LENGTH;
            in_msg.body().streambuf().getn((unsigned char *) buf, l);
            memset(sbuf, 0, MD_LENGTH);
            gzDecompress(buf, in_msg.length(), sbuf, MD_LENGTH);
            HANDLE_TEXT_MSG(sbuf)
        }
        else if (in_msg.message_type() == websocket_message_type::text_message){
            // in_msg.extract_string().then([&](const string &s){
            //     LOG_INFO("%s", s.c_str());
            // });
        }
        else if(in_msg.message_type() == websocket_message_type::ping){
            //LOG_INFO("%s ping got will reply pong",exchIdStr.c_str());
            //pong();
        }
        else if(in_msg.message_type() == websocket_message_type::pong){
            //LOG_INFO("%s pong got will reply pong",exchIdStr.c_str());
            //ping();
        }
        else{
            // DEBUGLINE
        }
    }
    catch(std::exception &e){
        LOG_ERROR("%s",e.what());
    }
}

void md::HuobiUnit::construct(){
//            cout << getString() << endl;
    LOG_INFO("%s", getString().c_str());
    exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
    //先构造 ws地址
    if(subMarketTypeEnum == SMT_SPOT_MD){
        if (marketTypeEnum == MBPType) {
            m_wsBaseUrl = HUOBI_WEBSOCKET_HOST_PUBLIC_MBP;
        }
        else {
            m_wsBaseUrl = HUOBI_WEBSOCKET_HOST_PUBLIC_SPOT;
        }
    }
    else{
        cryptothrow(exchIdStr + " not support your sub market " + instTypeStr + "type!",-1);
    }

    //开始构造订阅格式
    for(auto instId : subStrVec){
        string strOriginInstId = crypto::to_lower(instId);
        json::value value;
        char param[64];
        if(subMarketTypeEnum == SMT_SPOT_MD){
            if(marketTypeEnum == DEPTH1 || marketTypeEnum == DEPTH5
               ||marketTypeEnum == DEPTH10 ||marketTypeEnum == DEPTH20){
                if(marketTypeEnum == DEPTH1){
                    value["channel"] = json::value::string("spot.book_ticker");
                    value["payload"][0] = json::value::string(strOriginInstId.c_str());
                }
                else{
                    sprintf(param, "market.%s.mbp.refresh.", strOriginInstId.c_str());
                    if (marketTypeEnum == DEPTH5) {
                        value["sub"] = json::value::string(string(param) + string("5"));
                    }
                    else if (marketTypeEnum == DEPTH10) {
                        value["sub"] = json::value::string(string(param) + string("10"));
                    }
                    else if (marketTypeEnum == DEPTH20) {
                        value["sub"] = json::value::string(string(param) + string("20"));
                    }
                    else{
                        LOG_ERROR("not support markettype:%s now",marketTypeStr.c_str());
//                        cryptothrow("not support now " + marketTypeStr, -1);
                    }
                }
            }
            else if (marketTypeEnum == TRADES) {
                sprintf(param, "market.%s.trade.detail", strOriginInstId.c_str());
                value["sub"] = json::value::string(param);
            }
            else if (marketTypeEnum == MBPType) {
                sprintf(param, "market.%s.mbp.150", strOriginInstId.c_str());
                value["sub"] = json::value::string(param);
                needMBP = true;
            }
            else{
                LOG_ERROR("not support markettype:%s now",marketTypeStr.c_str());
//                cryptothrow("not support markettype: "+marketTypeStr, -1);
            }
        }
        subValueVec.push_back(value);
    }

}

void md::HuobiMarketClient::print_stat() {
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

void md::HuobiMarketClient::construct() {
//    m_spotRestBaseUrl = Huobi_REST_HOST_PUBLIC_SPOT;
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
                    HuobiUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
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
                        HuobiUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
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
                        HuobiUnit unit = {ExchangeTypeStr2EnumMap[exchId],InstTypeStr2EnumMap[instType],
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

void md::HuobiMarketClient::start(){
    //这里启动一个线程以便不阻塞主线程
    std::thread subThread([&](){
        construct();
        for(auto &unit: tokenUnitVec){
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

    });
    subThread.detach();

    std::thread spotRestThread(&md::HuobiMarketClient::req_spot_mbp, this);
    spotRestThread.detach();

    std::thread printStatThread(&md::HuobiMarketClient::print_stat, this);
    printStatThread.detach();
}

//处理消息 解析json并发送给redis或共享内存
void md::HuobiUnit::save_md_string(const MDMsg &mdMsg) {
    switch (mdMsg.header.subMarketTypeEnum) {
        case SMT_SPOT_MD:
            save_spot_md(mdMsg);
            break;
        case SMT_USDT_SWAP:
        case SMT_USD_SWAP:
        case SMT_USDT_FUTURES:
        case SMT_USD_FUTURES:
//            save_swap_md(mdMsg);
            break;
        default:
            break;
    }

}

void md::HuobiUnit::save_spot_md(const MDMsg &mdMsg){
    rapidjson::Document d;
    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(mdMsg.body.data.c_str());
    if(d.HasParseError() || !rawData.IsObject() ){
        LOG_ERROR("msg :%s is not a json object", mdMsg.body.data.c_str());
        return;
    }

    if (rawData.HasMember("ping")) {
        pong(rawData["ping"].GetString());
        return;
    }
    MarketType marketTypeEnum = mdMsg.header.marketTypeEnum;
    string ch;
    if(rawData.HasMember("ch")){//subscribe data
        ch = string(rawData["ch"].GetString());
//        mdMsg.header.marketTypeEnum = MBPType;
    }
    else if(rawData.HasMember("rep")){//req data
        ch = string(rawData["rep"].GetString());
        marketTypeEnum = REQ_MBPType;
    }
    else {
        //LOG_ERROR("%s.%s.%s useless data: %s",exchIdStr.c_str(), instTypeStr.c_str(),
        //          marketTypeStr.c_str(), mdMsg.body.data.c_str());
        return;
    }

    char key[64]={0};
    char mdStr[2048];
    vector<string> s_vec = crypto::split(ch, '.');
    string originInstId = crypto::to_upper(s_vec[1]);
    InstrumentInfo info;
    if(smc->get_instrument_info(exchIdStr.c_str(), instTypeStr.c_str(),
                                originInstId.c_str(), info) == false){
        LOG_ERROR("not found %s info in smc", originInstId.c_str());
        return;
    }
    sprintf(key, "%s.%s.%s.%s", exchIdStr.c_str(), instTypeStr.c_str(),
            marketTypeStr.c_str(), info.instId);
    if (marketTypeEnum == DEPTH5
        || marketTypeEnum == DEPTH10
        || marketTypeEnum == DEPTH20) {
        const rapidjson::Value &data = rawData["tick"];
        int nBids = data["bids"].Size();
        int nAsks = data["asks"].Size();
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
                exchIdStr.c_str(), instTypeStr.c_str(),
                marketTypeStr.c_str(), info.instId,
                asksStr.c_str(),
                bidsStr.c_str(),
                stol(rawData["ts"].GetString())* 1000,
                mdMsg.tsNet, crypto::getCurrentTime()
        );
    }
    else if (marketTypeEnum == MBPType) {
        string topic = key;
        const rapidjson::Value &data = rawData["tick"];
        long ts = atoll(rawData["ts"].GetString()) ;
        MBP mbp;

        mbp.seqNum = atoll(data["seqNum"].GetString());
        mbp.prevSeqNum = atoll(data["prevSeqNum"].GetString());
        if(data.HasMember("asks")){
            const rapidjson::Value &asks = data["asks"];
            for (rapidjson::SizeType i = 0; i < asks.Size(); i++) {
                DepthPair depthPair;
                depthPair.price = asks[i][0].GetString();
                depthPair.size = asks[i][1].GetString();
                mbp.asks.push_back(depthPair);
            }
        }
        if(data.HasMember("bids")){
            const rapidjson::Value &bids = data["bids"];
            for (rapidjson::SizeType i = 0; i < bids.Size(); i++) {
                DepthPair depthPair;
                depthPair.price = bids[i][0].GetString();
                depthPair.size = bids[i][1].GetString();
                mbp.bids.push_back(depthPair);
            }
        }

        if(lobDict.count(topic) > 0) {
            update_mbp(topic, mbp);
            if (lobDict[topic]->isReady == true) {
                long tsParse = crypto::getCurrentTime();
                string asksStr, bidsStr;
                lobDict[topic]->get_asks_bids(asksStr, bidsStr);
                char longMdStr[2048 * 64];//65536 is small and will cause segment fault
//                        long tsParse2 = crypto::getCurrentTime();
                sprintf(longMdStr, DEPTH_Format,
                        exchIdStr.c_str(), instTypeStr.c_str(), marketTypeStr.c_str(), info.instId,
                        asksStr.c_str(),
                        bidsStr.c_str(),
                        ts * 1000,
                        mdMsg.tsNet, tsParse
                );
                if(lobDict[topic]->check_data() == true){
                    //抹掉所有数据
                    lobDict.erase(topic);
                    LOG_ERROR("%s, orderbook check failed, will erase",key);
                    return;
                }
                else{
                    redis_cmd(mdMsg, key, longMdStr);
                }
                // redis_cmd(mdMsg, key, longMdStr);
                return;
            }
        }
        else{
            if(mbpCache.count(topic) > 0){
                mbpCache[topic].push_back(mbp);
            }
            else{
                vector<MBP> mbpVec ;
                mbpVec.push_back(mbp);
                mbpCache[topic] = mbpVec;
            }
        }
        return;
#if 0
        string topic = key;
        MBP mbp;
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
        const rapidjson::Value &asks = data["asks"];
        const rapidjson::Value &bids = data["bids"];
        mbp.seqNum = atoll(data["seqNum"].GetString());
        if(data.HasMember("prevSeqNum")){
            mbp.prevSeqNum = atoll(data["prevSeqNum"].GetString());
        }else{
            mbp.prevSeqNum = 0;
        }
        long ts = atoll(rawData["ts"].GetString()) * 1000;
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
                if(lobDict[topic]->check_data() == true){
                    //先抹掉所有数据，再重新订阅
                    lobDict.erase(topic);
                    LOG_ERROR("%s, orderbook check failed, will wait req mbp data",key);
//                    subscribe(originInstId);
                    return;
                }
                else{
                    redis_cmd(mdMsg, key, longMdStr);
                }
//                redis_cmd(mdMsg, key, longMdStr);
            }
        }
        else {
            mbpCache[topic].push_back(mbp);
        }
        return;
#endif
    }
    else if (marketTypeEnum == REQ_MBPType) {
        string topic;
        topic.append(exchIdStr).append(".").append(instTypeStr).append(".MBP.").append(info.instId);
        const rapidjson::Value &data = rawData["data"];
        MBP mbp;
        const rapidjson::Value &asks = data["asks"];
        const rapidjson::Value &bids = data["bids"];
//                mbp.seqNum = 0;//atol(data["U"].GetString());
//                mbp.prevSeqNum = atoll(data["lastUpdateId"].GetString());
        mbp.prevSeqNum  = 0;//atol(data["U"].GetString());
        mbp.seqNum = atoll(data["seqNum"].GetString());
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
        update_mbp(topic, mbp);//小u 是seqNum  大U是preSeqNum
        if(mbpCache.count(topic) > 0){
            for(auto m : mbpCache[topic]){
                if(m.seqNum > mbp.seqNum){
                    update_mbp(topic, m);
                }
            }
            mbpCache.erase(topic);
//                mbpCache.unsafe_erase(topic);
        }
        return;
    }
    else if (marketTypeEnum == TRADES) {
        const rapidjson::Value &data = rawData["tick"]["data"][0];
        sprintf(mdStr, Trades_Format,
                exchIdStr.c_str(), instTypeStr.c_str(),
                marketTypeStr.c_str(), info.instId,
                data["tradeId"].GetString(),
                stod(data["price"].GetString()),
                stod(data["amount"].GetString()),//TODO
                data["direction"].GetString(),
                stol(data["ts"].GetString()) * 1000,
                mdMsg.tsNet, crypto::getCurrentTime()
        );
    }
    else{
        LOG_ERROR("not support marketType:%s", marketTypeStr.c_str());
        return;
    }
    redis_cmd(mdMsg,key,mdStr);
}

void md::HuobiMarketClient::req_spot_mbp() {
    int sleepSeconds = 30;
    //防止websocket还未建立连接
    std::this_thread::sleep_for(std::chrono::seconds(6));
    while(1){
        try{
            LOG_INFO("mbp req circle");
            for(auto &unit: tokenUnitVec){
                if(unit.needMBP){
                    if(unit.m_IsConnected){
                        for(auto sb : unit.subStrVec){
                            if(unit.subMarketTypeEnum == SMT_SPOT_MD){
                                char param[64];
                                string symbol = crypto::to_lower(sb.c_str());
                                json::value value;
                                sprintf(param, "market.%s.mbp.150", symbol.c_str());
                                value["req"] = json::value::string(param);
                                value["id"] = json::value::string("id" + std::to_string(subId++));
                                websocket_outgoing_message outMsg;
                                outMsg.set_utf8_message(value.serialize().c_str());
                                // LOG_INFO("send success: %s", value.serialize().c_str());
                                unit.wsClient.send(outMsg);
                            }
                            usleep(100000);//0.01s
                        }
                    }
                }
            }
        }
        catch (std::exception& ex) {
            LOG_ERROR("Exception: %s",ex.what());
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
        LOG_INFO("mbp req end");
        std::this_thread::sleep_for(std::chrono::seconds(sleepSeconds));
    }
}
