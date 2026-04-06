#include "gateio/GateioMarket.h"


md::GateioUnit::GateioUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* passwd) : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    pWsClient = nullptr;
}

void md::GateioUnit::generateSubBody() {
    // LOG_INFO("%s", getString().c_str());
    std::string exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    std::string instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    std::string marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
    std::string lowerMarketTypeStr = crypto::to_lower(marketTypeStr);

    if (instTypeEnum == SPOT) {
        wsUrl = GATEIO_WS_PUBLIC_SPOT;
    }
    else if (instTypeEnum == USDT_SWAP) {
        wsUrl = GATEIO_WS_PUBLIC_USDT_SWAP;
    }
    else if (instTypeEnum == USDT_FUTURES) {
        wsUrl = GATEIO_WS_PUBLIC_USDT_FUTURES;
    }
    else if (instTypeEnum == BTC_SWAP) {
        wsUrl = GATEIO_WS_PUBLIC_BTC_SWAP;
    }
    else if (instTypeEnum == BTC_FUTURES) {
        wsUrl = GATEIO_WS_PUBLIC_BTC_FUTURES;
    }


    web::json::value value; 
    value["event"] = web::json::value::string("subscribe");

    for (auto info : vInstInfo) {
        if (instTypeEnum == SPOT) {
            if (marketTypeEnum == md::DEPTH1 || marketTypeEnum == md::DEPTH5 || marketTypeEnum == md::DEPTH10 || marketTypeEnum == md::DEPTH20) {
                if (marketTypeEnum == md::DEPTH1) {
                    value["channel"] = web::json::value::string("spot.book_ticker");
                    value["payload"][0] = web::json::value::string(info.originInstId);
                }
                else {
                    value["channel"] = web::json::value::string("spot.order_book");
                    value["payload"][0] = web::json::value::string(info.originInstId);
                    
                    if (marketTypeEnum == md::DEPTH5) {
                        value["payload"][1] = web::json::value::string("5");
                    }
                    else if (marketTypeEnum == md::DEPTH10) {
                        value["payload"][1] = web::json::value::string("10");
                    }
                    else if (marketTypeEnum == md::DEPTH20) {
                        value["payload"][1] = web::json::value::string("20");
                    }
                    else {
                        LOG_ERROR("not support {}", marketTypeStr);
                    }
                    value["payload"][2] = web::json::value::string("100ms");
                }
            }
            else if (marketTypeEnum == md::TRADES) {
                value["channel"] = web::json::value::string("spot.trades");
                value["payload"][0] = web::json::value::string(info.originInstId);   
            }
            else if (marketTypeEnum == md::KLINE_1m) {
                value["channel"] = web::json::value::string("spot.candlesticks");
                value["payload"][0] = web::json::value::string("1m"); 
                value["payload"][1] = web::json::value::string(info.originInstId);   
            }
            else {
                LOG_ERROR("not support {}", marketTypeStr);
            }
        }
        else if (instTypeEnum == USDT_SWAP || instTypeEnum == BTC_SWAP || instTypeEnum == USDT_FUTURES || instTypeEnum == BTC_FUTURES) {
            if (marketTypeEnum == md::DEPTH1 || marketTypeEnum == md::DEPTH5 || marketTypeEnum == md::DEPTH10 || marketTypeEnum == md::DEPTH20) {
                if (marketTypeEnum == md::DEPTH1) {
                    value["channel"] = web::json::value::string("futures.book_ticker");
                    value["payload"][0] = web::json::value::string(info.originInstId);
                }
                else {
                    value["channel"] = web::json::value::string("futures.order_book");
                    value["payload"][0] = web::json::value::string(info.originInstId);
                    
                    if (marketTypeEnum == md::DEPTH5) {
                        value["payload"][1] = web::json::value::string("5");
                    }
                    else if (marketTypeEnum == md::DEPTH10) {
                        value["payload"][1] = web::json::value::string("10");
                    }
                    else if (marketTypeEnum == md::DEPTH20) {
                        value["payload"][1] = web::json::value::string("20");
                    }
                    else {
                        LOG_ERROR("not support {}", marketTypeStr);
                    }
                    value["payload"][2] = web::json::value::string("0");
                }
            }
            else if (marketTypeEnum == md::TRADES) {
                value["channel"] = web::json::value::string("futures.trades");
                value["payload"][0] = web::json::value::string(info.originInstId);   
            }
            else if (marketTypeEnum == md::KLINE_1m) {
                value["channel"] = web::json::value::string("futures.candlesticks");
                value["payload"][0] = web::json::value::string("1m"); 
                value["payload"][1] = web::json::value::string(info.originInstId);   
            }
            else if (marketTypeEnum == md::FUNDING_RATE) {
                value["channel"] = web::json::value::string("futures.tickers");
                value["payload"][0] = web::json::value::string(info.originInstId);   
            }
            else {
                LOG_ERROR("not support {}", marketTypeStr);
            }     
        }
        else {
            LOG_ERROR("not support subMarketType: {}", instTypeStr);
        }
    }

    subValueVec.push_back(value);
}

void md::GateioUnit::subWebsocekt() {
START_SUB_WEBSOCKET()

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    for (auto& subValue : subValueVec) {
        subValue["time"] = crypto::getCurrentTimeSeconds();
        web::websockets::client::websocket_outgoing_message outMsg;
        outMsg.set_utf8_message(subValue.serialize().c_str());
        LOG_INFO("{} send {} to {}", ExchangeTypeEnum2StrMap[exchangeTypeEnum], subValue.serialize(), wsUrl);
        pWsClient->send(outMsg).wait();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

END_SUB_WEBSOCKET()
}

void md::GateioUnit::ping(){
    try {
        if (pWsClient != nullptr && isConnected) {
            web::websockets::client::websocket_outgoing_message outMsg;
            web::json::value pingSubValue;
            pingSubValue["time"] = crypto::getCurrentTimeSeconds();
            if (instTypeEnum == SPOT) {
                pingSubValue["channel"] = web::json::value::string("futures.ping");
            }
            else {
                pingSubValue["channel"] = web::json::value::string("spot.ping");
            }
            outMsg.set_utf8_message(pingSubValue.serialize().c_str());
            pWsClient->send(outMsg).wait();
        }
    }
    catch (const std::exception& e) {
        isConnected = false;
        LOG_ERROR("pong error: {}", e.what());
    }
}

void md::GateioUnit::pong() {

}

void md::GateioUnit::onWebsocketMsg(const web::websockets::client::websocket_incoming_message& msg) {
    latestDataUpdateTime = crypto::getCurrentTime();

    switch (msg.message_type()) {
        case  web::websockets::client::websocket_message_type::text_message: {
            const string& s = msg.extract_string().get();
            std::cout << "onWebsocketMsg: " << s << std::endl;
            mQueue.push(s);
            return;
        }
        case web::websockets::client::websocket_message_type::ping: {
            LOG_INFO("{}.{}.{} got ping message type.", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }
        case web::websockets::client::websocket_message_type::pong: {
            LOG_INFO("{}.{}.{} got pong message type.", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }
        case web::websockets::client::websocket_message_type::close: {
            LOG_WARN("{}.{}.{} got close message type.", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], md::MarketTypeEnum2StrMap[marketTypeEnum]);
            isConnected = false;
            return;
        }
        default: {
            LOG_ERROR("{}.{}.{} got unknown message type.", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], md::MarketTypeEnum2StrMap[marketTypeEnum]);
        }
    }
}

//处理消息 解析json并发送给redis或共享内存
void md::GateioUnit::parseMarketData(const std::string& msg) {
    switch (instTypeEnum) {
        case SPOT:
            parseSpotData(msg);
            return;
        case USDT_SWAP:
            parseSwapData(msg);
            return;
        default:
            return;
    }
}

void md::GateioUnit::parseSpotData(const std::string& msg) {
    long tsNet = crypto::getCurrentTime();

    rapidjson::Document d;
    rapidjson::Value& rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(msg.c_str());

    if (d.HasParseError() || !rawData.IsObject()) {
        LOG_ERROR("msg: {} is not a json object.", msg);
        return;
    }

    const std::string& event = rawData["event"].GetString();
    if (!crypto::str_cmp(event.c_str(), "update")) {
        LOG_DEBUG("msg: {} has no update data.", msg);
        return;
    }

    const rapidjson::Value& data = rawData["result"];
    std::string originInstId = "";
    
    switch (marketTypeEnum) {
        case md::DEPTH1:
        case md::DEPTH5:
        case md::DEPTH10:
        case md::DEPTH20: {
            originInstId = data["s"].GetString();
            break;
        }
        case md::TRADES: {
            originInstId = data["currency_pair"].GetString();
            break;        
        }
        case md::KLINE_1m: {
            const std::string& n = data["n"].GetString();
            std::vector<std::string> v = crypto::split(n, "_");
            if (v.size() >= 3) {
                originInstId = v[1] + "_" + v[2];
            }
            break;
        }
        default: {
            LOG_ERROR("not support marketType: {}", md::MarketTypeEnum2StrMap[marketTypeEnum]);
        }
    }

    md::InstrumentInfo info;
    if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false){
        LOG_ERROR("msg: {}", msg);
        return;
    }

    std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

    switch (marketTypeEnum) {
        case md::DEPTH1: {
            md::Depth1 depth1;
            memset(&depth1, 0, sizeof(md::Depth1));
            depth1.exchangeTypeEnum = exchangeTypeEnum;
            depth1.instTypeEnum = instTypeEnum;
            depth1.marketTypeEnum = marketTypeEnum;
            strncpy(depth1.instId, info.instId, INSTID_SIZE);

            long ts = std::stol(rawData["time_ms"].GetString()) * 1000;
            depth1.tsTrans = ts;
            depth1.tsEvent = ts;
            depth1.tsRecv = tsNet;
            depth1.tsParse = crypto::getCurrentTime();

            depth1.bp1 = std::stod(data["b"].GetString()) * info.reduceNumber;
            depth1.bv1 = std::stod(data["B"].GetString()) * info.magnifyNumber;
            depth1.ap1 = std::stod(data["a"].GetString()) * info.reduceNumber;
            depth1.av1 = std::stod(data["A"].GetString()) * info.magnifyNumber;
#ifdef NEED_SHM
            mDepth1Publisher[key]->push(depth1);                
#endif
            return;
        }

        case md::DEPTH5: {
            md::Depth5 depth5;
            memset(&depth5, 0, sizeof(md::Depth5));
            depth5.exchangeTypeEnum = exchangeTypeEnum;
            depth5.instTypeEnum = instTypeEnum;
            depth5.marketTypeEnum = marketTypeEnum;
            strncpy(depth5.instId, info.instId, INSTID_SIZE);

            long ts = std::stol(data["t"].GetString()) * 1000;
            depth5.tsTrans = ts;
            depth5.tsEvent = ts;
            depth5.tsRecv = tsNet;
            depth5.tsParse = crypto::getCurrentTime();

            int asksSize = data["asks"].Size();
            int bidsSize = data["bids"].Size();

            if (asksSize >= 5 && bidsSize >= 5) {
                depth5.bp1 = std::stod(data["bids"][0][0].GetString());
                depth5.bv1 = std::stod(data["bids"][0][1].GetString());
                depth5.bp2 = std::stod(data["bids"][1][0].GetString());
                depth5.bv2 = std::stod(data["bids"][1][1].GetString());
                depth5.bp3 = std::stod(data["bids"][2][0].GetString());
                depth5.bv3 = std::stod(data["bids"][2][1].GetString());
                depth5.bp4 = std::stod(data["bids"][3][0].GetString());
                depth5.bv4 = std::stod(data["bids"][3][1].GetString());
                depth5.bp5 = std::stod(data["bids"][4][0].GetString());
                depth5.bv5 = std::stod(data["bids"][4][1].GetString());

                depth5.ap1 = std::stod(data["asks"][0][0].GetString());
                depth5.av1 = std::stod(data["asks"][0][1].GetString());
                depth5.ap2 = std::stod(data["asks"][1][0].GetString());
                depth5.av2 = std::stod(data["asks"][1][1].GetString());
                depth5.ap3 = std::stod(data["asks"][2][0].GetString());
                depth5.av3 = std::stod(data["asks"][2][1].GetString());
                depth5.ap4 = std::stod(data["asks"][3][0].GetString());
                depth5.av4 = std::stod(data["asks"][3][1].GetString());
                depth5.ap5 = std::stod(data["asks"][4][0].GetString());
                depth5.av5 = std::stod(data["asks"][4][1].GetString());
            }

#ifdef NEED_SHM
            mDepth5Publisher[key]->push(depth5);                
#endif
            return;
        }  
        case md::DEPTH10: {
            md::Depth10 depth10;
            memset(&depth10, 0, sizeof(md::Depth10));
            depth10.exchangeTypeEnum = exchangeTypeEnum;
            depth10.instTypeEnum = instTypeEnum;
            depth10.marketTypeEnum = marketTypeEnum;
            strncpy(depth10.instId, info.instId, INSTID_SIZE);

            long ts = std::stol(data["t"].GetString()) * 1000;
            depth10.tsTrans = ts;
            depth10.tsEvent = ts;
            depth10.tsRecv = tsNet;
            depth10.tsParse = crypto::getCurrentTime();

            int asksSize = data["asks"].Size();
            int bidsSize = data["bids"].Size();

            if (asksSize >= 10 && bidsSize >= 10) {
                depth10.bp1 = std::stod(data["bids"][0][0].GetString());
                depth10.bv1 = std::stod(data["bids"][0][1].GetString());
                depth10.bp2 = std::stod(data["bids"][1][0].GetString());
                depth10.bv2 = std::stod(data["bids"][1][1].GetString());
                depth10.bp3 = std::stod(data["bids"][2][0].GetString());
                depth10.bv3 = std::stod(data["bids"][2][1].GetString());
                depth10.bp4 = std::stod(data["bids"][3][0].GetString());
                depth10.bv4 = std::stod(data["bids"][3][1].GetString());
                depth10.bp5 = std::stod(data["bids"][4][0].GetString());
                depth10.bv5 = std::stod(data["bids"][4][1].GetString());
                depth10.bp6 = std::stod(data["bids"][5][0].GetString());
                depth10.bv6 = std::stod(data["bids"][5][1].GetString());
                depth10.bp7 = std::stod(data["bids"][6][0].GetString());
                depth10.bv7 = std::stod(data["bids"][6][1].GetString());
                depth10.bp8 = std::stod(data["bids"][7][0].GetString());
                depth10.bv8 = std::stod(data["bids"][7][1].GetString());
                depth10.bp9 = std::stod(data["bids"][8][0].GetString());
                depth10.bv9 = std::stod(data["bids"][8][1].GetString());
                depth10.bp10 = std::stod(data["bids"][9][0].GetString());
                depth10.bv10 = std::stod(data["bids"][9][1].GetString());

                depth10.ap1 = std::stod(data["asks"][0][0].GetString());
                depth10.av1 = std::stod(data["asks"][0][1].GetString());
                depth10.ap2 = std::stod(data["asks"][1][0].GetString());
                depth10.av2 = std::stod(data["asks"][1][1].GetString());
                depth10.ap3 = std::stod(data["asks"][2][0].GetString());
                depth10.av3 = std::stod(data["asks"][2][1].GetString());
                depth10.ap4 = std::stod(data["asks"][3][0].GetString());
                depth10.av4 = std::stod(data["asks"][3][1].GetString());
                depth10.ap5 = std::stod(data["asks"][4][0].GetString());
                depth10.av5 = std::stod(data["asks"][4][1].GetString());
                depth10.ap6 = std::stod(data["asks"][5][0].GetString());
                depth10.av6 = std::stod(data["asks"][5][1].GetString());
                depth10.ap7 = std::stod(data["asks"][6][0].GetString());
                depth10.av7 = std::stod(data["asks"][6][1].GetString());
                depth10.ap8 = std::stod(data["asks"][7][0].GetString());
                depth10.av8 = std::stod(data["asks"][7][1].GetString());
                depth10.ap9 = std::stod(data["asks"][8][0].GetString());
                depth10.av9 = std::stod(data["asks"][8][1].GetString());
                depth10.ap10 = std::stod(data["asks"][9][0].GetString());
                depth10.av10 = std::stod(data["asks"][9][1].GetString());
            }

#ifdef NEED_SHM
            mDepth10Publisher[key]->push(depth10);                
#endif
            return;
        }
        case md::DEPTH20: {
            md::Depth20 depth20;
            memset(&depth20, 0, sizeof(md::Depth20));
            depth20.exchangeTypeEnum = exchangeTypeEnum;
            depth20.instTypeEnum = instTypeEnum;
            depth20.marketTypeEnum = marketTypeEnum;
            strncpy(depth20.instId, info.instId, INSTID_SIZE);

            long ts = std::stol(data["t"].GetString()) * 1000;
            depth20.tsTrans = ts;
            depth20.tsEvent = ts;
            depth20.tsRecv = tsNet;
            depth20.tsParse = crypto::getCurrentTime();

            int asksSize = data["asks"].Size();
            int bidsSize = data["bids"].Size();

            if (asksSize >= 20 && bidsSize >= 20) {
                depth20.bp1 = std::stod(data["bids"][0][0].GetString());
                depth20.bv1 = std::stod(data["bids"][0][1].GetString());
                depth20.bp2 = std::stod(data["bids"][1][0].GetString());
                depth20.bv2 = std::stod(data["bids"][1][1].GetString());
                depth20.bp3 = std::stod(data["bids"][2][0].GetString());
                depth20.bv3 = std::stod(data["bids"][2][1].GetString());
                depth20.bp4 = std::stod(data["bids"][3][0].GetString());
                depth20.bv4 = std::stod(data["bids"][3][1].GetString());
                depth20.bp5 = std::stod(data["bids"][4][0].GetString());
                depth20.bv5 = std::stod(data["bids"][4][1].GetString());
                depth20.bp6 = std::stod(data["bids"][5][0].GetString());
                depth20.bv6 = std::stod(data["bids"][5][1].GetString());
                depth20.bp7 = std::stod(data["bids"][6][0].GetString());
                depth20.bv7 = std::stod(data["bids"][6][1].GetString());
                depth20.bp8 = std::stod(data["bids"][7][0].GetString());
                depth20.bv8 = std::stod(data["bids"][7][1].GetString());
                depth20.bp9 = std::stod(data["bids"][8][0].GetString());
                depth20.bv9 = std::stod(data["bids"][8][1].GetString());
                depth20.bp10 = std::stod(data["bids"][9][0].GetString());
                depth20.bv10 = std::stod(data["bids"][9][1].GetString());
                depth20.bp11 = std::stod(data["bids"][10][0].GetString());
                depth20.bv11 = std::stod(data["bids"][10][1].GetString());
                depth20.bp12 = std::stod(data["bids"][11][0].GetString());
                depth20.bv12 = std::stod(data["bids"][11][1].GetString());
                depth20.bp13 = std::stod(data["bids"][12][0].GetString());
                depth20.bv13 = std::stod(data["bids"][12][1].GetString());
                depth20.bp14 = std::stod(data["bids"][13][0].GetString());
                depth20.bv14 = std::stod(data["bids"][13][1].GetString());
                depth20.bp15 = std::stod(data["bids"][14][0].GetString());
                depth20.bv15 = std::stod(data["bids"][14][1].GetString());
                depth20.bp16 = std::stod(data["bids"][15][0].GetString());
                depth20.bv16 = std::stod(data["bids"][15][1].GetString());
                depth20.bp17 = std::stod(data["bids"][16][0].GetString());
                depth20.bv17 = std::stod(data["bids"][16][1].GetString());
                depth20.bp18 = std::stod(data["bids"][17][0].GetString());
                depth20.bv18 = std::stod(data["bids"][17][1].GetString());
                depth20.bp19 = std::stod(data["bids"][18][0].GetString());
                depth20.bv19 = std::stod(data["bids"][18][1].GetString());
                depth20.bp20 = std::stod(data["bids"][19][0].GetString());
                depth20.bv20 = std::stod(data["bids"][19][1].GetString());

                depth20.ap1 = std::stod(data["asks"][0][0].GetString());
                depth20.av1 = std::stod(data["asks"][0][1].GetString());
                depth20.ap2 = std::stod(data["asks"][1][0].GetString());
                depth20.av2 = std::stod(data["asks"][1][1].GetString());
                depth20.ap3 = std::stod(data["asks"][2][0].GetString());
                depth20.av3 = std::stod(data["asks"][2][1].GetString());
                depth20.ap4 = std::stod(data["asks"][3][0].GetString());
                depth20.av4 = std::stod(data["asks"][3][1].GetString());
                depth20.ap5 = std::stod(data["asks"][4][0].GetString());
                depth20.av5 = std::stod(data["asks"][4][1].GetString());
                depth20.ap6 = std::stod(data["asks"][5][0].GetString());
                depth20.av6 = std::stod(data["asks"][5][1].GetString());
                depth20.ap7 = std::stod(data["asks"][6][0].GetString());
                depth20.av7 = std::stod(data["asks"][6][1].GetString());
                depth20.ap8 = std::stod(data["asks"][7][0].GetString());
                depth20.av8 = std::stod(data["asks"][7][1].GetString());
                depth20.ap9 = std::stod(data["asks"][8][0].GetString());
                depth20.av9 = std::stod(data["asks"][8][1].GetString());
                depth20.ap10 = std::stod(data["asks"][9][0].GetString());
                depth20.av10 = std::stod(data["asks"][9][1].GetString());
                depth20.ap11 = std::stod(data["asks"][10][0].GetString());
                depth20.av11 = std::stod(data["asks"][10][1].GetString());
                depth20.ap12 = std::stod(data["asks"][11][0].GetString());
                depth20.av12 = std::stod(data["asks"][11][1].GetString());
                depth20.ap13 = std::stod(data["asks"][12][0].GetString());
                depth20.av13 = std::stod(data["asks"][12][1].GetString());
                depth20.ap14 = std::stod(data["asks"][13][0].GetString());
                depth20.av14 = std::stod(data["asks"][13][1].GetString());
                depth20.ap15 = std::stod(data["asks"][14][0].GetString());
                depth20.av15 = std::stod(data["asks"][14][1].GetString());
                depth20.ap16 = std::stod(data["asks"][15][0].GetString());
                depth20.av16 = std::stod(data["asks"][15][1].GetString());
                depth20.ap17 = std::stod(data["asks"][16][0].GetString());
                depth20.av17 = std::stod(data["asks"][16][1].GetString());
                depth20.ap18 = std::stod(data["asks"][17][0].GetString());
                depth20.av18 = std::stod(data["asks"][17][1].GetString());
                depth20.ap19 = std::stod(data["asks"][18][0].GetString());
                depth20.av19 = std::stod(data["asks"][18][1].GetString());
                depth20.ap20 = std::stod(data["asks"][19][0].GetString());
                depth20.av20 = std::stod(data["asks"][19][1].GetString());

            }

#ifdef NEED_SHM
            mDepth20Publisher[key]->push(depth20);                
#endif
            return;
        }
        case md::TRADES: {
            md::Trades trades;
            memset(&trades, 0, sizeof(md::Trades));
            trades.exchangeTypeEnum = exchangeTypeEnum;
            trades.instTypeEnum = instTypeEnum;
            trades.marketTypeEnum = marketTypeEnum;
            strncpy(trades.instId, info.instId, INSTID_SIZE);

            long ts = stol(data["create_time_ms"].GetString()) * 1000;

            trades.tsTrans = ts;
            trades.tsEvent = ts;
            trades.tsRecv = tsNet;
            trades.tsParse = crypto::getCurrentTime();

            strncpy(trades.tradeId, data["id"].GetString(), INSTID_SIZE);
            trades.px = std::stod(data["price"].GetString());
            trades.sz = std::stod(data["amount"].GetString());
            std::string side = data["side"].GetString();
            if (crypto::str_cmp(side.c_str(), "sell")) {
                trades.direction = DT_SHORT;
            }
            else if (crypto::str_cmp(side.c_str(), "buy")) {
                trades.direction = DT_LONG;
            }

#ifdef NEED_SHM
            mTradesPublisher[key]->push(trades); 
#endif
            return;
        }
        case md::KLINE_1m: {
            md::Kline kline;
            memset(&kline, 0, sizeof(md::Kline));
            kline.exchangeTypeEnum = exchangeTypeEnum;
            kline.instTypeEnum = instTypeEnum;
            kline.marketTypeEnum = marketTypeEnum;
            strncpy(kline.instId, info.instId, INSTID_SIZE);

            long ts = stol(rawData["time_ms"].GetString()) * 1000;
            kline.tsTrans = ts;
            kline.tsEvent = ts;
            kline.tsRecv = tsNet;
            kline.tsParse = crypto::getCurrentTime();

            kline.barTime = stol(data["t"].GetString()) * 1000000;
            kline.highPrice = std::stod(data["h"].GetString());
            kline.lowPrice = std::stod(data["l"].GetString());
            kline.openPrice = std::stod(data["o"].GetString());
            kline.closePrice = std::stod(data["c"].GetString());

            double avgPrice = 0;
            double amount = std::stod(data["a"].GetString());
            double volume = std::stod(data["v"].GetString());
            if(amount > ZERO_NUM){
                avgPrice = amount / volume;
            }

            kline.avgPrice = avgPrice;
            kline.totalVolume = volume;
            kline.totalAmount = amount;
            kline.isFinished = true;

#ifdef NEED_SHM
            mKlinePublisher[key]->push(kline); 
#endif
            return;
        }
        default: {
            LOG_ERROR("not support marketType: {}", md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }
    }

}

void md::GateioUnit::parseSwapData(const std::string& msg) {
    long tsNet = crypto::getCurrentTime();

    rapidjson::Document d;
    rapidjson::Value& rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(msg.c_str());

    if (d.HasParseError() || !rawData.IsObject()) {
        LOG_ERROR("msg: {} is not a json object.", msg);
        return;
    }

    const std::string& event = rawData["event"].GetString();
    if (!crypto::str_cmp(event.c_str(), "all") && !crypto::str_cmp(event.c_str(), "update")) {
        LOG_DEBUG("msg: {} has no all or update data.", msg);
        return;
    }

    const rapidjson::Value& data = rawData["result"];
    std::string originInstId = "";

    switch (marketTypeEnum) {
        case md::DEPTH1:
            originInstId = data["s"].GetString();
            break;
        case md::DEPTH5:
        case md::DEPTH10:
        case md::DEPTH20: {
            originInstId = data["contract"].GetString();
            break;
        }
        case md::TRADES:
        case md::FUNDING_RATE: {
            originInstId = data[0]["contract"].GetString();
            break;        
        }
        case md::KLINE_1m: {
            for (rapidjson::SizeType i = 0; i < data.Size(); ++i) {
                const std::string& n = data[i]["n"].GetString();
                std::vector<std::string> v = crypto::split(n, "_");
                if (v.size() >= 3) {
                    originInstId = v[1] + "_" + v[2];
                    break;
                }
            }

            break;
        }
        default: {
            LOG_ERROR("not support marketType: {}", md::MarketTypeEnum2StrMap[marketTypeEnum]);
        }
    }

    md::InstrumentInfo info;
    if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false){
        LOG_ERROR("msg: {}", msg);
        return;
    }

    std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

    switch (marketTypeEnum) {
        case md::DEPTH1: {
            md::Depth1 depth1;
            memset(&depth1, 0, sizeof(md::Depth1));
            depth1.exchangeTypeEnum = exchangeTypeEnum;
            depth1.instTypeEnum = instTypeEnum;
            depth1.marketTypeEnum = marketTypeEnum;
            strncpy(depth1.instId, info.instId, INSTID_SIZE);

            long ts = std::stol(data["t"].GetString()) * 1000;
            depth1.tsTrans = ts;
            depth1.tsEvent = ts;
            depth1.tsRecv = tsNet;
            depth1.tsParse = crypto::getCurrentTime();

            depth1.bp1 = std::stod(data["b"].GetString()) * info.reduceNumber;
            depth1.bv1 = std::stod(data["B"].GetString()) * info.magnifyNumber;
            depth1.ap1 = std::stod(data["a"].GetString()) * info.reduceNumber;
            depth1.av1 = std::stod(data["A"].GetString()) * info.magnifyNumber;
#ifdef NEED_SHM
            mDepth1Publisher[key]->push(depth1);                
#endif
            return;
        }

        case md::DEPTH5: {
            md::Depth5 depth5;
            memset(&depth5, 0, sizeof(md::Depth5));
            depth5.exchangeTypeEnum = exchangeTypeEnum;
            depth5.instTypeEnum = instTypeEnum;
            depth5.marketTypeEnum = marketTypeEnum;
            strncpy(depth5.instId, info.instId, INSTID_SIZE);

            long ts = std::stol(rawData["time_ms"].GetString()) * 1000;
            depth5.tsTrans = ts;
            depth5.tsEvent = ts;
            depth5.tsRecv = tsNet;
            depth5.tsParse = crypto::getCurrentTime();

            int asksSize = data["asks"].Size();
            int bidsSize = data["bids"].Size();

            if (asksSize >= 5 && bidsSize >= 5) {
                depth5.bp1 = std::stod(data["bids"][0]["p"].GetString());
                depth5.bv1 = std::stod(data["bids"][0]["s"].GetString());
                depth5.bp2 = std::stod(data["bids"][1]["p"].GetString());
                depth5.bv2 = std::stod(data["bids"][1]["s"].GetString());
                depth5.bp3 = std::stod(data["bids"][2]["p"].GetString());
                depth5.bv3 = std::stod(data["bids"][2]["s"].GetString());
                depth5.bp4 = std::stod(data["bids"][3]["p"].GetString());
                depth5.bv4 = std::stod(data["bids"][3]["s"].GetString());
                depth5.bp5 = std::stod(data["bids"][4]["p"].GetString());
                depth5.bv5 = std::stod(data["bids"][4]["s"].GetString());

                depth5.ap1 = std::stod(data["asks"][0]["p"].GetString());
                depth5.av1 = std::stod(data["asks"][0]["s"].GetString());
                depth5.ap2 = std::stod(data["asks"][1]["p"].GetString());
                depth5.av2 = std::stod(data["asks"][1]["s"].GetString());
                depth5.ap3 = std::stod(data["asks"][2]["p"].GetString());
                depth5.av3 = std::stod(data["asks"][2]["s"].GetString());
                depth5.ap4 = std::stod(data["asks"][3]["p"].GetString());
                depth5.av4 = std::stod(data["asks"][3]["s"].GetString());
                depth5.ap5 = std::stod(data["asks"][4]["p"].GetString());
                depth5.av5 = std::stod(data["asks"][4]["s"].GetString());
            }

#ifdef NEED_SHM
            mDepth5Publisher[key]->push(depth5);                
#endif
            return;
        }  
        case md::DEPTH10: {
            md::Depth10 depth10;
            memset(&depth10, 0, sizeof(md::Depth10));
            depth10.exchangeTypeEnum = exchangeTypeEnum;
            depth10.instTypeEnum = instTypeEnum;
            depth10.marketTypeEnum = marketTypeEnum;
            strncpy(depth10.instId, info.instId, INSTID_SIZE);

            long ts = std::stol(rawData["time_ms"].GetString()) * 1000;
            depth10.tsTrans = ts;
            depth10.tsEvent = ts;
            depth10.tsRecv = tsNet;
            depth10.tsParse = crypto::getCurrentTime();

            int asksSize = data["asks"].Size();
            int bidsSize = data["bids"].Size();

            if (asksSize >= 10 && bidsSize >= 10) {
                depth10.bp1 = std::stod(data["bids"][0]["p"].GetString());
                depth10.bv1 = std::stod(data["bids"][0]["s"].GetString());
                depth10.bp2 = std::stod(data["bids"][1]["p"].GetString());
                depth10.bv2 = std::stod(data["bids"][1]["s"].GetString());
                depth10.bp3 = std::stod(data["bids"][2]["p"].GetString());
                depth10.bv3 = std::stod(data["bids"][2]["s"].GetString());
                depth10.bp4 = std::stod(data["bids"][3]["p"].GetString());
                depth10.bv4 = std::stod(data["bids"][3]["s"].GetString());
                depth10.bp5 = std::stod(data["bids"][4]["p"].GetString());
                depth10.bv5 = std::stod(data["bids"][4]["s"].GetString());
                depth10.bp6 = std::stod(data["bids"][5]["p"].GetString());
                depth10.bv6 = std::stod(data["bids"][5]["s"].GetString());
                depth10.bp7 = std::stod(data["bids"][6]["p"].GetString());
                depth10.bv7 = std::stod(data["bids"][6]["s"].GetString());
                depth10.bp8 = std::stod(data["bids"][7]["p"].GetString());
                depth10.bv8 = std::stod(data["bids"][7]["s"].GetString());
                depth10.bp9 = std::stod(data["bids"][8]["p"].GetString());
                depth10.bv9 = std::stod(data["bids"][8]["s"].GetString());
                depth10.bp10 = std::stod(data["bids"][9]["p"].GetString());
                depth10.bv10 = std::stod(data["bids"][9]["s"].GetString());

                depth10.ap1 = std::stod(data["asks"][0]["p"].GetString());
                depth10.av1 = std::stod(data["asks"][0]["s"].GetString());
                depth10.ap2 = std::stod(data["asks"][1]["p"].GetString());
                depth10.av2 = std::stod(data["asks"][1]["s"].GetString());
                depth10.ap3 = std::stod(data["asks"][2]["p"].GetString());
                depth10.av3 = std::stod(data["asks"][2]["s"].GetString());
                depth10.ap4 = std::stod(data["asks"][3]["p"].GetString());
                depth10.av4 = std::stod(data["asks"][3]["s"].GetString());
                depth10.ap5 = std::stod(data["asks"][4]["p"].GetString());
                depth10.av5 = std::stod(data["asks"][4]["s"].GetString());
                depth10.ap6 = std::stod(data["asks"][5]["p"].GetString());
                depth10.av6 = std::stod(data["asks"][5]["s"].GetString());
                depth10.ap7 = std::stod(data["asks"][6]["p"].GetString());
                depth10.av7 = std::stod(data["asks"][6]["s"].GetString());
                depth10.ap8 = std::stod(data["asks"][7]["p"].GetString());
                depth10.av8 = std::stod(data["asks"][7]["s"].GetString());
                depth10.ap9 = std::stod(data["asks"][8]["p"].GetString());
                depth10.av9 = std::stod(data["asks"][8]["s"].GetString());
                depth10.ap10 = std::stod(data["asks"][9]["p"].GetString());
                depth10.av10 = std::stod(data["asks"][9]["s"].GetString());
            }

#ifdef NEED_SHM
            mDepth10Publisher[key]->push(depth10);                
#endif
            return;
        }
        case md::DEPTH20: {
            md::Depth20 depth20;
            memset(&depth20, 0, sizeof(md::Depth20));
            depth20.exchangeTypeEnum = exchangeTypeEnum;
            depth20.instTypeEnum = instTypeEnum;
            depth20.marketTypeEnum = marketTypeEnum;
            strncpy(depth20.instId, info.instId, INSTID_SIZE);

            long ts = std::stol(rawData["time_ms"].GetString()) * 1000;
            depth20.tsTrans = ts;
            depth20.tsEvent = ts;
            depth20.tsRecv = tsNet;
            depth20.tsParse = crypto::getCurrentTime();

            int asksSize = data["asks"].Size();
            int bidsSize = data["bids"].Size();

            if (asksSize >= 20 && bidsSize >= 20) {
                depth20.bp1 = std::stod(data["bids"][0]["p"].GetString());
                depth20.bv1 = std::stod(data["bids"][0]["s"].GetString());
                depth20.bp2 = std::stod(data["bids"][1]["p"].GetString());
                depth20.bv2 = std::stod(data["bids"][1]["s"].GetString());
                depth20.bp3 = std::stod(data["bids"][2]["p"].GetString());
                depth20.bv3 = std::stod(data["bids"][2]["s"].GetString());
                depth20.bp4 = std::stod(data["bids"][3]["p"].GetString());
                depth20.bv4 = std::stod(data["bids"][3]["s"].GetString());
                depth20.bp5 = std::stod(data["bids"][4]["p"].GetString());
                depth20.bv5 = std::stod(data["bids"][4]["s"].GetString());
                depth20.bp6 = std::stod(data["bids"][5]["p"].GetString());
                depth20.bv6 = std::stod(data["bids"][5]["s"].GetString());
                depth20.bp7 = std::stod(data["bids"][6]["p"].GetString());
                depth20.bv7 = std::stod(data["bids"][6]["s"].GetString());
                depth20.bp8 = std::stod(data["bids"][7]["p"].GetString());
                depth20.bv8 = std::stod(data["bids"][7]["s"].GetString());
                depth20.bp9 = std::stod(data["bids"][8]["p"].GetString());
                depth20.bv9 = std::stod(data["bids"][8]["s"].GetString());
                depth20.bp10 = std::stod(data["bids"][9]["p"].GetString());
                depth20.bv10 = std::stod(data["bids"][9]["s"].GetString());
                depth20.bp11 = std::stod(data["bids"][10]["p"].GetString());
                depth20.bv11 = std::stod(data["bids"][10]["s"].GetString());
                depth20.bp12 = std::stod(data["bids"][11]["p"].GetString());
                depth20.bv12 = std::stod(data["bids"][11]["s"].GetString());
                depth20.bp13 = std::stod(data["bids"][12]["p"].GetString());
                depth20.bv13 = std::stod(data["bids"][12]["s"].GetString());
                depth20.bp14 = std::stod(data["bids"][13]["p"].GetString());
                depth20.bv14 = std::stod(data["bids"][13]["s"].GetString());
                depth20.bp15 = std::stod(data["bids"][14]["p"].GetString());
                depth20.bv15 = std::stod(data["bids"][14]["s"].GetString());
                depth20.bp16 = std::stod(data["bids"][15]["p"].GetString());
                depth20.bv16 = std::stod(data["bids"][15]["s"].GetString());
                depth20.bp17 = std::stod(data["bids"][16]["p"].GetString());
                depth20.bv17 = std::stod(data["bids"][16]["s"].GetString());
                depth20.bp18 = std::stod(data["bids"][17]["p"].GetString());
                depth20.bv18 = std::stod(data["bids"][17]["s"].GetString());
                depth20.bp19 = std::stod(data["bids"][18]["p"].GetString());
                depth20.bv19 = std::stod(data["bids"][18]["s"].GetString());
                depth20.bp20 = std::stod(data["bids"][19]["p"].GetString());
                depth20.bv20 = std::stod(data["bids"][19]["s"].GetString());

                depth20.ap1 = std::stod(data["asks"][0]["p"].GetString());
                depth20.av1 = std::stod(data["asks"][0]["s"].GetString());
                depth20.ap2 = std::stod(data["asks"][1]["p"].GetString());
                depth20.av2 = std::stod(data["asks"][1]["s"].GetString());
                depth20.ap3 = std::stod(data["asks"][2]["p"].GetString());
                depth20.av3 = std::stod(data["asks"][2]["s"].GetString());
                depth20.ap4 = std::stod(data["asks"][3]["p"].GetString());
                depth20.av4 = std::stod(data["asks"][3]["s"].GetString());
                depth20.ap5 = std::stod(data["asks"][4]["p"].GetString());
                depth20.av5 = std::stod(data["asks"][4]["s"].GetString());
                depth20.ap6 = std::stod(data["asks"][5]["p"].GetString());
                depth20.av6 = std::stod(data["asks"][5]["s"].GetString());
                depth20.ap7 = std::stod(data["asks"][6]["p"].GetString());
                depth20.av7 = std::stod(data["asks"][6]["s"].GetString());
                depth20.ap8 = std::stod(data["asks"][7]["p"].GetString());
                depth20.av8 = std::stod(data["asks"][7]["s"].GetString());
                depth20.ap9 = std::stod(data["asks"][8]["p"].GetString());
                depth20.av9 = std::stod(data["asks"][8]["s"].GetString());
                depth20.ap10 = std::stod(data["asks"][9]["p"].GetString());
                depth20.av10 = std::stod(data["asks"][9]["s"].GetString());
                depth20.ap11 = std::stod(data["asks"][10]["p"].GetString());
                depth20.av11 = std::stod(data["asks"][10]["s"].GetString());
                depth20.ap12 = std::stod(data["asks"][11]["p"].GetString());
                depth20.av12 = std::stod(data["asks"][11]["s"].GetString());
                depth20.ap13 = std::stod(data["asks"][12]["p"].GetString());
                depth20.av13 = std::stod(data["asks"][12]["s"].GetString());
                depth20.ap14 = std::stod(data["asks"][13]["p"].GetString());
                depth20.av14 = std::stod(data["asks"][13]["s"].GetString());
                depth20.ap15 = std::stod(data["asks"][14]["p"].GetString());
                depth20.av15 = std::stod(data["asks"][14]["s"].GetString());
                depth20.ap16 = std::stod(data["asks"][15]["p"].GetString());
                depth20.av16 = std::stod(data["asks"][15]["s"].GetString());
                depth20.ap17 = std::stod(data["asks"][16]["p"].GetString());
                depth20.av17 = std::stod(data["asks"][16]["s"].GetString());
                depth20.ap18 = std::stod(data["asks"][17]["p"].GetString());
                depth20.av18 = std::stod(data["asks"][17]["s"].GetString());
                depth20.ap19 = std::stod(data["asks"][18]["p"].GetString());
                depth20.av19 = std::stod(data["asks"][18]["s"].GetString());
                depth20.ap20 = std::stod(data["asks"][19]["p"].GetString());
                depth20.av20 = std::stod(data["asks"][19]["s"].GetString());
            }

#ifdef NEED_SHM
            mDepth20Publisher[key]->push(depth20);                
#endif
            return;
        }
        case md::TRADES: {
            md::Trades trades;
            memset(&trades, 0, sizeof(md::Trades));
            trades.exchangeTypeEnum = exchangeTypeEnum;
            trades.instTypeEnum = instTypeEnum;
            trades.marketTypeEnum = marketTypeEnum;
            strncpy(trades.instId, info.instId, INSTID_SIZE);

            long ts = stol(data[0]["create_time_ms"].GetString()) * 1000;

            trades.tsTrans = ts;
            trades.tsEvent = ts;
            trades.tsRecv = tsNet;
            trades.tsParse = crypto::getCurrentTime();

            double size = std::stod(data[0]["size"].GetString());
            strncpy(trades.tradeId, data[0]["id"].GetString(), INSTID_SIZE);
            trades.px = std::stod(data[0]["price"].GetString());
            trades.sz = fabs(size);
            
            if (size > 0) {
                trades.direction = DT_LONG; 
            }
            else {
                trades.direction = DT_SHORT;
            }

#ifdef NEED_SHM
            mTradesPublisher[key]->push(trades); 
#endif
            return;
        }
        case md::KLINE_1m: {
            long ts = stol(rawData["time_ms"].GetString()) * 1000;

            for (rapidjson::SizeType i = 0; i < data.Size(); ++i) {
                md::Kline kline;
                memset(&kline, 0, sizeof(md::Kline));
                kline.exchangeTypeEnum = exchangeTypeEnum;
                kline.instTypeEnum = instTypeEnum;
                kline.marketTypeEnum = marketTypeEnum;
                strncpy(kline.instId, info.instId, INSTID_SIZE);

                
                kline.tsTrans = ts;
                kline.tsEvent = ts;
                kline.tsRecv = tsNet;
                kline.tsParse = crypto::getCurrentTime();

                kline.barTime = stol(data[i]["t"].GetString()) * 1000000;
                kline.highPrice = std::stod(data[i]["h"].GetString());
                kline.lowPrice = std::stod(data[i]["l"].GetString());
                kline.openPrice = std::stod(data[i]["o"].GetString());
                kline.closePrice = std::stod(data[i]["c"].GetString());

                double avgPrice = 0;
                double amount = std::stod(data[i]["a"].GetString()); // 待确定
                double volume = std::stod(data[i]["v"].GetString());
                if(amount > ZERO_NUM){
                    avgPrice = amount / volume;
                }

                kline.avgPrice = avgPrice;
                kline.totalVolume = volume;
                kline.totalAmount = amount;
                kline.isFinished = true;

    #ifdef NEED_SHM
                mKlinePublisher[key]->push(kline); 
    #endif
            }

            return;
        }
        case md::FUNDING_RATE: {
            md::FundingRate fundingRate;
            memset(&fundingRate, 0, sizeof(md::FundingRate));
            fundingRate.exchangeTypeEnum = exchangeTypeEnum;
            fundingRate.instTypeEnum = instTypeEnum;
            fundingRate.marketTypeEnum = marketTypeEnum;
            strncpy(fundingRate.instId, info.instId, INSTID_SIZE);

            long ts = std::stol(rawData["time"].GetString()) * 1000 * 1000;
            fundingRate.tsTrans = ts;
            fundingRate.tsEvent = ts;
            fundingRate.tsRecv = tsNet;
            fundingRate.tsParse = crypto::getCurrentTime();

            fundingRate.fundingRate = std::stod(data[0]["funding_rate"].GetString());
            fundingRate.nextFundingRate = std::stol(data[0]["funding_rate_indicative"].GetString()) * 1000;

#ifdef NEED_SHM
            mFundingRatePublisher[key]->push(fundingRate); 
#endif
            return;
        }
        default: {
            LOG_ERROR("not support marketType: {}", md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }
    }

}


md::GateioMarket::GateioMarket(sm::SecurityManager* s, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot, const char* host, const int port, const char* passwd) : md::BaseMarket(s, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    strcpy(exchId, "GATEIO");

    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        std::cout << "start create gateio unit" << std::endl;
        auto& info = unitInfoVec[i];
        md::GateioUnit* unit = new md::GateioUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        std::cout << "start generate sub body" << std::endl;
        unit->generateSubBody();
        gateioUnitVec.push_back(unit);
    }
}

md::GateioMarket::~GateioMarket() {

}


void md::GateioMarket::start() {
    for (size_t i = 0; i < gateioUnitVec.size(); ++i) {
        gateioUnitVec[i]->start();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}
