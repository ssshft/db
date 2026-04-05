#include "binance/BinanceMarket.h"


md::BinanceUnit::BinanceUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* passwd) : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    subCount = 0;
    subId = crypto::get_int_rand(100,10000);
    pWsClient = nullptr;
}

void md::BinanceUnit::generateSubBody() {
    // LOG_INFO("%s", getString().c_str());
    std::string exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    std::string instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    std::string marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];
    std::string lowerMarketTypeStr = crypto::to_lower(marketTypeStr);

    if (instTypeEnum == SPOT) {
        wsUrl = BINANCE_WS_PUBLIC_SPOT;
    }
    else if (instTypeEnum == USDT_SWAP || instTypeEnum == USDT_FUTURES || instTypeEnum == USDC_SWAP) {
        wsUrl = BINANCE_WS_PUBLIC_USDT_SWAP_FUTURES;
    }
    else if (instTypeEnum == C_SWAP || instTypeEnum == C_FUTURES) {
        wsUrl = BINANCE_WS_PUBLIC_USD_SWAP_FUTURES;
    }

    subValue["method"] = json::value::string("SUBSCRIBE");

    for (auto info : vInstInfo) {
        std::string lowerOriginInstId = crypto::to_lower(info.originInstId);
        if (instTypeEnum == SPOT) {
            if (crypto::has_str(marketTypeStr, "DEPTH")) {
                if(crypto::str_cmp(marketTypeStr.c_str(), "DEPTH1")) {
                    std::string param = fmt::format("{}@bookTicker", lowerOriginInstId);
                    subValue["params"][subCount++] = web::json::value::string(param);
                }
                else {
                    std::string param = fmt::format("{}@{}@100ms", lowerOriginInstId, lowerMarketTypeStr);
                    subValue["params"][subCount++] = web::json::value::string(param);
                }
            }
            else if (crypto::has_str(marketTypeStr, "TRADE")) {
                std::string param = fmt::format("{}@trade", lowerOriginInstId);
                subValue["params"][subCount++] = web::json::value::string(param);
            }
            else if (crypto::has_str(marketTypeStr, "KLINE")) {
                std::string param = fmt::format("{}@{}", lowerOriginInstId, lowerMarketTypeStr);
                subValue["params"][subCount++] = web::json::value::string(param);
            }
            else {
                LOG_ERROR("not support {}", marketTypeStr);
            }
        }//这里usdt和busd本位都在这里处理
        else if (instTypeEnum == USDT_SWAP || instTypeEnum == USDT_FUTURES || instTypeEnum == USDC_SWAP) {
            if (crypto::has_str(marketTypeStr, "DEPTH")) {
                if (crypto::str_cmp(marketTypeStr.c_str(), "DEPTH1")) {
                    std::string param = fmt::format("{}@bookTicker", lowerOriginInstId);
                    subValue["params"][subCount++] = web::json::value::string(param);
                }
                else {
                    std::string param = fmt::format("{}@{}@100ms", lowerOriginInstId, lowerMarketTypeStr);
                    subValue["params"][subCount++] = web::json::value::string(param);
                }
            }
            else if (crypto::has_str(marketTypeStr, "TRADE")) {
                std::string param = fmt::format("{}@trade", lowerOriginInstId);
                subValue["params"][subCount++] = web::json::value::string(param);
            }
            else if (crypto::has_str(marketTypeStr, "KLINE")) {
                std::string param = fmt::format("{}@{}", lowerOriginInstId, lowerMarketTypeStr);
                subValue["params"][subCount++] = web::json::value::string(param);
            }
            else if (crypto::has_str(marketTypeStr, "FUNDING")) {
                if (instTypeEnum == USDT_SWAP || instTypeEnum == USDC_SWAP) {
                    std::string param = fmt::format("{}@markPrice@1s", lowerOriginInstId);
                    subValue["params"][subCount++] = web::json::value::string(param);
                }
            }
        }//币本位
        else if (instTypeEnum == C_SWAP || instTypeEnum == C_FUTURES) {
            if (crypto::has_str(marketTypeStr, "DEPTH")) {
                if (crypto::str_cmp(marketTypeStr.c_str(), "DEPTH1")) {
                    std::string param = fmt::format("{}@bookTicker", lowerOriginInstId);
                    subValue["params"][subCount++] = web::json::value::string(param);
                }
                else {
                    std::string param = fmt::format("{}@{}@100ms", lowerOriginInstId, lowerMarketTypeStr);
                    subValue["params"][subCount++] = web::json::value::string(param);
                }
            }
            else if (crypto::has_str(marketTypeStr, "TRADE")) {
                std::string param = fmt::format("{}@aggTrade", lowerOriginInstId);
                subValue["params"][subCount++] = web::json::value::string(param);
            }
            else if (crypto::has_str(marketTypeStr, "KLINE")) {
                std::string param = fmt::format("{}@{}", lowerOriginInstId, lowerMarketTypeStr);
                subValue["params"][subCount++] = json::value::string(param);
            }
            else if (crypto::has_str(marketTypeStr, "FUNDING")) {
                if (instTypeEnum == C_SWAP) {
                    std::string param = fmt::format("{}@markPrice@1s", lowerOriginInstId);
                    subValue["params"][subCount++] = json::value::string(param);
                }
            }
        }
        else {
            LOG_ERROR("not support subMarketType: {}", instTypeStr);
        }
    }
    LOG_INFO("sub body: {}", subValue.serialize());
    std::cout << "sub body: " << subValue.serialize() << std::endl;
}

void md::BinanceUnit::subWebsocekt() {
START_SUB_WEBSOCKET()

    web::websockets::client::websocket_outgoing_message outMsg;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    subValue["id"] = web::json::value::number(subId++);
    LOG_INFO("{} send {} to {}", ExchangeTypeEnum2StrMap[exchangeTypeEnum], subValue.serialize(), wsUrl);
    pWsClient->send(outMsg).wait();

END_SUB_WEBSOCKET()
}

void md::BinanceUnit::ping(){

}

void md::BinanceUnit::pong() {
    try {
        if (pWsClient != nullptr && isConnected) {
            web::websockets::client::websocket_outgoing_message outMsg;
            outMsg.set_pong_message();
            pWsClient->send(outMsg).wait();
        }
    }
    catch (const std::exception& e) {
        isConnected = false;
        LOG_ERROR("pong error: {}", e.what());
    }
}

void md::BinanceUnit::onWebsocketMsg(const web::websockets::client::websocket_incoming_message& msg) {
    latestDataUpdateTime = crypto::getCurrentTime();

    switch (msg.message_type()) {
        case  web::websockets::client::websocket_message_type::text_message: {
            const string& s = msg.extract_string().get();
            std::cout << "onWebsocketMsg: " << s << std::endl;
            mQueue.push(s);
            return;
        }
        case web::websockets::client::websocket_message_type::ping: {
            LOG_INFO("{}.{}.{} got ping, will reply pong.", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], md::MarketTypeEnum2StrMap[marketTypeEnum]);
            msg.extract_string().then([&](std::string payload) {
                LOG_INFO("Received ping with payload: {}", payload);
                websocket_outgoing_message pongMsg;
                pongMsg.set_pong_message(utility::conversions::to_string_t(payload));
                return pWsClient->send(pongMsg).then([]() {
                    LOG_INFO("Sent pong with same payload.");
                }).wait();
            }).wait();
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
void md::BinanceUnit::parseMarketData(const std::string& msg) {
    long tsNet = crypto::getCurrentTime();

    rapidjson::Document d;
    rapidjson::Value& rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(msg.c_str());

    if (d.HasParseError() || !rawData.HasMember("data")) {
        return;
    }

    std::string originInstId = "";
    if (rawData["data"].HasMember("s")) {
        originInstId = rawData["data"]["s"].GetString();
    }
    else {
        std::vector<std::string> v = crypto::split(rawData["stream"].GetString(), "@");
        originInstId = crypto::to_upper(v[0]);
    }

    md::InstrumentInfo info;
    if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false){
        LOG_ERROR("msg: {}", msg);
        return;
    }

    std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

    if (instTypeEnum == SPOT) {
        if (marketTypeEnum == md::DEPTH1) {
            const rapidjson::Value& data = rawData["data"];
            md::Depth1 depth1;
            memset(&depth1, 0, sizeof(md::Depth1));
            depth1.exchangeTypeEnum = exchangeTypeEnum;
            depth1.instTypeEnum = instTypeEnum;
            depth1.marketTypeEnum = marketTypeEnum;
            strncpy(depth1.instId, info.instId, INSTID_SIZE);
            depth1.tsTrans = tsNet;
            depth1.tsEvent = tsNet;
            depth1.tsRecv = tsNet;
            depth1.tsParse = crypto::getCurrentTime();
            depth1.bp1 = std::stod(data["b"].GetString()) * info.reduceNumber;
            depth1.bv1 = std::stod(data["B"].GetString()) * info.magnifyNumber;
            depth1.ap1 = std::stod(data["a"].GetString()) * info.reduceNumber;
            depth1.av1 = std::stod(data["A"].GetString()) * info.magnifyNumber;
#ifdef NEED_SHM
            
            mDepth1Publisher[key]->push(depth1);                
#endif
        }
        else if (marketTypeEnum == md::DEPTH5) {
            const rapidjson::Value& data = rawData["data"];
            md::Depth5 depth5;
            memset(&depth5, 0, sizeof(md::Depth5));
            depth5.exchangeTypeEnum = exchangeTypeEnum;
            depth5.instTypeEnum = instTypeEnum;
            depth5.marketTypeEnum = marketTypeEnum;
            strncpy(depth5.instId, info.instId, INSTID_SIZE);
            depth5.tsTrans = tsNet;
            depth5.tsEvent = tsNet;
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
        }
        else if (marketTypeEnum == md::DEPTH10) {
            const rapidjson::Value& data = rawData["data"];
            md::Depth10 depth10;
            memset(&depth10, 0, sizeof(md::Depth10));
            depth10.exchangeTypeEnum = exchangeTypeEnum;
            depth10.instTypeEnum = instTypeEnum;
            depth10.marketTypeEnum = marketTypeEnum;
            strncpy(depth10.instId, info.instId, INSTID_SIZE);
            depth10.tsTrans = tsNet;
            depth10.tsEvent = tsNet;
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
        }
        else if (marketTypeEnum == md::DEPTH20) {
            const rapidjson::Value& data = rawData["data"];
            md::Depth20 depth20;
            memset(&depth20, 0, sizeof(md::Depth20));
            depth20.exchangeTypeEnum = exchangeTypeEnum;
            depth20.instTypeEnum = instTypeEnum;
            depth20.marketTypeEnum = marketTypeEnum;
            strncpy(depth20.instId, info.instId, INSTID_SIZE);
            depth20.tsTrans = tsNet;
            depth20.tsEvent = tsNet;
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
        }
        else if(marketTypeEnum == md::TRADES) {
            const rapidjson::Value &data = rawData["data"];

            md::Trades trades;
            memset(&trades, 0, sizeof(md::Trades));
            trades.exchangeTypeEnum = exchangeTypeEnum;
            trades.instTypeEnum = instTypeEnum;
            trades.marketTypeEnum = marketTypeEnum;
            strncpy(trades.instId, info.instId, INSTID_SIZE);
            trades.tsTrans = std::stol(data["T"].GetString()) * 1000;
            trades.tsEvent = std::stol(data["E"].GetString()) * 1000;
            trades.tsRecv = tsNet;
            trades.tsParse = crypto::getCurrentTime();

            strncpy(trades.tradeId, data["t"].GetString(), INSTID_SIZE);
            trades.px = std::stod(data["p"].GetString());
            trades.sz = std::stod(data["q"].GetString());
            bool m = data["m"].GetBool();
            trades.direction = m ? DT_SHORT : DT_LONG;

#ifdef NEED_SHM
            mTradesPublisher[key]->push(trades); 
#endif
        }
        else if (marketTypeEnum == md::KLINE_1m) {
            const rapidjson::Value& data = rawData["data"];

            md::Kline kline;
            memset(&kline, 0, sizeof(md::Kline));
            kline.exchangeTypeEnum = exchangeTypeEnum;
            kline.instTypeEnum = instTypeEnum;
            kline.marketTypeEnum = marketTypeEnum;
            strncpy(kline.instId, info.instId, INSTID_SIZE);
            kline.tsTrans = std::stol(data["E"].GetString()) * 1000;
            kline.tsEvent = std::stol(data["E"].GetString()) * 1000;
            kline.tsRecv = tsNet;
            kline.tsParse = crypto::getCurrentTime();

            kline.barTime = std::stol(data["k"]["t"].GetString()) * 1000;
            kline.highPrice = std::stod(data["k"]["h"].GetString());
            kline.lowPrice = std::stod(data["k"]["l"].GetString());
            kline.openPrice = std::stod(data["k"]["o"].GetString());
            kline.closePrice = std::stod(data["k"]["c"].GetString());

            double avgPrice = 0;
            double amount = std::stod(data["k"]["q"].GetString());
            double volume = std::stod(data["k"]["v"].GetString());
            if(amount > ZERO_NUM){
                avgPrice = amount / volume;
            }

            kline.avgPrice = avgPrice;
            kline.totalVolume = volume;
            kline.totalAmount = amount;

            kline.isFinished = data["k"]["x"].GetBool();
            if (!kline.isFinished) {
                return;
            }

#ifdef NEED_SHM
            mKlinePublisher[key]->push(kline); 
#endif
        }
        else {
            return;
        }
    }
    else if(instTypeEnum == USDT_SWAP || instTypeEnum == USDT_FUTURES || instTypeEnum == USDC_SWAP || instTypeEnum == C_SWAP || instTypeEnum == C_FUTURES) {
        if(marketTypeEnum == md::DEPTH1) {
            const rapidjson::Value& data = rawData["data"];

            md::Depth1 depth1;
            memset(&depth1, 0, sizeof(md::Depth1));
            depth1.exchangeTypeEnum = exchangeTypeEnum;
            depth1.instTypeEnum = instTypeEnum;
            depth1.marketTypeEnum = marketTypeEnum;
            strncpy(depth1.instId, info.instId, INSTID_SIZE);
            depth1.tsTrans = std::stol(data["T"].GetString()) * 1000;
            depth1.tsEvent = std::stol(data["E"].GetString()) * 1000;
            depth1.tsRecv = tsNet;
            depth1.tsParse = crypto::getCurrentTime();
            depth1.bp1 = std::stod(data["b"].GetString()) * info.reduceNumber;
            depth1.bv1 = std::stod(data["B"].GetString()) * info.magnifyNumber;
            depth1.ap1 = std::stod(data["a"].GetString()) * info.reduceNumber;
            depth1.av1 = std::stod(data["A"].GetString()) * info.magnifyNumber;
#ifdef NEED_SHM
            mDepth1Publisher[key]->push(depth1);             
#endif
        }
        else if (marketTypeEnum == md::DEPTH5) {
            const rapidjson::Value &data = rawData["data"];

            md::Depth5 depth5;
            memset(&depth5, 0, sizeof(md::Depth5));
            depth5.exchangeTypeEnum = exchangeTypeEnum;
            depth5.instTypeEnum = instTypeEnum;
            depth5.marketTypeEnum = marketTypeEnum;
            strncpy(depth5.instId, info.instId, INSTID_SIZE);
            depth5.tsTrans = std::stol(data["T"].GetString()) * 1000;
            depth5.tsEvent = std::stol(data["E"].GetString()) * 1000;
            depth5.tsRecv = tsNet;
            depth5.tsParse = crypto::getCurrentTime();

            int asksSize = data["a"].Size();
            int bidsSize = data["b"].Size();

            if (asksSize >= 5 && bidsSize >= 5) {
                depth5.bp1 = std::stod(data["b"][0][0].GetString());
                depth5.bv1 = std::stod(data["b"][0][1].GetString());
                depth5.bp2 = std::stod(data["b"][1][0].GetString());
                depth5.bv2 = std::stod(data["b"][1][1].GetString());
                depth5.bp3 = std::stod(data["b"][2][0].GetString());
                depth5.bv3 = std::stod(data["b"][2][1].GetString());
                depth5.bp4 = std::stod(data["b"][3][0].GetString());
                depth5.bv4 = std::stod(data["b"][3][1].GetString());
                depth5.bp5 = std::stod(data["b"][4][0].GetString());
                depth5.bv5 = std::stod(data["b"][4][1].GetString());

                depth5.ap1 = std::stod(data["a"][0][0].GetString());
                depth5.av1 = std::stod(data["a"][0][1].GetString());
                depth5.ap2 = std::stod(data["a"][1][0].GetString());
                depth5.av2 = std::stod(data["a"][1][1].GetString());
                depth5.ap3 = std::stod(data["a"][2][0].GetString());
                depth5.av3 = std::stod(data["a"][2][1].GetString());
                depth5.ap4 = std::stod(data["a"][3][0].GetString());
                depth5.av4 = std::stod(data["a"][3][1].GetString());
                depth5.ap5 = std::stod(data["a"][4][0].GetString());
                depth5.av5 = std::stod(data["a"][4][1].GetString());
            }

#ifdef NEED_SHM
            mDepth5Publisher[key]->push(depth5);                
#endif   
        }
        else if (marketTypeEnum == md::DEPTH10) {
            const rapidjson::Value& data = rawData["data"];
            md::Depth10 depth10;
            memset(&depth10, 0, sizeof(md::Depth10));
            depth10.exchangeTypeEnum = exchangeTypeEnum;
            depth10.instTypeEnum = instTypeEnum;
            depth10.marketTypeEnum = marketTypeEnum;
            strncpy(depth10.instId, info.instId, INSTID_SIZE);
            depth10.tsTrans = std::stol(data["T"].GetString()) * 1000;
            depth10.tsEvent = std::stol(data["E"].GetString()) * 1000;
            depth10.tsRecv = tsNet;
            depth10.tsParse = crypto::getCurrentTime();

            int asksSize = data["a"].Size();
            int bidsSize = data["b"].Size();

            if (asksSize >= 10 && bidsSize >= 10) {
                depth10.bp1 = std::stod(data["b"][0][0].GetString());
                depth10.bv1 = std::stod(data["b"][0][1].GetString());
                depth10.bp2 = std::stod(data["b"][1][0].GetString());
                depth10.bv2 = std::stod(data["b"][1][1].GetString());
                depth10.bp3 = std::stod(data["b"][2][0].GetString());
                depth10.bv3 = std::stod(data["b"][2][1].GetString());
                depth10.bp4 = std::stod(data["b"][3][0].GetString());
                depth10.bv4 = std::stod(data["b"][3][1].GetString());
                depth10.bp5 = std::stod(data["b"][4][0].GetString());
                depth10.bv5 = std::stod(data["b"][4][1].GetString());
                depth10.bp6 = std::stod(data["b"][5][0].GetString());
                depth10.bv6 = std::stod(data["b"][5][1].GetString());
                depth10.bp7 = std::stod(data["b"][6][0].GetString());
                depth10.bv7 = std::stod(data["b"][6][1].GetString());
                depth10.bp8 = std::stod(data["b"][7][0].GetString());
                depth10.bv8 = std::stod(data["b"][7][1].GetString());
                depth10.bp9 = std::stod(data["b"][8][0].GetString());
                depth10.bv9 = std::stod(data["b"][8][1].GetString());
                depth10.bp10 = std::stod(data["b"][9][0].GetString());
                depth10.bv10 = std::stod(data["b"][9][1].GetString());

                depth10.ap1 = std::stod(data["a"][0][0].GetString());
                depth10.av1 = std::stod(data["a"][0][1].GetString());
                depth10.ap2 = std::stod(data["a"][1][0].GetString());
                depth10.av2 = std::stod(data["a"][1][1].GetString());
                depth10.ap3 = std::stod(data["a"][2][0].GetString());
                depth10.av3 = std::stod(data["a"][2][1].GetString());
                depth10.ap4 = std::stod(data["a"][3][0].GetString());
                depth10.av4 = std::stod(data["a"][3][1].GetString());
                depth10.ap5 = std::stod(data["a"][4][0].GetString());
                depth10.av5 = std::stod(data["a"][4][1].GetString());
                depth10.ap6 = std::stod(data["a"][5][0].GetString());
                depth10.av6 = std::stod(data["a"][5][1].GetString());
                depth10.ap7 = std::stod(data["a"][6][0].GetString());
                depth10.av7 = std::stod(data["a"][6][1].GetString());
                depth10.ap8 = std::stod(data["a"][7][0].GetString());
                depth10.av8 = std::stod(data["a"][7][1].GetString());
                depth10.ap9 = std::stod(data["a"][8][0].GetString());
                depth10.av9 = std::stod(data["a"][8][1].GetString());
                depth10.ap10 = std::stod(data["a"][9][0].GetString());
                depth10.av10 = std::stod(data["a"][9][1].GetString());
            }

#ifdef NEED_SHM
            mDepth10Publisher[key]->push(depth10);                
#endif
        }
        else if (marketTypeEnum == md::DEPTH20) {
            const rapidjson::Value& data = rawData["data"];
            md::Depth20 depth20;
            memset(&depth20, 0, sizeof(md::Depth20));
            depth20.exchangeTypeEnum = exchangeTypeEnum;
            depth20.instTypeEnum = instTypeEnum;
            depth20.marketTypeEnum = marketTypeEnum;
            strncpy(depth20.instId, info.instId, INSTID_SIZE);
            depth20.tsTrans = std::stol(data["T"].GetString()) * 1000;
            depth20.tsEvent = std::stol(data["E"].GetString()) * 1000;
            depth20.tsRecv = tsNet;
            depth20.tsParse = crypto::getCurrentTime();

            int asksSize = data["a"].Size();
            int bidsSize = data["b"].Size();

            if (asksSize >= 20 && bidsSize >= 20) {
                depth20.bp1 = std::stod(data["b"][0][0].GetString());
                depth20.bv1 = std::stod(data["b"][0][1].GetString());
                depth20.bp2 = std::stod(data["b"][1][0].GetString());
                depth20.bv2 = std::stod(data["b"][1][1].GetString());
                depth20.bp3 = std::stod(data["b"][2][0].GetString());
                depth20.bv3 = std::stod(data["b"][2][1].GetString());
                depth20.bp4 = std::stod(data["b"][3][0].GetString());
                depth20.bv4 = std::stod(data["b"][3][1].GetString());
                depth20.bp5 = std::stod(data["b"][4][0].GetString());
                depth20.bv5 = std::stod(data["b"][4][1].GetString());
                depth20.bp6 = std::stod(data["b"][5][0].GetString());
                depth20.bv6 = std::stod(data["b"][5][1].GetString());
                depth20.bp7 = std::stod(data["b"][6][0].GetString());
                depth20.bv7 = std::stod(data["b"][6][1].GetString());
                depth20.bp8 = std::stod(data["b"][7][0].GetString());
                depth20.bv8 = std::stod(data["b"][7][1].GetString());
                depth20.bp9 = std::stod(data["b"][8][0].GetString());
                depth20.bv9 = std::stod(data["b"][8][1].GetString());
                depth20.bp10 = std::stod(data["b"][9][0].GetString());
                depth20.bv10 = std::stod(data["b"][9][1].GetString());
                depth20.bp11 = std::stod(data["b"][10][0].GetString());
                depth20.bv11 = std::stod(data["b"][10][1].GetString());
                depth20.bp12 = std::stod(data["b"][11][0].GetString());
                depth20.bv12 = std::stod(data["b"][11][1].GetString());
                depth20.bp13 = std::stod(data["b"][12][0].GetString());
                depth20.bv13 = std::stod(data["b"][12][1].GetString());
                depth20.bp14 = std::stod(data["b"][13][0].GetString());
                depth20.bv14 = std::stod(data["b"][13][1].GetString());
                depth20.bp15 = std::stod(data["b"][14][0].GetString());
                depth20.bv15 = std::stod(data["b"][14][1].GetString());
                depth20.bp16 = std::stod(data["b"][15][0].GetString());
                depth20.bv16 = std::stod(data["b"][15][1].GetString());
                depth20.bp17 = std::stod(data["b"][16][0].GetString());
                depth20.bv17 = std::stod(data["b"][16][1].GetString());
                depth20.bp18 = std::stod(data["b"][17][0].GetString());
                depth20.bv18 = std::stod(data["b"][17][1].GetString());
                depth20.bp19 = std::stod(data["b"][18][0].GetString());
                depth20.bv19 = std::stod(data["b"][18][1].GetString());
                depth20.bp20 = std::stod(data["b"][19][0].GetString());
                depth20.bv20 = std::stod(data["b"][19][1].GetString());

                depth20.ap1 = std::stod(data["a"][0][0].GetString());
                depth20.av1 = std::stod(data["a"][0][1].GetString());
                depth20.ap2 = std::stod(data["a"][1][0].GetString());
                depth20.av2 = std::stod(data["a"][1][1].GetString());
                depth20.ap3 = std::stod(data["a"][2][0].GetString());
                depth20.av3 = std::stod(data["a"][2][1].GetString());
                depth20.ap4 = std::stod(data["a"][3][0].GetString());
                depth20.av4 = std::stod(data["a"][3][1].GetString());
                depth20.ap5 = std::stod(data["a"][4][0].GetString());
                depth20.av5 = std::stod(data["a"][4][1].GetString());
                depth20.ap6 = std::stod(data["a"][5][0].GetString());
                depth20.av6 = std::stod(data["a"][5][1].GetString());
                depth20.ap7 = std::stod(data["a"][6][0].GetString());
                depth20.av7 = std::stod(data["a"][6][1].GetString());
                depth20.ap8 = std::stod(data["a"][7][0].GetString());
                depth20.av8 = std::stod(data["a"][7][1].GetString());
                depth20.ap9 = std::stod(data["a"][8][0].GetString());
                depth20.av9 = std::stod(data["a"][8][1].GetString());
                depth20.ap10 = std::stod(data["a"][9][0].GetString());
                depth20.av10 = std::stod(data["a"][9][1].GetString());
                depth20.ap11 = std::stod(data["a"][10][0].GetString());
                depth20.av11 = std::stod(data["a"][10][1].GetString());
                depth20.ap12 = std::stod(data["a"][11][0].GetString());
                depth20.av12 = std::stod(data["a"][11][1].GetString());
                depth20.ap13 = std::stod(data["a"][12][0].GetString());
                depth20.av13 = std::stod(data["a"][12][1].GetString());
                depth20.ap14 = std::stod(data["a"][13][0].GetString());
                depth20.av14 = std::stod(data["a"][13][1].GetString());
                depth20.ap15 = std::stod(data["a"][14][0].GetString());
                depth20.av15 = std::stod(data["a"][14][1].GetString());
                depth20.ap16 = std::stod(data["a"][15][0].GetString());
                depth20.av16 = std::stod(data["a"][15][1].GetString());
                depth20.ap17 = std::stod(data["a"][16][0].GetString());
                depth20.av17 = std::stod(data["a"][16][1].GetString());
                depth20.ap18 = std::stod(data["a"][17][0].GetString());
                depth20.av18 = std::stod(data["a"][17][1].GetString());
                depth20.ap19 = std::stod(data["a"][18][0].GetString());
                depth20.av19 = std::stod(data["a"][18][1].GetString());
                depth20.ap20 = std::stod(data["a"][19][0].GetString());
                depth20.av20 = std::stod(data["a"][19][1].GetString());
            }

#ifdef NEED_SHM
            mDepth20Publisher[key]->push(depth20);                
#endif
        }
        else if(marketTypeEnum == md::TRADES){
            const rapidjson::Value &data = rawData["data"];

            md::Trades trades;
            memset(&trades, 0, sizeof(md::Trades));
            trades.exchangeTypeEnum = exchangeTypeEnum;
            trades.instTypeEnum = instTypeEnum;
            trades.marketTypeEnum = marketTypeEnum;
            strncpy(trades.instId, info.instId, INSTID_SIZE);
            trades.tsTrans = std::stol(data["T"].GetString()) * 1000;
            trades.tsEvent = std::stol(data["E"].GetString()) * 1000;
            trades.tsRecv = tsNet;
            trades.tsParse = crypto::getCurrentTime();

            if (instTypeEnum == C_SWAP || instTypeEnum == C_FUTURES) {
               strncpy(trades.tradeId, data["a"].GetString(), INSTID_SIZE); 
            }
            else {
                strncpy(trades.tradeId, data["t"].GetString(), INSTID_SIZE);
            }
            
            trades.px = std::stod(data["p"].GetString());
            trades.sz = std::stod(data["q"].GetString());
            bool m = data["m"].GetBool();
            trades.direction = m ? DT_SHORT : DT_LONG;

#ifdef NEED_SHM
            mTradesPublisher[key]->push(trades); 
#endif
        }
        else if (marketTypeEnum == md::KLINE_1m) {
            const rapidjson::Value& data = rawData["data"];

            md::Kline kline;
            memset(&kline, 0, sizeof(md::Kline));
            kline.exchangeTypeEnum = exchangeTypeEnum;
            kline.instTypeEnum = instTypeEnum;
            kline.marketTypeEnum = marketTypeEnum;
            strncpy(kline.instId, info.instId, INSTID_SIZE);
            kline.tsTrans = std::stol(data["E"].GetString()) * 1000;
            kline.tsEvent = std::stol(data["E"].GetString()) * 1000;
            kline.tsRecv = tsNet;
            kline.tsParse = crypto::getCurrentTime();

            kline.barTime = std::stol(data["k"]["t"].GetString()) * 1000;
            kline.highPrice = std::stod(data["k"]["h"].GetString());
            kline.lowPrice = std::stod(data["k"]["l"].GetString());
            kline.openPrice = std::stod(data["k"]["o"].GetString());
            kline.closePrice = std::stod(data["k"]["c"].GetString());

            double avgPrice = 0;
            double amount = std::stod(data["k"]["q"].GetString());
            double volume = std::stod(data["k"]["v"].GetString());
            if(amount > ZERO_NUM) {
                avgPrice = amount / volume;
            }
            kline.avgPrice = avgPrice;
            kline.totalVolume = volume;
            kline.totalAmount = amount;

            kline.isFinished = data["k"]["x"].GetBool();

            if (!kline.isFinished) {
                return;
            }

#ifdef NEED_SHM
            mKlinePublisher[key]->push(kline); 
#endif
        }
        else if (marketTypeEnum == md::FUNDING_RATE) {
            const rapidjson::Value &data = rawData["data"];

            md::FundingRate fundingRate;
            memset(&fundingRate, 0, sizeof(md::FundingRate));
            fundingRate.exchangeTypeEnum = exchangeTypeEnum;
            fundingRate.instTypeEnum = instTypeEnum;
            fundingRate.marketTypeEnum = marketTypeEnum;
            strncpy(fundingRate.instId, info.instId, INSTID_SIZE);
            fundingRate.tsTrans = std::stol(data["E"].GetString()) * 1000;
            fundingRate.tsEvent = std::stol(data["E"].GetString()) * 1000;
            fundingRate.tsRecv = tsNet;
            fundingRate.tsParse = crypto::getCurrentTime();

            fundingRate.fundingRate = std::stod(data["r"].GetString());
            fundingRate.fundingTime = std::stol(data["T"].GetString()) * 1000;

#ifdef NEED_SHM
            mFundingRatePublisher[key]->push(fundingRate); 
#endif
        }
        else {
            return;
        }
    }
    else {
        return;
    }
}


md::BinanceMarket::BinanceMarket(sm::SecurityManager* s, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot, const char* host, const int port, const char* passwd) : md::BaseMarket(s, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    strcpy(exchId, "BINANCE");

    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        std::cout << "start create binance unit" << std::endl;
        auto& info = unitInfoVec[i];
        md::BinanceUnit* unit = new md::BinanceUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum, info.vInstInfo, _host, _port, _passwd);
        std::cout << "start generate sub body" << std::endl;
        unit->generateSubBody();
        binanceUnitVec.push_back(unit);
    }

}

md::BinanceMarket::~BinanceMarket() {

}


void md::BinanceMarket::start() {
    for (size_t i = 0; i < binanceUnitVec.size(); ++i) {
        binanceUnitVec[i]->start();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}
