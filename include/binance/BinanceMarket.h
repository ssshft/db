#pragma once
#include "base/BaseMarket.h"


#ifdef NEED_TESTNET

constexpr auto BINANCE_WS_PUBLIC_SPOT = "wss://stream.binance.com:9443/stream?";

constexpr auto BINANCE_WS_PUBLIC_USDT_SWAP_FUTURES = "wss://fstream.binancefuture.com/stream?";

constexpr auto BINANCE_WS_PUBLIC_USD_SWAP_FUTURES = "wss://dstream.binancefuture.com/stream?";

#else

constexpr auto BINANCE_WS_PUBLIC_SPOT = "wss://stream.binance.com:443/stream?";

constexpr auto BINANCE_WS_PUBLIC_USDT_SWAP_FUTURES = "wss://fstream.binance.com/stream?";

constexpr auto BINANCE_WS_PUBLIC_USD_SWAP_FUTURES = "wss://dstream.binance.com/stream?";

#endif



namespace md {
    class BinanceUnit : public BaseUnit {
    public:
        BinanceUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379, const char* passwd = "");
    
        virtual void generateSubBody();
        virtual void subWebsocekt();
        virtual void onWebsocketMsg(const websocket_incoming_message &in_msg);
        virtual void parseMarketData(const std::string& msg);
        virtual void ping();
        virtual void pong();

    private:
        int subCount;
        int subId;
        web::json::value subValue; 
    };


    class BinanceMarket : public BaseMarket {
    public:
        BinanceMarket(sm::SecurityManager* s, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30, const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
        ~BinanceMarket();
        virtual void start();

    private:
        std::vector<BinanceUnit*> binanceUnitVec;
    };
}