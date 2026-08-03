#pragma once
#include "base/BaseMarket.h"


#ifdef NEED_TESTNET

constexpr auto BINANCE_WS_PUBLIC_SPOT = "wss://stream.binance.com:9443/stream?";

constexpr auto BINANCE_WS_PUBLIC_USDT_SWAP_FUTURES_PUBLIC = "wss://fstream.binancefuture.com/public/stream?";

constexpr auto BINANCE_WS_PUBLIC_USDT_SWAP_FUTURES_MARKET = "wss://fstream.binancefuture.com/market/stream?";

constexpr auto BINANCE_WS_PUBLIC_USD_SWAP_FUTURES = "wss://dstream.binancefuture.com/stream?";

#else

constexpr auto BINANCE_WS_PUBLIC_SPOT = "wss://stream.binance.com:443/stream?";

constexpr auto BINANCE_WS_PUBLIC_USDT_SWAP_FUTURES_PUBLIC = "wss://fstream.binance.com/public/stream?";

constexpr auto BINANCE_WS_PUBLIC_USDT_SWAP_FUTURES_MARKET = "wss://fstream.binance.com/market/stream?";

constexpr auto BINANCE_WS_PUBLIC_USD_SWAP_FUTURES = "wss://dstream.binance.com/stream?";

#endif



namespace md {
    class BinanceUnit : public BaseUnit {
    public:
        BinanceUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379, const char* passwd = "");
    
        virtual void generateSubBody();
        virtual void parseMarketData(const std::string& msg);
        virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns);
        
    private:
        int subId{0};
        std::vector<std::string> subParams;
    };


    class BinanceMarket : public BaseMarket {
    public:
        BinanceMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30, const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
        ~BinanceMarket();
        virtual void start();

    private:
        std::vector<BinanceUnit*> binanceUnitVec;
    };
}