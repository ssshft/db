#pragma once
#include "base/BaseMarket.h"


#ifdef NEED_TESTNET

constexpr auto BYBIT_WS_PUBLIC_SPOT = "wss://stream-testnet.bybit.com/v5/public/spot";

constexpr auto BYBIT_WS_PUBLIC_USDT_SWAP = "wss://stream-testnet.bybit.com/v5/public/linear";

constexpr auto BYBIT_WS_PUBLIC_C_SWAP = "wss://stream-testnet.bybit.com/v5/public/inverse";

#else

constexpr auto BYBIT_WS_PUBLIC_SPOT = "wss://stream.bybit.com/v5/public/spot";

constexpr auto BYBIT_WS_PUBLIC_USDT_SWAP = "wss://stream.bybit.com/v5/public/linear";

constexpr auto BYBIT_WS_PUBLIC_C_SWAP = "wss://stream.bybit.com/v5/public/inverse";

#endif



namespace md {
    class BybitUnit : public BaseUnit {
    public:
        BybitUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379, const char* passwd = "");
    
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
        std::unordered_map<std::string, long> mFundingTime;
    };


    class BybitMarket : public BaseMarket {
    public:
        BybitMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30, const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
        ~BybitMarket();
        virtual void start();

    private:
        std::vector<BybitUnit*> bybitUnitVec;
    };
}