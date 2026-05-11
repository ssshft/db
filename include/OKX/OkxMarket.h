#pragma once
#include "base/BaseMarket.h"


#ifdef NEED_TESTNET

constexpr auto OKX_WS_PUBLIC = "wss://wspap.okx.com:8443/ws/v5/public";

constexpr auto OKX_WS_PUBLIC_BUSINESS = "wss://wspap.okx.com:8443/ws/v5/business";

#else

constexpr auto OKX_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public";

constexpr auto OKX_WS_PUBLIC_BUSINESS = "wss://ws.okx.com:8443/ws/v5/business";

#endif



namespace md {
    class OkxUnit : public BaseUnit {
    public:
        OkxUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379, const char* passwd = "");
    
        virtual void generateSubBody();
        virtual void subWebsocekt();
        virtual void onWebsocketMsg(const websocket_incoming_message &in_msg);
        virtual void parseMarketData(const std::string& msg);
        virtual void ping();
        virtual void pong();

    private:
        int subCount;
        web::json::value subValue; 
    };


    class OkxMarket : public BaseMarket {
    public:
        OkxMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30, const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
        ~OkxMarket();
        virtual void start();

    private:
        std::vector<OkxUnit*> okxUnitVec;
    };
}