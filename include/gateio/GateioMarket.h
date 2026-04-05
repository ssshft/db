#pragma once
#include "base/BaseMarket.h"


#ifdef NEED_TESTNET

constexpr auto GATEIO_WS_PUBLIC_SPOT = "wss://ws-testnet.gate.com/ws/v4/spot";

constexpr auto GATEIO_WS_PUBLIC_USDT_SWAP = "wss://ws-testnet.gate.com/v4/ws/futures/usdt";

constexpr auto GATEIO_WS_PUBLIC_BTC_SWAP =  "wss://fx-ws-testnet.gateio.ws/v4/ws/btc";

constexpr auto GATEIO_WS_PUBLIC_USDT_FUTURES = "wss://fx-ws-testnet.gateio.ws/v4/ws/delivery/usdt";

constexpr auto GATEIO_WS_PUBLIC_BTC_FUTURES = "wss://fx-ws-testnet.gateio.ws/v4/ws/delivery/btc";

#else


constexpr auto GATEIO_WS_PUBLIC_SPOT = "wss://api.gateio.ws/ws/v4/";

constexpr auto GATEIO_WS_PUBLIC_USDT_SWAP = "wss://fx-ws.gateio.ws/v4/ws/usdt";

constexpr auto GATEIO_WS_PUBLIC_BTC_SWAP =  "wss://fx-ws.gateio.ws/v4/ws/btc";

constexpr auto GATEIO_WS_PUBLIC_USDT_FUTURES = "wss://fx-ws.gateio.ws/v4/ws/delivery/usdt";

constexpr auto GATEIO_WS_PUBLIC_BTC_FUTURES = "wss://fx-ws.gateio.ws/v4/ws/delivery/btc";

#endif



namespace md {
    class GateioUnit : public BaseUnit {
    public:
        GateioUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379, const char* passwd = "");
    
        virtual void generateSubBody();
        virtual void subWebsocekt();
        virtual void onWebsocketMsg(const websocket_incoming_message &in_msg);
        virtual void parseMarketData(const std::string& msg);
        virtual void ping();
        virtual void pong();

        void parseSpotData(const std::string& msg);
        void parseSwapData(const std::string& msg);

    private:
        std::vector<web::json::value> subValueVec; 
    };


    class GateioMarket : public BaseMarket {
    public:
        GateioMarket(sm::SecurityManager* s, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30, const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
        ~GateioMarket();
        virtual void start();

    private:
        std::vector<GateioUnit*> gateioUnitVec;
    };
}