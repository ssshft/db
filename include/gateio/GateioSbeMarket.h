#pragma once
#include "base/BaseMarket.h"
#include "gateio/GateioSbeDecoder.h"



constexpr auto GATEIO_WS_SBE_SPOT = "wss://api.gateio.ws/ws/v4/sbe";

constexpr auto GATEIO_WS_SBE_USDT_SWAP = "wss://fx-ws.gateio.ws/v4/ws/usdt/sbe";

constexpr auto GATEIO_WS_SBE_BTC_SWAP =  "wss://fx-ws.gateio.ws/v4/ws/btc/sbe";

constexpr auto GATEIO_WS_SBE_USDT_FUTURES = "wss://fx-ws.gateio.ws/v4/ws/delivery/usdt/sbe";

constexpr auto GATEIO_WS_SBE_BTC_FUTURES = "wss://fx-ws.gateio.ws/v4/ws/delivery/btc/sbe";



namespace md {
    class GateioSbeUnit : public BaseUnit {
    public:
        GateioSbeUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, SbeAccount sbeAcc, const char* host = "127.0.0.1", int port = 9379, const char* passwd = "");
    
        virtual void generateSubBody();
        virtual void parseMarketData(const std::string& msg);
        virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns);

        void parseSpotData(const uint8_t* data, size_t len, int64_t tsNet);
        void parseFuturesData(const uint8_t* data, size_t len, int64_t tsNet);

    private:
        bool lookupInfo(std::string_view sym, md::InstrumentInfo& out) const;

        SbeAccount sbeAccount;
    };


    class GateioSbeMarket : public BaseMarket {
    public:
        GateioSbeMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, SbeAccount sbeAccount, int lot = 30, const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
        ~GateioSbeMarket();
        virtual void start();

    private:
        std::vector<GateioSbeUnit*> gateioSbeUnitVec;
    };
}