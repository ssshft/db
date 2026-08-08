#pragma once
#include "base/BaseMarket.h"
#include "okx/OkxSbeDecoder.h"


#ifdef NEED_TESTNET

// SBE 模拟盘 endpoint
constexpr auto OKX_WS_SBE_PUBLIC = "wss://wspap.okx.com:8443/ws/v5/public-sbe
constexpr auto OKX_REST_HOST     = "wspap.okx.com";

#else

// SBE 实盘 endpoint (专用二进制推送通道)
constexpr auto OKX_WS_SBE_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public-sbe";
constexpr auto OKX_REST_HOST     = "www.okx.com";

#endif

constexpr auto OKX_REST_PORT     = "443";


namespace md {

    class OkxSbeUnit : public BaseUnit {
    public:
        OkxSbeUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, SbeAccount sbeAcc, const char* host = "127.0.0.1", int port = 9379, const char* passwd = "");

        virtual void generateSubBody();

        // BeastWsClient 回调:
        //   - isBinary=true : SBE 帧, 原样 memcpy 到 std::string 送 mQueue
        //   - isBinary=false: text 帧 (登录/订阅回执 / server "pong"), 不入队
        virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns);

        // consume 线程调, 从 mQueue 里取出 raw bytes, 走 OkxSbeDecoder 解码。
        virtual void parseMarketData(const std::string& msg);

    private:
        // -- SBE decode 分支 (只解 templateId 1000/1001/1005; 1002/1003/1004/1006 略过或后续扩展) --
        void handleBboTbt(const uint8_t* data, size_t len, long tsNet);
        void handleBooksL2(const uint8_t* data, size_t len, long tsNet);   // 1001 (asks+bids 全量)
        void handleTrades(const uint8_t* data, size_t len, long tsNet);
        void handleExpUpdate(const uint8_t* data, size_t len);             // 1002: 更新 exponent 表

        // 通过 instIdCode 反查 InstrumentInfo, 找不到返回 false
        bool lookupByCode(int64_t code, md::InstrumentInfo& out) const;

    private:
        SbeAccount sbeAccount; 
        std::vector<std::string> subArgs;

        // instIdCode → InstrumentInfo, 启动时 REST 拉一次填满, 之后 read-only。
        // 只放 vInstInfo 里配置的 instId, 避免 O(百万) 级 map。
        std::unordered_map<int64_t, md::InstrumentInfo> mCodeToInfo;
    };


    class OkxSbeMarket : public BaseMarket {
    public:
        OkxSbeMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, SbeAccount sbeAccount, int lot = 30, const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
        ~OkxSbeMarket();
        virtual void start();

    private:
        std::vector<OkxSbeUnit*> okxSbeUnitVec;
    };

} // namespace md