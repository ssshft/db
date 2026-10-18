#pragma once
//
// BingxMarket.h —— BingX 永续合约行情
//
// 【实测 2026-10-17】
//   wss://open-api-swap.bingx.com/swap-market
//   订阅: {"id":"d1","reqType":"sub","dataType":"BTC-USDT@depth5"}
//   推送: {"code":0,"dataType":"BTC-USDT@depth5","ts":<ms>,"data":{"bids":[["p","v"]],"asks":[[..]]}}
//         {"code":0,"dataType":"BTC-USDT@trade","data":[{"q":"..","p":"..","T":<ms>,"m":true,"s":"BTC-USDT"}]}
//
// 四个坑（都实测过）:
//   1. **没有 @depth1**（发过去回 80015 dataType not support）。最小档是 @depth5，
//      取第 0 档即可。@ticker 虽然也带 B/b/A/a，但只有 1s 一帧，太粗。
//   2. **帧是 gzip 压缩的 binary**，必须先 inflate。
//   3. **心跳是服务端发文本 "Ping"，客户端回文本 "Pong"**（不是 JSON，也不是协议级 ping）。
//      不回 → 约 30s 后 1006 断开。所以 ping_mode 用 Disabled，自己在 onWebsocketMsg 里回。
//   4. 资金费率**没有 WS 通道**（@markPrice 只推标记价）→ REST。
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"

namespace md {

constexpr auto BINGX_WS_SWAP = "wss://open-api-swap.bingx.com/swap-market";

class BingxUnit : public BaseUnit {
public:
    BingxUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
              std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1",
              int port = 9379, const char* passwd = "");

    void generateSubBody() override;
    void parseMarketData(const std::string& msg, int64_t tsNet) override;
    void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) override;

    bool isRestOnly() const { return marketTypeEnum == md::FUNDING_RATE; }
    void startRestOnly();

private:
    bool fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr);

    FundingRestPoller mFundingPoller;
    int               subId{0};
};

class BingxMarket : public BaseMarket {
public:
    BingxMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
                const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~BingxMarket();
    void start() override;

private:
    std::vector<BingxUnit*> bingxUnitVec;
};

} // namespace md
