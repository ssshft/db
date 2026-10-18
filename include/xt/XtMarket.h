#pragma once
//
// XtMarket.h —— XT 永续合约行情
//
// 【实测 2026-10-17】
//   wss://fstream.xt.com/ws/market
//   订阅: {"method":"SUBSCRIBE","params":["depth@btc_usdt,5,100ms"],"id":"d"}
//   回执: {"code":0,"msg":"success","id":"d","sessionId":"..."}
//   推送: {"topic":"depth","event":"depth@btc_usdt,5","data":{"s":"btc_usdt","a":[["p","v"]],"b":[[..]],"t":<ms>}}
//         {"topic":"trade","event":"trade@btc_usdt","data":{"s","p","a","m":"BID","t":<ms>}}
//
// 四个坑（都实测过）:
//   1. **WS host 是 fstream.xt.com/ws/market**（不是 /public，也不是 fapi.xt.com）。
//   2. **depth 没有 1 档**：格式是 depth@{symbol},{levels},{interval}，levels 只能是 5/10/20/50，
//      interval 只能是 100/250/500/1000(ms)。DEPTH1 取 depth@...,5,100ms 的第 0 档。
//      写 depth@btc_usdt / depth_1@ / depth_20@ 都是 "args error"。
//   3. **心跳是明文文本 "ping"**（不是 JSON！发 {"method":"PING"} 会回 "Invalid method" 并 30s 断开），
//      服务端回明文 "pong"。
//   4. symbol 是**小写** btc_usdt，与 contractinfo 存的 originInstId 一致。
//
// 资金费率走 REST。
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"

namespace md {

constexpr auto XT_WS_FUTURES = "wss://fstream.xt.com/ws/market";

class XtUnit : public BaseUnit {
public:
    XtUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
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
};

class XtMarket : public BaseMarket {
public:
    XtMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
             std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
             const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~XtMarket();
    void start() override;

private:
    std::vector<XtUnit*> xtUnitVec;
};

} // namespace md
