#pragma once
//
// MexcMarket.h —— MEXC 永续合约行情
//
// 【实测 2026-10-17】
//   wss://contract.mexc.com/edge
//   订阅: {"method":"sub.depth.full","param":{"symbol":"BTC_USDT","limit":5}}
//         {"method":"sub.deal","param":{"symbol":"BTC_USDT"}}
//   回执: {"channel":"rs.sub.deal","data":"success","ts":...}
//   推送: {"symbol":"BTC_USDT","channel":"push.depth.full","data":{"cts":..,"asks":[[p,v,cnt]],"bids":[[..]],"version":..}}
//         {"symbol":"BTC_USDT","channel":"push.deal","data":[{p,v,T,O,M,t,i,cts}]}
//
// 三个坑:
//   1. **不要用 sub.depth**：它是**增量**推送（带 version/begin/end），要做本地簿维护。
//      DEPTH1 用 sub.depth.full + limit=5，每帧都是完整快照，取第 0 档即可。
//   2. sub.bookTicker / sub.trade **这两个方法名不存在**（发了没有任何回执，静默不生效）。
//      正确的是 sub.depth.full / sub.deal。
//   3. 数量单位是**张**（合约张数），要 * info.value 才是 base 币。
//
// 资金费率: sub.ticker 有 fundingRate 但没有下次结算时间 → 统一走 REST。
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"

namespace md {

constexpr auto MEXC_WS_CONTRACT = "wss://contract.mexc.com/edge";

class MexcUnit : public BaseUnit {
public:
    MexcUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
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

class MexcMarket : public BaseMarket {
public:
    MexcMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
               std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
               const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~MexcMarket();
    void start() override;

private:
    std::vector<MexcUnit*> mexcUnitVec;
};

} // namespace md
