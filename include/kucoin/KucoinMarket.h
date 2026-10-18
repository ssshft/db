#pragma once
//
// KucoinMarket.h —— KuCoin 永续合约行情
//
// 【实测 2026-10-17】
//   1. **必须先换 token**：POST https://api-futures.kucoin.com/api/v1/bullet-public
//      → data.token + data.instanceServers[0].endpoint
//      → wss://ws-api-futures.kucoin.com/?token=<token>&connectId=<随机>
//      token 有效期约 24h，**过期后本进程无法自愈**（URL 在 cfg 里是死的）——
//      长跑要重连时重新换 token，目前靠重启进程（录数据场景可接受）。
//   2. 订阅: {"id":"1","type":"subscribe","topic":"/contractMarket/tickerV2:XBTUSDTM",
//             "response":true,"privateChannel":false}
//      ⚠️ **privateChannel 必须显式给 false**：不带这个字段时 execution 主题只 ack 不推送（实测）。
//   3. 推送 ts 是**纳秒**（19 位，> 2^53 ⇒ 必须按 int64 读，不能读 double）。
//
// 合约代码是 "XBTUSDTM"（BTC 在 KuCoin 叫 XBT），直接取 contractinfo 存的 originInstId。
// 资金费率走 REST。
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"

namespace md {

constexpr auto KUCOIN_REST_FUTURES = "api-futures.kucoin.com";
constexpr auto KUCOIN_WS_FUTURES   = "wss://ws-api-futures.kucoin.com/";

class KucoinUnit : public BaseUnit {
public:
    KucoinUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
               std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1",
               int port = 9379, const char* passwd = "");

    void generateSubBody() override;
    void parseMarketData(const std::string& msg, int64_t tsNet) override;
    void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) override;

    bool isRestOnly() const { return marketTypeEnum == md::FUNDING_RATE; }
    void startRestOnly();

private:
    bool fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr);
    bool fetchWsToken(std::string& endpoint, std::string& token);

    FundingRestPoller mFundingPoller;
};

class KucoinMarket : public BaseMarket {
public:
    KucoinMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                 std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
                 const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~KucoinMarket();
    void start() override;

private:
    std::vector<KucoinUnit*> kucoinUnitVec;
};

} // namespace md
