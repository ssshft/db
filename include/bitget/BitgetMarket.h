#pragma once
//
// BitgetMarket.h —— Bitget 永续合约行情
//
// 【实测 2026-10-17】
//   wss://ws.bitget.com/v2/ws/public
//   订阅: {"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"books1","instId":"BTCUSDT"}]}
//   回执: {"event":"subscribe","arg":{...}}
//   推送: {"action":"snapshot","arg":{...},"data":[{"asks":[["p","v"]],"bids":[["p","v"]],"ts":".."}],"ts":..}
//
// 三个要点:
//   1. **instId 是 "BTCUSDT"（无分隔符）**，与 contractinfo 存的 originInstId 一致，不要自己拼。
//   2. **books1 每一帧都是完整的一档快照**（action 恒为 "snapshot"），不需要做增量维护。
//   3. 所有价格/数量都是**字符串**；顶层 ts 是数字 ms。
//
// 资金费率有 WS (ticker) 但缺"下次结算时间"以外的口径，统一走 REST 轮询。
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"

namespace md {

constexpr auto BITGET_WS_PUBLIC = "wss://ws.bitget.com/v2/ws/public";

class BitgetUnit : public BaseUnit {
public:
    BitgetUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
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
    std::string       mProductType{"USDT-FUTURES"};   // Bitget 的 instType 叫 productType
};

class BitgetMarket : public BaseMarket {
public:
    BitgetMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                 std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
                 const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~BitgetMarket();
    void start() override;

private:
    std::vector<BitgetUnit*> bitgetUnitVec;
};

} // namespace md
