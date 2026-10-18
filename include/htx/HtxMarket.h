#pragma once
//
// HtxMarket.h —— HTX (火币) 永续合约行情
//
// 【实测 2026-10-17，不是查文档抄的】
//   现货与合约是两套完全不同的 host/协议，这里只做合约：
//     USDT 本位 (linear) : wss://api.hbdm.com/linear-swap-ws
//     币本位   (inverse): wss://api.hbdm.com/swap-ws
//   订阅: {"sub":"market.BTC-USDT.bbo","id":"1"}  → 回 {"id":"1","status":"ok",...}
//   推送: {"ch":"market.BTC-USDT.bbo","ts":...,"tick":{"bid":[p,v],"ask":[p,v],"ts":...}}
//         {"ch":"market.BTC-USDT.trade.detail","tick":{"data":[{amount,quantity,price,ts,id,direction}]}}
//
// 三个坑（都实测过）:
//   1. **所有帧都是 gzip 压缩的 binary frame**，包括订阅回执 ⇒ 必须先 inflate 再解析。
//   2. **服务端每 5s 发 {"ping":<ts>}，客户端必须回 {"pong":<ts>}**，否则直接断。
//      ⇒ cfg.ping_mode 用 Disabled（不要让 beast 发协议级 ping），在 onWebsocketMsg 里回。
//   3. depth.step0 是**全量 150 档**，DEPTH1 用 bbo（单档最优）—— 轻 20 倍。
//
// 资金费率没有 WS 通道，走 REST 轮询（见 base/FundingRestPoller.h）。
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"

namespace md {

constexpr auto HTX_WS_LINEAR_SWAP = "wss://api.hbdm.com/linear-swap-ws";
constexpr auto HTX_WS_C_SWAP      = "wss://api.hbdm.com/swap-ws";

class HtxUnit : public BaseUnit {
public:
    HtxUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
            std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1",
            int port = 9379, const char* passwd = "");

    void generateSubBody() override;
    void parseMarketData(const std::string& msg, int64_t tsNet) override;
    void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) override;

    // FUNDING_RATE 不建 WS 连接，只跑 REST 轮询；<X>Market::start() 据此分支
    bool isRestOnly() const { return marketTypeEnum == md::FUNDING_RATE; }
    void startRestOnly();

private:
    bool fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr);

    FundingRestPoller mFundingPoller;
    int               subId{0};
};

class HtxMarket : public BaseMarket {
public:
    HtxMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
              std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
              const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~HtxMarket();
    void start() override;

private:
    std::vector<HtxUnit*> htxUnitVec;
};

} // namespace md
