#pragma once
//
// BitunixMarket.h —— Bitunix 永续合约行情（只做 DEPTH1 / TRADES / FUNDING_RATE）
//
// 【实测 2026-10-20】
//   wss://fapi.bitunix.com/public/          （私有频道在 /private/，这里不用）
//   订阅: {"op":"subscribe","args":[{"symbol":"BTCUSDT","ch":"depth_book1"}]}
//         trade 频道名就是 "trade"
//   回执: {"op":"connect","ts":...,"data":{"result":true}}
//   推送: {"ch":"depth_book1","symbol":"BTCUSDT","ts":<ms>,"data":{"b":[["p","q"]],"a":[["p","q"]]}}
//         {"ch":"trade","symbol":"BTCUSDT","ts":<ms>,"data":[{"t":"2026-10-20T09:12:43Z","p","v","s":"sell"}]}
//
// 五个坑（前四个实测过，第五个是文档约束）:
//   1. **depth 里 b(bids) 排在 a(asks) 之前**。simdjson ondemand 前向单向，把 a 写在 b 前面
//      会让 b 静默取不到 ⇒ 买价恒为 0 ⇒ 整家所没有 DEPTH1。这是本文件最容易踩的一处。
//   2. **成交时间 t 是 ISO-8601 字符串且只到秒**（"2026-10-20T09:12:43Z"），不是毫秒时间戳，
//      也**没有 tradeId**。同一秒的多笔成交无法区分 ⇒ tradeId 直接用这串时间戳，
//      tsTrans 只有 1 s 分辨率（对"按秒聚合的成交量"够用，对逐笔排序不够）。
//   3. **心跳要带时间戳**：{"op":"ping","ping":<秒>}。实测 ping 填 0 服务端**不回 pong**
//      ⇒ 连接被判死。所以这里用 WsConfig::client_ping_builder 现取时间戳（见 BeastWsClient.h）。
//   4. **一个连接最多 300 个频道、每秒最多 5 条消息** ⇒ 所有 symbol 的订阅必须塞进**一条**
//      args 数组，不能一个 symbol 发一条（30 个 symbol 就是 30 条消息，超限直接被断）。
//   5. depth_book1 推的是"当前最优一档"的**全量**（b/a 各 1 条），不是增量 ⇒ 不需要 TopBook。
//
// 资金费率走 REST：GET fapi.bitunix.com/api/v1/futures/market/funding_rate?symbol=BTCUSDT
//   {"code":0,"data":{"fundingRate":"0.00441","fundingInterval":8,"nextFundingTime":"1792512000000"}}
//   注意 fundingRate / nextFundingTime 都是**字符串**，且 nextFundingTime 是毫秒。
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"

namespace md {

constexpr auto BITUNIX_WS_PUBLIC = "wss://fapi.bitunix.com/public/";

class BitunixUnit : public BaseUnit {
public:
    BitunixUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
                std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1",
                int port = 9379, const char* passwd = "");

    void generateSubBody() override;
    void parseMarketData(const std::string& msg, int64_t tsNet) override;
    void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) override;

    bool isRestOnly() const { return marketTypeEnum == md::FUNDING_RATE; }
    void startRestOnly();

private:
    bool fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr);
    void publishDepth1(md::InstrumentInfo& info, const std::string& key, double bp, double bv, double ap,
                       double av, int64_t tsMs, int64_t tsNet);
    void publishTrade(md::InstrumentInfo& info, const std::string& key, double px, double sz, Direction dir,
                      const char* tradeId, int64_t tsMs, int64_t tsNet);

    FundingRestPoller mFundingPoller;
};

class BitunixMarket : public BaseMarket {
public:
    BitunixMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                  std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
                  const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~BitunixMarket();
    void start() override;

private:
    std::vector<BitunixUnit*> unitVec;
};

} // namespace md
