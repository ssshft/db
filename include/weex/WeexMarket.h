#pragma once
//
// WeexMarket.h —— WEEX 永续合约行情（只做 DEPTH1 / TRADES / FUNDING_RATE）
//
// 【实测 2026-10-20】
//   wss://ws-contract.weex.com/v3/ws/public      ← 注意是 **v3**，不是 v1/v2
//   订阅: {"method":"SUBSCRIBE","params":["BTCUSDT@depth15","BTCUSDT@trade"],"id":1}
//   回执: {"cid":"...","event":"connected","time":"..."} → {"result":true,"id":1}
//   推送(深度): {"e":"depthSnapshot"|"depth","E":<ms>,"s":"BTCUSDT","U":..,"u":..,"l":15,
//                "d":"SNAPSHOT"|"CHANGED","b":[["p","q"]],"a":[["p","q"]]}
//   推送(成交): {"e":"tradeSnapshot"|"trade","E":<ms>,"s":"BTCUSDT",
//                "d":[{"T":<ms>,"t":"<uuid>","p":"81443.8","q":"0.0035","v":"285.05","m":false}]}
//
// 六个坑（全部实测）:
//   1. **v1/v2 路径会被 521 挡掉**（wss://ws-contract.weex.com/v1/ws 实测 HTTP 521），
//      只有 /v3/ws/public 能握手。旧文档里的 {"op":"subscribe","args":[...]} 是 v1 语法，
//      在 v3 上连上就立刻被服务端断开（连错误帧都不给）。
//   2. **深度是增量的**：d="SNAPSHOT" 是全量（要先清簿子），d="CHANGED" 是增量，
//      q 为 "0" 表示删档。只取每帧第一条会让买一/卖一停在已被吃掉的价位上。
//   3. **depth 里 b(bids) 排在 a(asks) 之前**（和 XT / Echobit 相反）。
//   4. **depth 最小档位是 15**（枚举只有 15 / 200），没有 1 档频道 ⇒ 必须自己从 15 档里取最优。
//   5. **成交帧会重复推**：tradeSnapshot 里的最近 N 笔，后续 trade 帧里会再出现一遍
//      （实测同一 uuid 在 snapshot 和 incremental 里各出现一次）。这里按
//      "T 严格递增 / 同 T 时 t 不同" 去重，否则成交量会被算两遍。
//   6. 心跳是 {"method":"PING"}（JSON，不是明文 ping）→ {"result":true}。
//
// 资金费率走 REST：GET api-contract.weex.com/capi/v3/market/fundingRate?symbol=BTCUSDT
//   [{"symbol":"BTCUSDT","fundingRate":"0.00007117","fundingTime":1792483200000,"markPrice":"81520.2"}, ...]
//   **返回的是历史序列（新→旧），没有"下次结算时间"** ⇒ 下次时间 = 最近一次 + 相邻两条的间隔
//   （实测间隔正好 8 h）。若只返回一条，退回 8 h 常量。
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"
#include "base/TopBook.h"

namespace md {

constexpr auto WEEX_WS_PUBLIC = "wss://ws-contract.weex.com/v3/ws/public";

class WeexUnit : public BaseUnit {
public:
    WeexUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
             std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379,
             const char* passwd = "");

    void generateSubBody() override;
    void parseMarketData(const std::string& msg, int64_t tsNet) override;
    void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) override;

    bool isRestOnly() const { return marketTypeEnum == md::FUNDING_RATE; }
    void startRestOnly();

private:
    bool fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr);

    void parseDepth(const std::string& msg, int64_t tsNet);
    void parseTrade(const std::string& msg, int64_t tsNet);

    FundingRestPoller mFundingPoller;

    std::unordered_map<std::string, TopBook> mBooks;

    // 成交去重：每个 symbol 记住上一笔的 (T, uuid)
    struct LastTrade {
        int64_t     tsMs{0};
        std::string id;
    };
    std::unordered_map<std::string, LastTrade> mLastTrade;
};

class WeexMarket : public BaseMarket {
public:
    WeexMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
               std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
               const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~WeexMarket();
    void start() override;

private:
    std::vector<WeexUnit*> unitVec;
};

} // namespace md
