#pragma once
//
// PhemexMarket.h —— Phemex 永续合约行情（只做 DEPTH1 / TRADES / FUNDING_RATE）
//
// 【实测 2026-10-20】
//   wss://ws.phemex.com
//   订阅: {"id":1,"method":"orderbook_p.subscribe","params":["BTCUSDT"]}     ← 深度
//         {"id":2,"method":"trade_p.subscribe","params":["BTCUSDT"]}         ← 成交
//   回执: {"error":null,"id":1,"result":{"status":"success"}}
//   推送(深度): {"depth":30,"dts":...,"mts":...,"orderbook_p":{"asks":[["p","q"]],"bids":[[..]]},
//                "sequence":...,"symbol":"BTCUSDT","timestamp":...,"type":"incremental"}
//   推送(成交): {"dts":...,"mts":...,"sequence":...,"symbol":"BTCUSDT",
//                "trades_p":[[<纳秒>,"Buy","81498.2","0.202"]],"type":"incremental"}
//
// 六个坑（全部实测）:
//   1. **深度是增量的**：首帧是全量快照（**没有 "type" 字段**），之后 type="incremental"，
//      只推变化的档位，q 为 "0" 表示删档。所以必须维护一本小簿子（base/TopBook.h），
//      否则买一/卖一会永远停在某个已经被吃掉的价位上。
//   2. **depth 里 asks 排在 bids 之前**，而且 **symbol 排在 orderbook_p 之后**。
//      simdjson ondemand 前向单向 ⇒ 不能"先取 symbol 再取档位"。本文件的解法是：
//      先把档位收进栈上小数组，读完 symbol 再回放到对应 symbol 的簿子里。
//   3. **成交时间的单位是纳秒**（1792488091742495384），不是毫秒也不是微秒。
//      trades_p 的数组元素顺序是 [ts, side, price, qty]，**side 在第 2 位**。
//   4. **orderbook_p 的 depth 参数交易所不认**：params 里传 {"depth":5} 依然每次给 30 档
//      （实测），所以 DEPTH1 的 ring maxMsg 必须够大（已在 BaseMarket.cpp 调到 8192）。
//   5. **心跳要客户端发** {"id":N,"method":"server.ping","params":[]} → {"result":"pong"}。
//      服务端不发 WS PING，所以不能用 ServerOnly。
//   6. 订阅可以在一个 params 里放多个 symbol，但这里按 symbol 一条一条发 ——
//      Phemex 对单条消息的 symbol 数有上限且各文档写法不一，30 条消息的成本可以忽略。
//
// 资金费率走 REST：GET api.phemex.com/contract-biz/public/real-funding-rates?symbol=BTCUSDT
//   {"code":0,"data":{"rows":[{"fundingRate":"0.00003931","nextfundingTime":1792512000000}]}}
//   注意是 nextfundingTime（小写 f），不是 nextFundingTime。
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"
#include "base/TopBook.h"

namespace md {

constexpr auto PHEMEX_WS = "wss://ws.phemex.com";

class PhemexUnit : public BaseUnit {
public:
    PhemexUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
               std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1",
               int port = 9379, const char* passwd = "");

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

    // originInstId → 最优价簿子。DEPTH1 unit 才有内容。
    std::unordered_map<std::string, TopBook> mBooks;
    // 簿子漏档时只告警一次，避免每帧刷屏
    bool bookWarned_{false};
};

class PhemexMarket : public BaseMarket {
public:
    PhemexMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                 std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
                 const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~PhemexMarket();
    void start() override;

private:
    std::vector<PhemexUnit*> unitVec;
};

} // namespace md
