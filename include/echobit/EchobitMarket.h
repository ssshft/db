#pragma once
//
// EchobitMarket.h —— Echobit 永续合约行情（只做 DEPTH1 / TRADES / FUNDING_RATE）
//
// 【实测 2026-10-20】两个端点，行情和资金费率是**不同端口**：
//   行情: wss://uapi.echobit.com/uapi/exchange/ws?timestamp=<ms>
//   费率: wss://uapi.echobit.com/uapi/ws/inform?timestamp=<ms>
//
//   订阅(深度): {"id":"depth.BTCUSDT","topic":"depth","event":"sub","symbol":"BTCUSDT","params":{}}
//   订阅(成交): {"id":"trade.BTCUSDT","topic":"trade","event":"sub","symbol":"BTCUSDT","params":{}}
//   订阅(费率): {"id":"fund_rates","topic":"fund_rates","event":"sub"}
//   推送(深度): {"symbol":"BTCUSDT","symbolName":"BTCUSDT","topic":"depth",
//                "params":{"realtimeInterval":"24h"},
//                "data":[{"s":"BTCUSDT","t":<ms>,"v":"575098044_18",
//                         "b":[["p","q"]],"a":[["p","q"]],"o":0}],"f":true,"sendTime":..,"id":".."}
//   推送(成交): {"symbol":"BTCUSDT","topic":"trade","data":[{"v":"<id>","t":<ms>,"p":"..","q":"..",
//                "f":false,"m":false}],"sendTime":..,"id":".."}
//   推送(费率): {"code":"200","topic":"fund_rates","event":"sub","data":[
//                {"symbolId":"BTC-SWAP-USDT","currentTime":"..","settleTime":<ms>,
//                 "settleRate":"..","nextSettleTime":<ms>,"fundRate":".."}]}
//
// 七个坑（全部实测）:
//   1. **symbol 两套写法**：REST/合约列表给的是 "BTC-SWAP-USDT"（= contractinfo 存的
//      originInstId），WS 行情要的是 "BTCUSDT"。而 WS 的**资金费率**又用 "BTC-SWAP-USDT"。
//      本文件在 generateSubBody 时建两张映射表，不靠字符串拼接猜。
//   2. **深度里 b(bids) 排在 a(asks) 之前**（和 XT 相同、和 WEEX 相反）。
//   3. **深度是整本推送**（实测 2621 B，b/a 各 30 档全量）⇒ 不需要 TopBook。
//   4. **成交有真正的 tradeId**（data[].v，雪花号），不需要像 XT/Hotcoin 那样拿时间戳兜底。
//   5. **`?timestamp=` 参数服务端不校验**（实测拿 1 小时前的值照样连上），
//      所以 cfg.url 里的时间戳只算"进门条"，重连时不需要刷新。
//   6. **资金费率一帧是全市场**（202 个合约 ≈ 42 KB，服务端按 8192 B 分片，Beast 自动重组）
//      ⇒ FUNDING_RATE 的 ring maxMsg 必须够大（已在 BaseMarket.cpp 调到 131072）。
//      而且**不能对每个 symbol 调 resolveInst**：那会对 170 多个不在本 unit 的合约
//      每 2 s 打一条 LOG_ERROR。本文件先用集合过滤再解析。
//   7. 心跳 {"ping":<毫秒>} → pong；60 s 内没有心跳服务端断连 ⇒ 用 client_ping_builder 现取时间。
//
// Echobit **没有**公开的资金费率 REST 接口（文档里只有 WS fund_rates），所以这家是八家里
// 唯一一家"FUNDING_RATE 也走 WS"的：不走 FundingRestPoller。
//

#include <unordered_set>

#include "base/BaseMarket.h"

namespace md {

constexpr auto ECHOBIT_WS_QUOTE  = "wss://uapi.echobit.com/uapi/exchange/ws";
constexpr auto ECHOBIT_WS_INFORM = "wss://uapi.echobit.com/uapi/ws/inform";

class EchobitUnit : public BaseUnit {
public:
    EchobitUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
                std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379,
                const char* passwd = "");

    void generateSubBody() override;
    void parseMarketData(const std::string& msg, int64_t tsNet) override;
    void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) override;

private:
    void parseDepth(const std::string& msg, int64_t tsNet);
    void parseTrade(const std::string& msg, int64_t tsNet);
    void parseFunding(const std::string& msg, int64_t tsNet);

    // WS 符号（BTCUSDT）→ originInstId（BTC-SWAP-USDT）。行情帧只带 WS 符号。
    std::unordered_map<std::string, std::string> mWsToOrigin;
    // 本 unit 关心的 originInstId 集合。资金费率帧是全市场的，先过滤再解析。
    std::unordered_set<std::string> mFundSyms;
};

class EchobitMarket : public BaseMarket {
public:
    EchobitMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                  std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
                  const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~EchobitMarket();
    void start() override;

private:
    std::vector<EchobitUnit*> unitVec;
};

} // namespace md
