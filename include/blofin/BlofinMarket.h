#pragma once
//
// BlofinMarket.h —— BloFin 永续合约行情（只做 DEPTH1 / TRADES / FUNDING_RATE）
//
// ⚠️⚠️ **这家是八家里唯一没有实测过的**：openapi.blofin.com 在本机一律返回
//      HTTP 451 Unavailable For Legal Reasons（REST 和 WS 都是，连文档站 docs.blofin.com 也是 451）。
//      下面的协议是照 OKX v5 公开规范写的 —— BloFin 的 REST/WS 字段名与 OKX 高度一致
//      （arg/data 信封、books1/trades 频道、instId 命名、funding-rate 接口）。
//      **部署机（境外 IP）第一次跑起来后必须核对三件事**，见文件末尾的核对清单。
//
// 【协议（按 OKX v5 推断，未实测）】
//   wss://openapi.blofin.com/ws/public
//   订阅: {"op":"subscribe","args":[{"channel":"books1","instId":"BTC-USDT"}]}
//         trade 频道名 "trades"
//   推送(深度): {"arg":{"channel":"books1","instId":"BTC-USDT"},
//                "data":[{"asks":[["p","q",..]],"bids":[[..]],"ts":"<ms>"}]}
//   推送(成交): {"arg":{"channel":"trades","instId":"BTC-USDT"},
//                "data":[{"instId":"BTC-USDT","tradeId":"...","px":"..","sz":"..",
//                         "side":"buy"|"sell","ts":"<ms>"}]}
//   心跳: 文本 "ping" → "pong"
//
// 三个已知风险点（都是"没实测"导致的）:
//   1. **字段顺序**：simdjson ondemand 前向单向，这里按 OKX 的顺序写
//      （arg 在 data 之前；books1 里 asks 在 bids 之前）。若顺序不同 ⇒ 静默零数据。
//      排查第一站：`db/test/db_depth1_shm_queue_reader` 读不到东西 + 日志里没有报错。
//   2. **books1 是否给 1 档**：OKX 的 books1 只给最优一档，BloFin 若给多档，这里取第 0 档即可。
//   3. **资金费率的 nextFundingTime 是否存在**：OKX 有，BloFin 文档缺失，取不到就退回 8 h。
//
// 资金费率走 REST：GET openapi.blofin.com/api/v5/market/funding-rate?instId=BTC-USDT
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"

namespace md {

constexpr auto BLOFIN_WS_PUBLIC = "wss://openapi.blofin.com/ws/public";

class BlofinUnit : public BaseUnit {
public:
    BlofinUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
               std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379,
               const char* passwd = "");

    void generateSubBody() override;
    void parseMarketData(const std::string& msg, int64_t tsNet) override;
    void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) override;

    bool isRestOnly() const { return marketTypeEnum == md::FUNDING_RATE; }
    void startRestOnly();

private:
    bool fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr);

    FundingRestPoller mFundingPoller;
};

class BlofinMarket : public BaseMarket {
public:
    BlofinMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                 std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
                 const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~BlofinMarket();
    void start() override;

private:
    std::vector<BlofinUnit*> unitVec;
};

} // namespace md
