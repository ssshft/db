#pragma once
//
// MgbxMarket.h —— MGBX 永续合约行情（只做 DEPTH1 / TRADES / FUNDING_RATE）
//
// 【实测 2026-10-20】
//   wss://open.mgbx.com/fut/v1/ws/market
//   订阅(一个 symbol 一条): {"req":"sub_symbol","symbol":"btc_usdt"}
//   回执: {"code":0,"msg":"success"}
//
//   ⚠️ **订阅一个 symbol 会把该 symbol 的 7 个频道全部推过来**，不能选频道：
//      push.deep      增量（实测 ~17 帧/s）
//      push.deep.full 全量（实测 ~0.5 帧/s，4017 B）
//      push.deal      成交
//      push.ticker / push.agg.ticker / push.mark.price / push.index.price  （本文件不用）
//
//   推送(增量): {"channel":"push.deep","data":{"id":"682772047314550853","s":"btc_usdt",
//                "ba":1,"p":"81387.1","q":"132291","t":<ms>}}       ba: 1=bid, 2=ask
//   推送(全量): {"channel":"push.deep.full","data":{"s":"btc_usdt","id":"...",
//                "a":[["p","q"]],"b":[["p","q"]]}}                  a 在 b 之前
//   推送(成交): {"channel":"push.deal","data":{"s":"btc_usdt","p":"81392.9","a":"1507",
//                "m":"BID","t":<ms>}}
//
// 七个坑（全部实测）:
//   1. **深度是增量的**：push.deep 一次只给一个档位的变更，q=0 表示删档；全量 push.deep.full
//      只有 ~0.5 帧/s ⇒ 光靠全量会有 2 s 陈旧，光靠增量又没法重建簿子。两者都要（TopBook）。
//   2. **push.deep.full 里 a(asks) 排在 b(bids) 之前**；push.deep 里 ba 是**数字**（1/2）不是字符串。
//   3. **数量/价格可能是科学计数法**："1.125E+4" / "2.5E+5"（实测在 4~5 位数量级上出现）。
//      `crypto::fast_atod` 走 std::from_chars，**不接受科学计数法，会返回 NAN**
//      ⇒ 用本文件的 safe_atod（先 fast_atod，NAN 再退回 strtod）。这条不处理会静默丢档。
//   4. **symbol 必须带下划线小写**（btc_usdt）；写 btcusdt 服务端只回一个 "succeed" 然后
//      **一条数据都不推**（不报错）。
//   5. **心跳是明文文本 "ping"**（不是 JSON），服务端回明文 "pong"，25 s 一次，30 s 没有就断。
//   6. 一个 symbol 的订阅要发一条消息，30 个 symbol 就是 30 条 —— 实测没有频率限制问题。
//   7. **MGBX 没有公开的资金费率接口**：REST 文档里只有 q/ticker、q/tickers、q/depth、q/deal、
//      q/kline、symbol/detail、time 七个公共端点，WS 也只有上面 7 个频道。
//      ⇒ 这家的 FUNDING_RATE 是**唯一缺口**，这里只打一条 LOG_ERROR 说明，不静默。
//

#include "base/BaseMarket.h"
#include "base/TopBook.h"

namespace md {

constexpr auto MGBX_WS_MARKET = "wss://open.mgbx.com/fut/v1/ws/market";

class MgbxUnit : public BaseUnit {
public:
    MgbxUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
             std::vector<md::InstrumentInfo>& instInfoVec, const char* host = "127.0.0.1", int port = 9379,
             const char* passwd = "");

    void generateSubBody() override;
    void parseMarketData(const std::string& msg, int64_t tsNet) override;
    void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) override;

    // MGBX 没有资金费率接口 ⇒ 这个 unit 什么都不做，start() 里要跳过它
    // （cfg.url 为空时 WsClient::validate 会抛 invalid_argument）。
    bool isRestOnly() const { return marketTypeEnum == md::FUNDING_RATE; }

private:
    void parseDepth(const std::string& msg, int64_t tsNet);
    void parseDeal(const std::string& msg, int64_t tsNet);

    std::unordered_map<std::string, TopBook> mBooks;
};

class MgbxMarket : public BaseMarket {
public:
    MgbxMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
               std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
               const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~MgbxMarket();
    void start() override;

private:
    std::vector<MgbxUnit*> unitVec;
};

} // namespace md
