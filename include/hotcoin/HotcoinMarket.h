#pragma once
//
// HotcoinMarket.h —— Hotcoin 永续合约行情（只做 DEPTH1 / TRADES / FUNDING_RATE）
//
// 【实测 2026-10-20】
//   wss://wss-ct.hotcoin.fit        ← 官方文档写的是 wss-ct.hotcoin.top，但 .top 在本机
//                                      TLS 握手直接失败，.fit 通（两个域名同一套服务）
//   订阅(每条一个主题): {"event":"subscribe","params":{"biz":"perpetual","type":"depth",
//                       "contractCode":"btcusdt","zip":false,"serialize":false}}
//                       type 还可取 "fills"
//   回执: {"channel":"subscribe","biz":"perpetual","type":"depth","contractCode":"btcusdt",
//          "env":0,"data":{"result":true}}
//   推送(深度): {"data":{"asks":[["p","q","累计q"]],"bids":[[..]]},"biz":"perpetual","type":"depth",
//                "contractCode":"btcusdt","env":0,"timestamp":<ms>}
//   推送(成交): {"data":[["81381.5","5","long",1792487631973,0]],"biz":"perpetual","type":"fills",
//                "contractCode":"btcusdt","env":0,"timestamp":<ms>}
//                ⚠️ data 是**数组的数组**（一帧可含多笔），每笔 = [价格, 数量, 方向, 毫秒, id]
//
// 七个坑（全部实测）:
//   1. **data 在最前面，contractCode 在最后** ⇒ simdjson ondemand 不能"先取 symbol"。
//      本文件的解法：先把 asks[0]/bids[0]（或成交记录）收进局部变量，读完 contractCode 再发。
//   2. **深度是整本推送**（实测 2724 B，30+ 档全量 + 累计量），不是增量 ⇒ 不需要 TopBook。
//      但也正因为整本，DEPTH1 的 ring maxMsg 必须 ≥ 4096（已在 BaseMarket.cpp 调到 8192）。
//   3. **成交帧的 data 是"数组的数组"**：[[价格, 数量, "long"|"short", 毫秒, id], ...]。
//      （2026-10-20 用真实帧回放才发现 —— 一开始按"扁平数组"写，一条都解析不出来。）
//   4. **第 5 个元素 id 实测恒为 0**（没有可用的成交编号）⇒ tradeId 用"毫秒_序号"兜底。
//   5. **contractCode 必须全小写**（btcusdt），大写会返回 {"message":"合约不存在"}。
//   6. 心跳是 {"event":"ping"} → {"event":"pong"}；服务端不发 WS PING。
//   7. 订阅回执里也有 "data":{"result":true}，必须过滤掉，否则会往 ring 里灌无用帧。
//
// 资金费率走 REST：GET api-ct.hotcoin.fit/api/v1/perpetual/public/{contractCode}/fee-rate
//   {"code":200,"data":{"rows":[{"feeRate":7.117e-05,"timeIndex":1792483219000,...}, ...]}}
//   ⚠️ feeRate 是 **JSON 数字且带科学计数法**（7.117e-05）⇒ 必须用 get_double()，
//      用 crypto::fast_atod（std::from_chars）读会得到 NAN。
//   同样没有"下次结算时间" ⇒ 用相邻两条 timeIndex 的间隔推（实测 8 h）。
//

#include "base/BaseMarket.h"
#include "base/FundingRestPoller.h"

namespace md {

constexpr auto HOTCOIN_WS = "wss://wss-ct.hotcoin.fit";

class HotcoinUnit : public BaseUnit {
public:
    HotcoinUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy,
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
    void parseFills(const std::string& msg, int64_t tsNet);

    FundingRestPoller mFundingPoller;
};

class HotcoinMarket : public BaseMarket {
public:
    HotcoinMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                  std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot = 30,
                  const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
    ~HotcoinMarket();
    void start() override;

private:
    std::vector<HotcoinUnit*> unitVec;
};

} // namespace md
