#include "htx/HtxMarket.h"

#include <cstdio>
#include <cstring>

#include "Net.h"
#include "base/GzipUtil.h"

md::HtxUnit::HtxUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy,
                     std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    subId = crypto::get_int_rand(100, 10000);

    // HTX 是**应用层**心跳（服务端发 {"ping":ts}，客户端回 {"pong":ts}），
    // 所以这里必须 Disabled：让 beast 发协议级 ping 没有意义，真正的保活靠 onWebsocketMsg。
    cfg.ping_mode               = net::WsConfig::PingMode::Disabled;
    cfg.idle_timeout_sec        = 60;   // 60s 一个 frame 都没有 = 连接真挂了
    cfg.data_idle_timeout_sec   = 0;    // bbo 一直在推，不需要业务级兜底
}

void md::HtxUnit::generateSubBody() {
    // 资金费率没有 WS 通道 → 不建连接（<X>Market::start() 会走 startRestOnly）
    if (marketTypeEnum == md::FUNDING_RATE) {
        LOG_INFO("HTX {} FUNDING_RATE uses REST polling, no ws connection", InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    if (instTypeEnum == USDT_SWAP) {
        cfg.url = HTX_WS_LINEAR_SWAP;
    }
    else if (instTypeEnum == C_SWAP) {
        cfg.url = HTX_WS_C_SWAP;
    }
    else {
        LOG_ERROR("HTX does not support instType: {} (only USDT_SWAP / C_SWAP)",
                  InstTypeEnum2StrMap[instTypeEnum]);
        return;
    }

    cfg.subscribe_messages.clear();
    for (auto& info : vInstInfo) {
        const std::string code(info.originInstId);      // "BTC-USDT" / 币本位 "BTC-USD"
        std::string sub;

        if (marketTypeEnum == md::DEPTH1) {
            sub = fmt::format("market.{}.bbo", code);           // 单档最优，别用 depth.step0
        }
        else if (marketTypeEnum == md::TRADES) {
            sub = fmt::format("market.{}.trade.detail", code);
        }
        else {
            LOG_ERROR("HTX {} not implemented (only DEPTH1 / TRADES / FUNDING_RATE)",
                      md::MarketTypeEnum2StrMap[marketTypeEnum]);
            return;
        }

        cfg.subscribe_messages.push_back(fmt::format(R"({{"sub":"{}","id":"{}"}})", sub, ++subId));
    }

    LOG_INFO("HTX ws url: {}, sub count: {}, first: {}", cfg.url, cfg.subscribe_messages.size(),
             cfg.subscribe_messages.empty() ? std::string("") : cfg.subscribe_messages.front());
}

void md::HtxUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns) {
    // ① 解压：HTX 所有帧都是 gzip（含订阅回执）。解不出来就按原文当 text 用（防御性）。
    std::string raw;
    if (isBinary) {
        if (!mdutil::gunzip(data, len, raw)) {
            raw.assign(reinterpret_cast<const char*>(data), len);
        }
    }
    else {
        raw.assign(reinterpret_cast<const char*>(data), len);
    }

    // ② 应用层心跳：{"ping":1792225038766} → {"pong":1792225038766}
    //    只改 "ping" → "pong"，其余原样回，避免 ts 精度/格式出岔子。
    if (raw.size() >= 7 && raw.compare(0, 7, R"({"ping")") == 0) {
        std::string pong = raw;
        pong.replace(0, 7, R"({"pong")");
        if (pWsClient) {
            pWsClient->send_text(std::move(pong));
        }
        return;
    }

    // ③ 订阅回执 {"id":"..","status":"ok","subbed":".."} / {"op":"pong"} 里没有 "ch"，
    //    直接丢掉，省一次跨线程拷贝 + 一次 simdjson 解析（回执里有 "subbed"，含 "ch" 的可能为 0）。
    if (raw.find("\"ch\"") == std::string::npos) {
        return;
    }

    pushRaw(raw.data(), raw.size(), crypto::getCurrentTime());
}

void md::HtxUnit::parseMarketData(const std::string& msg, int64_t tsNet) {
    simdjson::padded_string paddedMsg(msg);
    auto doc = parser.iterate(paddedMsg);
    if (doc.error()) {
        LOG_ERROR("HTX simdjson parse error: {}, msg: {}", simdjson::error_message(doc.error()), msg);
        return;
    }

    std::string_view chVal;
    if (doc["ch"].get(chVal) != simdjson::SUCCESS) {
        return;
    }

    // ch = "market.BTC-USDT.bbo" → 取中间那段当 originInstId
    const size_t p1 = chVal.find('.');
    const size_t p2 = (p1 == std::string_view::npos) ? std::string_view::npos : chVal.find('.', p1 + 1);
    if (p1 == std::string_view::npos || p2 == std::string_view::npos) {
        LOG_ERROR("HTX unexpected ch: {}", std::string(chVal));
        return;
    }
    const std::string originInstId(chVal.substr(p1 + 1, p2 - p1 - 1));

    md::InstrumentInfo info;
    std::string        key;
    if (!resolveInst(originInstId, info, key)) {
        return;
    }

    auto tick = doc["tick"];

    // ⚠️⚠️ 下面每个字段的**访问顺序不能动** —— simdjson ondemand 是**前向单向**迭代器：
    //       operator[] 只能找到"当前位置之后"的键，访问已经越过的键会返回 NO_SUCH_FIELD
    //       （不会抛、不会崩，就是**一个字段都读不到**，静默丢数据）。
    //       实测键顺序（2026-10-17 抓包，不是文档抄的）：
    //         bbo         : ch, ts, tick{mrid, id, bid, ask, ts, version, ch}
    //         trade.detail: ch, ts, tick{id, ts, data[]{amount, quantity, trade_turnover,
    //                                                     ts, id, price, direction, is_rpi_trade}}
    //       两条通道 tick 的键顺序**不一样**，所以 tickTs 必须各自在分支里读。

    if (marketTypeEnum == md::DEPTH1) {
        // ⚠️ bbo 的 bid/ask 是**扁平两元素数组** [price, size]（实测：{"bid":[81676.7,6012]}），
        //    **不是** [[p,v]] 的档位数组 —— 别照抄 depth 的写法套一层循环，那会对标量调 begin()。
        //    price 可能是 6535.0 这种，统一用 get(double)（parse_double 对整数也成立）。
        double bp = 0, bv = 0, ap = 0, av = 0;
        bool   gotBid = false, gotAsk = false;

        {
            auto bidArr = tick["bid"];      // 先落成具名变量：临时 simdjson_result 的 begin() 会悬垂
            auto it     = bidArr.begin();
            if ((*it).get(bp) == simdjson::SUCCESS) {
                ++it;
                if ((*it).get(bv) == simdjson::SUCCESS) {
                    gotBid = true;
                }
            }
        }
        {
            auto askArr = tick["ask"];
            auto it     = askArr.begin();
            if ((*it).get(ap) == simdjson::SUCCESS) {
                ++it;
                if ((*it).get(av) == simdjson::SUCCESS) {
                    gotAsk = true;
                }
            }
        }
        if (!gotBid || !gotAsk) {
            return;
        }

        int64_t tickTs = 0;                 // ms；tick 里 ts 排在 bid/ask **之后**，不能提前读
        tick["ts"].get(tickTs);

        md::Depth1 d1;
        std::memset(&d1, 0, sizeof(d1));
        d1.exchangeTypeEnum = exchangeTypeEnum;
        d1.instTypeEnum     = instTypeEnum;
        d1.marketTypeEnum   = marketTypeEnum;
        std::strncpy(d1.instId, info.instId, INSTID_SIZE);

        d1.bp1 = bp * info.reduceNumber;
        d1.bv1 = bv * info.value * info.magnifyNumber;
        d1.ap1 = ap * info.reduceNumber;
        d1.av1 = av * info.value * info.magnifyNumber;

        // 单边为空/价格为 0 的帧不发：dbp 的 dbsnap.h:107 会整帧丢（ap1 <= bp1），
        // 提前挡掉能少一半无效推送。
        if (d1.bp1 <= 0 || d1.ap1 <= 0) {
            return;
        }

        d1.tsTrans = tickTs * 1000;
        d1.tsEvent = tickTs * 1000;
        d1.tsRecv  = tsNet;
        d1.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
        mDepth1Publisher[key]->push(d1);
#endif
    }
    else if (marketTypeEnum == md::TRADES) {
        // tick = {id, ts, data[]}：ts 在 data **之前**，所以先读 ts 再读 data（顺序反了 data 就空了）
        int64_t tickTs = 0;                 // ms，整帧时间
        tick["ts"].get(tickTs);

        // trade.detail 是**合批**推送（一个 tick.data 里可能十几笔），逐笔发。
        // 单笔的键顺序：amount, quantity, trade_turnover, ts, id, price, direction, is_rpi_trade
        // ⇒ 必须 amount → ts → id → price → direction，别按"读起来顺"的顺序写。
        for (auto d : tick["data"]) {
            double amount = 0;
            if (d["amount"].get(amount) != simdjson::SUCCESS) continue;   // 张数（不是币）

            int64_t tradeTs = 0;
            if (d["ts"].get(tradeTs) != simdjson::SUCCESS || tradeTs <= 0) {
                tradeTs = tickTs;
            }

            int64_t tid = 0;
            d["id"].get(tid);                   // 19 位 > 2^53，必须 int64，读 double 会丢低位

            double px = 0;
            if (d["price"].get(px) != simdjson::SUCCESS) continue;

            std::string_view dir;
            bool             isSell = false;
            if (d["direction"].get(dir) == simdjson::SUCCESS && dir == "sell") {
                isSell = true;
            }

            md::Trades t;
            std::memset(&t, 0, sizeof(t));
            t.exchangeTypeEnum = exchangeTypeEnum;
            t.instTypeEnum     = instTypeEnum;
            t.marketTypeEnum   = marketTypeEnum;
            std::strncpy(t.instId, info.instId, INSTID_SIZE);

            std::snprintf(t.tradeId, sizeof(t.tradeId), "%lld", static_cast<long long>(tid));
            t.direction = isSell ? DT_SHORT : DT_LONG;

            t.px = px * info.reduceNumber;
            t.sz = amount * info.value * info.magnifyNumber;   // 张 → base 币，与 Bybit 口径一致

            t.tsTrans = tradeTs * 1000;
            t.tsEvent = tickTs * 1000;
            t.tsRecv  = tsNet;
            t.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
            mTradesPublisher[key]->push(t);
#endif
        }
    }
}

bool md::HtxUnit::fetchFunding(const md::InstrumentInfo& info, md::FundingRate& fr) {
    static const std::vector<std::pair<std::string, std::string>> kHeaders = {{"User-Agent", "utrade-db/1.0"}};

    const bool        isCoinM = (instTypeEnum == C_SWAP);
    const std::string host    = "api.hbdm.com";
    const std::string target  = fmt::format("{}/swap_funding_rate?contract_code={}",
                                            isCoinM ? "/swap-api/v1" : "/linear-swap-api/v1",
                                            info.originInstId);

    std::string body;
    int         status = 0;
    if (!Net::Instance().syncGet("DB_HTX_FUNDING", host, target, kHeaders, {}, body, status)) {
        return false;
    }
    if (status != 200) {
        LOG_WARN("HTX funding http status {} for {}", status, info.originInstId);
        return false;
    }

    simdjson::padded_string padded(body);
    auto                    doc = parser.iterate(padded);
    if (doc.error()) {
        LOG_ERROR("HTX funding json error: {}, body: {}", simdjson::error_message(doc.error()), body);
        return false;
    }

    std::string_view statusVal;
    if (doc["status"].get(statusVal) != simdjson::SUCCESS || statusVal != "ok") {
        LOG_WARN("HTX funding status != ok, body: {}", body);
        return false;
    }

    auto            data = doc["data"];
    std::string_view rateStr;
    if (data["funding_rate"].get(rateStr) != simdjson::SUCCESS) {
        return false;
    }
    fr.fundingRate = crypto::fast_atod(rateStr);

    // ⚠️ HTX 的 funding_time 是**下一次**结算时间（next_funding_time 实测恒为 null），
    //    与 Bitget 的 nextFundingTime 同义，不要当成"上次结算"用。
    std::string_view ftStr;
    if (data["funding_time"].get(ftStr) == simdjson::SUCCESS) {
        fr.fundingTime = crypto::fast_atol(ftStr) * 1000;   // ms → us
    }

    // estimated_rate 实测为 null，HTX 不给预测值；用当前值占位，避免下游拿到 0 当"费率是 0"
    fr.nextFundingRate = fr.fundingRate;
    fr.tsTrans         = crypto::getCurrentTime();
    return true;
}

void md::HtxUnit::startRestOnly() {
    mFundingPoller.start(
        [this](const md::InstrumentInfo& i, md::FundingRate& fr) { return this->fetchFunding(i, fr); },
        exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
}

md::HtxMarket::HtxMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec,
                         std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot,
                         const char* host, const int port, const char* passwd)
    : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        md::HtxUnit* unit = new md::HtxUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum,
                                            info.vInstInfo, _host, _port, _passwd);
        unit->generateSubBody();
        htxUnitVec.push_back(unit);
    }
}

md::HtxMarket::~HtxMarket() {}

void md::HtxMarket::start() {
    for (size_t i = 0; i < htxUnitVec.size(); ++i) {
        if (htxUnitVec[i]->isRestOnly()) {
            htxUnitVec[i]->startRestOnly();
        }
        else {
            htxUnitVec[i]->start();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}
