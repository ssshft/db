#pragma once
//
// FundingRestPoller.h —— FUNDING_RATE 的 REST 轮询器 (六家小所共用一份)
//
// 为什么资金费率统一走 REST 而不是 WS:
//   实测 2026-10-17, 六家里只有三家有 WS 资金费率通道, 且都不给"下次结算时间":
//     Bitget  ticker  → fundingRate + nextFundingTime   (有 WS, 但 1s 一帧, 附带一堆无用字段)
//     XT      fund_rate → r + t                          (60s 才推一次)
//     MEXC    sub.ticker → fundingRate                   (没有下次结算时间)
//     HTX / KuCoin / BingX → 没有 WS 通道
//   而 REST 六家都齐 (rate + 下次结算时间), 8 小时才变一次, 60s 轮询一次完全够。
//   ⇒ 统一 REST: 一份代码、一份口径、不用为三家写 WS 解析器再为三家写 REST。
//
// 用法 (在各 venue 的 Unit 里):
//   if (marketTypeEnum == md::FUNDING_RATE) {
//       mFundingPoller.start([this](const md::InstrumentInfo& i, md::FundingRate& fr) {
//           return this->fetchFunding(i, fr);
//       }, exchangeTypeEnum, instTypeEnum, &vInstInfo, &mFundingRatePublisher, 60);
//   }
//
// ⚠️ 各 venue 的 fetchFunding 只负责填 fundingRate / nextFundingRate / fundingTime / tsTrans,
//    身份字段 (exch/instType/marketType/instId) 与 tsRecv/tsParse 由本类统一填 —— 免得六家
//    各填一遍、漏一个字段就静默丢数据。
//

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "data_struct.h"
#include "key_util.h"
#include "log_engine.h"
#include "shm_global.h"
#include "time_util.h"

namespace md {

class FundingRestPoller {
public:
    // 返回 false = 这一轮这个 symbol 没拿到 (网络/解析失败), 上层只打日志不发布
    using Fetcher = std::function<bool(const md::InstrumentInfo&, md::FundingRate&)>;

    FundingRestPoller() = default;
    FundingRestPoller(const FundingRestPoller&)            = delete;
    FundingRestPoller& operator=(const FundingRestPoller&) = delete;

    void start(Fetcher fetcher, ExchangeType exchTy, InstType instTy,
               std::vector<md::InstrumentInfo>* insts,
               std::unordered_map<std::string, std::shared_ptr<FundingRateSpmc>>* pubs,
               int intervalSec = 60) {
        if (running_.exchange(true)) {
            return;                                  // 已启动, 幂等
        }
        fetcher_     = std::move(fetcher);
        exchTy_      = exchTy;
        instTy_      = instTy;
        insts_       = insts;
        pubs_        = pubs;
        intervalSec_ = intervalSec > 0 ? intervalSec : 60;

        // unit 的生命周期 = 进程生命周期 (与 Bybit 等现有实现一致, 从不 delete),
        // 所以 detach 是安全的。
        std::thread(&FundingRestPoller::loop, this).detach();
    }

    void stop() { running_.store(false); }

private:
    void loop() {
        LOG_INFO("{} {} FUNDING_RATE rest poller start, symbols: {}, interval: {}s",
                 ExchangeTypeEnum2StrMap[exchTy_], InstTypeEnum2StrMap[instTy_],
                 insts_ ? insts_->size() : 0, intervalSec_);

        while (running_.load(std::memory_order_relaxed)) {
            if (insts_ != nullptr && pubs_ != nullptr) {
                for (auto& info : *insts_) {
                    md::FundingRate fr;
                    std::memset(&fr, 0, sizeof(fr));

                    if (!fetcher_(info, fr)) {
                        LOG_WARN("{} {} {} funding fetch failed",
                                 ExchangeTypeEnum2StrMap[exchTy_], InstTypeEnum2StrMap[instTy_], info.originInstId);
                        continue;
                    }

                    fr.exchangeTypeEnum = exchTy_;
                    fr.instTypeEnum     = instTy_;
                    fr.marketTypeEnum   = md::FUNDING_RATE;
                    std::strncpy(fr.instId, info.instId, INSTID_SIZE);
                    fr.tsRecv  = crypto::getCurrentTime();
                    fr.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
                    const std::string key = crypto::get_md_channel_key(exchTy_, instTy_, md::FUNDING_RATE, info.instId);
                    auto it = pubs_->find(key);
                    if (it != pubs_->end() && it->second) {
                        it->second->push(fr);
                    }
                    else {
                        LOG_ERROR("funding publisher not found for key: {}", key);
                    }
#endif
                }
            }

            // 分段 sleep, 让 stop() 能及时生效
            for (int i = 0; i < intervalSec_ && running_.load(std::memory_order_relaxed); ++i) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        LOG_INFO("{} {} FUNDING_RATE rest poller stop", ExchangeTypeEnum2StrMap[exchTy_], InstTypeEnum2StrMap[instTy_]);
    }

    Fetcher        fetcher_;
    ExchangeType   exchTy_{ExchangeType_MIN};
    InstType       instTy_{InstType_MIN};
    int            intervalSec_{60};
    std::vector<md::InstrumentInfo>*                                              insts_{nullptr};
    std::unordered_map<std::string, std::shared_ptr<FundingRateSpmc>>*            pubs_{nullptr};
    std::atomic<bool> running_{false};
};

} // namespace md
