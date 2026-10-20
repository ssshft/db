#include "base/BaseMarket.h"
#include "key_util.h"
#include "SpinBackoff.h"


md::BaseUnit::BaseUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, const char* host, int port, const char* password) {
    smc = s;
    exchangeTypeEnum = exchTy;
    instTypeEnum = instTy;
    marketTypeEnum = marketTy;
    vInstInfo = instInfoVec;
    latestDataUpdateTime = 0;
    pWsClient = nullptr;

    int slotSize = 512;
    int maxMsg = 512;
    if (marketTypeEnum == md::DEPTH1) {
        // 2026-10-20: 原 (16384, 1024) → (2048, 8192)，内存不变 (16 MiB)，单帧上限 ×8。
        // 原因：原 1024 是照着"六家最大 424 B（HTX bbo）"定的，新接的八家里有**整本推送**：
        //   实测 MGBX push.deep.full 4017 B（30 档 a/b 全量，每 2 s 一次）
        //        Hotcoin depth          2724 B（asks/bids 全量 + 累计量）
        //        Echobit depth          2621 B（b/a 各 30 档全量）
        //        Phemex  orderbook_p    ~1500 B（depth=30 全量快照；**该参数交易所忽略，改不小**）
        //   1024 会让这几家的快照帧直接 kOverflow 丢掉 ⇒ 增量簿子永远初始化不出来 ⇒
        //   整家交易所静默零数据（只留一条一次性 LOG_ERROR）。
        // 8192 对 4017 B 留 2× 余量；slot 从 16384 降到 2048 是等价换算（16 MiB），
        // 2048 在"1 unit 30 symbol × 17 帧/s"下仍有 ~4 s 缓冲，而消费线程是自旋的、从不睡眠。
        slotSize = 2048;
        maxMsg = 8192;
    }
    else if (marketTypeEnum == md::DEPTH5) {
        slotSize = 16384;
        maxMsg = 512;
    }
    else if (marketTypeEnum == md::DEPTH10) {
        slotSize = 16384;
        maxMsg = 1024;
    }
    else if (marketTypeEnum == md::DEPTH20) {
        slotSize = 16384;
        maxMsg = 2048;   
    }
    else if (marketTypeEnum == md::TRADES) {
        // 2026-10-17: 原 (65536, 512) → (4096, 8192)，内存不变 (32 MiB)，单帧上限 ×16。
        // 原因：成交推送是**合批**的，512 B 会把整批静默丢掉（kOverflow 不写入）：
        //   实测 Bitget trade 的 action=snapshot 一次 50 笔 = 5236 B（~105 B/笔）；
        //   HTX trade.detail / MEXC sub.deal 同样合批。
        // 8192 B ≈ 78 笔，覆盖 Bitget 的 50 笔快照还有 56% 余量。
        // depth 4096 在 1 symbol/unit、100 笔/s 下有 40 s 容忍（录数据通常 1 个 unit 1~2 个 symbol）。
        slotSize = 4096;
        maxMsg = 8192;
    }
    else if (marketTypeEnum == md::FUNDING_RATE) {
        // 2026-10-20: 原 (1024, 512) → (32, 131072)，512 KiB → 4 MiB。
        // 原因：Echobit 的资金费率走 WS 的 inform 端点，一帧是**全市场 200+ 个 symbol** 的
        // 批量推送（202 个合约 × ~210 B ≈ 42 KB，服务端按 8192 B 分片、由 Beast 自动重组）。
        // 512 B 会被整帧丢掉。其余家的 REST 轮询一帧 ≤ 512 B，131072 对它们没有副作用。
        // slot 从 1024 降到 32 是因为费率最多 2 s 一帧（REST 是 60 s 一批 30 个 symbol），
        // 32 个槽足够吸收突发，多出来的槽纯属浪费内存。
        slotSize = 32;
        maxMsg = 131072;
    }
    else if (marketTypeEnum == md::KLINE_1m) {
        slotSize = 1024;
        maxMsg = 512;
    }

    mdRing = new md::MdSlotRing(slotSize, maxMsg);

#ifdef NEED_SHM
    for (size_t i = 0; i < vInstInfo.size(); ++i) {
        auto& info = vInstInfo[i];
        std::string key = crypto::get_md_channel_key(info.exchangeTypeEnum, info.instTypeEnum, marketTypeEnum, info.instId);
        if (marketTypeEnum == md::DEPTH1) {
            mDepth1Publisher[key] = std::make_shared<Depth1Spmc>(key);
        }
        else if (marketTypeEnum == md::DEPTH5) {
            mDepth5Publisher[key] = std::make_shared<Depth5Spmc>(key);
        }
        else if (marketTypeEnum == md::DEPTH10) {
            mDepth10Publisher[key] = std::make_shared<Depth10Spmc>(key.c_str());
        }
        else if (marketTypeEnum == md::DEPTH20) {
            mDepth20Publisher[key] = std::make_shared<Depth20Spmc>(key.c_str());
        }
        else if (marketTypeEnum == md::TRADES) {
            mTradesPublisher[key] = std::make_shared<TradesSpmc>(key.c_str());
        }
        else if (marketTypeEnum == md::KLINE_1m) {
            mKlinePublisher[key] = std::make_shared<KlineSpmc>(key.c_str());
        }
        else if (marketTypeEnum == md::FUNDING_RATE) {
            mFundingRatePublisher[key] = std::make_shared<FundingRateSpmc>(key.c_str());
        }
    }
#endif

    LOG_INFO("Unit construct complete, exchId: {} instType: {} marketType: {} instInfo size: {}", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], MarketTypeEnum2StrMap[marketTypeEnum], vInstInfo.size());
}

md::BaseUnit::~BaseUnit() {
    if (mdRing) {
        delete mdRing;
        mdRing = nullptr;
    }
}

void md::BaseUnit::start() {
    try {
        std::thread parseThread(&BaseUnit::consume, this);
        parseThread.detach();

        subWebsocekt();
    }
    catch (const std::exception& e) {
        LOG_ERROR("unit start error: {}", e.what());
    }
}

void md::BaseUnit::pushRaw(const char* data, size_t len, int64_t tsNet) {
    const md::WriteResult r = mdRing->write(data, len, tsNet);
    if (r == md::WriteResult::kOverflow && !overflowLogged_) {
        overflowLogged_ = true;
        LOG_ERROR("{}.{}.{} single frame too long: len={} > maxMsg={}, frame DROPPED "
                  "(本 unit 后续同类报文不再打日志；改 BaseMarket.cpp 里该 marketType 的 maxMsg)",
                  ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum],
                  md::MarketTypeEnum2StrMap[marketTypeEnum], len, mdRing->maxMsg());
    }
    // kDropped 是背压，由调用方按需观测 mdRing->dropped()，这里不打日志（否则刷屏）
}

bool md::BaseUnit::resolveInst(const std::string& originInstId, md::InstrumentInfo& info, std::string& key) {
    if (originInstId.empty()) {
        return false;
    }
    if (smc->get_instrument_info(exchangeTypeEnum, instTypeEnum, originInstId.c_str(), info) == false) {
        LOG_ERROR("{}.{}.{} smc cannot find originInstId: {}",
                  ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum],
                  md::MarketTypeEnum2StrMap[marketTypeEnum], originInstId);
        return false;
    }
    key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);
    return true;
}

void md::BaseUnit::subWebsocekt() {
    pWsClient = net::WsClient::create(cfg);

    pWsClient->on_message([this](const uint8_t* d, size_t n, bool b, int64_t t) {
        this->onWebsocketMsg(d, n, b, t);
    });

    pWsClient->on_open([this]() { 
        this->onOpen(); 
    });

    pWsClient->on_close([this](int c, const std::string& r) { 
        this->onClose(c, r); 
    });

    pWsClient->on_error([this](const std::string& m) { 
        this->onError(m); 
    });

    pWsClient->start();
}

void md::BaseUnit::consume() {
    const std::string* msg = nullptr;
    int64_t tsNet = 0;
    bool pending = false;                    // 已 peek、还没 release
    while (1) {
        try {
            if (mdRing->peek(msg, tsNet)) {
                pending = true;
                parseMarketData(*msg, tsNet);   // ← 解析发生在 r 被钉住的期间
                mdRing->release();                 // ← 读完才发布 r_
                pending = false;
                continue;
            }
        } 
        catch (const std::exception& e) {   // ← 异常也必须推进
            if (pending) { 
                mdRing->release(); 
                pending = false; 
            } 
            LOG_ERROR("{}.{}.{} exception {} happened, msg: {}", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], md::MarketTypeEnum2StrMap[marketTypeEnum], e.what(), msg);
        }
        spin::cpu_relax();
    }
}

void md::BaseUnit::onOpen() {
    LOG_INFO("Unit ws open, exchId: {} instType: {} marketType: {}", ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], MarketTypeEnum2StrMap[marketTypeEnum]);
}

void md::BaseUnit::onClose(int code, const std::string& reason) {
    LOG_WARN("Unit ws closed: code={} reason={} (auto-reconnect), exchId: {} instType: {} marketType: {} instInfo size: {}", code, reason, ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], MarketTypeEnum2StrMap[marketTypeEnum], vInstInfo.size());
}

void md::BaseUnit::onError(const std::string& msg) {
    LOG_ERROR("Unit ws error: {}, exchId: {} instType: {} marketType: {} instInfo size: {}", msg, ExchangeTypeEnum2StrMap[exchangeTypeEnum], InstTypeEnum2StrMap[instTypeEnum], MarketTypeEnum2StrMap[marketTypeEnum], vInstInfo.size());
}

md::BaseMarket::BaseMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, int lot, const char* host, int port, const char* passwd) {
    smc = s;
    strcpy(exchId, exId);
    _instTypeVec = instTypeVec;
    _marketTypeVec = marketTypeVec;
    _instIdVec = instIdVec;
    tokenLot = lot;
    strcpy(_host, host);
    _port = port;
    strcpy(_passwd, passwd);

    generateUnitInfo();
}

md::BaseMarket::~BaseMarket() {

}

void md::BaseMarket::generateUnitInfo() {
    unitInfoVec.clear();
    for (auto marketType : _marketTypeVec) {
        for (auto instType : _instTypeVec) {
            std::vector<std::string> validInstIdVec;
            for (auto instId : _instIdVec) {
                md::InstrumentInfo info;
                if (smc->get_instrument_info(exchId, instType.c_str(), instId.c_str(), info)) {
                    validInstIdVec.push_back(info.originInstId);
                }
                else {
                    LOG_ERROR("not found {}.{}.{} smc info", exchId, instType, instId);
                }
            }

            if (crypto::str_cmp(instType.c_str(), "SPOT") && !crypto::has_str(marketType.c_str(), "FUNDING")) {
                size_t validSize = validInstIdVec.size();
                size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                for (size_t us = 0; us < unitSize; us++) {
                    UnitInfo unitInfo;
                    unitInfo.exchangeTypeEnum = ExchangeTypeStr2EnumMap[exchId];
                    unitInfo.instTypeEnum = InstTypeStr2EnumMap[instType];
                    unitInfo.marketTypeEnum = MarketTypeStr2EnumMap[marketType];
                    size_t startValidNum = tokenLot * us;
                    size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                    for (size_t i = startValidNum; i < endValidNum; i++) {
                        md::InstrumentInfo info;
                        if (smc->get_instrument_info(exchId, instType.c_str(), validInstIdVec[i].c_str(), info)) {
                            unitInfo.vInstInfo.push_back(info);
                        }      
                    }
                    unitInfoVec.push_back(unitInfo);
                }
            }
            else if ((crypto::has_str(instType.c_str(), "SWAP")) || (crypto::has_str(instType.c_str(), "FUTURES") && crypto::has_str(marketType.c_str(), "FUNDING") == false)) {//交割合约没有费率信息
                std::vector<std::string> coinStrVec;
                std::vector<std::string> notCoinStrVec;
                for (std::string inst : validInstIdVec) {
                    md::InstrumentInfo info;
                    if (smc->get_instrument_info(exchId, instType.c_str(), inst.c_str(), info)) {
                        if (crypto::str_cmp(info.quote, "USD")) {
                            coinStrVec.push_back(inst);
                        }
                        else {
                            notCoinStrVec.push_back(inst);
                        }
                    }
                    else {
                        LOG_ERROR("will not execute here! {}.{}.{}", exchId, instType, inst);
                    }
                }

                if (coinStrVec.size() > 0) {
                    size_t validSize = coinStrVec.size();
                    size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                    for (size_t us = 0; us < unitSize; us++) {
                        UnitInfo unitInfo;
                        unitInfo.exchangeTypeEnum = ExchangeTypeStr2EnumMap[exchId];
                        unitInfo.instTypeEnum = InstTypeStr2EnumMap[instType];
                        unitInfo.marketTypeEnum = MarketTypeStr2EnumMap[marketType];

                        size_t startValidNum = tokenLot * us;
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        for (size_t i = startValidNum;i < endValidNum; i++) {
                            md::InstrumentInfo info;
                            if (smc->get_instrument_info(exchId, instType.c_str(), coinStrVec[i].c_str(), info)) {
                                unitInfo.vInstInfo.push_back(info);
                            }
                        }

                        unitInfoVec.push_back(unitInfo);
                    }
                }

                if (notCoinStrVec.size() > 0){
                    size_t validSize = notCoinStrVec.size();
                    size_t unitSize = validSize % tokenLot == 0 ? validSize / tokenLot : validSize / tokenLot + 1;
                    for (size_t us = 0; us < unitSize; us++) {
                        UnitInfo unitInfo;
                        unitInfo.exchangeTypeEnum = ExchangeTypeStr2EnumMap[exchId];
                        unitInfo.instTypeEnum = InstTypeStr2EnumMap[instType];
                        unitInfo.marketTypeEnum = MarketTypeStr2EnumMap[marketType];

                        size_t startValidNum = tokenLot * us;
                        size_t endValidNum = tokenLot * (us + 1) > validSize ? validSize : tokenLot * (us + 1);
                        for (size_t i = startValidNum; i < endValidNum; i++) {
                            md::InstrumentInfo info;
                            if (smc->get_instrument_info(exchId, instType.c_str(), notCoinStrVec[i].c_str(), info)) {
                                unitInfo.vInstInfo.push_back(info);
                            }
                        }

                        unitInfoVec.push_back(unitInfo);
                    }
                }
            }
        }
    }
}
