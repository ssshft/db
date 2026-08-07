#include "okx/OkxSbeMarket.h"
#include "base64.hpp"

#include <openssl/hmac.h>
#include <openssl/evp.h>

#include <cstring>
#include <string_view>


// ============================================================================
// 内部工具: HMAC-SHA256(secret, msg) → base64
// ============================================================================
namespace {

std::string hmac_sha256_base64(std::string_view secret, std::string_view msg) {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int  digest_len = 0;
    ::HMAC(EVP_sha256(),
           secret.data(), static_cast<int>(secret.size()),
           reinterpret_cast<const unsigned char*>(msg.data()), msg.size(),
           digest, &digest_len);
    return websocketpp::base64_encode(digest, digest_len);
}

} // anonymous namespace


// ============================================================================
// Ctor
// ============================================================================
md::OkxSbeUnit::OkxSbeUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, SbeAccount sbeAcc, const char* host, int port, const char* passwd)
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd)
{
    sbeAccount = sbeAcc;

    // OKX 30s 无消息自动断连, client 每 20s 发一次 "ping"。
    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text = "ping";
    cfg.idle_timeout_sec = 60;
    cfg.data_idle_timeout_sec = 0;
}


// ============================================================================
// generateSubBody
//   1. 从 SecurityManager 拉 OKX 全部 InstrumentInfo, 建 instIdCode → info 反查 map
//   2. subscribe_messages 里放两条: [login_json, subscribe_json]
//      BeastWsClient 会按顺序发 (登录 → 订阅), OKX 服务端拒绝未登录的订阅。
// ============================================================================
void md::OkxSbeUnit::generateSubBody() {
    std::string exchIdStr     = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    std::string instTypeStr   = InstTypeEnum2StrMap[instTypeEnum];
    std::string marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];

    cfg.url = OKX_WS_SBE_PUBLIC;

    // 1) 从 SecurityManager 拿 code 反查表
    buildCodeIndexFromSm();

    // 2) 拼订阅 JSON
    buildSubscribeJson();
    if (cfg.subscribe_messages.empty()) {
        LOG_ERROR("[OKX_SBE] no subscribe message built, unit {} skipped", marketTypeStr);
        return;
    }

    // 3) 登录 JSON 插到最前面, 保证先登录后订阅
    std::string login_json = buildLoginJson();
    if (!login_json.empty()) {
        cfg.subscribe_messages.insert(cfg.subscribe_messages.begin(), login_json);
    } else {
        LOG_WARN("[OKX_SBE] SbeAccount not configured, skipping login. "
                 "OKX SBE endpoint requires login; subscribe will likely fail.");
    }

    LOG_INFO("{} SBE ws url: {} , {} messages queued (login first)", exchIdStr, cfg.url, cfg.subscribe_messages.size());
    for (auto& m : cfg.subscribe_messages) LOG_INFO("  msg: {}", m);
}


// ============================================================================
// buildLoginJson: apiKey / passphrase / secretKey 签名逻辑
//   timestamp: Unix 秒 (docs 明确说秒不是毫秒)
//   sign = base64(HMAC-SHA256(secret, timestamp + "GET" + "/users/self/verify"))
// ============================================================================
std::string md::OkxSbeUnit::buildLoginJson() const {
    if (sbeAccount.apiKey.empty() || sbeAccount.secretKey.empty() || sbeAccount.password.empty()) {
        return "";
    }
    long ts_sec = crypto::getCurrentTimeSeconds();
    std::string ts = std::to_string(ts_sec);
    std::string payload = ts + "GET/users/self/verify";
    std::string sign = hmac_sha256_base64(sbeAccount.secretKey, payload);

    return fmt::format(
        R"({{"op":"login","args":[{{"apiKey":"{}","passphrase":"{}","timestamp":"{}","sign":"{}"}}]}})",
        sbeAccount.apiKey, sbeAccount.password, ts, sign);
}


// ============================================================================
// buildSubscribeJson: 遍历 vInstInfo 拼 args, 生成一条 subscribe。
// ============================================================================
void md::OkxSbeUnit::buildSubscribeJson() {
    const char* channel = channelForMarketType();
    if (!channel) {
        LOG_ERROR("[OKX_SBE] unsupported marketType: {}", MarketTypeEnum2StrMap[marketTypeEnum]);
        cfg.subscribe_messages.clear();
        return;
    }

    subArgs.clear();
    for (auto& info : vInstInfo) {
        subArgs.push_back(fmt::format(R"({{"channel":"{}","instId":"{}"}})", channel, info.originInstId));
    }

    std::string argsCsv;
    argsCsv.reserve(subArgs.size() * 64);
    for (size_t i = 0; i < subArgs.size(); ++i) {
        if (i) argsCsv += ',';
        argsCsv += subArgs[i];
    }
    std::string subJson = fmt::format(R"({{"op":"subscribe","args":[{}]}})", argsCsv);

    cfg.subscribe_messages.clear();
    cfg.subscribe_messages.push_back(std::move(subJson));
}


const char* md::OkxSbeUnit::channelForMarketType() const {
    std::string mt = MarketTypeEnum2StrMap[marketTypeEnum];
    if (crypto::has_str(mt.c_str(), "DEPTH1")) {
        return "bbo-tbt";
    }

    if (crypto::has_str(mt.c_str(), "DEPTH")) {
        return "books-l2-tbt";
    }

    if (crypto::has_str(mt.c_str(), "TRADE")) {
        return "trades";
    }

    return nullptr;
}


// ============================================================================
// buildCodeIndexFromSm: 从 SecurityManager 已装载的 InstrumentInfo 里,
//   过滤 exchange==OKX 且 instType 匹配的条目, 依 instIdCode 建 map。
//   数据源已经是 contractinfo 拉过 REST + 写入 SHM 的成果, 这里零 REST 开销。
// ============================================================================
void md::OkxSbeUnit::buildCodeIndexFromSm() {
    mCodeToInfo.clear();
    if (!smc) {
        LOG_ERROR("[OKX_SBE] SecurityManager is null, cannot build code index");
        return;
    }

    // 目标 originInstId 集合 (只留 vInstInfo 里配置了的, map 更精简)
    std::unordered_set<std::string> wanted;
    wanted.reserve(vInstInfo.size() * 2);
    for (auto& info : vInstInfo) wanted.insert(info.originInstId);

    std::vector<md::InstrumentInfo> all;
    if (!smc->get_all_instruments(all)) {
        LOG_ERROR("[OKX_SBE] get_all_instruments failed, code index will be empty");
        return;
    }

    for (auto& info : all) {
        if (info.instIdCode == 0) {
            continue;   // OKX 未分配 code 的产品, SBE 不覆盖
        }

        if (wanted.count(info.originInstId) == 0) {
            continue;
        }
        
        mCodeToInfo[info.instIdCode] = info;
    }

    LOG_INFO("[OKX_SBE] code index built from SecurityManager: {} entries (of {} wanted)", mCodeToInfo.size(), wanted.size());
    
    if (mCodeToInfo.empty()) {
        LOG_WARN("[OKX_SBE] no entries in code index — contractinfo may not have run yet, or instIdCode is not populated. SBE frames will be dropped.");
    }
}


bool md::OkxSbeUnit::lookupByCode(int64_t code, md::InstrumentInfo& out) const {
    auto it = mCodeToInfo.find(code);
    if (it == mCodeToInfo.end()) return false;
    out = it->second;
    return true;
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void md::OkxSbeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t /*ns*/) {
    latestDataUpdateTime = crypto::getCurrentTime();

    if (isBinary) {
        std::string msg(reinterpret_cast<const char*>(data), len);
        mQueue.push(std::move(msg));
        return;
    }

    // text: 登录/订阅回执、系统消息、pong 等
    if (len == 4 && std::string_view(reinterpret_cast<const char*>(data), len) == "pong") {
        return;
    }
    std::string txt(reinterpret_cast<const char*>(data), len);
    LOG_INFO("[OKX_SBE] recv text: {}", txt);
}


// ============================================================================
// parseMarketData: SBE 分派
// ============================================================================
void md::OkxSbeUnit::parseMarketData(const std::string& msg) {
    long tsNet = crypto::getCurrentTime();
    const uint8_t* data = reinterpret_cast<const uint8_t*>(msg.data());
    size_t len = msg.size();

    if (len < sizeof(okxsbe::MessageHeader)) {
        LOG_ERROR("[OKX_SBE] frame too short: {} bytes", len);
        return;
    }

    uint16_t tid = okxsbe::peek_template_id(data, len);
    switch (tid) {
        case okxsbe::kTemplateBboTbt:
            handleBboTbt(data, len, tsNet); break;
        case okxsbe::kTemplateBooksL2Tbt:
        case okxsbe::kTemplateBooksL2Elp:
            handleBooksL2(data, len, tsNet); break;
        case okxsbe::kTemplateBooksL2ExpUpdate:
        case okxsbe::kTemplateBooksL2ElpExpUpdate:
            handleExpUpdate(data, len); break;
        case okxsbe::kTemplateTrades:
            handleTrades(data, len, tsNet); break;
        case okxsbe::kTemplateSnapshotDepth:
            // REST /market/books-sbe 的回执, WS 通道不会推
            break;
        default:
            LOG_WARN("[OKX_SBE] unknown templateId: {}", tid);
            break;
    }
}


// ============================================================================
// handleBboTbt (templateId=1000)
// ============================================================================
void md::OkxSbeUnit::handleBboTbt(const uint8_t* data, size_t len, long tsNet) {
    const auto* v = okxsbe::view_of<okxsbe::BboTbtView>(data, len);
    if (!v) { LOG_ERROR("[OKX_SBE] BboTbt view failed, len: {}", len); return; }

    md::InstrumentInfo info;
    if (!lookupByCode(v->instIdCode, info)) {
        LOG_ERROR("[OKX_SBE] BboTbt: instIdCode {} not in code index", v->instIdCode);
        return;
    }
    if (marketTypeEnum != md::DEPTH1) return;

    mLatestExponent[v->instIdCode] = {v->pxExponent, v->szExponent};

    std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

    md::Depth1 depth1;
    memset(&depth1, 0, sizeof(md::Depth1));
    depth1.exchangeTypeEnum = exchangeTypeEnum;
    depth1.instTypeEnum     = instTypeEnum;
    depth1.marketTypeEnum   = marketTypeEnum;
    strncpy(depth1.instId, info.instId, INSTID_SIZE);

    long tsTransNs = static_cast<long>(v->tsUs) * 1000;
    depth1.tsTrans = tsTransNs;
    depth1.tsEvent = tsTransNs;
    depth1.tsRecv  = tsNet;

    double bidPx = okxsbe::to_double(v->bidPxMantissa, v->pxExponent);
    double bidSz = okxsbe::to_double(v->bidSzMantissa, v->szExponent);
    double askPx = okxsbe::to_double(v->askPxMantissa, v->pxExponent);
    double askSz = okxsbe::to_double(v->askSzMantissa, v->szExponent);

    depth1.bp1 = bidPx * info.reduceNumber;
    depth1.bv1 = bidSz * info.magnifyNumber;
    depth1.ap1 = askPx * info.reduceNumber;
    depth1.av1 = askSz * info.magnifyNumber;

    depth1.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
    auto it = mDepth1Publisher.find(key);
    if (it != mDepth1Publisher.end()) it->second->push(depth1);
#endif
}


// ============================================================================
// handleBooksL2 (templateId=1001 / 1003)
// ============================================================================
void md::OkxSbeUnit::handleBooksL2(const uint8_t* data, size_t len, long tsNet) {
    okxsbe::BooksL2Iter iter(data, len);
    if (!iter.ok() || !iter.root()) {
        LOG_ERROR("[OKX_SBE] BooksL2 iter failed, len: {}", len);
        return;
    }
    const auto* root = iter.root();

    md::InstrumentInfo info;
    if (!lookupByCode(root->instIdCode, info)) {
        LOG_ERROR("[OKX_SBE] BooksL2: instIdCode {} not in code index", root->instIdCode);
        return;
    }
    mLatestExponent[root->instIdCode] = {root->pxExponent, root->szExponent};

    long tsTransNs = static_cast<long>(root->tsUs) * 1000;
    int wantN = 0;
    if      (marketTypeEnum == md::DEPTH5)  wantN = 5;
    else if (marketTypeEnum == md::DEPTH10) wantN = 10;
    else if (marketTypeEnum == md::DEPTH20) wantN = 20;
    else return;

    auto push_depth = [&](auto& depthSnap) {
        depthSnap.exchangeTypeEnum = exchangeTypeEnum;
        depthSnap.instTypeEnum     = instTypeEnum;
        depthSnap.marketTypeEnum   = marketTypeEnum;
        strncpy(depthSnap.instId, info.instId, INSTID_SIZE);
        depthSnap.tsTrans = tsTransNs;
        depthSnap.tsEvent = tsTransNs;
        depthSnap.tsRecv  = tsNet;
    };

    if (marketTypeEnum == md::DEPTH5) {
        md::Depth5 d; memset(&d, 0, sizeof(d)); push_depth(d);
        double* apArr[5] = {&d.ap1,&d.ap2,&d.ap3,&d.ap4,&d.ap5};
        double* avArr[5] = {&d.av1,&d.av2,&d.av3,&d.av4,&d.av5};
        double* bpArr[5] = {&d.bp1,&d.bp2,&d.bp3,&d.bp4,&d.bp5};
        double* bvArr[5] = {&d.bv1,&d.bv2,&d.bv3,&d.bv4,&d.bv5};
        uint16_t cnt = std::min<uint16_t>(iter.ask_count(), 5);
        for (uint16_t i = 0; i < cnt; ++i) {
            const auto* e = iter.ask(i);
            *apArr[i] = okxsbe::to_double(e->pxMantissa, root->pxExponent) * info.reduceNumber;
            *avArr[i] = okxsbe::to_double(e->szMantissa, root->szExponent) * info.magnifyNumber;
        }
        cnt = std::min<uint16_t>(iter.bid_count(), 5);
        for (uint16_t i = 0; i < cnt; ++i) {
            const auto* e = iter.bid(i);
            *bpArr[i] = okxsbe::to_double(e->pxMantissa, root->pxExponent) * info.reduceNumber;
            *bvArr[i] = okxsbe::to_double(e->szMantissa, root->szExponent) * info.magnifyNumber;
        }
        d.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
        std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);
        auto it = mDepth5Publisher.find(key);
        if (it != mDepth5Publisher.end()) it->second->push(d);
#endif
    }
    else if (marketTypeEnum == md::DEPTH10) {
        md::Depth10 d; memset(&d, 0, sizeof(d)); push_depth(d);
        double* apArr[10] = {&d.ap1,&d.ap2,&d.ap3,&d.ap4,&d.ap5,&d.ap6,&d.ap7,&d.ap8,&d.ap9,&d.ap10};
        double* avArr[10] = {&d.av1,&d.av2,&d.av3,&d.av4,&d.av5,&d.av6,&d.av7,&d.av8,&d.av9,&d.av10};
        double* bpArr[10] = {&d.bp1,&d.bp2,&d.bp3,&d.bp4,&d.bp5,&d.bp6,&d.bp7,&d.bp8,&d.bp9,&d.bp10};
        double* bvArr[10] = {&d.bv1,&d.bv2,&d.bv3,&d.bv4,&d.bv5,&d.bv6,&d.bv7,&d.bv8,&d.bv9,&d.bv10};
        uint16_t cnt = std::min<uint16_t>(iter.ask_count(), 10);
        for (uint16_t i = 0; i < cnt; ++i) {
            const auto* e = iter.ask(i);
            *apArr[i] = okxsbe::to_double(e->pxMantissa, root->pxExponent) * info.reduceNumber;
            *avArr[i] = okxsbe::to_double(e->szMantissa, root->szExponent) * info.magnifyNumber;
        }
        cnt = std::min<uint16_t>(iter.bid_count(), 10);
        for (uint16_t i = 0; i < cnt; ++i) {
            const auto* e = iter.bid(i);
            *bpArr[i] = okxsbe::to_double(e->pxMantissa, root->pxExponent) * info.reduceNumber;
            *bvArr[i] = okxsbe::to_double(e->szMantissa, root->szExponent) * info.magnifyNumber;
        }
        d.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
        std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);
        auto it = mDepth10Publisher.find(key);
        if (it != mDepth10Publisher.end()) it->second->push(d);
#endif
    }
    else if (marketTypeEnum == md::DEPTH20) {
        md::Depth20 d; memset(&d, 0, sizeof(d)); push_depth(d);
        double* apArr[20] = {&d.ap1,&d.ap2,&d.ap3,&d.ap4,&d.ap5,&d.ap6,&d.ap7,&d.ap8,&d.ap9,&d.ap10,
                             &d.ap11,&d.ap12,&d.ap13,&d.ap14,&d.ap15,&d.ap16,&d.ap17,&d.ap18,&d.ap19,&d.ap20};
        double* avArr[20] = {&d.av1,&d.av2,&d.av3,&d.av4,&d.av5,&d.av6,&d.av7,&d.av8,&d.av9,&d.av10,
                             &d.av11,&d.av12,&d.av13,&d.av14,&d.av15,&d.av16,&d.av17,&d.av18,&d.av19,&d.av20};
        double* bpArr[20] = {&d.bp1,&d.bp2,&d.bp3,&d.bp4,&d.bp5,&d.bp6,&d.bp7,&d.bp8,&d.bp9,&d.bp10,
                             &d.bp11,&d.bp12,&d.bp13,&d.bp14,&d.bp15,&d.bp16,&d.bp17,&d.bp18,&d.bp19,&d.bp20};
        double* bvArr[20] = {&d.bv1,&d.bv2,&d.bv3,&d.bv4,&d.bv5,&d.bv6,&d.bv7,&d.bv8,&d.bv9,&d.bv10,
                             &d.bv11,&d.bv12,&d.bv13,&d.bv14,&d.bv15,&d.bv16,&d.bv17,&d.bv18,&d.bv19,&d.bv20};
        uint16_t cnt = std::min<uint16_t>(iter.ask_count(), 20);
        for (uint16_t i = 0; i < cnt; ++i) {
            const auto* e = iter.ask(i);
            *apArr[i] = okxsbe::to_double(e->pxMantissa, root->pxExponent) * info.reduceNumber;
            *avArr[i] = okxsbe::to_double(e->szMantissa, root->szExponent) * info.magnifyNumber;
        }
        cnt = std::min<uint16_t>(iter.bid_count(), 20);
        for (uint16_t i = 0; i < cnt; ++i) {
            const auto* e = iter.bid(i);
            *bpArr[i] = okxsbe::to_double(e->pxMantissa, root->pxExponent) * info.reduceNumber;
            *bvArr[i] = okxsbe::to_double(e->szMantissa, root->szExponent) * info.magnifyNumber;
        }
        d.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
        std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);
        auto it = mDepth20Publisher.find(key);
        if (it != mDepth20Publisher.end()) it->second->push(d);
#endif
    }
}


// ============================================================================
// handleTrades (templateId=1005)
// ============================================================================
void md::OkxSbeUnit::handleTrades(const uint8_t* data, size_t len, long tsNet) {
    const auto* v = okxsbe::view_of<okxsbe::TradesView>(data, len);
    if (!v) { LOG_ERROR("[OKX_SBE] Trades view failed, len: {}", len); return; }

    md::InstrumentInfo info;
    if (!lookupByCode(v->instIdCode, info)) {
        LOG_ERROR("[OKX_SBE] Trades: instIdCode {} not in code index", v->instIdCode);
        return;
    }
    if (marketTypeEnum != md::TRADES) return;

    mLatestExponent[v->instIdCode] = {v->pxExponent, v->szExponent};

    std::string key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

    md::Trades trades;
    memset(&trades, 0, sizeof(trades));
    trades.exchangeTypeEnum = exchangeTypeEnum;
    trades.instTypeEnum     = instTypeEnum;
    trades.marketTypeEnum   = marketTypeEnum;
    strncpy(trades.instId, info.instId, INSTID_SIZE);

    long tsTransNs = static_cast<long>(v->tsUs) * 1000;
    trades.tsTrans = tsTransNs;
    trades.tsEvent = tsTransNs;
    trades.tsRecv  = tsNet;

    std::string tidStr = std::to_string(v->tradeId);
    strncpy(trades.tradeId, tidStr.c_str(), INSTID_SIZE);

    double px = okxsbe::to_double(v->pxMantissa, v->pxExponent);
    double sz = okxsbe::to_double(v->szMantissa, v->szExponent);
    trades.px = px * info.reduceNumber;
    trades.sz = sz * info.magnifyNumber;
    trades.direction = (v->side == okxsbe::Side_Buy) ? DT_LONG : DT_SHORT;

    trades.tsParse = crypto::getCurrentTime();
#ifdef NEED_SHM
    auto it = mTradesPublisher.find(key);
    if (it != mTradesPublisher.end()) it->second->push(trades);
#endif
}


// ============================================================================
// handleExpUpdate (templateId=1002 / 1004)
// ============================================================================
void md::OkxSbeUnit::handleExpUpdate(const uint8_t* data, size_t len) {
    const auto* v = okxsbe::view_of<okxsbe::BooksL2ExpUpdateView>(data, len);
    if (!v) return;
    mLatestExponent[v->instIdCode] = {v->pxExponent, v->szExponent};
    LOG_INFO("[OKX_SBE] exp update: instIdCode={} pxExp={} szExp={}",
             v->instIdCode, (int)v->pxExponent, (int)v->szExponent);
}


// ============================================================================
// OkxSbeMarket wrapper
// ============================================================================
md::OkxSbeMarket::OkxSbeMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, SbeAccount sbeAccount, int lot, const char* host, const int port, const char* passwd) : md::BaseMarket(s, exId, instTypeVec, marketTypeVec, instIdVec, lot, host, port, passwd) {
    for (size_t i = 0; i < unitInfoVec.size(); ++i) {
        auto& info = unitInfoVec[i];
        auto* unit = new md::OkxSbeUnit(smc, info.exchangeTypeEnum, info.instTypeEnum, info.marketTypeEnum, info.vInstInfo, sbeAccount, _host, _port, _passwd);
        unit->generateSubBody();
        okxSbeUnitVec.push_back(unit);
    }
}

md::OkxSbeMarket::~OkxSbeMarket() {
    for (auto* u : okxSbeUnitVec) delete u;
    okxSbeUnitVec.clear();
}

void md::OkxSbeMarket::start() {
    for (auto* u : okxSbeUnitVec) {
        u->start();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}
