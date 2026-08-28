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
    : BaseUnit(s, exchTy, instTy, marketTy, instInfoVec, host, port, passwd) {
    sbeAccount = sbeAcc;

    // OKX 30s 无消息自动断连, client 每 20s 发一次 "ping"。
    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text = "ping";
    cfg.idle_timeout_sec = 60;
    cfg.data_idle_timeout_sec = 0;
}

void md::OkxSbeUnit::generateSubBody() {
    std::string exchIdStr = ExchangeTypeEnum2StrMap[exchangeTypeEnum];
    std::string instTypeStr = InstTypeEnum2StrMap[instTypeEnum];
    std::string marketTypeStr = MarketTypeEnum2StrMap[marketTypeEnum];

    cfg.url = OKX_WS_SBE_PUBLIC;

    int64_t ts_sec = crypto::getCurrentTimeSeconds();
    std::string ts = std::to_string(ts_sec);
    std::string payload = ts + "GET/users/self/verify";
    std::string sign = hmac_sha256_base64(sbeAccount.secretKey, payload);

    cfg.headers.emplace_back("OK-ACCESS-KEY", sbeAccount.apiKey);
    cfg.headers.emplace_back("OK-ACCESS-SIGN", sign);
    cfg.headers.emplace_back("OK-ACCESS-TIMESTAMP", ts);
    cfg.headers.emplace_back("OK-ACCESS-PASSPHRASE", sbeAccount.password);

    mCodeToInfo.clear();
    for (auto info : vInstInfo) {
        mCodeToInfo[info.instIdCode] = info;
    }

    subArgs.clear();
    for (auto info : vInstInfo) {
        std::string channel = "";
        if (crypto::has_str(marketTypeStr, "DEPTH1")) {
            channel = "bbo-tbt";
        }
        else if (crypto::has_str(marketTypeStr, "DEPTH")) {
            channel = "books-l2-tbt";
        }
        else if (crypto::has_str(marketTypeStr, "TRADE")) {
            channel = "trades";
        }

        if (channel.length() > 0) {
            subArgs.push_back(fmt::format(R"({{"channel":"{}","instIdCode":"{}"}})", channel, info.instIdCode));
        }
    }

    std::string paramsCsv;
    paramsCsv.reserve(subArgs.size() * 64);
    for (size_t i = 0; i < subArgs.size(); ++i) {
        if (i) {
            paramsCsv += ",";
        }
        paramsCsv += subArgs[i];
    }

    std::string subJson = fmt::format(R"({{"op":"subscribe","args":[{}]}})", paramsCsv);
    cfg.subscribe_messages.clear();
    cfg.subscribe_messages.push_back(std::move(subJson));

    LOG_INFO("{} ws url: {}, sub body: {}", exchIdStr, cfg.url, subJson);
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
    }
    else {
        std::string txt(reinterpret_cast<const char*>(data), len);
        LOG_INFO("[OKX_SBE] recv text: {}", txt);
    }
}


// ============================================================================
// parseMarketData: SBE 分派
// ============================================================================
void md::OkxSbeUnit::parseMarketData(const std::string& msg) {
    int64_t tsNet = crypto::getCurrentTime();
    const uint8_t* data = reinterpret_cast<const uint8_t*>(msg.data());
    size_t len = msg.size();

    if (len < sizeof(okxsbe::MessageHeader)) {
        LOG_ERROR("[OKX_SBE] frame too short: {} bytes", len);
        return;
    }

    uint16_t tid = okxsbe::peek_template_id(data, len);
    switch (tid) {
        case okxsbe::kTemplateBboTbt:
            handleBboTbt(data, len, tsNet); 
            break;
        case okxsbe::kTemplateBooksL2Tbt:
        case okxsbe::kTemplateBooksL2Elp:
            handleBooksL2(data, len, tsNet); 
            break;
        case okxsbe::kTemplateBooksL2ExpUpdate:
        case okxsbe::kTemplateBooksL2ElpExpUpdate:
            handleExpUpdate(data, len); 
            break;
        case okxsbe::kTemplateTrades:
            handleTrades(data, len, tsNet); 
            break;
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
void md::OkxSbeUnit::handleBboTbt(const uint8_t* data, size_t len, int64_t tsNet) {
    const auto* v = okxsbe::view_of<okxsbe::BboTbtView>(data, len);
    if (!v) { 
        LOG_ERROR("[OKX_SBE] BboTbt view failed, len: {}", len); 
        return; 
    }

    md::InstrumentInfo info;
    if (!lookupByCode(v->instIdCode, info)) {
        LOG_ERROR("[OKX_SBE] BboTbt: instIdCode {} not in code index", v->instIdCode);
        return;
    }

    if (marketTypeEnum != md::DEPTH1) {
        return;
    }

    const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

    md::Depth1 depth1;
    memset(&depth1, 0, sizeof(md::Depth1));
    depth1.exchangeTypeEnum = exchangeTypeEnum;
    depth1.instTypeEnum = instTypeEnum;
    depth1.marketTypeEnum = marketTypeEnum;
    strncpy(depth1.instId, info.instId, INSTID_SIZE);

    int64_t tsTrans = v->tsUs;
    depth1.tsTrans = tsTrans;
    depth1.tsEvent = tsTrans;
    depth1.tsRecv = tsNet;

    double bidPx = okxsbe::to_double(v->bidPxMantissa, v->pxExponent);
    double bidSz = okxsbe::to_double(v->bidSzMantissa, v->szExponent);
    double askPx = okxsbe::to_double(v->askPxMantissa, v->pxExponent);
    double askSz = okxsbe::to_double(v->askSzMantissa, v->szExponent);

    depth1.bp1 = bidPx * info.reduceNumber;
    depth1.bv1 = bidSz * info.magnifyNumber;
    depth1.ap1 = askPx * info.reduceNumber;
    depth1.av1 = askSz * info.magnifyNumber;

    depth1.tsParse = crypto::getCurrentTime();

    std::cout << depth1.getString() << std::endl;
#ifdef NEED_SHM
    mDepth1Publisher[key]->push(depth1);
#endif
}


// ============================================================================
// handleBooksL2 (templateId=1001 / 1003)
// ============================================================================
void md::OkxSbeUnit::handleBooksL2(const uint8_t* data, size_t len, int64_t tsNet) {
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
    const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

    int64_t tsTrans = root->tsUs;
    int wantN = 0;
    if (marketTypeEnum == md::DEPTH5)  {
        wantN = 5;
    }
    else if (marketTypeEnum == md::DEPTH10) {
        wantN = 10;
    }
    else if (marketTypeEnum == md::DEPTH20) {
        wantN = 20;
    }
    else {
        return;
    }

    auto push_depth = [&](auto& depthSnap) {
        depthSnap.exchangeTypeEnum = exchangeTypeEnum;
        depthSnap.instTypeEnum = instTypeEnum;
        depthSnap.marketTypeEnum = marketTypeEnum;
        strncpy(depthSnap.instId, info.instId, INSTID_SIZE);
        depthSnap.tsTrans = tsTrans;
        depthSnap.tsEvent = tsTrans;
        depthSnap.tsRecv = tsNet;
    };

    if (marketTypeEnum == md::DEPTH5) {
        md::Depth5 d; 
        memset(&d, 0, sizeof(d)); 
        push_depth(d);
        double* apArr[5] = {&d.ap1, &d.ap2, &d.ap3, &d.ap4, &d.ap5};
        double* avArr[5] = {&d.av1, &d.av2, &d.av3, &d.av4, &d.av5};
        double* bpArr[5] = {&d.bp1, &d.bp2, &d.bp3, &d.bp4, &d.bp5};
        double* bvArr[5] = {&d.bv1, &d.bv2, &d.bv3, &d.bv4, &d.bv5};
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
        mDepth5Publisher[key]->push(d);
#endif
    }
    else if (marketTypeEnum == md::DEPTH10) {
        md::Depth10 d; 
        memset(&d, 0, sizeof(d)); 
        push_depth(d);
        double* apArr[10] = {&d.ap1, &d.ap2, &d.ap3, &d.ap4, &d.ap5, &d.ap6, &d.ap7, &d.ap8, &d.ap9, &d.ap10};
        double* avArr[10] = {&d.av1, &d.av2, &d.av3, &d.av4, &d.av5, &d.av6, &d.av7, &d.av8, &d.av9, &d.av10};
        double* bpArr[10] = {&d.bp1, &d.bp2, &d.bp3, &d.bp4, &d.bp5, &d.bp6, &d.bp7, &d.bp8, &d.bp9, &d.bp10};
        double* bvArr[10] = {&d.bv1, &d.bv2, &d.bv3, &d.bv4, &d.bv5, &d.bv6, &d.bv7, &d.bv8, &d.bv9, &d.bv10};
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
        mDepth10Publisher[key]->push(d);
#endif
    }
    else if (marketTypeEnum == md::DEPTH20) {
        md::Depth20 d; 
        memset(&d, 0, sizeof(d)); 
        push_depth(d);
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
        mDepth20Publisher[key]->push(d);
#endif
    }
}


// ============================================================================
// handleTrades (templateId=1005)
// ============================================================================
void md::OkxSbeUnit::handleTrades(const uint8_t* data, size_t len, int64_t tsNet) {
    const auto* v = okxsbe::view_of<okxsbe::TradesView>(data, len);
    if (!v) { 
        LOG_ERROR("[OKX_SBE] Trades view failed, len: {}", len); 
        return; 
    }

    if (marketTypeEnum != md::TRADES) {
        return;
    }

    md::InstrumentInfo info;
    if (!lookupByCode(v->instIdCode, info)) {
        LOG_ERROR("[OKX_SBE] Trades: instIdCode {} not in code index", v->instIdCode);
        return;
    }
    const std::string& key = crypto::get_md_channel_key(exchangeTypeEnum, instTypeEnum, marketTypeEnum, info.instId);

    md::Trades trades;
    memset(&trades, 0, sizeof(trades));
    trades.exchangeTypeEnum = exchangeTypeEnum;
    trades.instTypeEnum = instTypeEnum;
    trades.marketTypeEnum = marketTypeEnum;
    strncpy(trades.instId, info.instId, INSTID_SIZE);

    int64_t tsTrans = v->tsUs;
    trades.tsTrans = tsTrans;
    trades.tsEvent = tsTrans;
    trades.tsRecv = tsNet;

    std::string tidStr = std::to_string(v->tradeId);
    strncpy(trades.tradeId, tidStr.c_str(), INSTID_SIZE);

    double px = okxsbe::to_double(v->pxMantissa, v->pxExponent);
    double sz = okxsbe::to_double(v->szMantissa, v->szExponent);
    trades.px = px * info.reduceNumber;
    trades.sz = sz * info.magnifyNumber;
    trades.direction = (v->side == okxsbe::Side_Buy) ? DT_LONG : DT_SHORT;

    trades.tsParse = crypto::getCurrentTime();

#ifdef NEED_SHM
    mTradesPublisher[key]->push(trades);
#endif
}

// ============================================================================
// handleExpUpdate (templateId=1002 / 1004)
// ============================================================================
void md::OkxSbeUnit::handleExpUpdate(const uint8_t* data, size_t len) {
    const auto* v = okxsbe::view_of<okxsbe::BooksL2ExpUpdateView>(data, len);
    if (!v) return;
    // mLatestExponent[v->instIdCode] = {v->pxExponent, v->szExponent};
    LOG_INFO("[OKX_SBE] exp update: instIdCode={} pxExp={} szExp={}", v->instIdCode, (int)v->pxExponent, (int)v->szExponent);
}

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
