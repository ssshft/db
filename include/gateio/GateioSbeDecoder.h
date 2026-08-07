// GateioSbeDecoder.h  
//  
// Gate.io SBE 解码器 (期货 FEX + 现货), 零拷贝、零依赖。  
// Schemas:  
//   期货  new_dev/gate_sbe.xml       package="gate_fex_ws_sbe"  schemaId=1 v1  
//   现货  new_dev/gate_sbe_spot.xml  package="gate_spot_ws_sbe" schemaId=1 v1  
// byteOrder=littleEndian (与 x86_64 native 一致)  
//  
// 官方 endpoint (docs/developers/{futures,apiv4}/ws/en/#sbe-data-push):  
//   期货 prod  USDT-M : wss://fx-ws.gateio.ws/v4/ws/usdt/sbe  
//   现货 prod         : wss://api.gateio.ws/ws/v4/sbe   (按 /sbe 规则推导; spot SBE 上线时间以官方为准)  
//   规则: 在现有 WS URL 后追加 /sbe, 可选 ?sbe_schema_id=1  
//  
// SBE 支持的公开频道 (channel 名照抄 JSON, 不用 schema 消息名):  
//   ── 期货 ──                          ── 现货 ──  
//   futures.book_ticker  → tid=1 bbo    spot.book_ticker  → tid=1 bbo  
//   futures.trades       → 2 publicTrade spot.trades      → 2 publicTrade  
//   futures.obu          → 3 obu        spot.obu          → 3 obu  
//   futures.order_book   → 4 orderBook  spot.order_book   → 4 orderBook  
//   futures.order_book_update → 5       spot.order_book_update → 5  
//  
// 同连接 JSON+SBE 共存: opcode=1 text (JSON, 订阅/ping/首响应),  
//                      opcode=2 binary (SBE 行情), 用 WsClient isBinary 分派。  
//  
// ⚠️ 期货 vs 现货的 wire 差异 (view 不能共享):  
//   1) bbo (id=1): 期货 ask 在前, 现货 bid 在前。  
//   2) publicTrade (id=2): 期货是 group (数组), 现货是**根字段单笔** (无 group)。  
//   3) orderBook (id=4): 期货 asks 先, 现货 bids 先; 期货多 id+level, 现货用 lastUpdateId。  
//   4) orderBookUpdate (id=5): 期货 asks 先, 现货 bids 先且多 bigE 字段。  
//   5) obu (id=3): 结构基本一致 (两边都 bids 先)。  
//  
// 因此期货放在 md::gateio::sbe::futures, 现货放在 md::gateio::sbe::spot,  
// 共用工具 (MessageHeader / GroupSize16 / varString8 / to_double) 放在外层  
// md::gateio::sbe。 使用示例:  
//   auto tid = md::gateio::sbe::peek_template_id(data, len);  
//   if (tid == md::gateio::sbe::futures::kTemplateBbo) {  
//       auto* v = md::gateio::sbe::view_of<md::gateio::sbe::futures::BboView>(data, len);  
//       double bp = md::gateio::sbe::to_double(v->bidMantissaPrice, v->pxExponent);  
//   }  
  
#pragma once  
  
#include <cmath>  
#include <cstddef>  
#include <cstdint>  
#include <string_view>  
  
  
// ============================================================================  
// 共享: 头 / group dim / varString8 / to_double  
// ============================================================================  
  
namespace gateiosbe {  
  
enum EventEnum : int8_t {  
    Event_Subscribe   = 0,  
    Event_Unsubscribe = 1,  
    Event_Update      = 2,  
    Event_All         = 3,  
    Event_Api         = 4,  
};  
  
enum SideEnum : int8_t {  
    Side_Sell = 0,  
    Side_Buy  = 1,  
};  
  
enum BoolEnum : uint8_t {  
    Bool_False = 0,  
    Bool_True  = 1,  
};  
  
#pragma pack(push, 1)  
  
struct MessageHeader {         // 8B  
    uint16_t blockLength;  
    uint16_t templateId;  
    uint16_t schemaId;  
    uint16_t version;  
};  
static_assert(sizeof(MessageHeader) == 8, "MessageHeader size mismatch");  
  
struct GroupSize16 {           // 4B  (Gate 目前无 uint32 版, 期货现货一律用它)  
    uint16_t blockLength;  
    uint16_t numInGroup;  
};  
static_assert(sizeof(GroupSize16) == 4, "GroupSize16 size mismatch");  
  
#pragma pack(pop)  
  
// ---- 探测头 / view helper ---------------------------------------------------  
  
inline uint16_t peek_template_id(const uint8_t* data, size_t len) noexcept {  
    if (len < sizeof(MessageHeader)) return 0;  
    return reinterpret_cast<const MessageHeader*>(data)->templateId;  
}  
  
inline uint16_t peek_schema_id(const uint8_t* data, size_t len) noexcept {  
    if (len < sizeof(MessageHeader)) return 0;  
    return reinterpret_cast<const MessageHeader*>(data)->schemaId;  
}  
  
inline const MessageHeader* header_of(const uint8_t* data) noexcept {  
    return reinterpret_cast<const MessageHeader*>(data);  
}  
  
template <typename T>  
inline const T* view_of(const uint8_t* data, size_t len) noexcept {  
    if (len < sizeof(MessageHeader) + sizeof(T)) return nullptr;  
    return reinterpret_cast<const T*>(data + sizeof(MessageHeader));  
}  
  
// ---- varString8 (1B len + N bytes UTF-8) -----------------------------------  
// 从 buf 起始读, 返回 payload string_view; out_next 指向下一字节 (供链式读)。  
  
inline std::string_view read_var_string(const uint8_t* p, const uint8_t* end,  
                                        const uint8_t** out_next) noexcept {  
    if (p >= end) {  
        if (out_next) *out_next = end;  
        return {};  
    }  
    uint8_t slen = *p;  
    const char* s = reinterpret_cast<const char*>(p + 1);  
    if (p + 1 + slen > end) {  
        if (out_next) *out_next = end;  
        return {};  
    }  
    if (out_next) *out_next = p + 1 + slen;  
    return std::string_view(s, slen);  
}  
  
// ---- mantissa × 10^exponent → double ---------------------------------------  
  
inline double to_double(int64_t mantissa, int8_t exponent) noexcept {  
    static constexpr double kPow10[31] = {  
        1e-15, 1e-14, 1e-13, 1e-12, 1e-11, 1e-10, 1e-9, 1e-8, 1e-7, 1e-6,  
        1e-5,  1e-4,  1e-3,  1e-2,  1e-1,  1.0,   1e1,  1e2,  1e3,  1e4,  
        1e5,   1e6,   1e7,   1e8,   1e9,   1e10,  1e11, 1e12, 1e13, 1e14, 1e15  
    };  
    int idx = static_cast<int>(exponent) + 15;  
    if (idx >= 0 && idx <= 30) {  
        return static_cast<double>(mantissa) * kPow10[idx];  
    }  
    return static_cast<double>(mantissa) * std::pow(10.0, static_cast<double>(exponent));  
}  
  
} // namespace md::gateio::sbe  
  
  
// ============================================================================  
// 期货 (FEX): namespace md::gateio::sbe::futures  
// ============================================================================  
  
namespace gateiosbefutures {  
  
enum : uint16_t {
    kTemplateBbo              = 1,
    kTemplatePublicTrade      = 2,
    kTemplateObu              = 3,
    kTemplateOrderBook        = 4,
    kTemplateOrderBookUpdate  = 5,
    kTemplateUserTrade        = 6,
    kTemplatePosition         = 7,
    kTemplateCandlestick      = 8,
    kTemplateFuturesTicker    = 9,
    kTemplateOrders           = 10,
};
 
#pragma pack(push, 1)  
  
// ---------- bbo (id=1) root: 59 字节 (ask 先) ----------  
struct BboView {  
    int64_t time;  
    int8_t  e;  
    int64_t t;  
    int64_t u;  
    int8_t  pxExponent;  
    int8_t  szExponent;  
    int64_t askMantissaPrice;  
    int64_t askMantissaSize;  
    int64_t bidMantissaPrice;  
    int64_t bidMantissaSize;  
};  
static_assert(sizeof(BboView) == 59, "futures BboView wire size mismatch");  
  
// ---------- publicTrade (id=2) root: 11 字节 + trades group ----------  
struct PublicTradeRoot {  
    int64_t time;  
    int8_t  e;  
    int8_t  pxExponent;  
    int8_t  szExponent;  
};  
static_assert(sizeof(PublicTradeRoot) == 11, "futures PublicTradeRoot wire size mismatch");  
  
struct PublicTradeEntry {      // 32B  
    int64_t  t;  
    uint64_t id;  
    int64_t  size;             // 带符号: 正=主动买(long), 负=主动卖(short)  
    int64_t  price;  
};  
static_assert(sizeof(PublicTradeEntry) == 32, "futures PublicTradeEntry wire size mismatch");  
  
// ---------- obu (id=3) root: 36 字节 ----------  
struct ObuRoot {  
    int64_t time;  
    int8_t  e;  
    int64_t t;  
    uint8_t full;              // 1=snapshot, 0=incremental  
    int64_t firstID;  
    int64_t lastID;  
    int8_t  pxExponent;  
    int8_t  szExponent;  
};  
static_assert(sizeof(ObuRoot) == 36, "futures ObuRoot wire size mismatch");  
  
// ---------- orderBook (id=4) root: 28 字节 ----------  
struct OrderBookRoot {  
    int64_t time;  
    int8_t  e;  
    int64_t t;  
    int64_t id;  
    int8_t  pxExponent;  
    int8_t  szExponent;  
    uint8_t level;  
};  
static_assert(sizeof(OrderBookRoot) == 28, "futures OrderBookRoot wire size mismatch");  
  
// ---------- orderBookUpdate (id=5) root: 36 字节 ----------  
struct OrderBookUpdateRoot {  
    int64_t time;  
    int8_t  e;  
    int64_t t;  
    int64_t firstID;  
    int64_t lastID;  
    int8_t  pxExponent;  
    int8_t  szExponent;  
    uint8_t level;  
};  
static_assert(sizeof(OrderBookUpdateRoot) == 36, "futures OrderBookUpdateRoot wire size mismatch");  
  
// depth 类 entry: 16B (bids/asks 同结构)  
struct DepthEntry {  
    int64_t pxMantissa;  
    int64_t szMantissa;  
};  
static_assert(sizeof(DepthEntry) == 16, "futures DepthEntry wire size mismatch");  


struct CandlestickRoot {
    int64_t time;
    int8_t  e;
    int8_t  pxExponent;
    int8_t  szExponent;
    int8_t  amountExponent;
};

static_assert(sizeof(CandlestickRoot)==12,"");

struct CandlestickEntry {
    int64_t t;

    int64_t openMantissa;
    int64_t highMantissa;
    int64_t lowMantissa;
    int64_t closeMantissa;

    int64_t volumeMantissa;
    int64_t amountMantissa;

    uint8_t complete;
};

static_assert(sizeof(CandlestickEntry)==57,"");


struct FuturesTickerRoot {

    int64_t time;

    int8_t e;

};

static_assert(sizeof(FuturesTickerRoot)==9,"");


struct FuturesTickerEntry {

    int64_t t;

    int8_t pxExponent;

    int64_t lastMantissa;
    int64_t changePriceMantissa;
    int64_t low24hMantissa;
    int64_t high24hMantissa;

    int8_t markPxExponent;
    int64_t markPriceMantissa;

    int8_t indexPxExponent;
    int64_t indexPriceMantissa;

    int8_t changePercentageExponent;
    int64_t changePercentageMantissa;

    int8_t fundingRateExponent;
    int64_t fundingRateMantissa;

    int8_t szExponent;
    int64_t totalSize;

    int8_t volume24hExponent;
    int64_t volume24hMantissa;

    int8_t volume24hBaseExponent;
    int64_t volume24hBaseMantissa;

    int8_t volume24hQuoteExponent;
    int64_t volume24hQuoteMantissa;

    int8_t volume24hSettleExponent;
    int64_t volume24hSettleMantissa;
};

static_assert(sizeof(FuturesTickerEntry)==121,"");

  
#pragma pack(pop)  
  
// ---- bbo 后跟 channel + symbol 两个 varString8 -----------------------------  
  
inline std::string_view bbo_symbol(const uint8_t* data, size_t len) noexcept {  
    if (len < sizeof(gateiosbe::MessageHeader) + sizeof(BboView)) return {};  
    const auto* hdr = gateiosbe::header_of(data);  
    const uint8_t* end = data + len;  
    const uint8_t* p = data + sizeof(gateiosbe::MessageHeader) + hdr->blockLength;  
    const uint8_t* next = nullptr;  
    (void)gateiosbe::read_var_string(p, end, &next);  
    return gateiosbe::read_var_string(next, end, nullptr);  
}  
  
// ---- publicTrade iter (root + trades group + channel + contract) -----------  
  
class PublicTradeIter {  
public:  
    PublicTradeIter(const uint8_t* data, size_t len) noexcept  
        : ok_(false)  
    {  
        if (len < sizeof(gateiosbe::MessageHeader) + sizeof(PublicTradeRoot) + sizeof(gateiosbe::GroupSize16)) return;  
  
        const auto* hdr = gateiosbe::header_of(data);  
        root_ = reinterpret_cast<const PublicTradeRoot*>(data + sizeof(gateiosbe::MessageHeader));  
  
        const uint8_t* end = data + len;  
        const uint8_t* p = data + sizeof(gateiosbe::MessageHeader) + hdr->blockLength;  
        if (p + sizeof(gateiosbe::GroupSize16) > end) return;  
  
        dim_ = reinterpret_cast<const gateiosbe::GroupSize16*>(p);  
        p += sizeof(gateiosbe::GroupSize16);  
        entries_ = reinterpret_cast<const PublicTradeEntry*>(p);  
        p += static_cast<size_t>(dim_->blockLength) * dim_->numInGroup;  
        if (p > end) return;  
  
        const uint8_t* next = nullptr;  
        channel_ = gateiosbe::read_var_string(p, end, &next);  
        symbol_  = gateiosbe::read_var_string(next, end, nullptr);  
  
        ok_ = true;  
    }  
  
    bool ok() const noexcept { return ok_; }  
    const PublicTradeRoot* root() const noexcept { return root_; }  
    uint16_t count() const noexcept { return dim_ ? dim_->numInGroup : 0; }  
    std::string_view channel() const noexcept { return channel_; }  
    std::string_view symbol()  const noexcept { return symbol_;  }  
  
    const PublicTradeEntry* entry(uint16_t idx) const noexcept {  
        const uint8_t* p = reinterpret_cast<const uint8_t*>(entries_)  
                         + static_cast<size_t>(idx) * dim_->blockLength;  
        return reinterpret_cast<const PublicTradeEntry*>(p);  
    }  
  
private:  
    bool ok_;  
    const PublicTradeRoot* root_       = nullptr;  
    const gateiosbe::GroupSize16* dim_            = nullptr;  
    const PublicTradeEntry* entries_   = nullptr;  
    std::string_view channel_;  
    std::string_view symbol_;  
};  
  
// ---- Depth iter 模板 (FirstIsAsks 决定 group 顺序) --------------------------  
//   期货 schema: obu(3) bids 先, orderBook(4)/orderBookUpdate(5) asks 先。  
  
template <typename RootT, bool FirstIsAsks>  
class DepthIterT {  
public:  
    DepthIterT(const uint8_t* data, size_t len) noexcept  
        : ok_(false)  
    {  
        if (len < sizeof(gateiosbe::MessageHeader) + sizeof(RootT) + 2 * sizeof(gateiosbe::GroupSize16)) return;  
  
        const auto* hdr = gateiosbe::header_of(data);  
        root_ = reinterpret_cast<const RootT*>(data + sizeof(gateiosbe::MessageHeader));  
  
        const uint8_t* end = data + len;  
        const uint8_t* p = data + sizeof(gateiosbe::MessageHeader) + hdr->blockLength;  
        if (p + sizeof(gateiosbe::GroupSize16) > end) return;  
  
        const gateiosbe::GroupSize16* dim1 = reinterpret_cast<const gateiosbe::GroupSize16*>(p);  
        p += sizeof(gateiosbe::GroupSize16);  
        const DepthEntry* entries1 = reinterpret_cast<const DepthEntry*>(p);  
        p += static_cast<size_t>(dim1->blockLength) * dim1->numInGroup;  
        if (p + sizeof(gateiosbe::GroupSize16) > end) return;  
  
        const gateiosbe::GroupSize16* dim2 = reinterpret_cast<const gateiosbe::GroupSize16*>(p);  
        p += sizeof(gateiosbe::GroupSize16);  
        const DepthEntry* entries2 = reinterpret_cast<const DepthEntry*>(p);  
        p += static_cast<size_t>(dim2->blockLength) * dim2->numInGroup;  
        if (p > end) return;  
  
        if constexpr (FirstIsAsks) {  
            asks_dim_ = dim1; asks_entries_ = entries1;  
            bids_dim_ = dim2; bids_entries_ = entries2;  
        } else {  
            bids_dim_ = dim1; bids_entries_ = entries1;  
            asks_dim_ = dim2; asks_entries_ = entries2;  
        }  
  
        const uint8_t* next = nullptr;  
        channel_ = gateiosbe::read_var_string(p, end, &next);  
        symbol_  = gateiosbe::read_var_string(next, end, nullptr);  
  
        ok_ = true;  
    }  
  
    bool ok() const noexcept { return ok_; }  
    const RootT* root() const noexcept { return root_; }  
    uint16_t bid_count() const noexcept { return bids_dim_ ? bids_dim_->numInGroup : 0; }  
    uint16_t ask_count() const noexcept { return asks_dim_ ? asks_dim_->numInGroup : 0; }  
  
    const DepthEntry* bid(uint16_t idx) const noexcept {  
        const uint8_t* p = reinterpret_cast<const uint8_t*>(bids_entries_)  
                         + static_cast<size_t>(idx) * bids_dim_->blockLength;  
        return reinterpret_cast<const DepthEntry*>(p);  
    }  
    const DepthEntry* ask(uint16_t idx) const noexcept {  
        const uint8_t* p = reinterpret_cast<const uint8_t*>(asks_entries_)  
                         + static_cast<size_t>(idx) * asks_dim_->blockLength;  
        return reinterpret_cast<const DepthEntry*>(p);  
    }  
  
    std::string_view channel() const noexcept { return channel_; }  
    std::string_view symbol()  const noexcept { return symbol_;  }  
  
private:  
    bool ok_;  
    const RootT* root_               = nullptr;  
    const gateiosbe::GroupSize16* bids_dim_     = nullptr;  
    const DepthEntry* bids_entries_  = nullptr;  
    const gateiosbe::GroupSize16* asks_dim_     = nullptr;  
    const DepthEntry* asks_entries_  = nullptr;  
    std::string_view channel_;  
    std::string_view symbol_;  
};  

class CandlestickIter {
public:
    CandlestickIter(const uint8_t* data, size_t len) noexcept {
        if (len < sizeof(MessageHeader) + sizeof(CandlestickRoot) + sizeof(GroupSize16)) {
            return;
        }

        const auto* hdr = header_of(data);
        root_ = reinterpret_cast<const CandlestickRoot*>(data + sizeof(MessageHeader));
        const uint8_t* end = data + len;
        const uint8_t* p = data + sizeof(MessageHeader) + hdr->blockLength;
        dim_ = reinterpret_cast<const GroupSize16*>(p);
        p += sizeof(GroupSize16);
        entries_ = p;
        size_t bytes = static_cast<size_t>(dim_->blockLength) * dim_->numInGroup;
        if (p + bytes > end)
            return;

        tail_ = p + bytes;
        end_ = end;
        ok_ = true;
    }

    bool ok() const noexcept {
        return ok_;
    }

    const CandlestickRoot* root() const noexcept {
        return root_;
    }

    uint16_t count() const noexcept {
        return dim_ ? dim_->numInGroup : 0;
    }

    const CandlestickEntry* entry(uint16_t i) const noexcept {
        return reinterpret_cast<const CandlestickEntry*>(entries_ + static_cast<size_t>(i) * dim_->blockLength);
    }

    std::string_view name(uint16_t i) const noexcept {
        const uint8_t* p = entries_ + static_cast<size_t>(i) * dim_->blockLength + sizeof(CandlestickEntry);
        const uint8_t* next=nullptr;

        return read_var_string(p, end_, &next);
    }

    std::string_view channel() const noexcept {
        const uint8_t* next=nullptr;
        return read_var_string(tail_, end_, &next);
    }

private:

    bool ok_{false};
    const CandlestickRoot* root_{nullptr};
    const GroupSize16* dim_{nullptr};
    const uint8_t* entries_{nullptr};
    const uint8_t* tail_{nullptr};
    const uint8_t* end_{nullptr};
};
  
using ObuIter             = DepthIterT<ObuRoot,             false>;   // bids 先  
using OrderBookIter       = DepthIterT<OrderBookRoot,       true>;    // asks 先  
using OrderBookUpdateIter = DepthIterT<OrderBookUpdateRoot, true>;    // asks 先  
  
} // namespace md::gateio::sbe::futures  
  
  
// ============================================================================  
// 现货: namespace md::gateio::sbe::spot  
// ============================================================================  
  
namespace gateiosbespot {  
  
enum : uint16_t {  
    kTemplateBbo              = 1,  
    kTemplatePublicTrade      = 2,  
    kTemplateObu              = 3,  
    kTemplateOrderBook        = 4,  
    kTemplateOrderBookUpdate  = 5,  
    kTemplateCandlestick      = 6,  
    kTemplateTicker           = 7,  
    // 8~17: 私有 / market meta, 未覆盖  
};  
  
#pragma pack(push, 1)  
  
// ---------- bbo (id=1) root: 59 字节 (⚠️ bid 先, 与期货相反) ----------  
struct BboView {  
    int64_t time;  
    int8_t  e;  
    int64_t t;  
    int64_t u;  
    int8_t  pxExponent;  
    int8_t  szExponent;  
    int64_t bidMantissaPrice;  
    int64_t bidMantissaSize;  
    int64_t askMantissaPrice;  
    int64_t askMantissaSize;  
};  
static_assert(sizeof(BboView) == 59, "spot BboView wire size mismatch");  
  
// ---------- publicTrade (id=2) root: 60 字节 (⚠️ 无 group, 单笔定长) ----------  
struct PublicTradeView {  
    int64_t  time;  
    int8_t   e;  
    int8_t   pxExponent;  
    int8_t   szExponent;  
    uint64_t id;  
    uint64_t idMarket;  
    int64_t  createTime;  
    int64_t  createTimeUs;  
    int8_t   side;             // SideEnum  
    int64_t  priceMantissa;  
    int64_t  amountMantissa;  
};  
static_assert(sizeof(PublicTradeView) == 60, "spot PublicTradeView wire size mismatch");  
  
// ---------- obu (id=3) root: 36 字节 ----------  
struct ObuRoot {  
    int64_t time;  
    int8_t  e;  
    int64_t t;  
    uint8_t full;  
    int64_t firstID;  
    int64_t lastID;  
    int8_t  pxExponent;  
    int8_t  szExponent;  
};  
static_assert(sizeof(ObuRoot) == 36, "spot ObuRoot wire size mismatch");  
  
// ---------- orderBook (id=4) root: 26 字节 ----------  
struct OrderBookRoot {  
    int64_t time;  
    int8_t  e;  
    int64_t t;  
    int64_t lastUpdateId;  
    int8_t  pxExponent;  
    int8_t  szExponent;  
};  
static_assert(sizeof(OrderBookRoot) == 27, "spot OrderBookRoot wire size mismatch");  
  
// ---------- orderBookUpdate (id=5) root: 44 字节 ----------  
struct OrderBookUpdateRoot {  
    int64_t time;  
    int8_t  e;  
    int64_t t;  
    int64_t bigE;              // JSON: E, us  
    uint8_t full;  
    int64_t firstID;  
    int64_t lastID;  
    int8_t  pxExponent;  
    int8_t  szExponent;  
};  
static_assert(sizeof(OrderBookUpdateRoot) == 44, "spot OrderBookUpdateRoot wire size mismatch");  
  
// depth 类 entry: 16B (bids/asks 同结构; 与 futures 布局兼容, 但独立类型避免误混用)  
struct DepthEntry {  
    int64_t pxMantissa;  
    int64_t szMantissa;  
};  
static_assert(sizeof(DepthEntry) == 16, "spot DepthEntry wire size mismatch");  
  

// ---------- candlestick (id=6) root ----------
struct CandlestickView {
    int64_t time;              // server timestamp us
    int8_t  e;                 // EventEnum
    int64_t t;                 // candle timestamp us
    int8_t  pxExponent;
    int8_t  szExponent;
    int8_t  amountExponent;
    int64_t openMantissa;
    int64_t highMantissa;
    int64_t lowMantissa;
    int64_t closeMantissa;
    int64_t volumeMantissa;
    int64_t amountMantissa;
    uint8_t complete;          // BoolEnum
};

static_assert(sizeof(CandlestickView) == 79, "spot CandlestickView wire size mismatch");

#pragma pack(pop)  
  
// ---- bbo symbol 读取 (跳 channel 后读 currency_pair) -----------------------  
  
inline std::string_view bbo_symbol(const uint8_t* data, size_t len) noexcept {  
    if (len < sizeof(gateiosbe::MessageHeader) + sizeof(BboView)) return {};  
    const auto* hdr = gateiosbe::header_of(data);  
    const uint8_t* end = data + len;  
    const uint8_t* p = data + sizeof(gateiosbe::MessageHeader) + hdr->blockLength;  
    const uint8_t* next = nullptr;  
    (void)gateiosbe::read_var_string(p, end, &next);  
    return gateiosbe::read_var_string(next, end, nullptr);  
}  
  
// ---- publicTrade symbol (跳 channel 后读 currency_pair) --------------------  
//   publicTrade 现货是无 group 的定长根, 后接 channel + currency_pair + range  
  
inline std::string_view public_trade_symbol(const uint8_t* data, size_t len) noexcept {  
    if (len < sizeof(gateiosbe::MessageHeader) + sizeof(PublicTradeView)) return {};  
    const auto* hdr = gateiosbe::header_of(data);  
    const uint8_t* end = data + len;  
    const uint8_t* p = data + sizeof(gateiosbe::MessageHeader) + hdr->blockLength;  
    const uint8_t* next = nullptr;  
    (void)gateiosbe::read_var_string(p, end, &next);  
    return gateiosbe::read_var_string(next, end, nullptr);  
}  


inline std::string_view candlestick_name(const uint8_t* data, size_t len) noexcept {
    if (len < sizeof(MessageHeader) + sizeof(CandlestickView))
        return {};

    const auto* hdr = header_of(data);
    const uint8_t* end = data + len;
    const uint8_t* p = data + sizeof(MessageHeader) + hdr->blockLength;
    const uint8_t* next = nullptr;

    // skip channel
    (void)read_var_string(p, end, &next);
    // return name
    return read_var_string(next, end, nullptr);
}

inline const CandlestickView* candlestick_view(const uint8_t* data, size_t len) noexcept {
    return view_of<CandlestickView>(data, len);
}
  
// ---- Depth iter (现货全都是 bids 先) ---------------------------------------  
  
template <typename RootT>  
class DepthIterT {  
public:  
    DepthIterT(const uint8_t* data, size_t len) noexcept  
        : ok_(false)  
    {  
        if (len < sizeof(gateiosbe::MessageHeader) + sizeof(RootT) + 2 * sizeof(gateiosbe::GroupSize16)) return;  
  
        const auto* hdr = gateiosbe::header_of(data);  
        root_ = reinterpret_cast<const RootT*>(data + sizeof(gateiosbe::MessageHeader));  
  
        const uint8_t* end = data + len;  
        const uint8_t* p = data + sizeof(gateiosbe::MessageHeader) + hdr->blockLength;  
        if (p + sizeof(gateiosbe::GroupSize16) > end) return;  
  
        bids_dim_ = reinterpret_cast<const gateiosbe::GroupSize16*>(p);  
        p += sizeof(gateiosbe::GroupSize16);  
        bids_entries_ = reinterpret_cast<const DepthEntry*>(p);  
        p += static_cast<size_t>(bids_dim_->blockLength) * bids_dim_->numInGroup;  
        if (p + sizeof(gateiosbe::GroupSize16) > end) return;  
  
        asks_dim_ = reinterpret_cast<const gateiosbe::GroupSize16*>(p);  
        p += sizeof(gateiosbe::GroupSize16);  
        asks_entries_ = reinterpret_cast<const DepthEntry*>(p);  
        p += static_cast<size_t>(asks_dim_->blockLength) * asks_dim_->numInGroup;  
        if (p > end) return;  
  
        const uint8_t* next = nullptr;  
        channel_ = gateiosbe::read_var_string(p, end, &next);  
        symbol_  = gateiosbe::read_var_string(next, end, nullptr);  
  
        ok_ = true;  
    }  
  
    bool ok() const noexcept { return ok_; }  
    const RootT* root() const noexcept { return root_; }  
    uint16_t bid_count() const noexcept { return bids_dim_ ? bids_dim_->numInGroup : 0; }  
    uint16_t ask_count() const noexcept { return asks_dim_ ? asks_dim_->numInGroup : 0; }  
  
    const DepthEntry* bid(uint16_t idx) const noexcept {  
        const uint8_t* p = reinterpret_cast<const uint8_t*>(bids_entries_)  
                         + static_cast<size_t>(idx) * bids_dim_->blockLength;  
        return reinterpret_cast<const DepthEntry*>(p);  
    }  
    const DepthEntry* ask(uint16_t idx) const noexcept {  
        const uint8_t* p = reinterpret_cast<const uint8_t*>(asks_entries_)  
                         + static_cast<size_t>(idx) * asks_dim_->blockLength;  
        return reinterpret_cast<const DepthEntry*>(p);  
    }  
  
    std::string_view channel() const noexcept { return channel_; }  
    std::string_view symbol()  const noexcept { return symbol_;  }  
  
private:  
    bool ok_;  
    const RootT* root_               = nullptr;  
    const gateiosbe::GroupSize16* bids_dim_     = nullptr;  
    const DepthEntry* bids_entries_  = nullptr;  
    const gateiosbe::GroupSize16* asks_dim_     = nullptr;  
    const DepthEntry* asks_entries_  = nullptr;  
    std::string_view channel_;  
    std::string_view symbol_;  
};  


struct CandlestickMeta {
    std::string_view channel;
    std::string_view name;

};

inline CandlestickMeta candlestick_meta(const uint8_t* data, size_t len) noexcept {
    CandlestickMeta ret{};

    if (len < sizeof(MessageHeader) + sizeof(CandlestickView))
        return ret;

    const auto* hdr = header_of(data);
    const uint8_t* end = data + len;
    const uint8_t* p = data + sizeof(MessageHeader) + hdr->blockLength;
    const uint8_t* next = nullptr;
    ret.channel = read_var_string(p, end, &next);
    ret.name = read_var_string(next, end, nullptr);
    return ret;
}

  
using ObuIter             = DepthIterT<ObuRoot>;  
using OrderBookIter       = DepthIterT<OrderBookRoot>;  
using OrderBookUpdateIter = DepthIterT<OrderBookUpdateRoot>;  
  
} // namespace md::gateio::sbe::spot  
