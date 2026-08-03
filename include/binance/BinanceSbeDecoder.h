// BinanceSbeDecoder.h
//
// Binance SBE (Simple Binary Encoding) 解码器, 零拷贝、零依赖。
// Schema: new_dev/binance_sbe.xml
//   package="spot_stream"  schemaId=1  version=0  semanticVersion=5.2
//   byteOrder="littleEndian"  (与 x86_64 native 一致)
//
// 消息模板:
//   10000  TradesStreamEvent            (成交推送, root 18B + trades group + varString8 symbol)
//   10001  BestBidAskStreamEvent        (bbo 推送, root 50B + varString8 symbol, 无 group)
//   10002  DepthSnapshotStreamEvent     (深度快照 e.g. depth20@btcusdt, root 18B + bids + asks + symbol)
//   10003  DepthDiffStreamEvent         (深度增量, root 26B + bids + asks + symbol)
//
// 与 OKX SBE 的关键区别:
//   1) 用 varString8 (1B len + UTF-8) 携带 symbol, 且放在**消息末尾**;
//      需要先跳过所有 group 才能读到。
//   2) trades group 的 dim 是 groupSizeEncoding (6B: u16 blockLength + u32 numInGroup),
//      而 depth 的 group 用 groupSize16Encoding (4B: u16 + u16), 两种都要支持。
//   3) Trade entry 里 isBestMatch 是 presence="constant" 不上线, 实际 wire size = 25 字节。
//
// 使用示例:
//   auto tid = md::binance::sbe::peek_template_id(data, len);
//   if (tid == md::binance::sbe::kTemplateBestBidAsk) {
//       auto* v   = md::binance::sbe::view_of<md::binance::sbe::BestBidAskView>(data, len);
//       double bp = md::binance::sbe::to_double(v->bidPrice, v->priceExponent);
//       std::string_view sym = md::binance::sbe::symbol_after_root<md::binance::sbe::BestBidAskView>(data, len);
//   }

#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace binancesbe {

// ============================================================================
// 常量
// ============================================================================

enum : uint16_t {
    kTemplateTrades         = 10000,
    kTemplateBestBidAsk     = 10001,
    kTemplateDepthSnapshot  = 10002,
    kTemplateDepthDiff      = 10003,
};

// boolEnum (uint8): False=0, True=1
enum BoolEnum : uint8_t {
    Bool_False = 0,
    Bool_True  = 1,
};

// ============================================================================
// 通用头 / group dim
// ============================================================================

#pragma pack(push, 1)

struct MessageHeader {           // 8 字节
    uint16_t blockLength;
    uint16_t templateId;
    uint16_t schemaId;
    uint16_t version;
};
static_assert(sizeof(MessageHeader) == 8, "MessageHeader size mismatch");

// SBE 默认的 dimensionType (blockLength u16 + numInGroup u32), 6 字节
// Binance trades group 用它
struct GroupSize32 {
    uint16_t blockLength;
    uint32_t numInGroup;
};
static_assert(sizeof(GroupSize32) == 6, "GroupSize32 size mismatch");

// Binance depth bids/asks 用 groupSize16Encoding (blockLength u16 + numInGroup u16), 4 字节
struct GroupSize16 {
    uint16_t blockLength;
    uint16_t numInGroup;
};
static_assert(sizeof(GroupSize16) == 4, "GroupSize16 size mismatch");

// ============================================================================
// 定长 View
//   ⚠️ 每个 View 的 sizeof(T) 必须等于 schema 的 root blockLength (SBE root wire size)。
//   Binance schema 显式声明了字段顺序, wire 层没有 padding, #pragma pack(1) 保证 layout。
// ============================================================================

// ---------- TradesStreamEvent (10000) root: 18 字节 ----------
struct TradesRoot {
    int64_t eventTime;
    int64_t transactTime;
    int8_t  priceExponent;
    int8_t  qtyExponent;
};
static_assert(sizeof(TradesRoot) == 18, "TradesRoot wire size mismatch");

// trades group entry: 25 字节 (isBestMatch 是 constant, 不上线)
struct TradeEntry {
    int64_t id;
    int64_t price;         // mantissa64, 用 root->priceExponent 还原
    int64_t qty;           // mantissa64, 用 root->qtyExponent 还原
    uint8_t isBuyerMaker;  // BoolEnum
};
static_assert(sizeof(TradeEntry) == 25, "TradeEntry wire size mismatch");

// ---------- BestBidAskStreamEvent (10001) root: 50 字节 ----------
struct BestBidAskView {
    int64_t eventTime;
    int64_t bookUpdateId;
    int8_t  priceExponent;
    int8_t  qtyExponent;
    int64_t bidPrice;
    int64_t bidQty;
    int64_t askPrice;
    int64_t askQty;
};
static_assert(sizeof(BestBidAskView) == 50, "BestBidAskView wire size mismatch");

// ---------- DepthSnapshotStreamEvent (10002) root: 18 字节 ----------
struct DepthSnapshotRoot {
    int64_t eventTime;
    int64_t bookUpdateId;
    int8_t  priceExponent;
    int8_t  qtyExponent;
};
static_assert(sizeof(DepthSnapshotRoot) == 18, "DepthSnapshotRoot wire size mismatch");

// ---------- DepthDiffStreamEvent (10003) root: 26 字节 ----------
struct DepthDiffRoot {
    int64_t eventTime;
    int64_t firstBookUpdateId;
    int64_t lastBookUpdateId;
    int8_t  priceExponent;
    int8_t  qtyExponent;
};
static_assert(sizeof(DepthDiffRoot) == 26, "DepthDiffRoot wire size mismatch");

// depth 两侧 entry (bids/asks 同结构): 16 字节
struct DepthEntry {
    int64_t price;         // mantissa64, 用 root->priceExponent
    int64_t qty;           // mantissa64, 用 root->qtyExponent
};
static_assert(sizeof(DepthEntry) == 16, "DepthEntry wire size mismatch");

#pragma pack(pop)

// ============================================================================
// 头部探测
// ============================================================================

inline uint16_t peek_template_id(const uint8_t* data, size_t len) noexcept {
    if (len < sizeof(MessageHeader)) return 0;
    return reinterpret_cast<const MessageHeader*>(data)->templateId;
}

inline const MessageHeader* header_of(const uint8_t* data) noexcept {
    return reinterpret_cast<const MessageHeader*>(data);
}

// 拿到 root fields 起始位置 (header 之后)
inline const uint8_t* body_of(const uint8_t* data) noexcept {
    return data + sizeof(MessageHeader);
}

// 定长 View 快速构造
template <typename T>
inline const T* view_of(const uint8_t* data, size_t len) noexcept {
    if (len < sizeof(MessageHeader) + sizeof(T)) return nullptr;
    return reinterpret_cast<const T*>(data + sizeof(MessageHeader));
}

// ============================================================================
// Trades 消息迭代器 (10000)
//   layout: header(8) + root(18) + tradesDim(GroupSize32, 6) + entries[numInGroup]*25
//           + varString8 symbol(1 + N)
// ============================================================================

class TradesIter {
public:
    TradesIter(const uint8_t* data, size_t len) noexcept
        : ok_(false)
    {
        if (len < sizeof(MessageHeader) + sizeof(TradesRoot) + sizeof(GroupSize32)) return;

        const auto* hdr = reinterpret_cast<const MessageHeader*>(data);
        root_ = reinterpret_cast<const TradesRoot*>(data + sizeof(MessageHeader));

        // root 段实际占用 = SBE header 声明的 blockLength (通常 == sizeof(TradesRoot) == 18)
        const uint8_t* p = data + sizeof(MessageHeader) + hdr->blockLength;
        if (p + sizeof(GroupSize32) > data + len) return;

        dim_ = reinterpret_cast<const GroupSize32*>(p);
        p += sizeof(GroupSize32);
        entries_ = reinterpret_cast<const TradeEntry*>(p);
        p += static_cast<size_t>(dim_->blockLength) * dim_->numInGroup;
        if (p > data + len) return;

        // symbol 紧跟在最后一条 entry 之后
        sym_ptr_ = p;
        sym_end_ = data + len;
        ok_ = true;
    }

    bool ok() const noexcept { return ok_; }
    const TradesRoot* root() const noexcept { return root_; }
    uint32_t count() const noexcept { return dim_ ? dim_->numInGroup : 0; }

    // group 里每条 entry 是 dim_->blockLength 字节, 而不一定是 sizeof(TradeEntry)。
    // Binance schema 声明 blockLength=25 == sizeof(TradeEntry), 所以直接下标即可。
    // 保险起见 (schema 未来加字段时兼容), 用 blockLength 步长。
    const TradeEntry* entry(uint32_t idx) const noexcept {
        const uint8_t* p = reinterpret_cast<const uint8_t*>(entries_)
                         + static_cast<size_t>(idx) * dim_->blockLength;
        return reinterpret_cast<const TradeEntry*>(p);
    }

    // varString8: 1B length + N bytes UTF-8 (不含 NUL)
    std::string_view symbol() const noexcept {
        if (!ok_ || sym_ptr_ >= sym_end_) return {};
        uint8_t slen = *sym_ptr_;
        const char* s = reinterpret_cast<const char*>(sym_ptr_ + 1);
        if (sym_ptr_ + 1 + slen > sym_end_) return {};
        return std::string_view(s, slen);
    }

private:
    bool ok_;
    const TradesRoot* root_    = nullptr;
    const GroupSize32* dim_    = nullptr;
    const TradeEntry* entries_ = nullptr;
    const uint8_t* sym_ptr_    = nullptr;
    const uint8_t* sym_end_    = nullptr;
};

// ============================================================================
// BestBidAsk symbol 读取 (无 group, 直接跟在 root 之后)
// ============================================================================

inline std::string_view bba_symbol(const uint8_t* data, size_t len) noexcept {
    if (len < sizeof(MessageHeader) + sizeof(BestBidAskView)) return {};
    const auto* hdr = reinterpret_cast<const MessageHeader*>(data);
    const uint8_t* p = data + sizeof(MessageHeader) + hdr->blockLength;
    if (p >= data + len) return {};
    uint8_t slen = *p;
    const char* s = reinterpret_cast<const char*>(p + 1);
    if (p + 1 + slen > data + len) return {};
    return std::string_view(s, slen);
}

// ============================================================================
// Depth 迭代器 (10002/10003)
//   layout: header(8) + root(blockLength) + bidsDim(GroupSize16,4) + bids*16
//           + asksDim(GroupSize16,4) + asks*16 + varString8 symbol
//   RootT: DepthSnapshotRoot 或 DepthDiffRoot
// ============================================================================

template <typename RootT>
class DepthIter {
public:
    DepthIter(const uint8_t* data, size_t len) noexcept
        : ok_(false)
    {
        if (len < sizeof(MessageHeader) + sizeof(RootT) + 2 * sizeof(GroupSize16)) return;

        const auto* hdr = reinterpret_cast<const MessageHeader*>(data);
        root_ = reinterpret_cast<const RootT*>(data + sizeof(MessageHeader));

        const uint8_t* p = data + sizeof(MessageHeader) + hdr->blockLength;
        if (p + sizeof(GroupSize16) > data + len) return;
        bids_dim_ = reinterpret_cast<const GroupSize16*>(p);
        p += sizeof(GroupSize16);
        bids_entries_ = reinterpret_cast<const DepthEntry*>(p);
        p += static_cast<size_t>(bids_dim_->blockLength) * bids_dim_->numInGroup;
        if (p + sizeof(GroupSize16) > data + len) return;

        asks_dim_ = reinterpret_cast<const GroupSize16*>(p);
        p += sizeof(GroupSize16);
        asks_entries_ = reinterpret_cast<const DepthEntry*>(p);
        p += static_cast<size_t>(asks_dim_->blockLength) * asks_dim_->numInGroup;
        if (p > data + len) return;

        sym_ptr_ = p;
        sym_end_ = data + len;
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

    std::string_view symbol() const noexcept {
        if (!ok_ || sym_ptr_ >= sym_end_) return {};
        uint8_t slen = *sym_ptr_;
        const char* s = reinterpret_cast<const char*>(sym_ptr_ + 1);
        if (sym_ptr_ + 1 + slen > sym_end_) return {};
        return std::string_view(s, slen);
    }

private:
    bool ok_;
    const RootT* root_               = nullptr;
    const GroupSize16* bids_dim_     = nullptr;
    const DepthEntry* bids_entries_  = nullptr;
    const GroupSize16* asks_dim_     = nullptr;
    const DepthEntry* asks_entries_  = nullptr;
    const uint8_t* sym_ptr_          = nullptr;
    const uint8_t* sym_end_          = nullptr;
};

using DepthSnapshotIter = DepthIter<DepthSnapshotRoot>;
using DepthDiffIter     = DepthIter<DepthDiffRoot>;

// ============================================================================
// mantissa × 10^exponent → double
//   [-15, +15] 预计算 lookup 表, 常见 exponent (-8..+8) 全部命中, 零分支 O(1)。
// ============================================================================

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

}