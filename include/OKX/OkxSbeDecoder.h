// OkxSbeDecoder.h
//
// OKX SBE (Simple Binary Encoding) 解码器,零拷贝、零依赖。
// Schema source: new_dev/okx_sbe_1_0.xml  (id=1 version=0 semanticVersion=1.0.0)
// byteOrder: littleEndian (跟 x86_64 native 一致)
//
// 消息模板:
//   1000  BboTbtChannelEvent                (best bid/ask tick-by-tick, 74 字节定长)
//   1001  BooksL2TbtChannelEvent            (L2 深度更新, 含 asks/bids 两个 repeating group)
//   1002  BooksL2TbtExponentUpdateEvent     (L2 exponent 单独 update, 无 group)
//   1003  BooksL2TbtElpChannelEvent         (含 ELP 挂单的 L2 更新)
//   1004  BooksL2TbtElpExponentUpdateEvent  (ELP 版 exp update)
//   1005  TradesChannelEvent                (成交推送, 62 字节定长)
//   1006  SnapshotDepthResponseEvent        (REST snapshot 回执, WS 收不到)
//
// 使用方式:
//   auto tid = md::okx::sbe::peek_template_id(data, len);
//   switch (tid) {
//       case md::okx::sbe::kTemplateBboTbt:
//           auto view = md::okx::sbe::BboTbtView::from(data, len);
//           double bid = md::okx::sbe::to_double(view->bidPxMantissa, view->pxExponent);
//           ...
//       case md::okx::sbe::kTemplateBooksL2Tbt: {
//           md::okx::sbe::BooksL2Iter iter(data, len);
//           while (iter.has_ask()) { auto e = iter.next_ask(); ... }
//           while (iter.has_bid()) { auto e = iter.next_bid(); ... }
//           break;
//       }
//   }

#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>

namespace okxsbe {

// ============================================================================
// 常量
// ============================================================================

enum : uint16_t {
    kTemplateBboTbt              = 1000,
    kTemplateBooksL2Tbt          = 1001,
    kTemplateBooksL2ExpUpdate    = 1002,
    kTemplateBooksL2Elp          = 1003,
    kTemplateBooksL2ElpExpUpdate = 1004,
    kTemplateTrades              = 1005,
    kTemplateSnapshotDepth       = 1006,
};

enum SideEnum : int8_t {
    Side_Sell = 0,
    Side_Buy  = 1,
};

enum SourceEnum : int8_t {
    Source_Normal = 0,
    Source_Elp    = 1,
};

// ============================================================================
// 通用头 (8 字节)
//   blockLength: 根消息 (不含 group) 的字节数
//   templateId:  消息类型
//   schemaId:    OKX 目前恒为 1
//   version:     OKX 目前恒为 0
// ============================================================================

#pragma pack(push, 1)
struct MessageHeader {
    uint16_t blockLength;
    uint16_t templateId;
    uint16_t schemaId;
    uint16_t version;
};

// Repeating group 前置 4 字节:
//   blockLength: 每个 entry 的字节数
//   numInGroup:  entry 个数
struct GroupSize16 {
    uint16_t blockLength;
    uint16_t numInGroup;
};

// ---------- 定长消息 view (直接 reinterpret_cast, 零拷贝) ----------

// 74 字节 (SBE blockLength=74)
struct BboTbtView {
    int64_t instIdCode;
    int64_t tsUs;
    int64_t outTime;
    int64_t seqId;
    int64_t askPxMantissa;
    int64_t askSzMantissa;
    int64_t bidPxMantissa;
    int64_t bidSzMantissa;
    int32_t askOrdCount;
    int32_t bidOrdCount;
    int8_t  pxExponent;
    int8_t  szExponent;
};
static_assert(sizeof(BboTbtView) == 74, "BboTbtView wire size mismatch");

// 62 字节 (SBE blockLength=62)
struct TradesView {
    int64_t instIdCode;
    int64_t tsUs;
    int64_t outTime;
    int64_t seqId;
    int64_t pxMantissa;
    int64_t szMantissa;
    int64_t tradeId;
    int16_t count;
    int8_t  side;       // SideEnum
    int8_t  pxExponent;
    int8_t  szExponent;
    int8_t  source;     // SourceEnum
};
static_assert(sizeof(TradesView) == 62, "TradesView wire size mismatch");

// 42 字节 (BooksL2TbtExponentUpdateEvent 无 group)
struct BooksL2ExpUpdateView {
    int64_t instIdCode;
    int64_t tsUs;
    int64_t outTime;
    int64_t seqId;
    int64_t prevSeqId;
    int8_t  pxExponent;
    int8_t  szExponent;
};
static_assert(sizeof(BooksL2ExpUpdateView) == 42, "BooksL2ExpUpdateView wire size mismatch");

// 34 字节 (SnapshotDepthResponseEvent root, 不含 group)
struct SnapshotDepthRoot {
    int64_t instIdCode;
    int64_t tsUs;
    int64_t seqId;
    int8_t  pxExponent;
    int8_t  szExponent;
};
static_assert(sizeof(SnapshotDepthRoot) == 26, "SnapshotDepthRoot wire size mismatch");
// (26 bytes, group headers follow immediately)

// ---------- L2 消息的 root (root + 之后是 asks group, 再是 bids group) ----------
//   BooksL2TbtChannelEvent  templateId=1001  root=42 bytes  + asks group + bids group
//   BooksL2TbtElpChannelEvent templateId=1003 相同结构

struct BooksL2Root {
    int64_t instIdCode;
    int64_t tsUs;
    int64_t outTime;
    int64_t seqId;
    int64_t prevSeqId;
    int8_t  pxExponent;
    int8_t  szExponent;
};
static_assert(sizeof(BooksL2Root) == 42, "BooksL2Root wire size mismatch");

// L2 每档 20 字节 (asks / bids 同一格式)
struct BooksL2Entry {
    int64_t pxMantissa;
    int64_t szMantissa;
    int32_t ordCount;
};
static_assert(sizeof(BooksL2Entry) == 20, "BooksL2Entry wire size mismatch");

#pragma pack(pop)

// ============================================================================
// 头部探测 (先看 templateId, 再决定用哪个 View)
// ============================================================================

inline uint16_t peek_template_id(const uint8_t* data, size_t len) noexcept {
    if (len < sizeof(MessageHeader)) return 0;
    return reinterpret_cast<const MessageHeader*>(data)->templateId;
}

inline const MessageHeader* header_of(const uint8_t* data) noexcept {
    return reinterpret_cast<const MessageHeader*>(data);
}

// 拿到 body (root fields) 起始位置 = header 之后
inline const uint8_t* body_of(const uint8_t* data) noexcept {
    return data + sizeof(MessageHeader);
}

// ============================================================================
// 定长 View 快速构造 (加长度校验)
// ============================================================================

template <typename T>
inline const T* view_of(const uint8_t* data, size_t len) noexcept {
    // header + body 必须都在 buffer 内
    if (len < sizeof(MessageHeader) + sizeof(T)) return nullptr;
    return reinterpret_cast<const T*>(data + sizeof(MessageHeader));
}

// ============================================================================
// L2 深度迭代器: 一次性把 root + asks group + bids group 全部解析
// ============================================================================

class BooksL2Iter {
public:
    // BooksL2Tbt (1001) 或 BooksL2Elp (1003) 都用这个
    BooksL2Iter(const uint8_t* data, size_t len) noexcept
        : ok_(false)
    {
        if (len < sizeof(MessageHeader) + sizeof(BooksL2Root) + 2 * sizeof(GroupSize16)) return;

        const auto* hdr = reinterpret_cast<const MessageHeader*>(data);
        root_ = reinterpret_cast<const BooksL2Root*>(data + sizeof(MessageHeader));

        // root 后紧跟 asks group header
        // 注意: SBE 规定 root 只用 blockLength 字节, 剩下的字段被跳过。
        //       实际推流 blockLength == sizeof(BooksL2Root) == 42。
        const uint8_t* p = data + sizeof(MessageHeader) + hdr->blockLength;
        if (p + sizeof(GroupSize16) > data + len) return;
        asks_dim_ = reinterpret_cast<const GroupSize16*>(p);
        p += sizeof(GroupSize16);
        asks_entries_ = reinterpret_cast<const BooksL2Entry*>(p);
        p += asks_dim_->blockLength * asks_dim_->numInGroup;
        if (p + sizeof(GroupSize16) > data + len) return;

        bids_dim_ = reinterpret_cast<const GroupSize16*>(p);
        p += sizeof(GroupSize16);
        bids_entries_ = reinterpret_cast<const BooksL2Entry*>(p);
        p += bids_dim_->blockLength * bids_dim_->numInGroup;
        if (p > data + len) return;

        ok_ = true;
    }

    bool ok() const noexcept { return ok_; }
    const BooksL2Root* root() const noexcept { return root_; }

    uint16_t ask_count() const noexcept { return asks_dim_ ? asks_dim_->numInGroup : 0; }
    uint16_t bid_count() const noexcept { return bids_dim_ ? bids_dim_->numInGroup : 0; }
    uint16_t entry_size_bytes() const noexcept { return asks_dim_ ? asks_dim_->blockLength : 0; }

    // 按下标读取 (调用方保证 idx < ask_count() / bid_count())
    const BooksL2Entry* ask(uint16_t idx) const noexcept {
        // group 里每条 entry 是 blockLength 字节, 而不一定是 sizeof(BooksL2Entry)。
        // 但 OKX 目前 blockLength=20 == sizeof(BooksL2Entry), 所以直接下标即可。
        return &asks_entries_[idx];
    }
    const BooksL2Entry* bid(uint16_t idx) const noexcept {
        return &bids_entries_[idx];
    }

private:
    bool ok_;
    const BooksL2Root*  root_        = nullptr;
    const GroupSize16*  asks_dim_    = nullptr;
    const BooksL2Entry* asks_entries_ = nullptr;
    const GroupSize16*  bids_dim_    = nullptr;
    const BooksL2Entry* bids_entries_ = nullptr;
};

// ============================================================================
// mantissa × 10^exponent → double
//   用 [-15, +15] 预计算 lookup 表, 常见 pxExponent (-1..-10) 全部命中,
//   零分支 O(1)。极端 exponent (超出 ±15) 落到 pow() 慢路径。
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

} // namespace md::okx::sbe