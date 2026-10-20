#pragma once
//
// TopBook.h —— 增量深度流用的"最小订单簿"（只维护最优买卖价）
//
// 【为什么需要它】
//   新接入的几家交易所里，Phemex / WEEX / MGBX 的深度频道都是**增量**的：
//   每帧只推"变化的档位"，不给全量快照（或只每 2 s 给一次全量）。
//     Phemex  orderbook_p : 首帧全量，之后 type="incremental"，qty=0 表示删档
//     WEEX    depth15     : d="SNAPSHOT" 全量 / d="CHANGED" 增量，qty=0 表示删档
//     MGBX    push.deep   : 单档 {ba:1|2, p, q}，qty=0 表示删档
//   如果只取"每帧第一条"，价格会永远停在某个**已经被吃掉的档位**上 ——
//   而且不报错、不断流，下游看到的是一份"看起来正常但其实过期"的盘口。
//   这是本项目最怕的静默失败类型，所以这里老老实实维护一本小簿子。
//
// 【用法】
//   md::TopBook book;
//   book.applyBid(px, sz);            // sz <= 0 等价于删除该档
//   book.applyAsk(px, sz);
//   double bp, bv, ap, av;
//   if (book.best(bp, bv, ap, av)) { /* 两边都非空才返回 true */ }
//   book.reset();                     // 收到全量快照前 / 重连后清空
//
// 【代价】
//   每个档位一次 std::map 插入/删除，O(log n)，n ≤ kMaxLevels(256)。
//   实测 MGBX 单 symbol 增量约 17 帧/s，一次 1~6 档 ⇒ 每帧几十纳秒，
//   相对于"整帧 JSON 解析"可以忽略。
//
// 【注意】
//   ① 只在**同一个线程**（BaseUnit::consume 的解析线程）里用，不加锁。
//   ② 不清洗 NaN：调用方必须保证 px/sz 是有限值（科学计数法要用 safe_atod）。
//

#include <cmath>
#include <functional>   // std::greater
#include <iterator>     // std::prev
#include <map>

namespace md {

class TopBook {
public:
    // 单边最多保留多少档。设上限不是为了省内存（256 档 ≈ 12 KB），
    // 而是防止某个交易所推了"永不删除"的档位把簿子撑成无界容器。
    static constexpr size_t kMaxLevels = 256;

    void reset() {
        bids_.clear();
        asks_.clear();
    }

    // 应用一条买档变更；sz <= 0 表示该档被吃掉 / 被撤掉
    void applyBid(double px, double sz) { apply(bids_, px, sz); }
    void applyAsk(double px, double sz) { apply(asks_, px, sz); }

    bool hasBid() const { return !bids_.empty(); }
    bool hasAsk() const { return !asks_.empty(); }

    // 两边都有档位才返回 true。bp/bv/ap/av 可传 nullptr（只关心价格时）。
    bool best(double* bp, double* bv, double* ap, double* av) const {
        if (bids_.empty() || asks_.empty()) {
            return false;
        }
        const auto& b = *bids_.begin();   // greater ⇒ 首元素是最高买价
        const auto& a = *asks_.begin();   // 首元素是最低卖价
        if (bp) *bp = b.first;
        if (bv) *bv = b.second;
        if (ap) *ap = a.first;
        if (av) *av = a.second;
        return b.first > 0 && a.first > 0;
    }

private:
    // 买档用 greater：begin() 就是最高价；卖档用默认 less：begin() 就是最低价
    using BidMap = std::map<double, double, std::greater<double>>;
    using AskMap = std::map<double, double>;

    template <typename Map>
    static void apply(Map& m, double px, double sz) {
        if (!std::isfinite(px) || px <= 0) {
            return;                                  // 脏数据直接丢，别污染簿子
        }
        if (!std::isfinite(sz) || sz <= 0) {
            m.erase(px);                             // 删档
            return;
        }
        auto it = m.find(px);
        if (it != m.end()) {
            it->second = sz;                         // 改量（绝大多数增量帧走这里）
            return;
        }
        m.emplace(px, sz);
        if (m.size() > kMaxLevels) {
            m.erase(std::prev(m.end()));             // 砍掉最差的一档（map 有序，末尾最远）
        }
    }

    BidMap bids_;
    AskMap asks_;
};

} // namespace md
