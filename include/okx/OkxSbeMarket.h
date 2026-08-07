#pragma once
#include "base/BaseMarket.h"
#include "okx/OkxSbeDecoder.h"

// ============================================================================
// OKX SBE Market Client
//
// 走 OKX 的二进制 SBE 行情通道 (schema=new_dev/okx_sbe_1_0.xml):
//   - WS 帧全部是 binary (opcode=BINARY), 内容 = MessageHeader(8B) + body(+group)
//   - SBE 帧的 instrument 标识是 int64 instIdCode (不是 instId 字符串)
//   - 客户端必须在启动时通过 REST 拉 /api/v5/public/instruments 建立
//     instIdCode → InstrumentInfo 的映射表, 否则收到 SBE 帧无法定位
//
// PING/PONG:
//   OKX 服务端 30s 无消息自动断连, 客户端需每 <30s 发一次 text "ping",
//   server 回 text "pong" (我们在 onWebsocketMsg 里过滤掉 pong, 避免解码器处理它)
//
// 订阅格式 (仍是 text 帧, 与 JSON 通道相同, 只是 channel 名走 SBE 版本):
//   { "op":"subscribe",
//     "args":[
//       {"channel":"bbo-tbt",       "instId":"BTC-USDT"},
//       {"channel":"books-l2-tbt",  "instId":"BTC-USDT"},
//       {"channel":"trades",        "instId":"BTC-USDT"}
//     ]
//   }
//   (OKX 的 SBE 通道对客户端而言是"服务端选择二进制推送格式", 订阅体本身仍是 JSON,
//    只是 endpoint 走 /ws/v5/ip/sbe 而不是 /ws/v5/public。)
// ============================================================================


#ifdef NEED_TESTNET

// SBE 模拟盘 endpoint
constexpr auto OKX_WS_SBE_PUBLIC = "wss://wspap.okx.com:8443/ws/v5/ip/sbe";
constexpr auto OKX_REST_HOST     = "wspap.okx.com";

#else

// SBE 实盘 endpoint (专用二进制推送通道)
constexpr auto OKX_WS_SBE_PUBLIC = "wss://ws.okx.com:8443/ws/v5/ip/sbe";
constexpr auto OKX_REST_HOST     = "www.okx.com";

#endif

constexpr auto OKX_REST_PORT     = "443";


namespace md {

    class OkxSbeUnit : public BaseUnit {
    public:
        OkxSbeUnit(sm::SecurityManager* s, ExchangeType exchTy, InstType instTy, md::MarketType marketTy, std::vector<md::InstrumentInfo>& instInfoVec, SbeAccount sbeAcc, const char* host = "127.0.0.1", int port = 9379, const char* passwd = "");

        // 组装 cfg.url / cfg.subscribe_messages / cfg.ping_mode。
        // 同时通过 REST 拉一次 /api/v5/public/instruments 建立
        //   instIdCode → InstrumentInfo 的映射, 存在 mCodeToInfo。
        virtual void generateSubBody();

        // BeastWsClient 回调:
        //   - isBinary=true : SBE 帧, 原样 memcpy 到 std::string 送 mQueue
        //   - isBinary=false: text 帧 (登录/订阅回执 / server "pong"), 不入队
        virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t ns);

        // consume 线程调, 从 mQueue 里取出 raw bytes, 走 OkxSbeDecoder 解码。
        virtual void parseMarketData(const std::string& msg);

    private:
        // 启动时 REST 拉产品列表, 填充 mCodeToInfo (int64 instIdCode → InstrumentInfo)
        // 只关心 vInstInfo 里出现过的 originInstId, 减少无效 entry。
        void buildCodeIndexFromSm();

        // 拼一条 {"op":"subscribe","args":[...]} 字符串, 塞入 cfg.subscribe_messages。
        void buildSubscribeJson();

        std::string buildLoginJson() const;

        // 每个 vInstInfo 里的 originInstId, 对应的 SBE channel 名 (根据 marketTypeEnum 决定)
        const char* channelForMarketType() const;

        // -- SBE decode 分支 (只解 templateId 1000/1001/1005; 1002/1003/1004/1006 略过或后续扩展) --
        void handleBboTbt(const uint8_t* data, size_t len, long tsNet);
        void handleBooksL2(const uint8_t* data, size_t len, long tsNet);   // 1001 (asks+bids 全量)
        void handleTrades(const uint8_t* data, size_t len, long tsNet);
        void handleExpUpdate(const uint8_t* data, size_t len);             // 1002: 更新 exponent 表

        // 通过 instIdCode 反查 InstrumentInfo, 找不到返回 false
        bool lookupByCode(int64_t code, md::InstrumentInfo& out) const;

    private:
        SbeAccount sbeAccount; 
        std::vector<std::string> subArgs;

        // instIdCode → InstrumentInfo, 启动时 REST 拉一次填满, 之后 read-only。
        // 只放 vInstInfo 里配置的 instId, 避免 O(百万) 级 map。
        std::unordered_map<int64_t, md::InstrumentInfo> mCodeToInfo;

        // 保底 exponent 表: OKX 允许通过 1002/1004 (BooksL2TbtExponentUpdateEvent) 更新 exponent。
        // 大部分场景 pxExponent 就在每条数据里, 我们直接用消息自带的, 这里保留 map 供 1002 更新记录。
        // key = instIdCode, value = {pxExponent, szExponent}
        struct ExpPair {
            int8_t pxExp{0};
            int8_t szExp{0};
        };
        std::unordered_map<int64_t, ExpPair> mLatestExponent;
    };


    class OkxSbeMarket : public BaseMarket {
    public:
        OkxSbeMarket(sm::SecurityManager* s, const char* exId, std::vector<std::string>& instTypeVec, std::vector<std::string>& marketTypeVec, std::vector<std::string>& instIdVec, SbeAccount sbeAccount, int lot = 30, const char* host = "127.0.0.1", const int port = 9379, const char* passwd = "");
        ~OkxSbeMarket();
        virtual void start();

    private:
        std::vector<OkxSbeUnit*> okxSbeUnitVec;
    };

} // namespace md