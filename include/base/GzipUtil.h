#pragma once
//
// GzipUtil.h —— gzip 解压
//
// 【实测 2026-10-17】HTX 与 BingX 的行情推送都是 **gzip 压缩的 binary frame**:
//   HTX  : wss://api.hbdm.com/linear-swap-ws   → 帧首字节 0x1f 0x8b
//   BingX: wss://open-api-swap.bingx.com/swap-market → 同上
// 其余四家 (Bitget/MEXC/KuCoin/XT) 是明文 text frame。
//
// ⚠️ 不要在 onWebsocketMsg 里直接 assume 是 text: BeastWsClient 会给 is_binary,
//    但 HTX 连订阅回执都是压缩的, 所以必须"先按 is_binary 判断, 再兜底试解压"。
//
// zlib 已经在 db/CMakeLists.txt 的 link_libraries 里 (z), 无需额外依赖。
//

#include <zlib.h>

#include <cstdint>
#include <cstring>
#include <string>

namespace mdutil {

// 解压 gzip / zlib 流到 out (out 先被 clear)。
// 返回 true = 解压成功; false = 这段数据不是 gzip/zlib, 调用方应回退成"原样当文本用"。
//
// 为什么不区分 gzip 与 zlib 头: inflateInit2 的 windowBits 用 15 + 32,
// zlib 会自己嗅探 gzip 头 (0x1f 0x8b) 还是 zlib 头 (0x78 ..)。
inline bool gunzip(const uint8_t* data, size_t len, std::string& out) {
    out.clear();
    if (data == nullptr || len < 2) {
        return false;
    }

    z_stream zs;
    std::memset(&zs, 0, sizeof(zs));
    if (inflateInit2(&zs, 15 + 32) != Z_OK) {
        return false;
    }

    zs.next_in  = const_cast<Bytef*>(data);
    zs.avail_in = static_cast<uInt>(len);

    char buf[8192];
    int  ret = Z_OK;
    do {
        zs.next_out  = reinterpret_cast<Bytef*>(buf);
        zs.avail_out = static_cast<uInt>(sizeof(buf));

        ret = inflate(&zs, Z_NO_FLUSH);
        if (ret != Z_OK && ret != Z_STREAM_END) {
            break;
        }
        out.append(buf, sizeof(buf) - zs.avail_out);

        // 解压炸弹保护: 单帧行情不可能超过 1 MiB (depth.step0 全量档也才 ~8 KiB)
        if (out.size() > (1u << 20)) {
            ret = Z_BUF_ERROR;
            break;
        }
    } while (ret != Z_STREAM_END && zs.avail_in > 0);

    inflateEnd(&zs);

    if (ret != Z_STREAM_END) {
        out.clear();
        return false;
    }
    return true;
}

} // namespace mdutil
