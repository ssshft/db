
#ifndef CPPSDK_UTILS_H
#define CPPSDK_UTILS_H

#include <ctime>
#include <string>
#include <map>
#include <zlib.h>
#include <sys/time.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
//char * GetTimestamp(char *timestamp, int len);
//std::string BuildParams(std::string requestPath, std::map<std::string,std::string> m);
//std::string JsonFormat(std::string jsonStr);
inline int gzDecompress(const char *src, int srcLen, const char *dst, int dstLen)
{
    z_stream strm;
    strm.zalloc = NULL;
    strm.zfree = NULL;
    strm.opaque = NULL;

    strm.avail_in = srcLen;
    strm.avail_out = dstLen;
    strm.next_in = (Bytef *) src;
    strm.next_out = (Bytef *) dst;

    int err = -1;//, ret = -1;
    err = inflateInit2(&strm, MAX_WBITS + 16);
    if (err == Z_OK) {
        err = inflate(&strm, Z_FINISH);
        if (err == Z_STREAM_END) {
            //ret = strm.total_out;
        } else {
            inflateEnd(&strm);
            return err;
        }
    } else {
        inflateEnd(&strm);
        return err;
    }
    inflateEnd(&strm);
    return err;
}
inline int gzDecompress(Byte *zdata, uLong nzdata, Byte *data, uLong *ndata)
{
    int err = 0;
    z_stream d_stream = {0}; /* decompression stream */

    static char dummy_head[2] = {
            0x8 + 0x7 * 0x10,
            (((0x8 + 0x7 * 0x10) * 0x100 + 30) / 31 * 31) & 0xFF,
    };

    d_stream.zalloc = NULL;
    d_stream.zfree = NULL;
    d_stream.opaque = NULL;
    d_stream.next_in = zdata;
    d_stream.avail_in = 0;
    d_stream.next_out = data;


    if (inflateInit2(&d_stream, -MAX_WBITS) != Z_OK) {
        return -1;
    }
    // if(inflateInit2(&d_stream, 47) != Z_OK) return -1;
    while (d_stream.total_out < *ndata && d_stream.total_in < nzdata) {
        d_stream.avail_in = d_stream.avail_out = 1; /* force small buffers */
        if((err = inflate(&d_stream, Z_NO_FLUSH)) == Z_STREAM_END)
            break;

        if (err != Z_OK) {
            if (err == Z_DATA_ERROR) {
                d_stream.next_in = (Bytef*) dummy_head;
                d_stream.avail_in = sizeof(dummy_head);
                if((err = inflate(&d_stream, Z_NO_FLUSH)) != Z_OK) {
                    return -1;
                }
            } else {
                return -1;
            }
        }
    }

    if (inflateEnd(&d_stream)!= Z_OK)
        return -1;
    *ndata = d_stream.total_out;
    return 0;
}
//int gzDecompress(const char *src, int srcLen, const char *dst, int dstLen);
//std::string GetSign(std::string key, std::string timestamp, std::string method, std::string requestPath, std::string body);
//unsigned int str_hex(unsigned char *str,unsigned char *hex);
//void hex_str(unsigned char *inchar, unsigned int len, unsigned char *outtxt);
inline long getCurrentTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);    //该函数在sys/time.h头文件中
    return tv.tv_sec * 1000000 + tv.tv_usec ;
}
//std::string JsonToString(const rapidjson::Value& valObj);
//{
//    rapidjson::StringBuffer sbBuf;
//    rapidjson::Writer<rapidjson::StringBuffer> jWriter(sbBuf);
//    valObj.Accept(jWriter);
//    return std::string(sbBuf.GetString());
//}
//{
//    struct timeval tv;
//    gettimeofday(&tv, NULL);    //该函数在sys/time.h头文件中
//    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
//}
#endif //CPPSDK_UTILS_H
