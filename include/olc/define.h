//
// Created by qyang on 9/20/21.
//

#ifndef DB_DEFINE_H
#define DB_DEFINE_H

#if 0
#define Depth5_Format "{\"exchId\":\"%s\",\"instType\":\"%s\",\"marketType\":\"%s\",\"instId\":\"%s\"," \
"\"ap1\":%.12f,\"ap2\":%.12f,\"ap3\":%.12f,\"ap4\":%.12f,\"ap5\":%.12f,"\
"\"av1\":%.8f,\"av2\":%.8f,\"av3\":%.8f,\"av4\":%.8f,\"av5\":%.8f,"\
"\"bp1\":%.12f,\"bp2\":%.12f,\"bp3\":%.12f,\"bp4\":%.12f,\"bp5\":%.12f,"\
"\"bv1\":%.8f,\"bv2\":%.8f,\"bv3\":%.8f,\"bv4\":%.8f,\"bv5\":%.8f,"\
"\"ts\":%ld,\"tsNet\":%ld,\"tsParse\":%ld}"

#define Depth10_Format "{\"exchId\":\"%s\",\"instType\":\"%s\",\"marketType\":\"%s\",\"instId\":\"%s\"," \
"\"ap1\":%.12f,\"ap2\":%.12f,\"ap3\":%.12f,\"ap4\":%.12f,\"ap5\":%.12f,"\
"\"ap6\":%.12f,\"ap7\":%.12f,\"ap8\":%.12f,\"ap9\":%.12f,\"ap10\":%.12f,"\
"\"av1\":%.8f,\"av2\":%.8f,\"av3\":%.8f,\"av4\":%.8f,\"av5\":%.8f,"\
"\"av6\":%.8f,\"av7\":%.8f,\"av8\":%.8f,\"av9\":%.8f,\"av10\":%.8f,"\
"\"bp1\":%.12f,\"bp2\":%.12f,\"bp3\":%.12f,\"bp4\":%.12f,\"bp5\":%.12f,"\
"\"bp6\":%.12f,\"bp7\":%.12f,\"bp8\":%.12f,\"bp9\":%.12f,\"bp10\":%.12f,"\
"\"bv1\":%.8f,\"bv2\":%.8f,\"bv3\":%.8f,\"bv4\":%.8f,\"bv5\":%.8f,"\
"\"bv6\":%.8f,\"bv7\":%.8f,\"bv8\":%.8f,\"bv9\":%.8f,\"bv10\":%.8f,"\
"\"ts\":%ld,\"tsNet\":%ld,\"tsParse\":%ld}"

#define Depth20_Format "{\"exchId\":\"%s\",\"instType\":\"%s\",\"marketType\":\"%s\",\"instId\":\"%s\"," \
"\"ap1\":%.12f,\"ap2\":%.12f,\"ap3\":%.12f,\"ap4\":%.12f,\"ap5\":%.12f,"\
"\"ap6\":%.12f,\"ap7\":%.12f,\"ap8\":%.12f,\"ap9\":%.12f,\"ap10\":%.12f,"\
"\"ap11\":%.12f,\"ap12\":%.12f,\"ap13\":%.12f,\"ap14\":%.12f,\"ap15\":%.12f,"\
"\"ap16\":%.12f,\"ap17\":%.12f,\"ap18\":%.12f,\"ap19\":%.12f,\"ap20\":%.12f,"\
"\"av1\":%.8f,\"av2\":%.8f,\"av3\":%.8f,\"av4\":%.8f,\"av5\":%.8f,"\
"\"av6\":%.8f,\"av7\":%.8f,\"av8\":%.8f,\"av9\":%.8f,\"av10\":%.8f,"\
"\"av11\":%.8f,\"av12\":%.8f,\"av13\":%.8f,\"av14\":%.8f,\"av15\":%.8f,"\
"\"av16\":%.8f,\"av17\":%.8f,\"av18\":%.8f,\"av19\":%.8f,\"av20\":%.8f,"\
"\"bp1\":%.12f,\"bp2\":%.12f,\"bp3\":%.12f,\"bp4\":%.12f,\"bp5\":%.12f,"\
"\"bp6\":%.12f,\"bp7\":%.12f,\"bp8\":%.12f,\"bp9\":%.12f,\"bp10\":%.12f,"\
"\"bp11\":%.12f,\"bp12\":%.12f,\"bp13\":%.12f,\"bp14\":%.12f,\"bp15\":%.12f,"\
"\"bp16\":%.12f,\"bp17\":%.12f,\"bp18\":%.12f,\"bp19\":%.12f,\"bp20\":%.12f,"\
"\"bv1\":%.8f,\"bv2\":%.8f,\"bv3\":%.8f,\"bv4\":%.8f,\"bv5\":%.8f,"\
"\"bv6\":%.8f,\"bv7\":%.8f,\"bv8\":%.8f,\"bv9\":%.8f,\"bv10\":%.8f,"\
"\"bv11\":%.8f,\"bv12\":%.8f,\"bv13\":%.8f,\"bv14\":%.8f,\"bv15\":%.8f,"\
"\"bv16\":%.8f,\"bv17\":%.8f,\"bv18\":%.8f,\"bv19\":%.8f,\"bv20\":%.8f,"\
"\"ts\":%ld,\"tsNet\":%ld,\"tsParse\":%ld}"
#endif
//,\"raw_json\":%s

#define Bar_Format "{\"exchId\":\"%s\",\"instType\":\"%s\",\"marketType\":\"%s\",\"instId\":\"%s\"," \
"\"barTime\":%ld,\"highPrice\":%.12f,\"lowPrice\":%.12f,\"openPrice\":%.12f,\"closePrice\":%.12f, "  \
"\"avgPrice\":%.12f,\"totalVolume\":%.2f,\"totalAmount\":%.2f, "\
"\"takerLongVolume\":%.2f,\"takerLongAmount\":%.2f,\"takerShortVolume\":%.2f,\"takerShortAmount\":%.2f, "\
 "\"numOfTrade\":%d, \"isFinished\":\"%s\", "\
"\"ts\":%ld,\"tsNet\":%ld,\"tsParse\":%ld}"

#define Trades_Format "{\"exchId\":\"%s\",\"instType\":\"%s\",\"marketType\":\"%s\",\"instId\":\"%s\"," \
"\"tradeId\":\"%s\",\"px\":%.12f,\"sz\":%.8f,\"side\":\"%s\", "\
"\"ts\":%ld,\"tsNet\":%ld,\"tsParse\":%ld}"

#define Funding_Rate_Format "{\"exchId\":\"%s\",\"instType\":\"%s\",\"marketType\":\"%s\",\"instId\":\"%s\"," \
" \"fundingRate\":%.10f,\"nextFundingRate\":%.10f,\"fundingTime\":%ld, "\
"\"ts\":%ld,\"tsNet\":%ld,\"tsParse\":%ld}"

#define DEPTH_Format "{\"exchId\":\"%s\",\"instType\":\"%s\",\"marketType\":\"%s\",\"instId\":\"%s\"," \
" \"asksList\":%s,\"bidsList\":%s, "\
"\"ts\":%ld,\"tsNet\":%ld,\"tsParse\":%ld}"

#define Funding_Rate_Format_FMT "{{\"exchId\":\"{}\",\"instType\":\"{}\",\"marketType\":\"{}\",\"instId\":\"{}\"," \
"\"fundingRate\":\"{:.12f}\",\"nextFundingRate\":{:.12f},\"fundingTime\":{}, "\
"\"ts\":{},\"tsNet\":{},\"tsParse\":{} }}"

#define Trades_Format_FMT "{{\"exchId\":\"{}\",\"instType\":\"{}\",\"marketType\":\"{}\",\"instId\":\"{}\"," \
"\"tradeId\":\"{}\",\"px\":{:.12f},\"sz\":{:.8f},\"side\":\"{}\", "\
"\"ts\":{},\"tsNet\":{},\"tsParse\":{} }}"

#define DEPTH_Format_FMT "{{\"exchId\":\"{}\",\"instType\":\"{}\",\"marketType\":\"{}\",\"instId\":\"{}\"," \
" \"asksList\":{},\"bidsList\":{}, "\
"\"ts\":{},\"tsNet\":{},\"tsParse\":{} }}"

#define Bar_Format_FMT "{{\"exchId\":\"{}\",\"instType\":\"{}\",\"marketType\":\"{}\",\"instId\":\"{}\"," \
"\"barTime\":\"{}\",\"highPrice\":\"{:.12f}\",\"lowPrice\":\"{:.12f}\",\"openPrice\":\"{:.12f}\",\"closePrice\":\"{:.12f}\", "  \
"\"avgPrice\":\"{:.12f}\",\"totalVolume\":\"{:.12f}\",\"totalAmount\":\"{:.12f}\", "\
"\"takerLongVolume\":\"{:.12f}\",\"takerLongAmount\":\"{:.12f}\",\"takerShortVolume\":\"{:.12f}\",\"takerShortAmount\":\"{:.12f}\", "\
 "\"numOfTrade\":\"{}\", \"isFinished\":\"{}\", "\
"\"ts\":{},\"tsNet\":{},\"tsParse\":{} }}"

// #define STORE_REDIS_CHANNEL "STORE_REDIS_CHANNEL"
#endif //DB_DEFINE_H
