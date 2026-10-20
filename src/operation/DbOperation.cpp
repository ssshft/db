#include "operation/DbOperation.h"
#include "binance/BinanceMarket.h"
#include "binance/BinanceSbeMarket.h"
#include "gateio/GateioMarket.h"
#include "gateio/GateioSbeMarket.h"
#include "okx/OkxMarket.h"
#include "okx/OkxSbeMarket.h"
#include "bybit/BybitMarket.h"
#include "htx/HtxMarket.h"
#include "bitget/BitgetMarket.h"
#include "mexc/MexcMarket.h"
#include "kucoin/KucoinMarket.h"
#include "bingx/BingxMarket.h"
#include "xt/XtMarket.h"
// 2026-10-20 新增七家（只做永续：DEPTH1 / TRADES / FUNDING_RATE）
// 第八家 FAMEEX 暂未实现，原因见 preStart 末尾的 LOG_WARN
#include "bitunix/BitunixMarket.h"
#include "phemex/PhemexMarket.h"
#include "weex/WeexMarket.h"
#include "hotcoin/HotcoinMarket.h"
#include "echobit/EchobitMarket.h"
#include "mgbx/MgbxMarket.h"
#include "blofin/BlofinMarket.h"


DbOperation::DbOperation() {
    redisClient = nullptr;
}

DbOperation::~DbOperation() {
    if (smc) {
        delete smc;
        smc = nullptr;
    }
}

bool DbOperation::preStart(Config* config) {
    std::string host = "";
    std::string port = "";
    std::string password = "";

    if (config->get_redis_config("host", host) && config->get_redis_config("port", port) && config->get_redis_config("password", password)) {
        smc = new sm::SecurityManager(host.c_str(), std::stoi(port), password.c_str(), true);
    }

    std::unordered_map<std::string, ExchangeNode> mExchange;
    config->get_exchange_md_info(mExchange);
    
    for (auto iter = mExchange.begin(); iter != mExchange.end(); ++iter) {
        std::string exchId = iter->first;
        auto& node = iter->second;

        std::cout << "exchId: " << exchId << std::endl;

        if (crypto::str_cmp(exchId.c_str(), "BINANCE")) {
            mMarket[exchId] = new md::BinanceMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "BINANCE_SBE")) {
            mMarket[exchId] = new md::BinanceSbeMarket(smc, "BINANCE", node.instType, node.marketType, node.instId, node.sbeAccount, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "GATEIO")) {
            mMarket[exchId] = new md::GateioMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "GATEIO_SBE")) {
            mMarket[exchId] = new md::GateioSbeMarket(smc, "GATEIO", node.instType, node.marketType, node.instId, node.sbeAccount, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "OKX")) {
            mMarket[exchId] = new md::OkxMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "OKX_SBE")) {
            mMarket[exchId] = new md::OkxSbeMarket(smc, "OKX", node.instType, node.marketType, node.instId, node.sbeAccount, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "BYBIT")) {
            mMarket[exchId] = new md::BybitMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "HTX")) {
            mMarket[exchId] = new md::HtxMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "BITGET")) {
            mMarket[exchId] = new md::BitgetMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "MEXC")) {
            mMarket[exchId] = new md::MexcMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "KUCOIN")) {
            mMarket[exchId] = new md::KucoinMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "BINGX")) {
            mMarket[exchId] = new md::BingxMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "XT")) {
            mMarket[exchId] = new md::XtMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "BITUNIX")) {
            mMarket[exchId] = new md::BitunixMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "PHEMEX")) {
            mMarket[exchId] = new md::PhemexMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "WEEX")) {
            mMarket[exchId] = new md::WeexMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "HOTCOIN")) {
            mMarket[exchId] = new md::HotcoinMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "ECHOBIT")) {
            mMarket[exchId] = new md::EchobitMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "MGBX")) {
            mMarket[exchId] = new md::MgbxMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "BLOFIN")) {
            mMarket[exchId] = new md::BlofinMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "FAMEEX")) {
            // FAMEEX 的公开 WS 协议**查不到**：官网 API 文档是 SPA、抓不到正文，
            // 其前端 bundle 里也没有 wss:// 地址；REST 侧 /fapi/v1/depth 对
            // E-BTC-USDT / BTCUSDT / btcusdt 全部返回 "无效的合约"。
            // 与其按猜测写一套必然静默零数据的代码，不如在这里明确报出来。
            LOG_ERROR("DB does not support FAMEEX yet: no public WS protocol could be verified "
                      "(see docs/db-新增七家交易所.md). exchangeId: {}", exchId);
        }
        else {
            // 原来没有 else：配置里写错一个字母（或写了还没实现的交易所）就**静默不启动**，
            // 进程照跑、日志干净、一条行情都没有 —— 最费时间的排查。
            LOG_WARN("DB does not support exchangeId: {}, skipped (check config key spelling)", exchId);
        }
    }


    // 原来判的是 mExchange.size()（配置里解析到的条目数）—— 全是拼错/未实现的交易所时
    // 它仍然 > 0，进程照常启动、一个 unit 都没有。改成判真正建出来的 market 数。
    LOG_INFO("DB preStart done: config exchanges = {}, markets created = {}", mExchange.size(), mMarket.size());
    if (mMarket.size() > 0) {
        return true;
    }

    return false;
}

void DbOperation::run() {
    LOG_INFO("DbOperation start run!");
    for (auto iter = mMarket.begin(); iter != mMarket.end(); ++iter) {
        iter->second->start();
    }
}