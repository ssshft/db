#include "operation/DbOperation.h"
#include "binance/BinanceMarket.h"
#include "binance/BinanceSbeMarket.h"
#include "gateio/GateioMarket.h"
#include "gateio/GateioSbeMarket.h"
#include "okx/OkxMarket.h"
#include "okx/OkxSbeMarket.h"
#include "bybit/BybitMarket.h"


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
            mMarket[exchId] = new md::BinanceSbeMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.sbeAccount, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "GATEIO")) {
            mMarket[exchId] = new md::GateioMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "GATEIO_SBE")) {
            mMarket[exchId] = new md::GateioSbeMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.sbeAccount, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "OKX")) {
            mMarket[exchId] = new md::OkxMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "OKX_SBE")) {
            mMarket[exchId] = new md::OkxSbeMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.sbeAccount, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "BYBIT")) {
            mMarket[exchId] = new md::BybitMarket(smc, exchId.c_str(), node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
    }


    if (mExchange.size() > 0) {
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