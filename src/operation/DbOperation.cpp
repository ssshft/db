#include "operation/DbOperation.h"
#include "binance/BinanceMarket.h"
#include "gateio/GateioMarket.h"


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

        if (crypto::str_cmp(exchId.c_str(), "BINANCE")) {
            mMarket[exchId] = new md::BinanceMarket(smc, node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }
        else if (crypto::str_cmp(exchId.c_str(), "GATEIO")) {
            mMarket[exchId] = new md::GateioMarket(smc, node.instType, node.marketType, node.instId, node.tokenLot, host.c_str(), std::stoi(port), password.c_str());
        }


        // else if (crypto::str_cmp(exchId.c_str(), "OKX")) {
        //     mExchInfo[exchId] = new OkxMarket(redisClient, config);
        // }
        // else if (crypto::str_cmp(exchId.c_str(), "BYBIT")) {
        //     mExchInfo[exchId] = new BybitMarket(redisClient, config);
        // }
        // else if (crypto::str_cmp(exchId.c_str(), "GATEIO")) {
        //     mExchInfo[exchId] = new GateioMarket(redisClient, config);
        // }   
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