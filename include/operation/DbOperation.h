#pragma once
#include "base/BaseMarket.h"
#include "securitymanager.h"
#include "config.h"
#include <unordered_map>

class DbOperation {
public:
    DbOperation();
    ~DbOperation();
    bool preStart(Config* config);
    void run();

private:
    sm::SecurityManager* smc;
    RedisClient* redisClient;
    std::unordered_map<std::string, md::BaseMarket*> mMarket;
};