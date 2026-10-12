#pragma once

#include <set>
#include <string>
#include <vector>
#include <map>
#include <thread>
#include <chrono>
#include <algorithm>

#include "redis_client.h"
#include "securitymanager.h"
#include "log_engine.h"
#include "concurrent_queue.h"
#include "shm_spmc_queue.h"

#include "shm_global.h"
#include "time_util.h"

#include "crypto_exception.h"
#include "precision_util.h"

using Data = md::Depth5;

int main(int argc, char* argv[]) {
    std::vector<std::shared_ptr<Depth5Spmc>> subers;
    for (int i = 1; i < argc; ++i) {
        const char* topic = argv[i];
        std::cout << topic << std::endl;
        if (crypto::has_str(topic, "DEPTH5")) {
            std::shared_ptr<Depth5Spmc> suber = std::make_shared<Depth5Spmc>(topic);
            subers.push_back(suber);
        }
        else {
            std::cout << "topic not correct! " << topic << std::endl;
            exit(-1);
        }
    }

    Data cmd;
    while (1) {
        for (auto& suber : subers) {
            if (suber->tryPopLatest(cmd)) {
                std::cout << cmd.getString() << std::endl;
            }
        }
    }
}