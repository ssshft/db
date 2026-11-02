#include <iostream>
#include <csignal>
#include <cstdio>
#include "log_engine.h"
#include "program_util.h"
#include "config.h"
#include "crypto_exception.h"
#include "operation/DbOperation.h"
#include "hft_metrics.h"



static void signal_handler(int signum) {
    LOG_INFO("handler interrupt signal {} received, DB will exit now.", signum);
    fprintf(stdout, "handler interrupt signal (%d) received.\n", signum);
    fmtlog::poll();
    sleep(0.5);//wait logger write data to disk
    exit(signum);
}

static void usage(void){
    fprintf(stderr, "\nusage:\n");
    fprintf(stderr, "./application json_config_file \n");
    fprintf(stderr, "\n");
    exit(-1);
}

int main(int argc, char* argv[]) {
    if ( argc != 2 ){
        usage();
    }

    signal(SIGINT,  signal_handler);
    signal(SIGTERM, signal_handler);

    Config* config = Config::instance();
    config->load(argv[1]);

    std::string tag = "";
    std::string logPath = "";
    std::string logLevel = "";

    if (config->get_string("tag", tag) && config->get_log_config("log_path", logPath) && config->get_log_config("level", logLevel)) {
        if (crypto::create_directory(logPath)) {
            log_maintain(tag, logPath, logLevel);
        }
        else {
            exit(-1);
        }
    }
    else {
        fprintf(stderr, "not found log config info in %s,\nneed to add log config\n", argv[1]);
        exit(-1);
    }

    std::string program = tag;
    long currentPid = getpid();
    long filePid = crypto::get_program_pid(program);
    if(!crypto::ensure_one_instance(program) && currentPid != filePid) {
        std::string errormsg = tag + " with pid=" + std::to_string(filePid) + " already exists, aborted";
        cryptothrow(errormsg.c_str(), -1);
    }
    crypto::write_program_pid(program);

    // 时钟校准 + 把主线程的分片/直方图先分配掉（起工作线程之前）。
    // 不调也不会错，只是第一次埋点会多做一次 malloc。
#if HFT_METRICS_ENABLED
    hft::metrics::warmup();
#endif

    DbOperation* op = new DbOperation();
    if(op->preStart(config)) {
        op->run();
    }
    else {
        cryptothrow("DB start failed!", -1);
    }

    // ------------------------------------------------------------------
    // 延时统计输出（默认关：要开就 cmake -DHFT_METRICS=ON）
    // ------------------------------------------------------------------
#if HFT_METRICS_ENABLED
    // 落地策略（TextFileSink）：
    //   ① 按天命名 metrics_YYYYMMDD.log —— 和运维现有的 {tag}_{YYYYMMDD}.log 同一套清理规则；
    //   ② 超过 8 MB 就地滚成 .1（只留一份）⇒ 单文件、整个目录都有上限，跑一年也不涨；
    //   ③ 列头只在每个新文件的第一块写一次，之后只写 "# 时间戳" + 数据行。
    // 周期 10 s：看 p99 有没有劣化够用了（1 s 一块 ≈ 21 MB/天，10 s ≈ 2 MB/天）。
    // 真要排查再临时改回 1 s。
    hft::metrics::TextFileSink metricsSink(logPath, "metrics", 8u << 20);
    hft::metrics::Poller metricsPoller(
        10000000000LL, [&metricsSink](const hft::metrics::Snapshot& s) {
            if (!metricsSink.write(s)) {
                // 目录没了 / 没权限 / 磁盘满 ⇒ 退到 stderr。
                std::fputs(s.to_text().c_str(), stderr);
            }
        });
#else
    // 默认关，而"关了"在运行期是没有痕迹的（没有文件、没有日志、没有指标）。
    // 那种沉默最坑人 —— 盯着 log 目录找不到 metrics 文件时会以为是坏了。
    // 所以在启动日志里喊一句，把"怎么打开"直接写出来。
    LOG_WARN("latency metrics disabled at compile time (-DHFT_METRICS=OFF); no metrics file will be written. rebuild with -DHFT_METRICS=ON to enable");
#endif

    while(1) {
        log_maintain(tag, logPath, logLevel);
#if HFT_METRICS_ENABLED
        metricsPoller.poll();   // 0 参 = 自己读时钟
#endif
        usleep(1000);
    }

    return 0;
}