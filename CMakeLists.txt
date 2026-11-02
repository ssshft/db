cmake_minimum_required(VERSION 3.1)
project(DB)

# 设置编译器 c++17 required
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_BUILD_TYPE "Release")
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -fPIC -Wall -g -ggdb")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -fPIC -DABI -DNDEBUG -DOPENBLAS  -O3 -Wall -flto -fuse-linker-plugin -minline-all-stringops -malign-double -ffast-math -march=native -ffunction-sections -fdata-sections -fmessage-length=0 -msse -msse2 -msse3 -funroll-loops")

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/bin)


add_definitions("-DNEED_SHM")
add_definitions("-DNEED_SET_REDIS")
add_definitions("-D_TURN_OFF_PLATFORM_STRING")
add_definitions(-DUSE_INFO_SHM)

# 延时统计（include/hft_metrics.h）的编译期开关。**默认关**。
# 头文件里也兜底成 0，所以这里的显式写法主要是让开关在构建配置里可见、可 grep。
#   cmake .. && make            → 关（宏展开成空，热路径零残留、不建 metrics 文件）
#   cmake -DHFT_METRICS=ON ..   → 开（实盘要量耗时就打开，见下面的开销说明）
#
# 实盘开着的代价（arm64 实测，x86_64 同一量级）：每次 HFT_SCOPE ≈ 35 ns（两次读钟），
# 一次 db.parse 埋点 = 每帧行情 +35 ns；100 万帧/秒 = 35 ms/秒，摊到 17 个 unit 线程上
# 约 0.2% 单核。热路径无锁、无 malloc、无系统调用、无额外线程。
# 内存：每线程 24 KB 分片 + 每指标 2 KB 直方图 ⇒ 17 线程 ≈ 440 KB。
option(HFT_METRICS "编译延时统计埋点（hft_metrics.h）" OFF)
if(HFT_METRICS)
    add_definitions(-DHFT_METRICS_ENABLED=1)
else()
    add_definitions(-DHFT_METRICS_ENABLED=0)
endif()


#include
set(INC_DIR              ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(SYSTEM_INC_DIR          ../include)

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src       DIR_MAIN_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/base     DIR_BASE_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/operation  DIR_OPERATION_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/binance   DIR_BINANCE_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/gateio    DIR_GATEIO_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/okx     DIR_OKX_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/bybit    DIR_BYBIT_SRCS)
# 2026-10-17 新增六家（只做永续：DEPTH1 / TRADES / FUNDING_RATE）
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/htx      DIR_HTX_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/bitget   DIR_BITGET_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/mexc     DIR_MEXC_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/kucoin   DIR_KUCOIN_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/bingx    DIR_BINGX_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/xt       DIR_XT_SRCS)
# 2026-10-20 新增七家（只做永续：DEPTH1 / TRADES / FUNDING_RATE）
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/bitunix  DIR_BITUNIX_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/phemex   DIR_PHEMEX_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/weex     DIR_WEEX_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/hotcoin  DIR_HOTCOIN_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/echobit  DIR_ECHOBIT_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/mgbx     DIR_MGBX_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/blofin   DIR_BLOFIN_SRCS)



include_directories(
        ${INC_DIR}
        ${SYSTEM_INC_DIR}
)

link_libraries(
        pthread
        boost_system 
        crypto 
        ssl 
        z 
        rt  
        dl 
        cpp_redis 
        tacopie 
        tbb 
        stdc++fs 
        fmt
        simdjson
)

add_executable(DB
        ${DIR_MAIN_SRCS}
        ${DIR_BASE_SRCS}
        ${DIR_OPERATION_SRCS}
        ${DIR_BINANCE_SRCS}
        ${DIR_GATEIO_SRCS}
        ${DIR_OKX_SRCS}
        ${DIR_BYBIT_SRCS}
        ${DIR_HTX_SRCS}
        ${DIR_BITGET_SRCS}
        ${DIR_MEXC_SRCS}
        ${DIR_KUCOIN_SRCS}
        ${DIR_BINGX_SRCS}
        ${DIR_XT_SRCS}
        ${DIR_BITUNIX_SRCS}
        ${DIR_PHEMEX_SRCS}
        ${DIR_WEEX_SRCS}
        ${DIR_HOTCOIN_SRCS}
        ${DIR_ECHOBIT_SRCS}
        ${DIR_MGBX_SRCS}
        ${DIR_BLOFIN_SRCS}
)

add_executable(db_depth1_shm_queue_reader test/db_depth1_shm_queue_reader.cpp)
add_executable(db_depth5_shm_queue_reader test/db_depth5_shm_queue_reader.cpp)
add_executable(db_depth10_shm_queue_reader test/db_depth10_shm_queue_reader.cpp)
add_executable(db_depth20_shm_queue_reader test/db_depth20_shm_queue_reader.cpp)
add_executable(db_trades_shm_queue_reader test/db_trades_shm_queue_reader.cpp)
add_executable(db_fundingrate_shm_queue_reader test/db_fundingrate_shm_queue_reader.cpp)
add_executable(db_kline_shm_queue_reader test/db_kline_shm_queue_reader.cpp)


add_executable(db_shm_latency tools/db_shm_latency.cpp)
target_include_directories(db_shm_latency PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tools)

add_executable(db_shm_perf tools/db_shm_perf.cpp)
target_include_directories(db_shm_perf PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tools)