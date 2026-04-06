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


# 读取整个文件
file(READ /inc/version.inc FILE_CONTENT)

# 查找 COMPILE_VERSION= 之后的内容
string(FIND "${FILE_CONTENT}" "COMPILE_VERSION=" COMPILE_POS)
if(COMPILE_POS GREATER -1)
    math(EXPR VALUE_START "${COMPILE_POS} + 16")  # "COMPILE_VERSION=" 的长度是 16
    string(SUBSTRING "${FILE_CONTENT}" ${VALUE_START} -1 COMPILE_VERSION)
    
    # 去除换行符
    string(REGEX REPLACE "\n.*$" "" COMPILE_VERSION "${COMPILE_VERSION}")
    message("COMPILE_VERSION = ${COMPILE_VERSION}")
endif()
set(COMPILE_PATH ${COMPILE_VERSION})

set(3RD_PATH /opt/version/3rd)

#include
set(INC_DIR              ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(SYSTEM_INC_DIR          ${COMPILE_PATH}/include)
set(FMT_INCLUDE_DIR          ${3RD_PATH}/fmt/include)


#link
set(FMT_LIBRARY        ${3RD_PATH}/fmt/lib)



aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src       DIR_MAIN_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/base     DIR_BASE_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/operation  DIR_OPERATION_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/binance   DIR_BINANCE_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/gateio    DIR_GATEIO_SRCS)
# aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/okx     DIR_OKX_SRCS)
# aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/bybit    DIR_BYBIT_SRCS)



include_directories(
        ${3RD_PATH}
        ${INC_DIR}
        ${SYSTEM_INC_DIR}
        ${FMT_INCLUDE_DIR}
)

link_directories(
        ${FMT_LIBRARY}
)

link_libraries(
        pthread
        boost_system 
        crypto 
        ssl 
        cpprest 
        z 
        rt  
        dl 
        cpp_redis 
        tacopie 
        tbb 
        stdc++fs 
        fmt
        zmq
        uv
)

add_executable(DB
        ${DIR_MAIN_SRCS}
        ${DIR_BASE_SRCS}
        ${DIR_OPERATION_SRCS}
        ${DIR_BINANCE_SRCS}
        ${DIR_GATEIO_SRCS}
        # ${HUOBI_SRCS}
        # ${XT_SRCS}
        # ${KUCOIN_SRCS}
        # ${OKX_SRCS}
        # ${BYBIT_SRCS}
)

add_executable(db_depth1_shm_queue_reader test/db_depth1_shm_queue_reader.cpp)
add_executable(db_depth5_shm_queue_reader test/db_depth5_shm_queue_reader.cpp)
add_executable(db_depth10_shm_queue_reader test/db_depth10_shm_queue_reader.cpp)
add_executable(db_depth20_shm_queue_reader test/db_depth20_shm_queue_reader.cpp)
add_executable(db_trades_shm_queue_reader test/db_trades_shm_queue_reader.cpp)
add_executable(db_fundingrate_shm_queue_reader test/db_fundingrate_shm_queue_reader.cpp)
add_executable(db_kline_shm_queue_reader test/db_kline_shm_queue_reader.cpp)