# ===================================================== #
# from https://github.com/DominikHorn/hashing-benchmark #
# ===================================================== #

cmake_minimum_required(VERSION 3.19)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -march=native -fopenmp -std=c++20")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -fsanitize=address,leak,undefined")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

# ==== Non compatible compile flags ====
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-static-libsan HAS_LIBSAN)
if (HAS_LIBSAN)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -static-libsan")
else()
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -static-libasan")
endif()

# ==== General Dependencies ====
include(${PROJECT_SOURCE_DIR}/thirdparty/hashing.cmake)
include(${PROJECT_SOURCE_DIR}/thirdparty/learned_hashing.cmake)
include(${PROJECT_SOURCE_DIR}/thirdparty/exotic_hashing.cmake)
include(${PROJECT_SOURCE_DIR}/thirdparty/hashtable.cmake)
include(${PROJECT_SOURCE_DIR}/thirdparty/json.cmake)

# ==== Benchmark executable ====
add_executable(benchmarks datasets.cpp benchmarks.cpp)
add_executable(perf_bm datasets.cpp perf_bm.cpp)
add_executable(coroutines datasets.cpp coroutines.cpp)

target_link_libraries(benchmarks PRIVATE ${PROJECT_NAME} ${HASHING_LIBRARY} ${LEARNED_HASHING_LIBRARY} ${EXOTIC_HASHING_LIBRARY} ${HASHTABLE_LIBRARY} nlohmann_json::nlohmann_json)
target_link_libraries(perf_bm PRIVATE ${PROJECT_NAME} ${HASHING_LIBRARY} ${LEARNED_HASHING_LIBRARY} ${EXOTIC_HASHING_LIBRARY} ${HASHTABLE_LIBRARY} nlohmann_json::nlohmann_json)
target_link_libraries(coroutines PRIVATE ${PROJECT_NAME} ${HASHING_LIBRARY} ${LEARNED_HASHING_LIBRARY} ${EXOTIC_HASHING_LIBRARY} ${HASHTABLE_LIBRARY} nlohmann_json::nlohmann_json)
