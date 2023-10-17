#!/bin/bash

# ===================================================== #
# from https://github.com/DominikHorn/hashing-benchmark #
# ===================================================== #

# Setup script
cd "$(dirname "$0")"
source .env
set -e

# Parse arguments
TARGET=${1:-"benchmarks perf_bm"}
BUILD_TYPE=${2:-"RELEASE"}
BUILD_DIR="cmake-build-$(echo "${BUILD_TYPE}" | awk '{print tolower($0)}')/"

# check in which branch are we
if [ -d "${BUILD_DIR}" ] && [ -e "${BUILD_DIR}/_deps/hashtable-src/include/thirdparty/spinlock.hpp" ]; then
    read -p "It looks like you built the multiT branch. Do you want to remove it and build the singleT one? [y/N] " remove_choice
    if [ "$remove_choice" = "y" ] || [ "$remove_choice" = "Y" ]; then
        rm -fr ${BUILD_DIR}
    else
        echo "Operation aborted. No files removed."
        exit 1
    fi
fi

# Generate cmake project files
cmake \
  -D CMAKE_BUILD_TYPE=${BUILD_TYPE} \
  -D CMAKE_C_COMPILER=${C_COMPILER} \
  -D CMAKE_CXX_COMPILER=${CXX_COMPILER} \
  -B ${BUILD_DIR} \
  .

# Link compile_commands.json
ln -fs ${BUILD_DIR}compile_commands.json compile_commands.json

# Build tests code
cmake \
  --build ${BUILD_DIR} \
  --target ${TARGET} \
  -j
