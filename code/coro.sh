#!/bin/bash
# Another lazy script (I am really lazy)

# Setup script
# go in the directory that contains this script
cd "$(dirname "$0")"
# run the benchmarks
cmake-build-release/src/coroutines -i ../data -o ../output $@
