#!/bin/bash
set -e # stop if there is an error

fb="fb_200M_uint64"
osm="osm_cellids_200M_uint64"
wiki="wiki_ts_200M_uint64"

INITIAL_DIR=$(pwd)
_source_dir_=$(dirname "$0")
BASE_DIR=$(readlink -f "${_source_dir_}/..")     # /home/ilaria/Documents/stage/hashing-benchmark-docker

# Check if the user has provided an argument
if [ $# -eq 0 ]; then
  echo -e "\n\033[1;35m\tbash run.sh <input_dir> [--fast] [--perf] \033[0m"
  echo -e "Runs the previously built container."
  echo -e "<input_dir> contains the directory where the datasets are [or will be] saved."
  echo -e "Use the --fast [or -f] option to skip the checksum computation, for a faster run."
  echo -e "Use the --perf [or -p] option to run 'perf' inside the container [\033[1;93mwarning \033[0m-- modifies the security options!].\n"
  exit
fi

input_dir=""
fast=false
perf=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fast | -f)
            fast=true
            shift
            ;;
        --perf | -p)
            perf=true
            shift
            ;;
        *)
            input_dir=$(realpath $1)
            shift
            ;;
    esac
done

if [ "$input_dir" = "" ]; then
  echo -e "\n\033[1;35m\tbash run.sh <input_dir> [--fast] [--perf] \033[0m"
  echo -e "Runs the previously built container."
  echo -e "<input_dir> contains the directory where the datasets are [or will be] saved."
  echo -e "Use the --fast [or -f] option to skip the checksum computation, for a faster run."
  echo -e "Use the --perf [or -p] option to run 'perf' inside the container [\033[1;93m warning \033[0m- modifies the security options! ].\n"
  exit
fi

# Check if input_dir stores all datasets
if [[ $(ls $input_dir/{$fb,$osm,$wiki} 2>/dev/null | wc -l) -ne 3 ]]; then
    echo "It looks like the provided directory does not contain the required datasets."
    echo "Do you want to download them now? [y/N]"
    read answer

    if [[ $answer == "y" || $answer == "Y" ]]; then
        bash ${BASE_DIR}/scripts/setup_datasets.sh $input_dir
    else
        echo "Operation aborted"
        exit 1
    fi
else
  if [ "$fast" = false ]; then
    # check anyway (for checksum)
    bash ${BASE_DIR}/scripts/setup_datasets.sh $input_dir
  fi
fi

if [ "$perf" = false ]; then
  docker run --rm -v $BASE_DIR:/home/benchmarker/new-hashing-benchmark \
      -v $input_dir:/home/benchmarker/new-hashing-benchmark/data \
      -it new-docker-benchmark
else
    docker run --rm -v $BASE_DIR:/home/benchmarker/new-hashing-benchmark \
      -v $input_dir:/home/benchmarker/new-hashing-benchmark/data \
      --security-opt seccomp="${BASE_DIR}/docker/perf-seccomp.json" \
      -it new-docker-benchmark
fi