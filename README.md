# New Hashing Benchmarking

A repository based on the same components of [hashing-benchmark](https://github.com/DominikHorn/hashing-benchmark), with the aim of reproducing results of the [Can Learned Models Replace Hash Functions?](https://dl.acm.org/doi/10.14778/3570690.3570702) article.

## 0 | Clone the repository
The repository can be cloned by running: 
```sh
git clone https://github.com/IlariaPilo/new-hashing-benchmark
```

## 1 | Setup
### 🐋 Docker Image [recommended]
It is recommended to use the provided Docker Image to run the experiments, to avoid environment issues. 

_[tested on Docker 23.0.1, Ubuntu 22.04]_ 

Build the Docker Image with:
```bash
cd docker
bash build.sh
```
If everything goes according to plans, the image can be later run with `bash run.sh`. The script is intended to be used as follows:
```bash
bash run.sh <input_dir> [--fast] [--perf]
```
where `<input_dir>` refers to the directory that stores (or will store) the required datasets. 
The `--fast` [or `-f`] option skips the checksum control for a faster (but less safe) run of the container.
The `--perf` [or `-p`] option allows to run 'perf' inside the container. This option must be used wisely, as it modifies the security options of the container [read also [here](https://stackoverflow.com/questions/44745987/use-perf-inside-a-docker-container-without-privileged#answer-44748260)].

The script automatically checks whether the input directory actually contains the dataset. If it does not, it is possible to download them or to abort the program.

### 🌊 Native environment
If you prefer running the experiments on your native environment, you can download and setup the datasets by simply running:
```sh
cd scripts
bash setup_datasets.sh <input_dir>
```
where `<input_dir>` refers to the directory that will store the required datasets.

## 2 | Run the experiments
### ⚙️ Parameters configuration
Some benchmark general parameters can be found in the [`configs.hpp`](./code/src/include/configs.hpp) file. These parameters are set in order to reproduce the article results as closely as possible.

The set of runnable benchmarks can be found at the end of the [`benchmarks.cpp`](./code/src/benchmarks.cpp).

### 🔨 Compile and run
To compile the code, simply run:
```sh
cd code
bash build.sh
```
The executable file is called `cmake-build-release/src/benchmarks`, and it can be used as follows:
```
./benchmarks [ARGS]
Arguments:
  -i, --input INPUT_DIR     Directory storing the datasets
  -o, --output OUTPUT_DIR   Directory that will store the output
  -t, --threads THREADS     Number of threads to use (default: all)
  -f, --filter FILTER       Type of benchmark to execute, *comma-separated*
                            Options = collisions,gaps,probe,build,distribution,point,range,all (default: all) 
  -h, --help                Display this help message
```
Results are saved in the specified output directory, in a file called `<filter>_<timestamp>.json`.

⚠️ *__Warning :__* the thread option is currently ignored due to a concurrency bug. Hopefully it will come back soon!
<!-- TODO hopefully remove -->

### 📌 Benchmark types
Notice that the numbers in the parenthesis refer to the experiment number in the article.
- _collisions_ : compute the throughput/collisions tradeoff for different hash functions on different datasets [7.2]
- _gaps_ : compute the gap distribution of various datasets [7.1-datasets]
- _probe_ : compute the insert and probe throughput in three types of tables for different hash functions on different datasets [7.3-probe throughput;insert throughput]
- _build_ : compare the build time for different hash functions [7.4-build time]
- _distribution_ : compare the number of collisions when changing the variance of the gap distribution, as well as the load factor [7.4-gap distribution]
- _point_ : a range query experiment, comparing the performance of different tables undergoing mixed workloads point-range queries [7.5-point queries percentage]
- _range_ : a range query experiment, comparing the performance of different tables undergoing range queries fo various sizes [7.5-range query size]
<!-- TODO add more -->

### 📟 perf
perf benchmarks are more delicate, and they can be run by using a separate script.
```sh
cd code
bash perf-benchmarks.sh [ARGS]
```
This script can be used as follows:
```
./perf-benchmarks.sh [ARGS]
Arguments:
  -i, --input  INPUT_DIR    Directory storing the datasets
  -o, --output OUTPUT_DIR   Directory that will store the output
  -h, --help                Display this help message
```
Results are saved in the specified output directory, in a file called `perf_<timestamp>.csv`.

#### 🚨 DON'T PANIC [perf troubleshooting]
If the perf benchmark script returns "Error opening counter cycles", try the following steps.

Before running perf benchmarks, it might be necessary to run on the *__host machine__*: 
```sh
sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'
```
If you are using the Docker container, don't forget to add the `--perf` option when executing `run.sh`.

## 3 | Process the results
<!-- TODO add more -->


## 🧩 Third party components
This repository is based on the following components:
- [`hashtable`](https://github.com/DominikHorn/hashtable) by Dominik Horn — A C++ library exposing various hashtable implementations;
- [`hashing`](https://github.com/DominikHorn/hashing) by Dominik Horn — A C++ library exposing various state-of-the-art non-cryptographic hash functions and reduction algorithms;
- [`exotic-hashing`](https://github.com/DominikHorn/exotic-hashing) by Dominik Horn — A C++ library exposing various state-of-the-art exotic hash functions;
- [`learned-hashing`](https://github.com/DominikHorn/learned-hashing) by Dominik Horn — A C++ library exposing various state-of-the-art learned hash functions;
- [`json`](https://github.com/nlohmann/json) by Niels Lohmann — A library to handle json serialization in C++;
- [`perfevent`](https://github.com/viktorleis/perfevent), by Viktor Leis — A header-only C++ wrapper for Linux' perf event API.

