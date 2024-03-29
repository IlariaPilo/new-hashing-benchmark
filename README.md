# New Hashing Benchmarking

A repository based on the same components of [hashing-benchmark](https://github.com/DominikHorn/hashing-benchmark), with the aim of reproducing results of the [Can Learned Models Replace Hash Functions?](https://dl.acm.org/doi/10.14778/3570690.3570702) article.

⚠️ This is the _serial_ version of the repository, meaning that the _probe_, _point_ and _range_ experiments build and query the table in a serial fashion. For the _parallel_ version, take a look at the [multiT](https://github.com/IlariaPilo/new-hashing-benchmark/tree/multiT) branch.

## 0 | Clone the repository
The repository can be cloned by running: 
```sh
git clone --recursive https://github.com/IlariaPilo/new-hashing-benchmark
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
  -f, --filter FILTER       Type of benchmark to execute, *comma-separated*
                            Options = collisions,gaps,probe[80_20],build,distribution,point[80_20],range[80_20],join,all (default: all) 
  -h, --help                Display this help message
```
Results are saved in the specified output directory, in a file called `<filter>_<timestamp>.json`.

### 📌 Benchmark types
Notice that the numbers in the parenthesis refer to the experiment number in the article.
- _collisions_ : compute the throughput/collisions tradeoff for different hash functions on different datasets [7.2]
- _gaps_ : compute the gap distribution of various datasets [7.1-datasets]
- _probe_ : compute the insert and probe throughput in three types of tables for different hash functions on different datasets [7.3-probe throughput;insert throughput]
- _probe80\_20_ : the _probe_ experiment using the 80-20 distribution to simulate real-world data access [new]
- _build_ : compare the build time for different hash functions [7.4-build time]
- _distribution_ : compare the number of collisions when changing the variance of the gap distribution, as well as the load factor [7.4-gap distribution]
- _point_ : a range query experiment, comparing the performance of different tables undergoing mixed workloads point-range queries [7.5-point queries percentage]
- _point80\_20_ : the _point_ experiment using the 80-20 distribution to simulate real-world data access [new]
- _range_ : a range query experiment, comparing the performance of different tables undergoing range queries fo various sizes [7.5-range query size]
- _range80\_20_ : the _range_ experiment using the 80-20 distribution to simulate real-world data access [new]
- _join_ : compute the running time for the Non Partitioned Join using three types of tables and different hash functions [7.6]
### 📟 `perf`
`perf` benchmarks are more delicate, and they can be run by using a separate script.
```sh
cd code
bash perf-benchmarks.sh [ARGS]
```
This script can be used as follows:
```
./perf-benchmarks.sh [ARGS]
Arguments:
  -i, --input INPUT_DIR     Directory storing the datasets
  -o, --output OUTPUT_DIR   Directory that will store the output
  -f, --filter FILTER       Type of benchmark to execute. Options = probe,join
  -h, --help                Display this help message
```
Results are saved in the specified output directory, in a file called `perf-<filter>_<timestamp>.csv`.

#### 🚨 DON'T PANIC [perf troubleshooting]
If the `perf` benchmark script returns "Error opening counter cycles", try the following steps.

Before running `perf` benchmarks, it might be necessary to run on the *__host machine__*: 
```sh
sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'
```
If you are using the Docker container, don't forget to add the `--perf` option when executing `run.sh`.

### 🌀 [ new! ] Coroutines
Coroutine-powered benchmarks can be run with the `cmake-build-release/src/coroutines` executable.
```
./coroutines [ARGS]
Arguments:
  -i, --input INPUT_DIR     Directory storing the datasets
  -o, --output OUTPUT_DIR   Directory that will store the output
  -c, --coro COROUTINES     Number of streams (default: 8, maximum: 16)
  -f, --filter FILTER       Type of benchmark to execute, *comma-separated* (default: all)
                            Options = rmi,probe[80_20],probe_rmi,batch,all
  -h, --help                Display this help message
```
Results are saved in the specified output directory, in a file called `coroutines-<filter>_<timestamp>.json`.

Here are the available coroutine benchmarks:
- _rmi_ : compute the hashing throughput for RMI functions with different number of submodels, in a sequential and an interleaved fashion
- _probe_ : compute the probe throughput for hash tables using different functions, in a sequential and an interleaved fashion
- _probe80\_20_ : the _probe_ experiment using the 80-20 distribution to simulate real-world data access
- _probe\_rmi_ : compute the probe throughput for hash tables using different RMI functions, in a sequential and an interleaved fashion. In this case, the hash computation is embedded in the lookup function, to enable the submodel prefetching
- _batch_ : compute the probe throughput using data batches (instead of the full dataset), in a sequential and an interleaved fashion

## 3 | Process the results
### 🎨 Figure generation
The [`print_figures`](./scripts/print_figures.py) Python script can be used to generate all possible plots included in the original article starting from a .json or .csv output file. 

It also automatically averages results of different runs of the same benchmark, when available.
```sh
python3 scripts/print_figures.py <json_or_csv_file>
```

Figures are saved in the `figs/` directory, in files called `<json_or_csv_file>_<fig_suffix>.png`.

### 🧶 Merging benchmarks
The [`merge_results`](./scripts/merge_results.py) Python script can be used to merge two or more .json or .csv output files. 
```sh
python3 scripts/merge_results.py -o <merged_file> <list_of_input_files>
```
Results are saved in the specified `<merged_file>.json` and `<merged_file>.csv` files.

This script can be particularly useful to leverage the average capability of `print_figures`.

## 🔎 RAM peak usage analysis
The provided Docker Image comes with the [`heaptrack`](https://github.com/KDE/heaptrack) tool already installed. `heaptrack` can be used to detect (among other statistics) the RAM peak usage during the program run.
```sh
heaptrack <benchmark_executable> <bechmark_arguments>
```
The tool will generate an output file in the current directory, called `heaptrack.<benchmark_executable>.<some_number>.gz`. The file is not human-readable, but it can be interpreted by running:
```sh
heaptrack --analyze heaptrack.<benchmark_executable>.<some_number>.gz
```
As the output file size might be quite big, it is recommended to remove the output file once done.

## 🌳 Repository structure
[`cloud/`](./cloud/) : contains some csv files useful to run the repository on Cloud platforms like Azure or AWS.

[`code/`](./code/) : contains the source code of the repository, as well as three bash scripts.

1. [`benchmarks.sh`](./code/benchmarks.sh) : a shortcut to run the benchmark program using the default folders `data/` and `output/` as input and output folders, respectively.
2. [`build.sh`](./code/build.sh), to build the project.
3. [`coro.sh`](./code/build.sh) : a shortcut to run the coroutine program using the default folders `data/` and `output/` as input and output folders, respectively.
4. [`perf-benchmarks.sh`](./code/perf-benchmarks.sh), to run the `perf` experiments.

[`docker/`](./docker/) : contains all the needed files to build and run the Docker container of the project.

[`figs/`](./figs/) : will contain all plots generated with [`print_figures`](./scripts/print_figures.py). Some figures are already present.

[`output/`](./output/) : stores experimental results run on the research group's server. Such results were used to generate the available plots.

[`scripts/`](./scripts/) : contains five utility scripts.

1. [`coro_plot.py`](./scripts/coro_plot.py) : generates the [`coroutines_cmp_ratio.png`](./figs/coroutines_cmp_ratio.png) and the [`coroutines_cmp_throughput.png`](./figs/coroutines_cmp_throughput.png) plots.
2. [`merge_results.py`](./scripts/merge_results.py) : allows to merge two or more .json files containing benchmark results.  
3. [`print_figures.py`](./scripts/merge_results.py) : generates plots starting from a .json or .csv result file.
4. [`setup_datasets.sh`](./scripts/setup_datasets.sh) : downloads SOSD datasets required to run the benchmarks.
5. [`thread_plot.py`](./scripts/thread_plot.py) : generates the [`join_cmp.png`](./figs/join_cmp.png) plot.

## 🧩 Third party components
This repository is based on the following components:
- [`coroutines`](https://github.com/turingcompl33t/coroutines) by Kyle Dotterrer — A repository exploring behavior and applications of C++ coroutines;
- [`exotic-hashing`](https://github.com/DominikHorn/exotic-hashing) by Dominik Horn — A C++ library exposing various state-of-the-art exotic hash functions;
- [`hashing`](https://github.com/DominikHorn/hashing) by Dominik Horn — A C++ library exposing various state-of-the-art non-cryptographic hash functions and reduction algorithms;
- [`hashtable`](https://github.com/DominikHorn/hashtable) by Dominik Horn — A C++ library exposing various hashtable implementations;
- [`json`](https://github.com/nlohmann/json) by Niels Lohmann — A library to handle json serialization in C++;
- [`learned-hashing`](https://github.com/DominikHorn/learned-hashing) by Dominik Horn — A C++ library exposing various state-of-the-art learned hash functions;
- [`perfevent`](https://github.com/viktorleis/perfevent) by Viktor Leis — A header-only C++ wrapper for Linux' perf event API.

