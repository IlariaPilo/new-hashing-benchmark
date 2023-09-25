# New Hashing Benchmarking

A repository based on the same components of [hashing-benchmark](https://github.com/DominikHorn/hashing-benchmark), with the aim of reproducing results of the [Can Learned Models Replace Hash Functions?](https://dl.acm.org/doi/10.14778/3570690.3570702) article.

## 0 | Clone the repository
The repository can be cloned by running: 
```sh
git clone https://github.com/IlariaPilo/new-hashing-benchmark
```

## 1 | Setup
### Docker Image [recommended]
It is recommended to use the provided Docker Image to run the experiments, to avoid environment issues. 

_[tested on Docker 23.0.1, Ubuntu 22.04]_ 

Build the Docker Image with:
```bash
cd docker
bash build.sh
```
If everything goes according to plans, the image can be later run with `bash run.sh`. The script is intended to be used as follows:
```bash
bash run.sh <input_dir> [--fast]
```
where `<input_dir>` refers to the directory that stores (or will store) the required datasets. 
The `--fast` [or `-f`] option skips the checksum control for a faster (but less safe) run of the container.

The script automatically checks whether the input directory actually contains the dataset. If it does not, it is possible to download them or to abort the program.

Notice that **all directories refer to the host machine**. <!-- TODO : maybe remove this part? -->

### Native environment
If you prefer running the experiments on your native environment, you can download and setup the datasets by simply running:
```sh
cd scripts
bash setup_datasets.sh <input_dir>
```
where `<input_dir>` refers to the directory that will store the required datasets.

## 2 | Run the experiments
### Compile and run
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
                            Options = collisions,gaps,all (default: all) 
  -h, --help                Display this help message
```
Results are saved in the specified output directory.
### Benchmark types
Notice that the numbers in the parenthesis refer to the experiment number in the article.
- _collisions_ : compute the throughput/collisions tradeoff for different hash functions on different datasets [7.2]
- _gaps_ : compute the gap distribution of various datasets [7.1-datasets]
<!-- TODO add more -->

## 3 | Process the results

