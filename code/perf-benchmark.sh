#!/bin/bash

# Default values for input and output directories
input_dir=""
output_dir=""
filter=""
threads=$(nproc --all)

# Function to display usage instructions
usage() {
    echo -e "\n\033[1;95m./perf-benchmarks.sh [ARGS]\033[0m"
    echo "Arguments:"
    echo "  -i, --input  INPUT_DIR    Directory storing the datasets"
    echo "  -o, --output OUTPUT_DIR   Directory that will store the output"
    echo "  -f, --filter FILTER       The type of benchmark we want to execute. Options = probe,join"
    echo "  -t, --threads THREADS     The number of threads to be used (default: all)"
    echo -e "  -h, --help                Display this help message\n"
    exit 1
}

# Parse command line options
while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
        -i|--input)
            input_dir="$2"
            shift 2
            ;;
        -o|--output)
            output_dir="$2"
            shift 2
            ;;
        -t|--threads)
            threads="$2"
            shift 2
            ;;
        -f|--filter)
            filter="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $key"
            usage
            ;;
    esac
done

# Check if required options are provided
if [ -z "$input_dir" ] || [ -z "$output_dir" ]; then
    echo "Error: Both input and output directories are required."
    usage
fi
if [ -z "$filter" ]; then
    echo "Error: Please specify the type of benchmark."
    usage
fi

echo -e "\n\033[1;95m================== \033[0m"
echo -e "\033[1;95m= perf-benchmark = \033[0m"
echo -e "\033[1;95m================== \033[0m"
echo -e "Running on $filter threads.\n"

input_dir=$(realpath $input_dir)
output_dir=$(realpath $output_dir)

# go in the directory that contains this script
cd "$(dirname "$0")"

# define the arrays
functions=("rmi" "mult" "mwhc")
tables=("chain" "linear" "cuckoo")
datasets=("first" "fb")
probe=("uniform" "80-20")

# define the filename
current_datetime=$(date +%Y-%m-%d_%H-%M)
output_file="${output_dir}/perf-${filter}_${current_datetime}.csv"

# plot the header
echo -n "threads,function,table,dataset,probe," > $output_file
if [ "$filter" == "join" ]; then
    echo -n "phase,sizes," >> $output_file
fi
echo "cycles,kcycles,instructions,L1-misses,LLC-misses,branch-misses,task-clock,scale,IPC,CPUs,GHz" >> $output_file

for ds in "${datasets[@]}"; do
    for tab in "${tables[@]}"; do
        for fun in "${functions[@]}"; do
            for prb in "${probe[@]}"; do
                echo -n "$threads,$fun,$tab,$ds,$prb," >> $output_file
                cmake-build-release/src/perf_bm -i $input_dir -o $output_file -F $fun -T $tab -D $ds -P $prb -t $threads -f $filter
                if [ "$filter" == "join" ]; then
                    break
                fi
            done
        done
    done
done

# remove temporary files
rm tmp.json

