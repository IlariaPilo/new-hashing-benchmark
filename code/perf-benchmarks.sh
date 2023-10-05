#!/bin/bash

# Default values for input and output directories
input_dir=""
output_dir=""

# Function to display usage instructions
usage() {
    echo -e "\n\033[1;96m./perf-benchmarks.sh [ARGS]\033[0m"
    echo "Arguments:"
    echo "  -i, --input  INPUT_DIR    Directory storing the datasets"
    echo "  -o, --output OUTPUT_DIR   Directory that will store the output"
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

echo -e "\n\033[1;96m================== \033[0m"
echo -e "\033[1;96m= perf-benchmark = \033[0m"
echo -e "\033[1;96m================== \033[0m\n"

input_dir=$(realpath $input_dir)
output_dir=$(realpath $output_dir)

# go in the directory that contains this script
cd "$(dirname "$0")"

# define the arrays
functions=("rmi" "mult" "mwhc")
tables=("chain" "linear" "cuckoo")
datasets=("gap10" "fb")

# define the filename (TODO)
current_datetime=$(date +%Y-%m-%d_%H-%M)
output_file="${output_dir}/perf_${current_datetime}.csv"

# plot the header
echo "function,table,dataset,cycles,L1-dcache-load-misses,LLC-load-misses,branch-misses" > $output_file

for ds in "${datasets[@]}"; do
    for tab in "${tables[@]}"; do
        for fun in "${functions[@]}"; do
            echo -n "$fun,$tab,$ds," >> $output_file
            sudo perf stat -e cycles,L1-dcache-load-misses,LLC-load-misses,branch-misses -c 100 \
                cmake-build-release/src/perf_bm -i $input_dir -o $output_dir -f $fun -t $tab -d $ds \
                2>&1 | tee tmp.out
            # get interesting values
            cat tmp.out | grep -E 'cycles|L1-dcache-load-misses|LLC-load-misses|branch-misses' | awk '{
            if (NR == 1) {
                printf $1;
            } else {
                printf ",%s", $1;
            }
            }
            END {
            print "";
            }' >> $output_file
        done
    done
done

# remove temporary file
rm tmp.out