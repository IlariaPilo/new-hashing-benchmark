#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <unistd.h>
#include <cstdint>

#include "include/output_json.hpp"
#include "include/benchmark_logic.hpp"
#include "include/function_aliases.hpp"

#define MAX_SIZE 100000000      // 10^8, 100M

/* ======== options ======== */
    std::string input_dir = "";
    std::string output_dir = "";
    size_t threads;
    std::string filter = "all";
/* ========================= */

// Function to print the usage information
void show_usage() {
    std::cout << "\n\033[1;96m./benchmarks [ARGS]\033[0m" << std::endl;
    std::cout << "Arguments:" << std::endl;
    std::cout << "  -i, --input INPUT_DIR     Directory storing the datasets" << std::endl;
    std::cout << "  -o, --output OUTPUT_DIR   Directory that will store the output" << std::endl;
    std::cout << "  -t, --threads THREADS     Number of threads to use (default: all)" << std::endl;
    std::cout << "  -f, --filter FILTER       Type of benchmark to execute, *comma-separated* (default: all)" << std::endl;
    std::cout << "                            Options = collisions,gaps,all" << std::endl;    // TODO - add more
    std::cout << "  -h, --help                Display this help message\n" << std::endl;
}
int pars_args(const int& argc, char* const* const& argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            show_usage();
            return 1;
        }
        if (arg == "--input" || arg == "-i") {
            if (i + 1 < argc) {
                input_dir = argv[i + 1];
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --input requires an argument." << std::endl;
                return 2;
            }
        }
        if (arg == "--output" || arg == "-o") {
            if (i + 1 < argc) {
                output_dir = argv[i + 1];
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --output requires an argument." << std::endl;
                return 2;
            }
        }
        if (arg == "--threads" || arg == "-t") {
            if (i + 1 < argc) {
                threads = std::stoi(argv[i + 1]);
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --threads requires an argument." << std::endl;
                return 2;
            }
        }
        if (arg == "--filter" || arg == "-f") {
            if (i + 1 < argc) {
                filter = argv[i + 1];
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --filter requires an argument." << std::endl;
                return 2;
            }
        }
        // if we are here, then the option is unknown 
        std::cerr << "Error: Unknown option " << arg << std::endl;
        show_usage();
        return 2;
    }
    return 0;
}

void load_bm_list(std::vector<bm::BMtype<Data,Key>>& bm_list, const std::vector<bm::BMtype<Data,Key>>& collision_bm,
        const bm::BMtype<Data,Key>& gap_bm /*TODO - add more*/) {
    std::string part;
    size_t start;
    size_t end = 0;
    while ((start = filter.find_first_not_of(',', end)) != std::string::npos) {
        end = filter.find(',', start);
        part = filter.substr(start, end-start);
        if (part == "collision" || part == "collisions" || part == "all") {
            for (const bm::BMtype<Data,Key>& bm : collision_bm) {
                bm_list.push_back(bm);
            }
            if (part != "all") continue;
        }
        if (part == "gap" || part == "gaps" || part == "all") {
            bm_list.push_back(gap_bm);
            if (part != "all") continue;
        }
        // if we are here, the filter is unknown
        std::cout << "\033[1;93m [warning]\033[0m filter " << part << " is unknown." << std::endl;
    }
}

int main(int argc, char* argv[]) {
    // get thread number
    threads = sysconf(_SC_NPROCESSORS_ONLN);
    // Parse command-line arguments
    int do_exit = pars_args(argc, argv);
    if (do_exit)
        return do_exit-1;
    // Check if mandatory options are provided
    if (input_dir == "" || output_dir == "") {
        std::cerr << "Error: Mandatory options --input and --output must be provided." << std::endl;
        show_usage();
        return 1;
    }

    std::cout << std::endl << "\033[1;96m===================== \033[0m" << std::endl;
    std::cout << "\033[1;96m= hashing-benchmark = \033[0m" << std::endl;
    std::cout << "\033[1;96m===================== \033[0m" << std::endl;
    std::cout << "Running on " << threads << " thread" << (threads>1? "s.":".") << std::endl << std::endl;

    // Create a JsonWriter instance (for the output file)
    JsonOutput writer(output_dir, argv[0], filter);
    
    // Create the collection of datasets
    std::cout << "Starting dataset loading procedure... ";
    dataset::CollectionDS<Data> collection(static_cast<size_t>(MAX_SIZE), input_dir, threads);
    std::cout << "done!" << std::endl << std::endl;

    // for (const dataset::Dataset<Data>& ds : collection.get_collection() ) {
    //     if (ds.get_ds().size() != ds.get_size()) {
    //         // Throw a runtime exception
    //         throw std::runtime_error("\033[1;91mAssertion failed\033[0m ds.size()==dataset_size\n           In --> " + dataset::name(ds.get_id()) + "\n           [ds.size()] " + std::to_string(ds.get_ds().size()) + "\n           [dataset_size] " + std::to_string(ds.get_size()) + "\n");
    //     }
    // }

    // Benchmark arrays definition
    std::vector<bm::BMtype<Data,Key>> bm_list;
    // ------------- collisions ------------- //
    std::vector<bm::BMtype<Data,Key>> collision_bm = {
        // RMI
        &bm::collision_stats<RMIHash_2,Data,Key>,
        &bm::collision_stats<RMIHash_10,Data,Key>,
        &bm::collision_stats<RMIHash_100,Data,Key>,
        &bm::collision_stats<RMIHash_1k,Data,Key>,
        &bm::collision_stats<RMIHash_10k,Data,Key>,
        &bm::collision_stats<RMIHash_100k,Data,Key>,
        &bm::collision_stats<RMIHash_1M,Data,Key>,
        &bm::collision_stats<RMIHash_10M,Data,Key>,
        &bm::collision_stats<RMIHash_100M,Data,Key>,
        // RadixSpline
        &bm::collision_stats<RadixSplineHash_4,Data,Key>,
        &bm::collision_stats<RadixSplineHash_16,Data,Key>,
        &bm::collision_stats<RadixSplineHash_128,Data,Key>,
        &bm::collision_stats<RadixSplineHash_1k,Data,Key>,
        &bm::collision_stats<RadixSplineHash_100k,Data,Key>,
        // PGM
        &bm::collision_stats<PGMHash_2,Data,Key>,
        &bm::collision_stats<PGMHash_32,Data,Key>,
        &bm::collision_stats<PGMHash_100,Data,Key>,
        &bm::collision_stats<PGMHash_1k,Data,Key>,
        &bm::collision_stats<PGMHash_100k,Data,Key>,
        // Classic
        &bm::collision_stats<MURMUR,Data,Key>,
        &bm::collision_stats<MultPrime64,Data,Key>,
        &bm::collision_stats<FibonacciPrime64,Data,Key>,
        &bm::collision_stats<AquaHash,Data,Key>,
        &bm::collision_stats<XXHash3,Data,Key>,
        // Perfect
        &bm::collision_stats<MWHC,Data,Key>,
        &bm::collision_stats<BitMWHC,Data,Key>,
        &bm::collision_stats<RecSplit,Data,Key>
    };
    // ---------------- gaps ---------------- //
    bm::BMtype<Data,Key> gap_bm = &bm::gap_stats<RMIHash_1M,Data,Key>;
    // TODO - add more
    load_bm_list(bm_list, collision_bm, gap_bm);

    if (bm_list.size()==0) {
        std::cerr << "Error: no benchmark functions selected.\nHint: double-check your filters! Available filters: collisions, gaps, all." << std::endl;   // TODO - add more
        return 1;
    }

    // Run!
    std::cout << "Begin benchmarking on "<< bm_list.size() <<" function" << (bm_list.size()>1? "s...":"...") << std::endl;
    bm::run_bms<Data,Key>(bm_list, threads, collection, writer);
    std::cout << "done!" << std::endl << std::endl;
    
    return 0;
}
