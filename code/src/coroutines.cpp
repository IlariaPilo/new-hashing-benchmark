#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <unistd.h>
#include <cstdint>

#include "include/output_json.hpp"
#include "include/benchmark_logic.hpp"
#include "include/configs.hpp"

/* ======== options ======== */
    std::string input_dir = "";
    std::string output_dir = "";
    size_t threads;
    size_t n_coro = 4;
    std::string filter = "all";
/* ========================= */

// Function to print the usage information
void show_usage() {
    std::cout << "\n\033[1;96m./coroutines [ARGS]\033[0m" << std::endl;
    std::cout << "Arguments:" << std::endl;
    std::cout << "  -i, --input INPUT_DIR     Directory storing the datasets" << std::endl;
    std::cout << "  -o, --output OUTPUT_DIR   Directory that will store the output" << std::endl;
    std::cout << "  -c, --coro COROUTINES     Number of streams in the coroutines (default: 4)" << std::endl;
    // std::cout << "  -t, --threads THREADS     Number of threads to use (default: all)" << std::endl;
    std::cout << "  -f, --filter FILTER       Type of benchmark to execute, *comma-separated* (default: all)" << std::endl;
    std::cout << "                            Options = probe[80_20],all" << std::endl;    // TODO - add more
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
        if (arg == "--coro" || arg == "-c") {
            if (i + 1 < argc) {
                n_coro = std::stoi(argv[i + 1]);
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --coro requires an argument." << std::endl;
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

template <class HashFn>
void dilate_coro_fn(std::vector<bm::BM>& bm_out, dataset::ID id, bm::ProbeType type = bm::ProbeType::UNIFORM) {
    auto cp = n_coro;
    for (size_t lf : chained_lf) {
        bm::BMtype lambda = [lf, cp, type](const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
            bm::probe_coroutines<HashFn>(ds_obj, writer, lf, type, cp);
        };
        bm_out.push_back({lambda,id});
    }
}

void load_bm_list(std::vector<bm::BM>& bm_list,
        const std::vector<bm::BM>& probe_bm, const std::vector<bm::BM>& probe_pareto_bm
        /*TODO - add more*/) {
    std::string part;
    size_t start;
    size_t end = 0;
    while ((start = filter.find_first_not_of(',', end)) != std::string::npos) {
        end = filter.find(',', start);
        part = filter.substr(start, end-start);
        if (part == "probe" || part == "all") {
            for (const bm::BM& bm_struct : probe_bm) {
                bm_list.push_back(bm_struct);
            }
            if (part != "all") continue;
        }
        if (part == "probe80_20" || part == "all") {
            for (const bm::BM& bm_struct : probe_pareto_bm) {
                bm_list.push_back(bm_struct);
            }
            //if (part != "all") continue;
            continue;
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

    std::cout << std::endl << "\033[1;96m============== \033[0m" << std::endl;
    std::cout << "\033[1;96m= coroutines = \033[0m" << std::endl;
    std::cout << "\033[1;96m============== \033[0m" << std::endl;
    // std::cout << "Running on " << threads << " thread" << (threads>1? "s.":".") << std::endl << std::endl;

    // Create a JsonWriter instance (for the output file)
    JsonOutput writer(output_dir, argv[0], "coroutines-"+filter);

    // Benchmark arrays definition
    std::vector<bm::BM> bm_list;
    // ---------------- probe --------------- //
    std::vector<bm::BM> probe_bm = {};
    dilate_coro_fn<RMIHash_10>(probe_bm,dataset::ID::GAP_10);
    dilate_coro_fn<RMIHash_100>(probe_bm,dataset::ID::NORMAL);
    dilate_coro_fn<RMIHash_1k>(probe_bm,dataset::ID::WIKI);
    dilate_coro_fn<RMIHash_10M>(probe_bm,dataset::ID::FB);
    dilate_coro_fn<RMIHash_10M>(probe_bm,dataset::ID::OSM);
    // for each dataset
    for (dataset::ID id : probe_insert_ds) {
        dilate_coro_fn<RadixSplineHash_128>(probe_bm,id);
        dilate_coro_fn<PGMHash_100>(probe_bm,id);
        dilate_coro_fn<MURMUR>(probe_bm,id);
        dilate_coro_fn<MultPrime64>(probe_bm,id);
        dilate_coro_fn<MWHC>(probe_bm,id);

    }
    // ---------------- probe PARETO --------------- //
    std::vector<bm::BM> probe_pareto_bm = {};
    dilate_coro_fn<RMIHash_10>(probe_pareto_bm,dataset::ID::GAP_10,bm::ProbeType::PARETO_80_20);
    dilate_coro_fn<RMIHash_100>(probe_pareto_bm,dataset::ID::NORMAL,bm::ProbeType::PARETO_80_20);
    dilate_coro_fn<RMIHash_1k>(probe_pareto_bm,dataset::ID::WIKI,bm::ProbeType::PARETO_80_20);
    dilate_coro_fn<RMIHash_10M>(probe_pareto_bm,dataset::ID::FB,bm::ProbeType::PARETO_80_20);
    dilate_coro_fn<RMIHash_10M>(probe_pareto_bm,dataset::ID::OSM,bm::ProbeType::PARETO_80_20);
    // for each dataset
    for (dataset::ID id : probe_insert_ds) {
        dilate_coro_fn<RadixSplineHash_128>(probe_pareto_bm,id,bm::ProbeType::PARETO_80_20);
        dilate_coro_fn<PGMHash_100>(probe_pareto_bm,id,bm::ProbeType::PARETO_80_20);
        dilate_coro_fn<MURMUR>(probe_pareto_bm,id,bm::ProbeType::PARETO_80_20);
        dilate_coro_fn<MultPrime64>(probe_pareto_bm,id,bm::ProbeType::PARETO_80_20);
        dilate_coro_fn<MWHC>(probe_pareto_bm,id,bm::ProbeType::PARETO_80_20);

    }

    load_bm_list(bm_list, probe_bm, probe_pareto_bm);

    if (bm_list.size()==0) {
        std::cerr << "Error: no benchmark functions selected.\nHint: double-check your filters! \nAvailable filters: collisions,gaps,probe[80_20],build,distribution,point[80_20],range[80_20],join,all." << std::endl;   // TODO - add more
        return 1;
    }

    // Create the collection of datasets
    std::cout << "Starting dataset loading procedure... ";
    dataset::CollectionDS<Data> collection(static_cast<size_t>(MAX_DS_SIZE), input_dir, threads, dataset::ID_COUNT);
    std::cout << "done!" << std::endl << std::endl;

    /*
    // Uncomment to get extra safety checks
    for (const dataset::Dataset<Data>& ds : collection.get_collection() ) {
        if (ds.get_ds().size() != ds.get_size()) {
            // Throw a runtime exception
            throw std::runtime_error("\033[1;91mAssertion failed\033[0m ds.size()==dataset_size\n           In --> " + dataset::name(ds.get_id()) + "\n           [ds.size()] " + std::to_string(ds.get_ds().size()) + "\n           [dataset_size] " + std::to_string(ds.get_size()) + "\n");
        }
    }
    */

    // Run!
    std::cout << "Begin benchmarking on "<< bm_list.size() <<" function" << (bm_list.size()>1? "s...":"...") << std::endl;
    bm::run_bms(bm_list, collection, writer);
    std::cout << "done!" << std::endl << std::endl;
    
    return 0;
}