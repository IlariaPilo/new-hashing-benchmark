#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <unistd.h>
#include <cstdint>

#include "include/output_json.hpp"
#include "include/benchmark_logic.hpp"
#include "include/configs.hpp"
#include "include/datasets.hpp"

#define LOAD_PERC 80

/* ======== options ======== */
    std::string input_dir = "";
    std::string output_dir = "";
    std::string bm_name = "";
    std::ofstream output_file;
    JsonOutput writer;
/* ========================= */

// Function to print the usage information
void show_usage() {
    std::cout << "\n\033[1;96m./perf_bm [ARGS]\033[0m" << std::endl;
    std::cout << "Arguments:" << std::endl;
    std::cout << "  -i, --input INPUT_DIR     Directory storing the datasets" << std::endl;
    std::cout << "  -o, --output OUTPUT_DIR   Directory that will store the output" << std::endl;
    std::cout << "  -f, --filter FILTER       Type of benchmark to execute." << std::endl;
    std::cout << "                            Options = probe,join" << std::endl;    // TODO - add more
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
        if (arg == "--filter" || arg == "-f") {
            if (i + 1 < argc) {
                bm_name = argv[i + 1];
                if (bm_name != "probe" && bm_name != "join") {
                    std::cerr << "Error: Unknown option for --filter -> " << bm_name << std::endl;
                    return 2;
                }
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --filter requires an argument." << std::endl;
                return 2;
            }
        }
        // if we are here, then the argument is unknown 
        std::cerr << "Error: Unknown argument " << arg << std::endl;
        show_usage();
        return 2;
    }
    return 0;
}
// choose the function
template<class HashFn>
void run_fn(const dataset::Dataset<Data>& ds_obj, bm::ProbeType probe_type, std::string perf_config){
    if (bm_name == "probe") {
        // chain
        bm::probe_throughput<HashFn, ChainedTable<HashFn>>(ds_obj, writer, static_cast<size_t>(LOAD_PERC),
            probe_type, perf_config+"chain,", output_file);
        // linear
        bm::probe_throughput<HashFn, LinearTable<HashFn>>(ds_obj, writer, static_cast<size_t>(LOAD_PERC),
            probe_type, perf_config+"linear,", output_file);
        // cuckoo
        bm::probe_throughput<HashFn, CuckooTable<HashFn>>(ds_obj, writer, static_cast<size_t>(LOAD_PERC),
            probe_type, perf_config+"cuckoo,", output_file);
    } else {
        std::cout << "TODO\n";
        return;
    }
}

int main(int argc, char* argv[]) {
    // Parse command-line arguments
    int do_exit = pars_args(argc, argv);
    if (do_exit)
        return do_exit-1;
    // Check if mandatory options are provided
    if (input_dir == "" || output_dir == "" || bm_name == "") {
        std::cerr << "Error: all arguments must be provided." << std::endl;
        show_usage();
        return 1;
    }
    std::cout << std::endl << "\033[1;96m================== \033[0m" << std::endl;
    std::cout << "\033[1;96m= perf-benchmark = \033[0m" << std::endl;
    std::cout << "\033[1;96m================== \033[0m" << std::endl;

    // Create a JsonWriter instance -- useless --
    writer.init(output_dir, argv[0], "perf-" + bm_name);
    
    // first, get current time
    std::time_t current_time = std::time(nullptr);
    std::tm* local_time = std::localtime(&current_time);
    // Format the date and time as "YYYY-MM-DD-HH-MM"
    std::stringstream ss;
    ss << std::put_time(local_time, "_%Y-%m-%d-%H-%M");
    // define filename
    std::string filename = output_dir + "/" + "perf-" + bm_name + ss.str() + ".csv";
    output_file.open(filename);
    if (!output_file.is_open()) {
        throw std::runtime_error("Error opening output file.\n           [Hint!] Check that directory " + output_dir + " exists.\n");
    }
    
    // Load the datasets
    dataset::ID ds1_id;
    std::string ds1_name;
    if (bm_name == "probe") {
        ds1_id = dataset::ID::GAP_10;
        ds1_name = "gap10";
    } else {
        ds1_id = dataset::ID::WIKI;
        ds1_name = "wiki";
    }
    const dataset::Dataset<Data> ds1(ds1_id, static_cast<size_t>(MAX_DS_SIZE), input_dir);
    // the second one is fb
    const dataset::Dataset<Data> ds_fb(dataset::ID::FB, static_cast<size_t>(MAX_DS_SIZE), input_dir);

    // do arrays
    const std::vector<const dataset::Dataset<Data>*> datasets = {&ds1, &ds_fb};
    const std::vector<std::string> ds_names = {ds1_name, "fb"};
    const std::vector<dataset::ID> ds_ids = {ds1_id, dataset::ID::FB};
    const std::vector<bm::ProbeType> probe_types = {bm::ProbeType::UNIFORM, bm::ProbeType::PARETO_80_20};
    const std::vector<std::string> probe_names = {"uniform", "80-20"};

    // init benchmarks values
    bm::init(true);

    // run benchmarks
    for (int ds = 0; ds < datasets.size(); ds++) {
        for (int probe = 0; probe < probe_types.size(); probe++) {
            std::string config_core = ds_names[ds] + "," + probe_names[probe] + ",";
            // rmi
            switch(ds_ids[ds]) {
                case dataset::ID::GAP_10:
                    run_fn<RMIHash_10>(*(datasets[ds]), probe_types[probe], "rmi,"+config_core);
                    break;
                case dataset::ID::FB:
                    run_fn<RMIHash_10M>(*(datasets[ds]), probe_types[probe], "rmi,"+config_core);
                    break;
                default:
                    std::cout << "TODO!\n";
            }
            // mult
            run_fn<MultPrime64>(*(datasets[ds]), probe_types[probe], "mult,"+config_core);
            // mwhc
            run_fn<MWHC>(*(datasets[ds]), probe_types[probe], "mwhc,"+config_core);
        }
    }

    return 0;
}
