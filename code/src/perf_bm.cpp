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
    std::string h_fun_name = "";
    std::string table_name = "";
    std::string ds_name = "";
/* ========================= */

// Function to print the usage information
void show_usage() {
    std::cout << "\n\033[1;96m./perf_bm [ARGS]\033[0m" << std::endl;
    std::cout << "Arguments:" << std::endl;
    std::cout << "  -i, --input INPUT_DIR     Directory storing the datasets" << std::endl;
    std::cout << "  -o, --output OUTPUT_DIR   Directory that will store the output" << std::endl;
    std::cout << "  -f, --function HASH_FN    Function to use. Options = rmi,mult,mwhc" << std::endl;
    std::cout << "  -t, --table TABLE         Table to use. Options = chain,linear,cuckoo" << std::endl;
    std::cout << "  -d, --dataset DATASET     Dataset that will be used. Options = gap10,fb" << std::endl;
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
        if (arg == "--function" || arg == "-f") {
            if (i + 1 < argc) {
                h_fun_name = argv[i + 1];
                if (h_fun_name != "rmi" && h_fun_name != "mult" && h_fun_name != "mwhc") {
                    std::cerr << "Error: Unknown option for --function -> " << h_fun_name << std::endl;
                    return 2;
                }
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --function requires an argument." << std::endl;
                return 2;
            }
        }
        if (arg == "--table_name" || arg == "-t") {
            if (i + 1 < argc) {
                table_name = argv[i + 1];
                if (table_name != "chain" && table_name != "linear" && table_name != "cuckoo") {
                    std::cerr << "Error: Unknown option for --table_name -> " << table_name << std::endl;
                    return 2;
                }
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --table_name requires an argument." << std::endl;
                return 2;
            }
        }
        if (arg == "--dataset" || arg == "-d") {
            if (i + 1 < argc) {
                ds_name = argv[i + 1];
                if (ds_name != "gap10" && ds_name != "fb") {
                    std::cerr << "Error: Unknown option for --dataset -> " << ds_name << std::endl;
                    return 2;
                }
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --dataset requires an argument." << std::endl;
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

int main(int argc, char* argv[]) {
    // Parse command-line arguments
    int do_exit = pars_args(argc, argv);
    if (do_exit)
        return do_exit-1;
    // Check if mandatory options are provided
    if (input_dir == "" || h_fun_name == "" || table_name == "" || ds_name == "") {
        std::cerr << "Error: all arguments must be provided." << std::endl;
        show_usage();
        return 1;
    }

    // Create a JsonWriter instance (for the output file)
    JsonOutput writer(output_dir, argv[0], "perf-" + h_fun_name + "-" + table_name + "-" + ds_name);
    
    // Load the dataset
    dataset::ID ds_id;
    if (ds_name == "gap10")
        ds_id = dataset::ID::GAP_10;
    else ds_id = dataset::ID::FB;
    dataset::Dataset<Data> ds(ds_id, static_cast<size_t>(MAX_DS_SIZE), input_dir);

    // Call the right function
    if (ds_name == "gap10" && h_fun_name == "rmi" && table_name == "chain") {
        bm::probe_throughput<RMIHash_10, ChainedTable<RMIHash_10>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }
    else if (ds_name == "fb" && h_fun_name == "rmi" && table_name == "chain") {
        bm::probe_throughput<RMIHash_10M, ChainedTable<RMIHash_10M>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }
    else if (h_fun_name == "mult" && table_name == "chain") {
        bm::probe_throughput<MultPrime64, ChainedTable<MultPrime64>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }
    else if (h_fun_name == "mwhc" && table_name == "chain") {
        bm::probe_throughput<MWHC, ChainedTable<MWHC>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }

    else if (ds_name == "gap10" && h_fun_name == "rmi" && table_name == "linear") {
        bm::probe_throughput<RMIHash_10, LinearTable<RMIHash_10>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }
    else if (ds_name == "fb" && h_fun_name == "rmi" && table_name == "linear") {
        bm::probe_throughput<RMIHash_10M, LinearTable<RMIHash_10M>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }
    else if (h_fun_name == "mult" && table_name == "linear") {
        bm::probe_throughput<MultPrime64, LinearTable<MultPrime64>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }
    else if (h_fun_name == "mwhc" && table_name == "linear") {
        bm::probe_throughput<MWHC, LinearTable<MWHC>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }

    else if (ds_name == "gap10" && h_fun_name == "rmi" && table_name == "cuckoo") {
        bm::probe_throughput<RMIHash_10, CuckooTable<RMIHash_10>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }
    else if (ds_name == "fb" && h_fun_name == "rmi" && table_name == "cuckoo") {
        bm::probe_throughput<RMIHash_10M, CuckooTable<RMIHash_10M>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }
    else if (h_fun_name == "mult" && table_name == "cuckoo") {
        bm::probe_throughput<MultPrime64, CuckooTable<MultPrime64>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }
    else if (h_fun_name == "mwhc" && table_name == "cuckoo") {
        bm::probe_throughput<MWHC, CuckooTable<MWHC>>(ds, writer, static_cast<size_t>(LOAD_PERC));
    }
    
    return 0;
}
