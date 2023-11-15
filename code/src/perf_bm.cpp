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
    std::string filename = "";
    std::ofstream output_file;
    std::string h_fun_name = "";
    std::string table_name = "";
    std::string probe_distr = "uniform";
    std::string filter = "";
    std::string ds_name = "";
/* ========================= */

// Function to print the usage information
void show_usage() {
    std::cout << "\n\033[1;96m./perf_bm [ARGS]\033[0m" << std::endl;
    std::cout << "Arguments:" << std::endl;
    std::cout << "  -i, --input INPUT_DIR      Directory storing the datasets" << std::endl;
    std::cout << "  -o, --output OUTPUT_FILE   File that will store the output" << std::endl;
    std::cout << "  -f, --filter FILTER        The type of benchmark we want to execute. Options = probe,join" << std::endl;
    std::cout << "  -D, --dataset DATASET      Dataset that will be used. Options = gap10,fb" << std::endl;
    std::cout << "  -F, --function HASH_FN     Function to use. Options = rmi,mult,mwhc" << std::endl;
    std::cout << "  -T, --table TABLE          Table to use. Options = chain,linear,cuckoo" << std::endl;
    std::cout << "  -D, --probe DISTRIBUTION   Distribution used to probe. Options = uniform,80-20 (default: uniform)" << std::endl;
    std::cout << "  -h, --help                 Display this help message\n" << std::endl;
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
                filename = argv[i + 1];
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --output requires an argument." << std::endl;
                return 2;
            }
        }
        if (arg == "--filter" || arg == "-f") {
            if (i + 1 < argc) {
                filter = argv[i + 1];
                if (filter != "probe" && filter != "join") {
                    std::cerr << "Error: Unknown option for --filter -> " << h_fun_name << std::endl;
                    return 2;
                }
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --filter requires an argument." << std::endl;
                return 2;
            }
        }
        if (arg == "--function" || arg == "-F") {
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
        if (arg == "--table_name" || arg == "-T") {
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
        if (arg == "--dataset" || arg == "-D") {
            if (i + 1 < argc) {
                ds_name = argv[i + 1];
                if (ds_name != "gap10" && ds_name != "wiki" && ds_name != "first" && ds_name != "fb") {
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
        if (arg == "--probe" || arg == "-P") {
            if (i + 1 < argc) {
                probe_distr = argv[i + 1];
                if (probe_distr != "uniform" && probe_distr != "80-20") {
                    std::cerr << "Error: Unknown option for --probe -> " << ds_name << std::endl;
                    return 2;
                }
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --probe requires an argument." << std::endl;
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
    if (input_dir == "" || filename == "" || ds_name == "" || h_fun_name=="" || table_name=="" || filter=="") {
        std::cerr << "Error: all arguments must be provided." << std::endl;
        show_usage();
        return 1;
    }
    
    // Load the dataset
    dataset::ID ds_id;
    if (ds_name == "gap10" || (ds_name == "first" && filter == "probe")) {
        ds_id = dataset::ID::GAP_10;
        ds_name = "gap10";
    }
    else if (ds_name == "wiki" || (ds_name == "first" && filter == "join")) {
        ds_id = dataset::ID::WIKI;
        ds_name = "wiki";
    }
    else ds_id = dataset::ID::FB;
    dataset::Dataset<Data> ds(ds_id, static_cast<size_t>(MAX_DS_SIZE), input_dir);

    // Choose the probe distribution
    bm::ProbeType probe_type;
    if (probe_distr == "uniform")
        probe_type = bm::ProbeType::UNIFORM;
    else if (probe_distr == "80-20")
        probe_type = bm::ProbeType::PARETO_80_20;

    // Create a JsonWriter instance (for the output file)
    JsonOutput writer(".", argv[0], "tmp");
    // Open the output file in append mode
    output_file.open(filename, std::ios_base::app);
    if (!output_file.is_open()) {
        throw std::runtime_error("Error opening output file.\n");
    }
    // init benchmarks
    bm::init(true, probe_type);

    // make perf_config
    // function,table,dataset,probe,
    std::string perf_config = h_fun_name + "," + table_name + "," + ds_name + "," + probe_distr + ",";

    // Call the right function

    // ------------- probe ------------- //
    if (filter == "probe") {
        // RMI
        if (h_fun_name == "rmi") {
            if (ds_name == "gap10") {
                if (table_name == "chain")
                    bm::probe_throughput<RMIHash_10, ChainedTable<RMIHash_10>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
                if (table_name == "linear")
                    bm::probe_throughput<RMIHash_10, LinearTable<RMIHash_10>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
                if (table_name == "cuckoo")
                    bm::probe_throughput<RMIHash_10, CuckooTable<RMIHash_10>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
            }
            if (ds_name == "fb") {
                if (table_name == "chain")
                    bm::probe_throughput<RMIHash_10M, ChainedTable<RMIHash_10M>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
                if (table_name == "linear")
                    bm::probe_throughput<RMIHash_10M, LinearTable<RMIHash_10M>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
                if (table_name == "cuckoo")
                    bm::probe_throughput<RMIHash_10M, CuckooTable<RMIHash_10M>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
            }
        }
        // MultiPrime
        if (h_fun_name == "mult") {
            if (table_name == "chain")
                bm::probe_throughput<MultPrime64, ChainedTable<MultPrime64>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
            if (table_name == "linear")
                bm::probe_throughput<MultPrime64, LinearTable<MultPrime64>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
            if (table_name == "cuckoo")
                bm::probe_throughput<MultPrime64, CuckooTable<MultPrime64>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
        }
        // MWHC
        if (h_fun_name == "mwhc") {
            if (table_name == "chain")
                bm::probe_throughput<MWHC, ChainedTable<MWHC>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
            if (table_name == "linear")
                bm::probe_throughput<MWHC, LinearTable<MWHC>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
            if (table_name == "cuckoo")
                bm::probe_throughput<MWHC, CuckooTable<MWHC>>(ds, writer, LOAD_PERC, probe_type, perf_config, output_file);
        }
    }
    
    // ------------- join ------------- //
    if (filter == "join") {
        // RMI
        if (h_fun_name == "rmi") {
            if (ds_name == "wiki") {
                if (table_name == "chain")
                    bm::join_helper<RMIHash_1k, ChainedTable<RMIHash_1k>>(ds, writer, perf_config, output_file);
                if (table_name == "linear")
                    bm::join_helper<RMIHash_1k, LinearTable<RMIHash_1k>>(ds, writer, perf_config, output_file);
                if (table_name == "cuckoo")
                    bm::join_helper<RMIHash_1k, CuckooTable<RMIHash_1k>>(ds, writer, perf_config, output_file);
            }
            if (ds_name == "fb") {
                if (table_name == "chain")
                    bm::join_helper<RMIHash_1M, ChainedTable<RMIHash_1M>>(ds, writer, perf_config, output_file);
                if (table_name == "linear")
                    bm::join_helper<RMIHash_1M, LinearTable<RMIHash_1M>>(ds, writer, perf_config, output_file);
                if (table_name == "cuckoo")
                    bm::join_helper<RMIHash_1M, CuckooTable<RMIHash_1M>>(ds, writer, perf_config, output_file);
            }
        }
        // MultiPrime
        if (h_fun_name == "mult") {
            if (table_name == "chain")
                bm::join_helper<MultPrime64, ChainedTable<MultPrime64>>(ds, writer, perf_config, output_file);
            if (table_name == "linear")
                bm::join_helper<MultPrime64, LinearTable<MultPrime64>>(ds, writer, perf_config, output_file);
            if (table_name == "cuckoo")
                bm::join_helper<MultPrime64, CuckooTable<MultPrime64>>(ds, writer, perf_config, output_file);
        }
        // MWHC
        if (h_fun_name == "mwhc") {
            if (table_name == "chain")
                bm::join_helper<MWHC, ChainedTable<MWHC>>(ds, writer, perf_config, output_file);
            if (table_name == "linear")
                bm::join_helper<MWHC, LinearTable<MWHC>>(ds, writer, perf_config, output_file);
            if (table_name == "cuckoo")
                bm::join_helper<MWHC, CuckooTable<MWHC>>(ds, writer, perf_config, output_file);
        }
    }
    return 0;
}