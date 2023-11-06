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
    std::string filter = "all";
    size_t how_many = dataset::ID_COUNT;
/* ========================= */

// Function to print the usage information
void show_usage() {
    std::cout << "\n\033[1;96m./benchmarks [ARGS]\033[0m" << std::endl;
    std::cout << "Arguments:" << std::endl;
    std::cout << "  -i, --input INPUT_DIR     Directory storing the datasets" << std::endl;
    std::cout << "  -o, --output OUTPUT_DIR   Directory that will store the output" << std::endl;
    // std::cout << "  -t, --threads THREADS     Number of threads to use (default: all)" << std::endl;
    std::cout << "  -f, --filter FILTER       Type of benchmark to execute, *comma-separated* (default: all)" << std::endl;
    std::cout << "                            Options = collisions,gaps,probe[80_20],build,distribution,point[80_20],range[80_20],join,all" << std::endl;    // TODO - add more
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
        /*
        if (arg == "--threads" || arg == "-t") {
            if (i + 1 < argc) {
                threads = std::stoi(argv[i + 1]);
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --threads requires an argument." << std::endl;
                return 2;
            }
        }*/
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

template <class HashFn, class ReductionFn = FastModulo>
void dilate_probe_list(std::vector<bm::BM>& probe_bm_out,dataset::ID id, bm::ProbeType probe_type = bm::ProbeType::UNIFORM) {
    // Chained
    for (size_t load_perc : chained_lf) {
        bm::BMtype lambda = [load_perc, probe_type](const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
            bm::probe_throughput<HashFn, ChainedTable<HashFn,ReductionFn>>(ds_obj, writer, load_perc, probe_type);
        };
        probe_bm_out.push_back({lambda, id});
    }
    // Linear
    for (size_t load_perc : linear_lf) {
        bm::BMtype lambda = [load_perc, probe_type](const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
            bm::probe_throughput<HashFn, LinearTable<HashFn,ReductionFn>>(ds_obj, writer, load_perc, probe_type);
        };
        probe_bm_out.push_back({lambda, id});
    }
    // Cuckoo
    for (size_t load_perc : cuckoo_lf) {
        bm::BMtype lambda = [load_perc, probe_type](const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
            bm::probe_throughput<HashFn, CuckooTable<HashFn,FastModulo>>(ds_obj, writer, load_perc, probe_type);
        };
        probe_bm_out.push_back({lambda, id});
    }
}

void dilate_function_list(std::vector<bm::BMtype>& bm_out, const bm::BMtemplate _bm_function_, const size_t sizes[], const size_t len) {
    for (size_t i=0; i<len; i++) {
        size_t s = sizes[i];
        bm::BMtype lambda = [s, _bm_function_](const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
            _bm_function_(ds_obj, writer, s);
        };
        bm_out.push_back(lambda);
    }
}

void load_bm_list(std::vector<bm::BM>& bm_list,
        const std::vector<bm::BMtype>& collision_bm,
        const bm::BMtype& gap_bm, 
        const std::vector<bm::BM>& probe_bm, const std::vector<bm::BM>& probe_pareto_bm,
        const std::vector<bm::BMtype>& build_bm,
        const std::vector<bm::BMtype>& collisions_vs_gaps_bm,
        const std::vector<bm::BMtype>& point_vs_range_bm, const std::vector<bm::BMtype>& point_vs_range_pareto_bm,
        const std::vector<bm::BMtype>& range_size_bm, const std::vector<bm::BMtype>& range_size_pareto_bm,
        const std::vector<bm::BM>& join_bm
    /*TODO - add more*/) {
    std::string part;
    size_t start;
    size_t end = 0;
    while ((start = filter.find_first_not_of(',', end)) != std::string::npos) {
        end = filter.find(',', start);
        part = filter.substr(start, end-start);
        if (part == "collision" || part == "collisions" || part == "all") {
            for (const bm::BMtype& bm_fn : collision_bm) {
                for (dataset::ID id : collisions_ds)
                    bm_list.push_back({bm_fn, id});
            }
            if (part != "all") continue;
        }
        if (part == "gap" || part == "gaps" || part == "all") {
            for (dataset::ID id : gaps_ds)
                bm_list.push_back({gap_bm, id});
            if (part != "all") continue;
        }
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
            if (part != "all") continue;
        }
        if (part == "build" || part == "all") {
            for (const bm::BMtype& bm_fn : build_bm) {
                for (dataset::ID id : build_time_ds)
                    bm_list.push_back({bm_fn, id});
            }
            if (part != "all") continue;
        }
        if (part == "distribution" || part == "all") {
            how_many = dataset::ID_ALL_COUNT;
            for (const bm::BMtype& bm_fn : collisions_vs_gaps_bm) {
                for (dataset::ID id : collisions_vs_gaps_ds)
                    bm_list.push_back({bm_fn, id});
            }
            if (part != "all") continue;
        }
        if (part == "point" || part == "all") {
            for (const bm::BMtype& bm_fn : point_vs_range_bm) {
                for (dataset::ID id : range_ds)
                    bm_list.push_back({bm_fn, id});
            }
            if (part != "all") continue;
        }
        if (part == "point80_20" || part == "all") {
            for (const bm::BMtype& bm_fn : point_vs_range_pareto_bm) {
                for (dataset::ID id : range_ds)
                    bm_list.push_back({bm_fn, id});
            }
            if (part != "all") continue;
        }
        if (part == "range" || part == "all") {
            for (const bm::BMtype& bm_fn : range_size_bm) {
                for (dataset::ID id : range_ds)
                    bm_list.push_back({bm_fn, id});
            }
            if (part != "all") continue;
        }
        if (part == "range80_20" || part == "all") {
            for (const bm::BMtype& bm_fn : range_size_pareto_bm) {
                for (dataset::ID id : range_ds)
                    bm_list.push_back({bm_fn, id});
            }
            if (part != "all") continue;
        }
        if (part == "join" || part == "all") {
            for (const bm::BM& bm_struct : join_bm) {
                bm_list.push_back(bm_struct);
            }
            // if (part != "all") continue;
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

    std::cout << std::endl << "\033[1;96m===================== \033[0m" << std::endl;
    std::cout << "\033[1;96m= hashing-benchmark = \033[0m" << std::endl;
    std::cout << "\033[1;96m===================== \033[0m" << std::endl;
    // std::cout << "Running on " << threads << " thread" << (threads>1? "s.":".") << std::endl << std::endl;

    // Create a JsonWriter instance (for the output file)
    JsonOutput writer(output_dir, argv[0], filter);

    // Benchmark arrays definition
    std::vector<bm::BM> bm_list;
    // ------------- collisions ------------- //
    std::vector<bm::BMtype> collision_bm = {
        // RMI
        &bm::collision_stats<RMIHash_2>,
        &bm::collision_stats<RMIHash_10>,
        &bm::collision_stats<RMIHash_100>,
        &bm::collision_stats<RMIHash_1k>,
        &bm::collision_stats<RMIHash_10k>,
        &bm::collision_stats<RMIHash_100k>,
        &bm::collision_stats<RMIHash_1M>,
        &bm::collision_stats<RMIHash_10M>,
        &bm::collision_stats<RMIHash_100M>,
        // RadixSpline
        &bm::collision_stats<RadixSplineHash_4>,
        &bm::collision_stats<RadixSplineHash_16>,
        &bm::collision_stats<RadixSplineHash_128>,
        &bm::collision_stats<RadixSplineHash_1k>,
        &bm::collision_stats<RadixSplineHash_100k>,
        // PGM
        &bm::collision_stats<PGMHash_2>,
        &bm::collision_stats<PGMHash_32>,
        &bm::collision_stats<PGMHash_100>,
        &bm::collision_stats<PGMHash_1k>,
        &bm::collision_stats<PGMHash_100k>,
        // Classic
        &bm::collision_stats<MURMUR>,
        &bm::collision_stats<MultPrime64>,
        &bm::collision_stats<FibonacciPrime64>,
        &bm::collision_stats<AquaHash>,
        &bm::collision_stats<XXHash3>,
        // Perfect
        &bm::collision_stats<MWHC>,
        &bm::collision_stats<BitMWHC>,
        &bm::collision_stats<RecSplit>
    };
    // ---------------- gaps ---------------- //
    bm::BMtype gap_bm = &bm::gap_stats<RMIHash_1M>;
    // ---------------- probe --------------- //
    std::vector<bm::BM> probe_bm = {};
    dilate_probe_list<RMIHash_10>(probe_bm,dataset::ID::GAP_10);
    dilate_probe_list<RMIHash_100>(probe_bm,dataset::ID::NORMAL);
    dilate_probe_list<RMIHash_1k>(probe_bm,dataset::ID::WIKI);
    dilate_probe_list<RMIHash_10M>(probe_bm,dataset::ID::FB);
    dilate_probe_list<RMIHash_10M>(probe_bm,dataset::ID::OSM);
    // for each dataset
    for (dataset::ID id : probe_insert_ds) {
        dilate_probe_list<RadixSplineHash_128>(probe_bm,id);
        dilate_probe_list<PGMHash_100>(probe_bm,id);
        dilate_probe_list<MURMUR>(probe_bm,id);
        dilate_probe_list<MultPrime64>(probe_bm,id);
        dilate_probe_list<MWHC>(probe_bm,id);

    }
    // ---------------- probe PARETO --------------- //
    std::vector<bm::BM> probe_pareto_bm = {};
    dilate_probe_list<RMIHash_10>(probe_pareto_bm,dataset::ID::GAP_10,bm::ProbeType::PARETO_80_20);
    dilate_probe_list<RMIHash_100>(probe_pareto_bm,dataset::ID::NORMAL,bm::ProbeType::PARETO_80_20);
    dilate_probe_list<RMIHash_1k>(probe_pareto_bm,dataset::ID::WIKI,bm::ProbeType::PARETO_80_20);
    dilate_probe_list<RMIHash_10M>(probe_pareto_bm,dataset::ID::FB,bm::ProbeType::PARETO_80_20);
    dilate_probe_list<RMIHash_10M>(probe_pareto_bm,dataset::ID::OSM,bm::ProbeType::PARETO_80_20);
    // for each dataset
    for (dataset::ID id : probe_insert_ds) {
        dilate_probe_list<RadixSplineHash_128>(probe_pareto_bm,id,bm::ProbeType::PARETO_80_20);
        dilate_probe_list<PGMHash_100>(probe_pareto_bm,id,bm::ProbeType::PARETO_80_20);
        dilate_probe_list<MURMUR>(probe_pareto_bm,id,bm::ProbeType::PARETO_80_20);
        dilate_probe_list<MultPrime64>(probe_pareto_bm,id,bm::ProbeType::PARETO_80_20);
        dilate_probe_list<MWHC>(probe_pareto_bm,id,bm::ProbeType::PARETO_80_20);

    }
    // ---------------- build time --------------- //
    std::vector<bm::BMtype> build_bm = {};
    size_t build_size = sizeof(build_entries)/sizeof(build_entries[0]);
    dilate_function_list(build_bm, &bm::build_time<RMIHash_100>, build_entries, build_size);
    dilate_function_list(build_bm, &bm::build_time<RadixSplineHash_1k>, build_entries, build_size);
    dilate_function_list(build_bm, &bm::build_time<PGMHash_1k>, build_entries, build_size);
    dilate_function_list(build_bm, &bm::build_time<MWHC>, build_entries, build_size);
    // ---------------- collisions-vs-gaps --------------- //
    std::vector<bm::BMtype> collisions_vs_gaps_bm = {};
    size_t lf_size = sizeof(collisions_vs_gaps_lf)/sizeof(collisions_vs_gaps_lf[0]);
    dilate_function_list(collisions_vs_gaps_bm, &bm::collisions_vs_gaps<RMIHash_1k>, collisions_vs_gaps_lf, lf_size);
    // ---------------- point-vs-range --------------- //
    std::vector<bm::BMtype> point_vs_range_bm = {};
    size_t point_size = sizeof(point_queries_perc)/sizeof(point_queries_perc[0]);
    dilate_function_list(point_vs_range_bm, &bm::point_vs_range<RMIMonotone,ChainedRange<RMIMonotone>>, point_queries_perc, point_size);
    dilate_function_list(point_vs_range_bm, &bm::point_vs_range<RadixSplineHash_1k,ChainedRange<RadixSplineHash_1k>>, point_queries_perc, point_size);
    dilate_function_list(point_vs_range_bm, &bm::point_vs_range<RMIMonotone,RMISortRange<RMIMonotone>>, point_queries_perc, point_size);
    // ---------------- point-vs-range PARETO --------------- //
    std::vector<bm::BMtype> point_vs_range_pareto_bm = {};
    dilate_function_list(point_vs_range_pareto_bm, &bm::point_vs_range_pareto<RMIMonotone,ChainedRange<RMIMonotone>>, point_queries_perc, point_size);
    dilate_function_list(point_vs_range_pareto_bm, &bm::point_vs_range_pareto<RadixSplineHash_1k,ChainedRange<RadixSplineHash_1k>>, point_queries_perc, point_size);
    dilate_function_list(point_vs_range_pareto_bm, &bm::point_vs_range_pareto<RMIMonotone,RMISortRange<RMIMonotone>>, point_queries_perc, point_size);
    // ---------------- range-size --------------- //
    std::vector<bm::BMtype> range_len_bm = {};
    size_t range_size = sizeof(range_len)/sizeof(range_len[0]);
    dilate_function_list(range_len_bm, &bm::range_throughput<RMIMonotone,ChainedRange<RMIMonotone>>, range_len, range_size);
    dilate_function_list(range_len_bm, &bm::range_throughput<RadixSplineHash_1k,ChainedRange<RadixSplineHash_1k>>, range_len, range_size);
    dilate_function_list(range_len_bm, &bm::range_throughput<RMIMonotone,RMISortRange<RMIMonotone>>, range_len, range_size);
    // ---------------- range-size PARETO --------------- //
    std::vector<bm::BMtype> range_len_pareto_bm = {};
    dilate_function_list(range_len_pareto_bm, &bm::range_throughput_pareto<RMIMonotone,ChainedRange<RMIMonotone>>, range_len, range_size);
    dilate_function_list(range_len_pareto_bm, &bm::range_throughput_pareto<RadixSplineHash_1k,ChainedRange<RadixSplineHash_1k>>, range_len, range_size);
    dilate_function_list(range_len_pareto_bm, &bm::range_throughput_pareto<RMIMonotone,RMISortRange<RMIMonotone>>, range_len, range_size);
    
    // ---------------- join --------------- //
    std::vector<bm::BM> join_bm = {};
    // RMI, wiki
    join_bm.push_back({&bm::join_throughput<RMIHash_1k, ChainedTable<RMIHash_1k>>, dataset::ID::WIKI});
    join_bm.push_back({&bm::join_throughput<RMIHash_1k, LinearTable<RMIHash_1k>>, dataset::ID::WIKI});
    join_bm.push_back({&bm::join_throughput<RMIHash_1k, CuckooTable<RMIHash_1k>>, dataset::ID::WIKI});
    // RMI, fb
    join_bm.push_back({&bm::join_throughput<RMIHash_1M, ChainedTable<RMIHash_1M>>, dataset::ID::FB});
    join_bm.push_back({&bm::join_throughput<RMIHash_1M, LinearTable<RMIHash_1M>>, dataset::ID::FB});
    join_bm.push_back({&bm::join_throughput<RMIHash_1M, CuckooTable<RMIHash_1M>>, dataset::ID::FB});
    // the other 2
    for (dataset::ID id : join_ds) {    
        join_bm.push_back({&bm::join_throughput<MultPrime64, ChainedTable<MultPrime64>>, id});
        join_bm.push_back({&bm::join_throughput<MultPrime64, LinearTable<MultPrime64>>, id});
        join_bm.push_back({&bm::join_throughput<MultPrime64, CuckooTable<MultPrime64>>, id});
        join_bm.push_back({&bm::join_throughput<MWHC, ChainedTable<MWHC>>, id});
        join_bm.push_back({&bm::join_throughput<MWHC, LinearTable<MWHC>>, id});
        join_bm.push_back({&bm::join_throughput<MWHC, CuckooTable<MWHC>>, id});
    }

    load_bm_list(bm_list, collision_bm, gap_bm, probe_bm, probe_pareto_bm, build_bm, collisions_vs_gaps_bm, point_vs_range_bm, point_vs_range_pareto_bm, range_len_bm, range_len_pareto_bm, join_bm);

    if (bm_list.size()==0) {
        std::cerr << "Error: no benchmark functions selected.\nHint: double-check your filters! \nAvailable filters: collisions,gaps,probe[80_20],build,distribution,point[80_20],range[80_20],join,all." << std::endl;   // TODO - add more
        return 1;
    }

    // Create the collection of datasets
    std::cout << "Starting dataset loading procedure... ";
    dataset::CollectionDS<Data> collection(static_cast<size_t>(MAX_DS_SIZE), input_dir, threads, how_many);
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