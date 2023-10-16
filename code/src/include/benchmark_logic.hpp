#pragma once
#include <chrono>
#include <vector>
#include <omp.h>
#include <cstdint>
#include <random>

#include "generic_function.hpp"
#include "output_json.hpp"
#include "datasets.hpp"
#include "configs.hpp"

// For a detailed description of the benchmarks, please consult the README of the project

// declare specific reduction
#pragma omp declare reduction(vec_int_sum : std::vector<int> : \
                              std::transform(omp_out.begin(), omp_out.end(), omp_in.begin(), omp_out.begin(), std::plus<int>())) \
                    initializer(omp_priv = decltype(omp_orig)(omp_orig.size()))

namespace bm {
    // bm function pointer type
    using BMtype = std::function<void(const dataset::Dataset<Data>&, JsonOutput&)>;
    // utility version
    using BMtemplate = std::function<void(const dataset::Dataset<Data>&, JsonOutput&, size_t)>;
    // struct function+dataset
    typedef struct BM {
        BMtype function;
        dataset::ID dataset;
    } BM;

    // ----------------- utility things ----------------- //
    size_t THREADS;
    std::vector<int> order_insert;          // will store all values from 0 to N-1
    std::vector<int> order_probe_uniform;   // will store uniformly sampled values from 0 to N-1
    std::vector<int> order_probe_80_20;     // will store sampled values from 0 to N-1 using the 80-20 rule
    std::vector<int> ranges;                // will store random values in the interval [25,50]

    enum class ProbeType {
        UNIFORM = 0,
        PARETO_80_20 = 1
    };
    void generate_insert_order(size_t N = 100000000) {
        std::random_device rd;
        std::default_random_engine rng(rd());
        order_insert.clear();
        order_insert.reserve(N);
        for (size_t i = 0; i < N; ++i) {
            order_insert.push_back(i);
        }
        std::shuffle(order_insert.begin(), order_insert.end(), rng);
    }
    void generate_probe_order_uniform(size_t N = 100000000) {
        order_probe_uniform.clear();
        order_probe_uniform.reserve(N);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> distribution(0, N-1);
        for (size_t i = 0; i < N; ++i) {
            int random_value = distribution(gen);
            order_probe_uniform.push_back(random_value);
        }
    }
    void generate_probe_order_80_20(size_t N = 100000000) {
        order_probe_80_20.clear();
        order_probe_80_20.reserve(N);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<double> access_percent_dist(0.0, 1.0);
        
        // First,create a weighted distribution
        std::vector<double> weights(N);
        for (size_t i = 0; i < N; ++i) {
            // get the access percent
            double access_perc = access_percent_dist(gen);
            if (access_perc >= 0.8)
                // 80% weight with 20% of probability
                weights[i] = 0.8;
            else
                // 20% weight with 80% of probability
                weights[i] = 0.2;
        }
        std::discrete_distribution<int> distribution(weights.begin(), weights.end());
        for (size_t i = 0; i < N; ++i) {
            int random_value = distribution(gen);
            order_probe_80_20.push_back(random_value);
        }
    }
    void fill_ranges(size_t N = 100000000) {
        ranges.clear();
        ranges.reserve(N);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> distribution(25,50);
        for (size_t i = 0; i < N; ++i) {
            int random_value = distribution(gen);
            ranges.push_back(random_value);
        }
    }
    std::pair<int, int> get_bm_slice(int threadID, size_t thread_num, std::vector<BM>& bm_list) {
        int BM_COUNT = bm_list.size();
        int mod = BM_COUNT % thread_num;
        int div = BM_COUNT / thread_num;
        size_t slice = (size_t)div;
        /* We also take a look in the past to set the starting point! */
        int past_mod_threads = -1;      // the ones with +1 in the slice
        int past_threads = -1;          // all the other ones
        if (threadID < mod) {
            slice++;
            past_mod_threads = threadID;
            past_threads = 0;
        } else {
            past_mod_threads = mod;
            past_threads = threadID - mod;
        }
        /* Define start and end */
        int start = past_mod_threads*(div+1) + past_threads*div;
        int end = start+slice;

        std::pair<int,int> output(start,end);
        return output;
    }

    /**
     * Init all global variable to support benchmarks
     * @param the number of threads that will be used in the parallel build & probe
    */
    void init(size_t thread_num) {
        std::cout << "BEGIN INIT\n"; 
        THREADS = thread_num;
        #pragma omp parallel num_threads(4) 
        {
        int threadID = omp_get_thread_num();
        switch(threadID) {
            case 0:
                generate_insert_order(MAX_DS_SIZE);
                break;
            case 1:
                generate_probe_order_uniform(MAX_DS_SIZE);
                break;
            case 2:
                generate_probe_order_80_20(MAX_DS_SIZE);
                break;
            case 3:
                fill_ranges(MAX_DS_SIZE);
                break;
            default:
                // this should never happen in practice
                throw std::runtime_error("\033[1;Error\033[0m this thread should not exist...\n           [threadID] " + std::to_string(threadID) + "\n");
        }
        }
        std::cout << "END INIT\n"; 
    }

    /**
    The function that will run all selected benchmarks
    @param bm_list the list of benchmarks that will be run
    @param thread_num the number of threads that will be used in the parallel build & probe
    @param collection the list of datasets benchmarks will be run on
    @param writer the object that handles the output json file 
    */
    void run_bms(std::vector<BM>& bm_list, size_t thread_num,
            dataset::CollectionDS<Data>& collection, JsonOutput& writer) {
        int BM_COUNT = bm_list.size();
        init(thread_num);
        // sort the BM array by dataset ID
        std::sort(bm_list.begin(), bm_list.end(), [](BM lhs, BM rhs) {
            return static_cast<int>(lhs.dataset) < static_cast<int>(rhs.dataset);
        });
        // begin computation
        for(int i=0; i<BM_COUNT; i++) {
            BM bm = bm_list[i];
            // get the dataset
            const dataset::Dataset<Data>& ds = collection.get_ds(bm.dataset);
            // run the function
            bm.function(ds, writer);
        }
        // done!
    }

    // ----------------- benchmarks list ----------------- //
    // collision+distribution
    template <class HashFn>
    void collisions_vs_gaps(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t load_perc) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        // Compute capacity given the laod% and the dataset_size
        size_t capacity;
        if (load_perc==0)
            capacity=dataset_size;
        else capacity = dataset_size*100/load_perc;

        _generic_::GenericFn<HashFn> fn(ds.begin(), ds.end(), capacity);
        const std::string label = "Collisions:" + fn.name() + ":" + dataset_name + ":" + std::to_string(load_perc);

        // now, start counting collisions

        // stores the hash value (that is, the key) for each entry in the dataset
        std::vector<int> keys_count;
        keys_count.resize(capacity, 0);

        // ====================== collision counters ====================== //
        Key index;
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_;
        /*volatile*/ std::chrono::duration<double> tot_time(0);
        size_t collisions_count = 0;
        size_t NOT_collisions_count = 0;
        // ================================================================ //

        _start_ = std::chrono::high_resolution_clock::now();
        #pragma omp parallel for reduction(vec_int_sum:keys_count) num_threads(THREADS)
        for (int i : order_insert) {
            // check if the index exists
            if (i < (int)dataset_size) {
                Data data = ds[i];
                index = fn(data);
                keys_count[index]++;
            }
        }
        _end_ = std::chrono::high_resolution_clock::now();
        // count collisions
        for (auto k : keys_count) {
            if (k > 1)
                collisions_count += k;
            else NOT_collisions_count += k;
        }
        if (collisions_count+NOT_collisions_count != dataset_size) {
            // Throw a runtime exception
            throw std::runtime_error("\033[1;91mAssertion failed\033[0m collisions_count+NOT_collisions_count==dataset_size\n           In --> " + label + "\n           [collisions_count] " + std::to_string(collisions_count) + "\n           [NOT_collisions_count] " + std::to_string(NOT_collisions_count) + "\n           [dataset_size] " + std::to_string(dataset_size) + "\n");
        }

        json benchmark;

        benchmark["dataset_size"] = dataset_size;
        benchmark["tot_time_s"] = tot_time.count();
        benchmark["collisions"] = collisions_count;
        benchmark["dataset_name"] = dataset_name;
        benchmark["load_factor_%"] = load_perc;
        benchmark["label"] = label; 
        std::cout << label + "\n";
        writer.add_data(benchmark);
    }
    
    // collision wrapper
    template <class HashFn>
    inline void collision_stats(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
        collisions_vs_gaps<HashFn>(ds_obj, writer, 0);
    }

    // gaps
    template <class HashFn>
    void gap_stats(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        _generic_::GenericFn<HashFn> fn(ds.begin(), ds.end(), dataset_size);
        const std::string label = "Gaps:" + fn.name() + ":" + dataset_name;

        // now, start counting collisions

        // stores the list of hash values (keys)
        std::vector<Key> keys;
        keys.reserve(dataset_size);

        size_t index;

        for (auto data : ds) {
            index = fn(data);
            keys.push_back(index);
        }

        // now, sort keys
        std::sort(keys.begin(), keys.end());

        // compute differences
        std::vector<int> differences;
        int max_diff = 0;
        for (size_t i = 1; i < keys.size(); i++) {
            int diff = keys[i] - keys[i - 1];
            differences.push_back(diff);
            if (diff > max_diff)
                max_diff = diff;
        }
        // Count the discretized differences
        std::vector<int> count(max_diff+1, 0);
        for (int diff : differences) {
            count[diff]++;
        }
        // Convert the std::vector to a JSON array
        json count_json = json::array();
        for (const int& c : count) {
            count_json.push_back(c);
        }

        json benchmark;

        benchmark["dataset_size"] = dataset_size;
        benchmark["dataset_name"] = dataset_name;
        benchmark["count"] = count_json;
        benchmark["label"] = label; 
        std::cout << label + "\n";
        writer.add_data(benchmark);
    }

    // probe throughput helper
    template <class HashFn, class HashTable>
    void probe_throughput(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t load_perc, ProbeType probe_type) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        // Choose probe distribution
        std::vector<int>* order_probe = nullptr;
        std::string probe_label;
        switch(probe_type) {
            case ProbeType::UNIFORM:
                order_probe = &order_probe_uniform;
                probe_label = "uniform";
                break;
            case ProbeType::PARETO_80_20:
                order_probe = &order_probe_80_20;
                probe_label = "80-20";
        }

        // Compute capacity given the laod% and the dataset_size
        size_t capacity = dataset_size*100/load_perc;
        
        // now, create the table
        HashFn fn;
        _generic_::GenericFn<HashFn>::init_fn(fn,ds.begin(),ds.end(),capacity);
        HashTable table(capacity, fn);
        const std::string label = "Probe:" + table.name() + ":" + dataset_name + ":" + std::to_string(load_perc) + ":" + probe_label;

        // ====================== throughput counters ====================== //
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_;
        /*volatile*/ std::chrono::duration<double> tot_time_insert(0), tot_time_probe(0);
        size_t insert_count = 0;
        size_t probe_count = 0;
        std::string fail_what = "";
        bool insert_fail = false;
        // ================================================================ //

        // Build the table
        Payload count = 0;
        _start_ = std::chrono::high_resolution_clock::now();
        #pragma omp parallel for reduction(+:insert_count) private(count) num_threads(THREADS)
        for (int i : order_insert) {
            #pragma omp cancellation point for
            // check if the index exists
            if (i < (int)dataset_size) {
                // get the data
                Data data = ds[i];
                try {
                    table.insert(data, count);                
                    count++;
                    insert_count++;
                } catch(std::runtime_error& e) {
                    // if we are here, we failed the insertion
                    #pragma omp cancel for
                    insert_fail = true;
                    fail_what = e.what();
                }
            }
        }
        _end_ = std::chrono::high_resolution_clock::now(); 
        tot_time_insert += _end_ - _start_;
        if (insert_fail) goto done;


        _start_ = std::chrono::high_resolution_clock::now();
        #pragma omp parallel for reduction(+:probe_count) private(_start_,_end_) num_threads(THREADS)
        for (int i : *order_probe) {
            // check if the index exists
            if (i < (int)dataset_size) {
                // get the data
                Data data = ds[i];
                std::optional<Payload> payload = table.lookup(data);
                if (!payload.has_value()) {
                    throw std::runtime_error("\033[1;91mError\033[0m Data not found...\n           [data] " + std::to_string(data) + "\n           [label] " + label + "\n");
                }
                probe_count++;
            }
        }
        _end_ = std::chrono::high_resolution_clock::now();
        tot_time_probe += _end_ - _start_;
    done:
        json benchmark;
        benchmark["dataset_size"] = dataset_size;
        benchmark["probe_elem_count"] = probe_count;
        benchmark["insert_elem_count"] = insert_count;
        benchmark["tot_time_probe_s"] = tot_time_probe.count();
        benchmark["tot_time_insert_s"] = tot_time_insert.count();
        benchmark["load_factor_%"] = load_perc;
        benchmark["dataset_name"] = dataset_name;
        benchmark["function_name"] = HashFn::name();
        benchmark["insert_fail_message"] = fail_what;
        benchmark["label"] = label;
        benchmark["probe_type"] = probe_label;

        if (insert_fail)
            std::cout << "\033[1;91mInsert failed >\033[0m " + label + "\n";
        else std::cout << label + "\n";
        writer.add_data(benchmark);
    }

    template <class HashFn, class HashTable>
    void range_helper(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t point_query_perc, 
            size_t range_size = 0, ProbeType probe_type = ProbeType::UNIFORM) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();
        
        // Choose probe distribution
        std::vector<int>* order_probe = nullptr;
        std::string probe_label;
        switch(probe_type) {
            case ProbeType::UNIFORM:
                order_probe = &order_probe_uniform;
                probe_label = "uniform";
                break;
            case ProbeType::PARETO_80_20:
                order_probe = &order_probe_80_20;
                probe_label = "80-20";
        }

        // Compute capacity given the laod% and the dataset_size
        size_t capacity;
        if (typeid(HashTable)==typeid(RMISortRange<HashFn>))
            capacity = dataset_size;
        else capacity = dataset_size*100/RANGE_LOAD_PERC;
        
        // now, create the table
        HashFn fn;
        _generic_::GenericFn<HashFn>::init_fn(fn,ds.begin(),ds.end(),capacity);
        HashTable table(capacity, fn);
        const std::string label = "Range:" + table.name() + ":" + dataset_name + ":" + std::to_string(point_query_perc) + ":" + std::to_string(range_size) + ":" + probe_label;

        // get X (the number of point queries)
        size_t X = dataset_size*point_query_perc/100;

        // ====================== throughput counters ====================== //
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_;
        /*volatile*/ std::chrono::duration<double> tot_time_probe(0);
        size_t probe_count = 0;
        std::string fail_what = "";
        bool insert_fail = false;
        // ================================================================ //

        // Build the table
        Payload count = 0;
        #pragma omp parallel for private(count) num_threads(THREADS)
        for (int idx : order_insert) {
            #pragma omp cancellation point for
            // check if the index exists
            if (idx < (int)dataset_size) {
                // get the data
                Data data = ds[idx];
                try {
                    table.insert(data, count);
                } catch(std::runtime_error& e) {
                    // if we are here, we failed the insertion
                    #pragma omp cancel for
                    insert_fail = true;
                    fail_what = e.what();
                }
                count++;
            }
        }
        if (insert_fail) goto done;


        _start_ = std::chrono::high_resolution_clock::now();
        #pragma omp parallel for reduction(+:probe_count) private(_start_,_end_) num_threads(THREADS)
        // Begin with the point queries
        for (size_t i=0; i<dataset_size; i++) {
            int idx_min = (*order_probe)[i];
            // check if the index exists
            if (idx_min < (int)dataset_size) {
                // get the data
                Data min = ds[idx_min];
                // point queries
                if (i<X) {
                    std::optional<Payload> payload = table.lookup(min);
                    if (!payload.has_value()) {
                        throw std::runtime_error("\033[1;91mError\033[0m Data not found...\n           [data] " + std::to_string(min) + "\n           [label] " + label + "\n");
                    }
                }
                // range queries
                else {
                    // get the idx_max
                    size_t increment = (range_size?range_size:ranges[i])-1; // remove 1 cause the upper bound is included
                    size_t idx_max = idx_min + increment;
                    idx_max = idx_max<dataset_size?idx_max:dataset_size-1;
                    increment = idx_max - idx_min +1;                       // add 1 cause the upper bound is included
                    // get the max
                    Data max = ds[idx_max];
                    if (max<min) {
                        throw std::runtime_error("\033[1;91mError\033[0m Something went wrong...\n           [min] " + std::to_string(min) + "\n           [max] " + std::to_string(max) + "\n           [label] " + label + "\n");
                    }
                    _start_ = std::chrono::high_resolution_clock::now();
                    std::vector<Payload> payload = table.lookup_range(min,max);
                    _end_ = std::chrono::high_resolution_clock::now();
                    if (payload.size() != increment) {
                        throw std::runtime_error("\033[1;91mError\033[0m Data not found...\n           [min] " + std::to_string(min) + "\n           [max] " + std::to_string(max) + "\n           [size] " + std::to_string(payload.size()) + "\n           [increment] " + std::to_string(increment) + "\n           [label] " + label + "\n");
                    }
                }
                probe_count++;
            }
        }
        _end_ = std::chrono::high_resolution_clock::now();
        tot_time_probe += _end_ - _start_;
    done:
        json benchmark;
        benchmark["dataset_size"] = dataset_size;
        benchmark["range_size"] = range_size;
        benchmark["probe_elem_count"] = probe_count;
        benchmark["tot_time_probe_s"] = tot_time_probe.count();
        benchmark["point_query_%"] = point_query_perc;
        benchmark["dataset_name"] = dataset_name;
        benchmark["function_name"] = HashFn::name();
        benchmark["insert_fail_message"] = fail_what;
        benchmark["label"] = label;
        benchmark["probe_type"] = probe_label;

        if (insert_fail)
            std::cout << "\033[1;91mInsert failed >\033[0m " + label + "\n";
        else std::cout << label + "\n";
        writer.add_data(benchmark);
    }

    // point-vs-range
    template <class HashFn, class HashTable>
    inline void point_vs_range(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t point_query_perc) {
        range_helper<HashFn,HashTable>(ds_obj, writer, point_query_perc, /* range size*/ 0, ProbeType::UNIFORM);
    }
    // full range
    template <class HashFn, class HashTable>
    inline void range_throughput(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t range_size) {
        range_helper<HashFn,HashTable>(ds_obj, writer, /* % of point queries */ 0, range_size, ProbeType::UNIFORM);
    }
    // point-vs-range
    template <class HashFn, class HashTable>
    inline void point_vs_range_pareto(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t point_query_perc) {
        range_helper<HashFn,HashTable>(ds_obj, writer, point_query_perc, /* range size*/ 0, ProbeType::PARETO_80_20);
    }
    // full range
    template <class HashFn, class HashTable>
    inline void range_throughput_pareto(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t range_size) {
        range_helper<HashFn,HashTable>(ds_obj, writer, /* % of point queries */ 0, range_size, ProbeType::PARETO_80_20);
    }

    // build time
    template <class HashFn>
    void build_time(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t entry_number) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        size_t actual_size;
        if (entry_number>=dataset_size)
            actual_size = dataset_size;
        else actual_size = entry_number;
        auto it_end = ds.begin() + actual_size;
        
        // ====================== time counters ====================== //
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_;
        /*volatile*/ std::chrono::duration<double> build_time(0);
        // ================================================================ //

        _start_ = std::chrono::high_resolution_clock::now();
        _generic_::GenericFn<HashFn> fn(ds.begin(), it_end, actual_size);
        _end_ = std::chrono::high_resolution_clock::now();
        build_time += _end_ - _start_;
        const std::string label = "Build_time:" + fn.name() + ":" + dataset_name + ":" + std::to_string(actual_size);

        // avoid optimization
        Key _ = fn(ds[0]);

        json benchmark;

        benchmark["actual_size"] = actual_size;
        benchmark["build_time_s"] = build_time.count();
        benchmark["dataset_name"] = dataset_name;
        benchmark["label"] = label; 
        benchmark["_"] = _; // useless, just to avoid optimizing out the build
        std::cout << label + "\n";
        writer.add_data(benchmark);
    }

}
