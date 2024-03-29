#pragma once
#include <chrono>
#include <vector>
#include <omp.h>
#include <cstdint>
#include <random>
#include <cmath>

#include "generic_function.hpp"
#include "npj.hpp"
#include "output_json.hpp"
#include "datasets.hpp"
#include "configs.hpp"
#include "thirdparty/perfevent/PerfEvent.hpp"

#include "coroutines/cppcoro/coroutine.hpp"
#include "coroutines/cppcoro/generator.hpp"

// For a detailed description of the benchmarks, please consult the README of the project

namespace bm {
    enum class ProbeType {
        UNIFORM = 0,
        PARETO_80_20 = 1
    };
    // bm function pointer type
    using BMtype = std::function<void(const dataset::Dataset<Data>&, JsonOutput&)>;
    // utility version
    using BMtemplate = std::function<void(const dataset::Dataset<Data>&, JsonOutput&, size_t)>;
    // coroutine version
    using BMcoroutine = std::function<void(const dataset::Dataset<Data>&, JsonOutput&, size_t, ProbeType, size_t)>;
    // struct function+dataset
    typedef struct BM {
        BMtype function;
        dataset::ID dataset;
    } BM;

    // ----------------- utility things ----------------- //
    size_t N;
    bool is_perf;
    std::vector<int> order_insert;          // will store all values from 0 to N-1
    std::vector<int> order_probe_uniform;   // will store uniformly sampled values from 0 to N-1
    std::vector<int> order_probe_80_20;     // will store sampled values from 0 to N-1 using the 80-20 rule
    std::vector<int> ranges;                // will store random values in the interval [25,50]

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
        std::vector<int> weights(N);
        for (size_t i = 0; i < N; ++i) {
            // get the access percent
            double access_perc = access_percent_dist(gen);
            if (access_perc >= 0.8)
                // 80% weight with 20% of probability
                weights[i] = 16;
            else
                // 20% weight with 80% of probability
                weights[i] = 1;
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

    /**
     * Init all global variable to support benchmarks
     * @param the number of threads that will be used in the parallel build & probe
    */
    void init(bool perf = false, ProbeType probe = ProbeType::UNIFORM) {
            is_perf = perf;
            N = MAX_DS_SIZE;
            generate_insert_order(N);
            if (!perf || probe == ProbeType::UNIFORM) generate_probe_order_uniform(N);
            if (!perf || probe == ProbeType::PARETO_80_20) generate_probe_order_80_20(N);
            if (!perf) fill_ranges(N);
    }

    /**
    The function that will run all selected benchmarks
    @param  bm_list the list of benchmarks that will be run
    @param collection the list of datasets benchmarks will be run on
    @param writer the object that handles the output json file 
    */
    void run_bms(std::vector<BM>& bm_list,
            dataset::CollectionDS<Data>& collection, JsonOutput& writer) {
        int BM_COUNT = bm_list.size();
        // initialize arrays to keep things sorted
        init();
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
        std::vector<Key> keys_count;
        keys_count.resize(capacity, 0);

        // ====================== collision counters ====================== //
        Key index;
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_, start_for, end_for;
        /*volatile*/ std::chrono::duration<double> tot_time(0), tot_for(0);
        size_t collisions_count = 0;
        size_t NOT_collisions_count = 0;
        // ================================================================ //

        start_for = std::chrono::high_resolution_clock::now();
        for (int i : order_insert) {
            // check if the index exists
            if (i < (int)dataset_size) {
                Data data = ds[i];
                _start_ = std::chrono::high_resolution_clock::now();
                index = fn(data);
                _end_ = std::chrono::high_resolution_clock::now();
                keys_count[index]++;
                tot_time += _end_ - _start_;
            }
        }
        end_for = std::chrono::high_resolution_clock::now();
        tot_for = end_for - start_for;

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
        benchmark["tot_for_time_s"] = tot_for.count();
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
    void probe_throughput(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t load_perc, ProbeType probe_type, 
            /* perf stuff */ std::string perf_config = "", std::ostream& perf_out = std::cout) {
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
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_, start_for, end_for;
        /*volatile*/ std::chrono::duration<double> tot_time_insert(0), tot_time_probe(0), tot_for_insert(0), tot_for_probe(0);
        size_t insert_count = 0;
        size_t probe_count = 0;
        std::string fail_what = "";
        bool insert_fail = false;
        PerfEvent e(!is_perf);      /* silence errors if it's not perf */
        // ================================================================ //

        // Build the table
        Payload count = 0;
        start_for = std::chrono::high_resolution_clock::now();
        for (int i : order_insert) {
            // check if the index exists
            if (i < (int)dataset_size) {
                // get the data
                Data data = ds[i];
                try {
                    _start_ = std::chrono::high_resolution_clock::now();
                    table.insert(data, count);
                    _end_ = std::chrono::high_resolution_clock::now();
                } catch(std::runtime_error& e) {
                    // if we are here, we failed the insertion
                    insert_fail = true;
                    fail_what = e.what();
                    goto done;
                }
                count++;
                insert_count++;
                tot_time_insert += _end_ - _start_;
            }
        }
        end_for = std::chrono::high_resolution_clock::now();
        tot_for_insert = end_for - start_for;

        if (is_perf)
            e.startCounters();
        start_for = std::chrono::high_resolution_clock::now();
        for (int i : *order_probe) {
            // check if the index exists
            if (i < (int)dataset_size) {
                // get the data
                Data data = ds[i];
                _start_ = std::chrono::high_resolution_clock::now();
                std::optional<Payload> payload = table.lookup(data);
                _end_ = std::chrono::high_resolution_clock::now();
                if (!payload.has_value()) {
                    throw std::runtime_error("\033[1;91mError\033[0m Data not found...\n           [data] " + std::to_string(data) + "\n           [label] " + label + "\n");
                }
                probe_count++;
                tot_time_probe += _end_ - _start_;
            }
        }
        end_for = std::chrono::high_resolution_clock::now();
        if (is_perf)
            e.stopCounters();
        tot_for_probe = end_for - start_for;

    done:
        json benchmark;
        benchmark["dataset_size"] = dataset_size;
        benchmark["probe_elem_count"] = probe_count;
        benchmark["insert_elem_count"] = insert_count;
        benchmark["tot_time_probe_s"] = tot_time_probe.count();
        benchmark["tot_time_insert_s"] = tot_time_insert.count();
        benchmark["tot_for_time_probe_s"] = tot_for_probe.count();
        benchmark["tot_for_time_insert_s"] = tot_for_insert.count();
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
        if (is_perf) {
            // print data
            perf_out << perf_config;
            e.printReport(perf_out, dataset_size, /*printHeader*/ false, /*printData*/ true);
        }
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
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_, start_for, end_for;
        /*volatile*/ std::chrono::duration<double> tot_time_probe(0), tot_for_probe(0);
        size_t probe_count = 0;
        std::string fail_what = "";
        bool insert_fail = false;
        // ================================================================ //

        // Build the table
        Payload count = 0;
        for (int idx : order_insert) {
            // check if the index exists
            if (idx < (int)dataset_size) {
                // get the data
                Data data = ds[idx];
                try {
                    table.insert(data, count);
                } catch(std::runtime_error& e) {
                    // if we are here, we failed the insertion
                    insert_fail = true;
                    fail_what = e.what();
                    goto done;
                }
                count++;
            }
        }
        start_for = std::chrono::high_resolution_clock::now();
        // Begin with the point queries
        for (size_t i=0; i<N; i++) {
            int idx_min = (*order_probe)[i];
            // check if the index exists
            if (idx_min < (int)dataset_size) {
                // get the data
                Data min = ds[idx_min];
                // point queries
                if (i<X) {
                    _start_ = std::chrono::high_resolution_clock::now();
                    std::optional<Payload> payload = table.lookup(min);
                    _end_ = std::chrono::high_resolution_clock::now();
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
                    _start_ = std::chrono::high_resolution_clock::now();
                    std::vector<Payload> payload = table.lookup_range(min,max);
                    _end_ = std::chrono::high_resolution_clock::now();
                    if (payload.size() != increment) {
                        throw std::runtime_error("\033[1;91mError\033[0m Data not found...\n           [min] " + std::to_string(min) + "\n           [max] " + std::to_string(max) + "\n           [size] " + std::to_string(payload.size()) + "\n           [increment] " + std::to_string(increment) + "\n           [label] " + label + "\n");
                    }
                }
                probe_count++;
                tot_time_probe += _end_ - _start_;
            }
        }
        end_for = std::chrono::high_resolution_clock::now();
        tot_for_probe = end_for - start_for;
        
    done:
        json benchmark;
        benchmark["dataset_size"] = dataset_size;
        benchmark["range_size"] = range_size;
        benchmark["probe_elem_count"] = probe_count;
        benchmark["tot_time_probe_s"] = tot_time_probe.count();
        benchmark["tot_for_time_probe_s"] = tot_for_probe.count();
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

    // join throughput helper
    template <class HashFn, class HashTable>
    void join_helper(const dataset::Dataset<Key>& ds_obj, JsonOutput& writer,
            /* perf stuff */ std::string perf_config = "", std::ostream& perf_out = std::cout) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Key>& ds = ds_obj.get_ds();

        const std::string label = "Join:" + HashTable::name() + ":" + HashFn::name() + ":" + dataset_name;

        // do 10M and 25M variants
        std::vector<Key> keys_10M;
        std::vector<Key> keys_25M;
        std::vector<Key> keys_10M_dup;
        std::vector<Key> keys_25M_dup;
        std::vector<Payload> payloads_10M;
        std::vector<Payload> payloads_25M;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist_10M(0, M(10)-1);
        std::uniform_int_distribution<int> dist_25M(0, M(25)-1);

        keys_10M.resize(M(10));
        keys_25M.resize(M(25));
        keys_10M_dup.resize(M(25));
        keys_25M_dup.resize(M(25));
        payloads_10M.resize(M(10));
        payloads_25M.resize(M(25));

        // not-duplicated ones
        size_t i, idx;
        for (i=0, idx=0; i<M(10) && idx<N; idx++) {
            if (order_insert[idx] < (int)dataset_size) {
                keys_10M[i] = ds[order_insert[idx]];
                payloads_10M[i] = idx;
                i++;
            }
        }
        for (i=0; i<M(25) && idx<N; idx++) {
            if (order_insert[idx] < (int)dataset_size) {
                keys_25M[i] = ds[order_insert[idx]];
                payloads_25M[i] = idx;
                i++;
            }
        }
        // duplicated ones
        for (i=0; i<M(25); i++) {
            int rand_idx_10M = dist_10M(gen);
            int rand_idx_25M = dist_25M(gen);
            keys_10M_dup[i] = keys_10M[rand_idx_10M];
            keys_25M_dup[i] = keys_25M[rand_idx_25M];
        }

        // prepare output arrays
        std::vector<Key> keys_out;
        std::vector<std::pair<Payload,Payload>> payloads_out;
        
        // ******************** 10x25 ******************** //
        auto time_10_25 = join::npj_hash<Key,Payload,HashFn,HashTable,JOIN_LOAD_PERC>(
            keys_10M, payloads_10M, keys_10M_dup, payloads_25M, keys_out, payloads_out,
            /* perf things */ is_perf, perf_config+"10Mx25M,", perf_out
        );
        if (time_10_25.has_value() && keys_out.size()!=M(25)) {
            throw std::runtime_error("\033[1;91mError!\033[0m join operation didn't find all pairs\n           In --> " + label + " (10Mx25M)\n           [keys_out.size()] " + std::to_string(keys_out.size()) + "\n");
        }
        json benchmark_10_25;
        benchmark_10_25["join_size"] = "(10Mx25M)";
        benchmark_10_25["dataset_name"] = dataset_name;
        benchmark_10_25["function_name"] = HashFn::name();
        benchmark_10_25["label"] = label;
        if (!time_10_25.has_value()) {
            std::cout << "\033[1;91mInsert failed >\033[0m " + label + "\t[ 10M x 25M ]\n";
            benchmark_10_25["has_failed"] = true;
        }
        else {
            std::cout << label + "\t[ 10M x 25M ]\n";
            benchmark_10_25["has_failed"] = false;
            benchmark_10_25["tot_time_build_s"] = std::get<1>(time_10_25.value()).count();
            benchmark_10_25["tot_time_join_s"] = std::get<2>(time_10_25.value()).count();
            benchmark_10_25["tot_time_sort_s"] = std::get<0>(time_10_25.value()).count();
        }
        writer.add_data(benchmark_10_25);

        // ******************** 25x25 ******************** //
        keys_out.clear();
        payloads_out.clear();
        auto time_25_25 = join::npj_hash<Key,Payload,HashFn,HashTable,JOIN_LOAD_PERC>(
            keys_25M, payloads_25M, keys_25M_dup, payloads_25M, keys_out, payloads_out,
            /* perf things */ is_perf, perf_config+"25Mx25M,", perf_out    
        );
        if (time_25_25.has_value() && keys_out.size()!=M(25)) {
            throw std::runtime_error("\033[1;91mError!\033[0m join operation didn't find all pairs\n           In --> " + label + " (25Mx25M)\n           [keys_out.size()] " + std::to_string(keys_out.size()) + "\n");
        }
        json benchmark_25_25;
        benchmark_25_25["join_size"] = "(25Mx25M)";
        benchmark_25_25["dataset_name"] = dataset_name;
        benchmark_25_25["function_name"] = HashFn::name();
        benchmark_25_25["label"] = label;
        if (!time_25_25.has_value()) {
            std::cout << "\033[1;91mInsert failed >\033[0m " + label + "\t[ 25M x 25M ]\n";
            benchmark_25_25["has_failed"] = true;
        }
        else {
            std::cout << label + "\t[ 25M x 25M ]\n";
            benchmark_25_25["has_failed"] = false;
            benchmark_25_25["tot_time_build_s"] = std::get<1>(time_25_25.value()).count();
            benchmark_25_25["tot_time_join_s"] = std::get<2>(time_25_25.value()).count();
            benchmark_25_25["tot_time_sort_s"] = std::get<0>(time_25_25.value()).count();
        }
        writer.add_data(benchmark_25_25);
    }
    // join throughput
    template <class HashFn, class HashTable>
    inline void join_throughput(const dataset::Dataset<Key>& ds_obj, JsonOutput& writer) { 
        join_helper<HashFn,HashTable>(ds_obj, writer);
    }

    // ********************** COROUTINES ********************** //

    // inline static coro::generator<Data> make_lookup_vector(std::vector<Data> const &ds, std::vector<int> const *order_probe, size_t *count) {
    //     size_t dataset_size = ds.size();
    //     for (int idx : *order_probe) {
    //         if (idx < (int)dataset_size) {
    //             (*count)++;
    //             Data data = ds[idx];
    //             co_yield data;
    //         }
    //     }
    // }
    inline static void make_lookup_vector(std::vector<Data> const &ds, std::vector<Data>& lookup, std::vector<int> const *order_probe, size_t *count) {
        size_t dataset_size = ds.size();
        lookup.reserve(dataset_size);
        for (int idx : *order_probe) {
            if (idx < (int)dataset_size) {
                (*count)++;
                Data data = ds[idx];
                lookup.push_back(data);
            }
        }
    }
    /**
     * A function to generate a lookup batch of size `batch_size`.
     * @param ds the dataset array
     * @param batch_size the size of the batch, should be equal to the number of coroutines
     * @param batch_index the number of batch we are considering right now
     * @param lookup the output array
     * @param order_probe the pointer to the probe order array we are considering (uniform vs pareto)
     * @param count the effective size of the batch we generate
    */
    inline static void make_lookup_batch(std::vector<Data> const &ds, size_t batch_size, size_t batch_index, std::vector<Data>& lookup, std::vector<int> const *order_probe, size_t *count) {
        // check if batch_index is valid
        size_t dataset_size = ds.size(), _count_ = 0, batch_count = 0, idx;
        lookup.clear();
        lookup.reserve(batch_size);
        // look for this specific batch
        for (idx=0; idx<N && (batch_count%batch_size==0 && batch_count/batch_size==batch_index); idx++) {
            if (idx < dataset_size) {
                batch_count++;
            }
        }
        for (; idx<N && _count_<batch_size; idx++) {
            if (idx < dataset_size) {
                _count_++;
                Data data = ds[idx];
                lookup.push_back(data);
            }
        }
        *count = _count_;
    }

    // probe coroutines
    template <class HashFn, class CoroTable = ChainedTableCoro<HashFn>>
    void probe_coroutines(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t load_perc, ProbeType probe_type,
            /* coroutines stuff */ size_t n_coro) {
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

        const std::string label = "Coro:" + HashFn::name() + ":" + dataset_name + ":" + std::to_string(load_perc) + ":" + probe_label + ":" + std::to_string(n_coro);

        std::cout << "BEGIN " + label + "\n";

        // Compute capacity given the laod% and the dataset_size
        std::size_t capacity = dataset_size*100/load_perc;
        
        // now, create the table
        HashFn fn;
        _generic_::GenericFn<HashFn>::init_fn(fn,ds.begin(),ds.end(),capacity);
        CoroTable table(capacity, fn);
        
        // ====================== throughput counters ====================== //
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_, start_for, end_for;
        /*volatile*/ std::chrono::duration<double> tot_time_insert(0), tot_for_insert(0), tot_for_interleaved(0), tot_for_sequential(0);
        size_t insert_count = 0;
        size_t probe_count = 0;
        std::string fail_what = "";
        bool insert_fail = false;
        // ================================================================ //

        // Build the table
        bool done = true;
        Payload count = 0;
        start_for = std::chrono::high_resolution_clock::now();
        for (int i : order_insert) {
            // check if the index exists
            if (i < (int)dataset_size) {
                // get the data
                Data data = ds[i];
                _start_ = std::chrono::high_resolution_clock::now();
                done &= table.insert(data, count);
                _end_ = std::chrono::high_resolution_clock::now();
                count++;
                insert_count++;
                tot_time_insert += _end_ - _start_;
            }
        }
        end_for = std::chrono::high_resolution_clock::now();
        tot_for_insert = end_for - start_for;

        // check if everything went well!
        if (!done) {
            throw std::runtime_error("\033[1;91mAssertion failed\033[0m done\n           In --> " + label + "\n");
        }

        // prepare lookup and output arrays   
        std::vector<ResultType> results{};
        std::vector<Data> lookup;
        make_lookup_vector(ds, lookup, order_probe, &probe_count);
        results.reserve(probe_count);

        start_for = std::chrono::high_resolution_clock::now();
        table.interleaved_multilookup(lookup.begin(), lookup.end(), std::back_inserter(results), n_coro);
        end_for = std::chrono::high_resolution_clock::now();
        tot_for_interleaved = end_for - start_for;

        // check if everything went well!
        if (results.size() != probe_count) {
            throw std::runtime_error("\033[1;91mAssertion failed\033[0m results.size()==probe_count\n           In --> " + label + "\n           [results.size()] " + std::to_string(results.size()) + "\n           [probe_count] " + std::to_string(probe_count) + "\n");
        }
        std::cout << " |- [t] interleaved lookup: " << tot_for_interleaved.count() << "s\n";

        results.clear();
        results.reserve(probe_count);

        start_for = std::chrono::high_resolution_clock::now();
        table.sequential_multilookup(lookup.begin(), lookup.end(), std::back_inserter(results));
        end_for = std::chrono::high_resolution_clock::now();
        tot_for_sequential = end_for - start_for;

        // check if everything went well!
        if (results.size() != probe_count) {
            throw std::runtime_error("\033[1;91mAssertion failed\033[0m results.size()==probe_count\n           In --> " + label + "\n           [results.size()] " + std::to_string(results.size()) + "\n           [probe_count] " + std::to_string(probe_count) + "\n");
        }
        std::cout << " |- [t] sequential lookup: " << tot_for_sequential.count() << "s\n";

        json benchmark;
        benchmark["dataset_size"] = dataset_size;
        benchmark["probe_elem_count"] = probe_count;
        benchmark["insert_elem_count"] = insert_count;
        benchmark["tot_time_insert_s"] = tot_time_insert.count();
        benchmark["tot_for_time_interleaved_s"] = tot_for_interleaved.count();
        benchmark["tot_for_time_sequential_s"] = tot_for_sequential.count();
        benchmark["tot_for_time_insert_s"] = tot_for_insert.count();
        benchmark["load_factor_%"] = load_perc;
        benchmark["dataset_name"] = dataset_name;
        benchmark["function_name"] = HashFn::name();
        benchmark["insert_fail_message"] = fail_what;
        benchmark["label"] = label;
        benchmark["probe_type"] = probe_label;
        benchmark["n_coro"] = n_coro;

        if (insert_fail)
            std::cout << " `- \033[1;91mInsert failed\033[0m\n";
        else std::cout << " `- DONE\n";
        writer.add_data(benchmark);
    }

    // probe coroutines
    template <class HashFn, class CoroTable = ChainedTableCoro<HashFn>>
    void batch_coroutines(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t load_perc, ProbeType probe_type,
            /* coroutines stuff */ size_t n_coro) {
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

        const std::string label = "Coro-batch:" + HashFn::name() + ":" + dataset_name + ":" + std::to_string(load_perc) + ":" + probe_label + ":" + std::to_string(n_coro);

        std::cout << "BEGIN " + label + "\n";

        // Compute capacity given the laod% and the dataset_size
        std::size_t capacity = dataset_size*100/load_perc;
        
        // now, create the table
        HashFn fn;
        _generic_::GenericFn<HashFn>::init_fn(fn,ds.begin(),ds.end(),capacity);
        CoroTable table(capacity, fn);
        
        // ====================== throughput counters ====================== //
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_, start_for, end_for;
        /*volatile*/ std::chrono::duration<double> tot_time_insert(0), tot_for_insert(0), tot_for_interleaved(0), tot_for_sequential(0);
        size_t insert_count = 0;
        size_t probe_count = 0, _probe_count_;
        std::string fail_what = "";
        bool insert_fail = false;
        // ================================================================ //

        // Build the table
        bool done = true;
        Payload count = 0;
        start_for = std::chrono::high_resolution_clock::now();
        for (int i : order_insert) {
            // check if the index exists
            if (i < (int)dataset_size) {
                // get the data
                Data data = ds[i];
                _start_ = std::chrono::high_resolution_clock::now();
                done &= table.insert(data, count);
                _end_ = std::chrono::high_resolution_clock::now();
                count++;
                insert_count++;
                tot_time_insert += _end_ - _start_;
            }
        }
        end_for = std::chrono::high_resolution_clock::now();
        tot_for_insert = end_for - start_for;

        // check if everything went well!
        if (!done) {
            throw std::runtime_error("\033[1;91mAssertion failed\033[0m done\n           In --> " + label + "\n");
        }

        // prepare lookup and output arrays   
        std::vector<ResultType> results{};
        std::vector<Data> lookup;
        // begin iterating over the possible batches
        int batch_number = std::ceil(N/n_coro);
        //             //
        // INTERLEAVED //
        //             //
        for (int i : order_insert) {
            // check if the index exists
            if (i < batch_number) {
                // get the batch
                results.clear();
                results.reserve(n_coro);
                make_lookup_batch(ds, n_coro, (size_t)i, lookup, order_probe, &_probe_count_);
                if (_probe_count_ == 0)
                    continue;
                probe_count += _probe_count_;
                _start_ = std::chrono::high_resolution_clock::now();
                table.interleaved_multilookup(lookup.begin(), lookup.end(), std::back_inserter(results), n_coro);
                _end_ = std::chrono::high_resolution_clock::now();
                tot_for_interleaved += _end_ - _start_;
                // check if everything went well!
                if (results.size() != _probe_count_) {
                    throw std::runtime_error("\033[1;91mAssertion failed\033[0m results.size()==probe_count\n           In --> " + label + "\n           [results.size()] " + std::to_string(results.size()) + "\n           [probe_count] " + std::to_string(probe_count) + "\n");
                }
            }
        }
        std::cout << " |- [t] interleaved lookup: " << tot_for_interleaved.count() << "s\n";
        //            //
        // SEQUENTIAL //
        //            //
        for (int i : order_insert) {
            // check if the index exists
            if (i < batch_number) {
                // get the batch
                results.clear();
                results.reserve(n_coro);
                make_lookup_batch(ds, n_coro, (size_t)i, lookup, order_probe, &_probe_count_);
                if (_probe_count_ == 0)
                    continue;
                _start_ = std::chrono::high_resolution_clock::now();
                table.sequential_multilookup(lookup.begin(), lookup.end(), std::back_inserter(results));
                _end_ = std::chrono::high_resolution_clock::now();
                tot_for_sequential += _end_ - _start_;
                // check if everything went well!
                if (results.size() != _probe_count_) {
                    throw std::runtime_error("\033[1;91mAssertion failed\033[0m results.size()==probe_count\n           In --> " + label + "\n           [results.size()] " + std::to_string(results.size()) + "\n           [probe_count] " + std::to_string(probe_count) + "\n");
                }
            }
        }
        std::cout << " |- [t] sequential lookup: " << tot_for_sequential.count() << "s\n";

        json benchmark;
        benchmark["dataset_size"] = dataset_size;
        benchmark["probe_elem_count"] = probe_count;
        benchmark["batch_number"] = std::ceil(probe_count/n_coro);
        benchmark["insert_elem_count"] = insert_count;
        benchmark["tot_time_insert_s"] = tot_time_insert.count();
        benchmark["tot_for_time_interleaved_s"] = tot_for_interleaved.count();
        benchmark["tot_for_time_sequential_s"] = tot_for_sequential.count();
        benchmark["tot_for_time_insert_s"] = tot_for_insert.count();
        benchmark["load_factor_%"] = load_perc;
        benchmark["dataset_name"] = dataset_name;
        benchmark["function_name"] = HashFn::name();
        benchmark["insert_fail_message"] = fail_what;
        benchmark["label"] = label;
        benchmark["probe_type"] = probe_label;
        benchmark["n_coro"] = n_coro;

        if (insert_fail)
            std::cout << " `- \033[1;91mInsert failed\033[0m\n";
        else std::cout << " `- DONE\n";
        writer.add_data(benchmark);
    }

    // RMI coro
    template <class RMI>
    void rmi_coro_throughput(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer,
            /* coroutines stuff */ size_t n_coro) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        RMI fn(ds.begin(), ds.end(), dataset_size);
        const std::string label = "Coro-RMI:" + fn.name() + ":" + dataset_name + ":" + std::to_string(n_coro);

        // ====================== throughput counters ====================== //
        /*volatile*/ std::chrono::high_resolution_clock::time_point start_for, end_for;
        /*volatile*/ std::chrono::duration<double> tot_sequential(0), tot_interleaved(0);
        size_t insert_count = 0;
        // ================================================================ //

        // prepare lookup and output arrays   
        std::vector<ResultRMIType<RMI>> results{};
        results.reserve(dataset_size);

        std::vector<Data> lookup;
        make_lookup_vector(ds, lookup, &order_insert, &insert_count);
        // check if everything went well!
        if (insert_count != dataset_size) {
            throw std::runtime_error("\033[1;91mAssertion failed\033[0m dataset_size==insert_count\n           In --> " + label + "\n           [dataset_size] " + std::to_string(dataset_size) + "\n           [insert_count] " + std::to_string(insert_count) + "\n");
        }
        
        // sequential
        start_for = std::chrono::high_resolution_clock::now();
        fn.sequential_multihash(lookup.begin(), lookup.end(), std::back_inserter(results));
        end_for = std::chrono::high_resolution_clock::now();
        tot_sequential = end_for - start_for;

        // check if everything went well!
        if (results.size() != dataset_size) {
            throw std::runtime_error("\033[1;91mAssertion failed\033[0m results.size()==dataset_size\n           In --> " + label + "\n           [results.size()] " + std::to_string(results.size()) + "\n           [dataset_size] " + std::to_string(dataset_size) + "\n");
        }
        results.clear();
        results.reserve(dataset_size);

        // interleaved
        start_for = std::chrono::high_resolution_clock::now();
        fn.interleaved_multihash(lookup.begin(), lookup.end(), std::back_inserter(results), n_coro);
        end_for = std::chrono::high_resolution_clock::now();
        tot_interleaved = end_for - start_for;

        // check if everything went well!
        if (results.size() != dataset_size) {
            throw std::runtime_error("\033[1;91mAssertion failed\033[0m results.size()==dataset_size\n           In --> " + label + "\n           [results.size()] " + std::to_string(results.size()) + "\n           [dataset_size] " + std::to_string(dataset_size) + "\n");
        }

        json benchmark;

        benchmark["dataset_size"] = dataset_size;
        benchmark["tot_interleaved_time_s"] = tot_interleaved.count();
        benchmark["tot_sequential_time_s"] = tot_sequential.count();
        benchmark["dataset_name"] = dataset_name;
        benchmark["label"] = label;
        benchmark["n_coro"] = n_coro; 
        std::cout << label + "\n";
        writer.add_data(benchmark);
    }
    
}
