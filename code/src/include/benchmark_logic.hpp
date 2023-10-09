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
    std::vector<int> order_insert; // will store all values from 0 to N-1
    std::vector<int> order_probe;  // will store uniformly sampled values from 0 to N-1
    std::vector<int> ranges;       // will store random values in the interval [25,50]

    void generate_insert_order(size_t N = 100000000) {
        std::random_device rd;
        std::default_random_engine rng(rd());
        order_insert.clear();
        for (size_t i = 0; i < N; ++i) {
            order_insert.push_back(i);
        }
        std::shuffle(order_insert.begin(), order_insert.end(), rng);
    }
    void generate_probe_order(size_t N = 100000000) {
        order_probe.clear();
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> distribution(0, N-1);
        for (size_t i = 0; i < N; ++i) {
            int random_value = distribution(gen);
            order_probe.push_back(random_value);
        }
    }
    void fill_ranges(size_t N = 100000000) {
        ranges.clear();
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
    The function that will run all selected benchmarks (PARALLEL VERSION).
    @param  bm_list the list of benchmarks that will be run
    @param collection the list of datasets benchmarks will be run on
    @param writer the object that handles the output json file 
    */
    void run_bms(std::vector<BM>& bm_list,
            size_t thread_num, dataset::CollectionDS<Data>& collection, JsonOutput& writer) {
        // first of all, check thread compatibility
        if (thread_num > bm_list.size())
            thread_num = bm_list.size();
        // initialize arrays to keep things sorted
        generate_insert_order(MAX_DS_SIZE);
        generate_probe_order(MAX_DS_SIZE);
        fill_ranges(MAX_DS_SIZE);
        // sort the BM array by dataset ID
        std::sort(bm_list.begin(), bm_list.end(), [](BM lhs, BM rhs) {
            return static_cast<int>(lhs.dataset) < static_cast<int>(rhs.dataset);
        });
        // begin parallel computation
        #pragma omp parallel num_threads(thread_num)
        {
            int threadID = omp_get_thread_num();
            int BM_COUNT = bm_list.size();
            // get slice of benchmarks
            auto slice = get_bm_slice(threadID, thread_num, bm_list);
            int start = slice.first;
            int end = slice.second;
            // for each function
            for(int i=start; i<end && i<BM_COUNT; i++) {
                BM bm = bm_list[i];
                // get the dataset
                const dataset::Dataset<Data>& ds = collection.get_ds(bm.dataset);
                // run the function
                bm.function(ds, writer);
            }
        }
        // done!
    }
    /**
    The function that will run all selected benchmarks (SERIAL VERSION).
    @param  bm_list the list of benchmarks that will be run
    @param collection the list of datasets benchmarks will be run on
    @param writer the object that handles the output json file 
    */
    void run_bms(std::vector<BM>& bm_list,
            dataset::CollectionDS<Data>& collection, JsonOutput& writer) {
        int BM_COUNT = bm_list.size();
        // initialize arrays to keep things sorted
        generate_insert_order(MAX_DS_SIZE);
        generate_probe_order(MAX_DS_SIZE);
        fill_ranges(MAX_DS_SIZE);
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
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_;
        /*volatile*/ std::chrono::duration<double> tot_time(0);
        size_t collisions_count = 0;
        size_t NOT_collisions_count = 0;
        // ================================================================ //

        for (auto data : ds) {
            _start_ = std::chrono::high_resolution_clock::now();
            index = fn(data);
            _end_ = std::chrono::high_resolution_clock::now();
            keys_count[index]++;
            tot_time += _end_ - _start_;
        }
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

    // probe throughput
    template <class HashFn, class HashTable>
    void probe_throughput(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t load_perc) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        // Compute capacity given the laod% and the dataset_size
        size_t capacity = dataset_size*100/load_perc;
        
        // now, create the table
        HashFn fn;
        _generic_::GenericFn<HashFn>::init_fn(fn,ds.begin(),ds.end(),capacity);
        HashTable table(capacity, fn);
        const std::string label = "Probe:" + table.name() + ":" + dataset_name + ":" + std::to_string(load_perc);

        // ====================== throughput counters ====================== //
        /*volatile*/ std::chrono::high_resolution_clock::time_point _start_, _end_;
        /*volatile*/ std::chrono::duration<double> tot_time_insert(0), tot_time_probe(0);
        size_t insert_count = 0;
        size_t probe_count = 0;
        std::string fail_what = "";
        bool insert_fail = false;
        // ================================================================ //

        // Build the table
        // std::cout << "Start building table... ";
        Payload count = 0;
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
                
                tot_time_insert += _end_ - _start_;
                count++;
                insert_count++;
                // if (count % 10000000 == 1)
                //     std::cout << "Done " << count << " inserts.\n";
            }
        }
        // std::cout << "done\n";

        // std::cout << "Start benchmarking... ";
        for (int i : order_probe) {
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
                tot_time_probe += _end_ - _start_;
                probe_count++;
                // if (probe_count % 10000000 == 1)
                //     std::cout << "Done " << probe_count << " lookups.\n";
            }
        }
        // std::cout << "done\n";
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

        if (insert_fail)
            std::cout << "\033[1;91mInsert failed >\033[0m " + label + "\n";
        else std::cout << label + "\n";
        writer.add_data(benchmark);
    }

    template <class HashFn, class HashTable>
    void range_helper(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t point_query_perc, size_t range_size = 0) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        // Compute capacity given the laod% and the dataset_size
        size_t capacity;
        if (typeid(HashTable)==typeid(RMISortRange<HashFn>))
            capacity = dataset_size;
        else capacity = dataset_size*100/RANGE_LOAD_PERC;
        
        // now, create the table
        HashFn fn;
        _generic_::GenericFn<HashFn>::init_fn(fn,ds.begin(),ds.end(),capacity);
        HashTable table(capacity, fn);
        const std::string label = "Range:" + table.name() + ":" + dataset_name + ":" + std::to_string(point_query_perc);

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
        // Begin with the point queries
        size_t i;
        for (i=0; i<X; i++) {
            int idx = order_probe[i];
            // check if the index exists
            if (idx < (int)dataset_size) {
                // get the data
                Data data = ds[idx];
                _start_ = std::chrono::high_resolution_clock::now();
                std::optional<Payload> payload = table.lookup(data);
                _end_ = std::chrono::high_resolution_clock::now();
                if (!payload.has_value()) {
                    throw std::runtime_error("\033[1;91mError\033[0m Data not found...\n           [data] " + std::to_string(data) + "\n           [label] " + label + "\n");
                }
                tot_time_probe += _end_ - _start_;
                probe_count++;
            }
        }
        // Now the range queries
        while (i<dataset_size) {
            int idx_min = order_probe[i];
            // check if the index exists
            if (idx_min < (int)dataset_size) {
                // get the min
                Data min = ds[idx_min];
                // get the idx_max
                size_t increment = range_size?range_size:ranges[i];
                size_t idx_max = idx_min + increment-1;
                idx_max = idx_max<dataset_size?idx_max:dataset_size-1;
                increment = idx_max - idx_min +1;
                // get the max
                Data max = ds[idx_max];
                if (max<min) {
                    throw std::runtime_error("\033[1;91mError\033[0m Something went wrong...\n           [min] " + std::to_string(min) + "\n           [max] " + std::to_string(max) + "\n           [label] " + label + "\n");
                }
                _start_ = std::chrono::high_resolution_clock::now();
                std::vector<Payload> payload = table.lookup_range(min,max);
                _end_ = std::chrono::high_resolution_clock::now();
                if (payload.size() != increment) {
                    throw std::runtime_error("\033[1;91mError\033[0m Data not found...\n           [min] " + std::to_string(min) + "\n           [max] " + std::to_string(max) + "\n           [label] " + label + "\n");
                }
                tot_time_probe += _end_ - _start_;
                probe_count += increment;
                // update i
                i += increment;
            } else i++;
        }
    done:
        json benchmark;
        benchmark["dataset_size"] = dataset_size;
        benchmark["probe_elem_count"] = probe_count;
        benchmark["tot_time_probe_s"] = tot_time_probe.count();
        benchmark["point_query_%"] = point_query_perc;
        benchmark["dataset_name"] = dataset_name;
        benchmark["function_name"] = HashFn::name();
        benchmark["insert_fail_message"] = fail_what;
        benchmark["label"] = label;

        if (insert_fail)
            std::cout << "\033[1;91mInsert failed >\033[0m " + label + "\n";
        else std::cout << label + "\n";
        writer.add_data(benchmark);
    }

    // point-vs-range
    template <class HashFn, class HashTable>
    inline void point_vs_range(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t point_query_perc) {
        range_helper<HashFn,HashTable>(ds_obj, writer, point_query_perc);
    }

    // full range
    template <class HashFn, class HashTable>
    inline void range_throughput(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, size_t range_size) {
        range_helper<HashFn,HashTable>(ds_obj, writer, /* % of point queries */ 0, range_size);
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
