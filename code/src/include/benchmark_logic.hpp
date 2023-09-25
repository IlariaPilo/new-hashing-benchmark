#pragma once
#include <chrono>
#include <vector>
#include <omp.h>
#include <cstdint>
#include <random>

#include "hash_categories.hpp"
#include "output_json.hpp"
#include "datasets.hpp"
#include "configs.hpp"

namespace bm {
    // bm function pointer type
    using BMtype = void(*)(const dataset::Dataset<Data>&, JsonOutput&);

    // ----------------- utility things ----------------- //
    std::vector<int> order_insert; // will store all values from 0 to N-1
    std::vector<int> order_probe;  // will store uniformly sampled values from 0 to N-1

    void generate_insert_order(size_t N = 100000000) {
        order_insert.clear();
        for (size_t i = 0; i < N; ++i) {
            order_insert.push_back(i);
        }
        std::random_shuffle(order_insert.begin(), order_insert.end());
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

    std::vector<BMtype> get_bm_slice(int threadID, size_t thread_num, std::vector<BMtype>& bm_list) {
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

        /* Create the vector */
        std::vector<BMtype> output;
        output.resize(slice);
        for(int i=start, j=0; i<end && i<BM_COUNT; i++, j++)
            output[j] = bm_list[i];
        return output;
    }

    // thread utility
    void run_bms(std::vector<BMtype>& bm_list, 
            size_t thread_num, dataset::CollectionDS<Data>& collection, JsonOutput& writer, size_t N = 100000000) {
        // first of all, check thread compatibility
        if (thread_num > bm_list.size())
            thread_num = bm_list.size();
        // initialize arrays to keep things sorted
        generate_insert_order(N);
        generate_probe_order(N);
        // begin parallel computation
        #pragma omp parallel num_threads(thread_num)
        {
            int threadID = omp_get_thread_num();
            // get slice of benchmarks
            auto slice = get_bm_slice(threadID, thread_num, bm_list);
            // for each ds
            for (int i=0; i<dataset::ID_COUNT; i++) {
                // get the ds
                const dataset::Dataset<Data>& ds = collection.get_ds(i);
                // for each function
                for (BMtype bm : slice) {
                    // run the function
                    bm(ds, writer);
                }
            }
        }
        // done!
    }

    // ----------------- benchmarks list ----------------- //
    // collision
    template <class HashFn>
    void collision_stats(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        HashFn fn;
        const std::string label = "Collisions:" + fn.name() + ":" + dataset_name;

        // LEARNED FN
        if constexpr (has_train_method<HashFn>::value) {
            // train model on sorted data
            fn.train(ds.begin(), ds.end(), dataset_size);
        }
        // PERFECT FN
        if constexpr (has_construct_method<HashFn>::value) {
            // construct perfect hash table
            fn.construct(ds.begin(), ds.end());
        }

        // now, start counting collisions

        // stores the hash value (that is, the key) for each entry in the dataset
        std::vector<Key> keys_count;
        keys_count.resize(dataset_size, 0);

        HashCategories type = get_fn_type<HashFn>();
        if (type == HashCategories::UNKNOWN) {
            std::cerr << "Error: Hash function type is unknown. Failed :c" << std::endl;
            return;
        }

        // ====================== collision counters ====================== //
        Key index;
        std::chrono::time_point<std::chrono::steady_clock> _start_, _end_;
        std::chrono::duration<double> tot_time(0);
        size_t collisions_count = 0;
        size_t NOT_collisions_count = 0;
        // ================================================================ //

        for (auto data : ds) {
            /* TODO - remove this useless logic as soon as the index is out of boundaries thing is fixed */
            switch (type) {
            case HashCategories::PERFECT:
            case HashCategories::CLASSIC:
                _start_ = std::chrono::steady_clock::now();
                index = fn(data) % dataset_size;
                _end_ = std::chrono::steady_clock::now();
                break;
            case HashCategories::LEARNED:
                _start_ = std::chrono::steady_clock::now();
                index = fn(data); // /dataset_size;
                _end_ = std::chrono::steady_clock::now();
                if (index >= dataset_size) {
                    // Throw a runtime exception
                    throw std::runtime_error("Hash is out of boundaries ("+std::to_string(index)+")");
                }
                break;
            // to remove the warning
            case HashCategories::UNKNOWN:
                break;
            }
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

        benchmark["data_elem_count"] = dataset_size;
        benchmark["tot_time_s"] = tot_time.count();
        benchmark["collisions"] = collisions_count;
        benchmark["dataset_name"] = dataset_name;
        // benchmark["extra"] = extra;
        benchmark["label"] = label; 
        std::cout << label + "\n";
        writer.add_data(benchmark);
    }

    // gaps
    template <class HashFn>
    void gap_stats(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        HashFn fn;
        const std::string label = "Gaps:" + fn.name() + ":" + dataset_name;

        // LEARNED FN
        if constexpr (has_train_method<HashFn>::value) {
            // train model on sorted data
            fn.train(ds.begin(), ds.end(), dataset_size);
        }
        // PERFECT FN
        if constexpr (has_construct_method<HashFn>::value) {
            // construct perfect hash table
            fn.construct(ds.begin(), ds.end());
        }

        // now, start counting collisions

        // stores the list of hash values (keys)
        std::vector<Key> keys;
        keys.reserve(dataset_size);

        HashCategories type = get_fn_type<HashFn>();
        if (type == HashCategories::UNKNOWN) {
            std::cerr << "Error: Hash function type is unknown. Failed :c" << std::endl;
            return;
        }

        size_t index;

        for (auto data : ds) {
            /* TODO - remove this useless logic as soon as the index is out of boundaries thing is fixed */
            switch (type) {
            case HashCategories::PERFECT:
            case HashCategories::CLASSIC:
                index = fn(data) % dataset_size;
                break;
            case HashCategories::LEARNED:
                index = fn(data); // /dataset_size;
                if (index >= dataset_size) {
                    // Throw a runtime exception
                    throw std::runtime_error("Hash is out of boundaries ("+std::to_string(index)+")");
                }
                break;
            // to remove the warning
            case HashCategories::UNKNOWN:
                break;
            }
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

        benchmark["data_elem_count"] = dataset_size;
        benchmark["dataset_name"] = dataset_name;
        benchmark["count"] = count_json;
        benchmark["label"] = label; 
        std::cout << label + "\n";
        writer.add_data(benchmark);
    }

    // // probe throughput HIDDEN
    // template <class HashTable, class HashFn>
    // void _probe_throughput_(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer, const size_t& load_perc) {
    //     // Extract variables
    //     const size_t dataset_size = ds_obj.get_size();
    //     const std::string dataset_name = dataset::name(ds_obj.get_id());
    //     const std::vector<Data>& ds = ds_obj.get_ds();

    //     // Compute capacity given the laod% and the dataset_size
    //     sitze_t capacity = load_perc*dataset_size/100;

    //     // first, setup the hash function if necessary
    //     HashFn fn;
    //     // LEARNED FN
    //     if constexpr (has_train_method<HashFn>::value) {
    //         // train model on sorted data
    //         fn.train(ds.begin(), ds.end(), capacity);
    //     }
    //     // PERFECT FN
    //     if constexpr (has_construct_method<HashFn>::value) {
    //         // construct perfect hash table
    //         fn.construct(ds.begin(), ds.end());
    //     }
    //     // now, create the table
    //     HashTable /*<Key,Payload,HashFn>*/ table(capacity, fn /*fn2 if cuckoo*/);

    //     const std::string label = "Probe:" + table.name + ":" + fn.name() + ":" + dataset_name + ":" + std::to_string(load_perc);

    //     // Build the table
    //     Payload count = 0;
    //     for (int i : order_insert) {
    //         // check if the index exists
    //         if (i < dataset_size) {
    //             // get the data
    //             Data data = ds[i];
    //             // get the key
    //             Key k = fn(data) % capacity;
    //             table.insert(k, count);
    //             count++;
    //         }
    //     }
    //     // ====================== throughput counters ====================== //
    //     std::chrono::time_point<std::chrono::steady_clock> _start_, _end_;
    //     std::chrono::duration<double> tot_time(0);
    //     size_t probe_count = 0;
    //     // ================================================================ //

    //     for (int i : order_probe) {
    //         // check if the index exists
    //         if (i < dataset_size) {
    //             // get the data
    //             Data data = ds[i];
    //             _start_ = std::chrono::steady_clock::now();
    //             // get the key
    //             Key k = fn(data) % capacity;
    //             /*std::optional<Payload> payload = */ table.lookup(k);
    //             _end_ = std::chrono::steady_clock::now();
    //             probe_count++;
    //         }
    //     }
    //     json benchmark;

    //     benchmark["data_elem_count"] = dataset_size;
    //     benchmark["probe_elem_count"] = probe_count;
    //     benchmark["tot_time_s"] = tot_time.count();
    //     benchmark["load_factor_%"] = load_perc;
    //     benchmark["dataset_name"] = dataset_name;
    //     benchmark["label"] = label; 
    //     std::cout << label + "\n";
    //     writer.add_data(benchmark);
    // }

}
