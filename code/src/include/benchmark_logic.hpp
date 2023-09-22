#pragma once
#include <chrono>
#include <vector>
#include <omp.h>
#include <cstdint>

#include "hash_categories.hpp"
#include "output_json.hpp"
#include "datasets.hpp"

namespace bm {
    // bm function pointer type
    template <class Data = std::uint64_t, class Key = std::uint64_t>
    using BMtype = void(*)(const dataset::Dataset<Data>&, JsonOutput&);

    template <class Data = std::uint64_t, class Key = std::uint64_t>
    std::vector<BMtype<Data,Key>> get_bm_slice(int threadID, size_t thread_num, std::vector<BMtype<Data,Key>>& bm_list) {
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
        std::vector<BMtype<Data,Key>> output;
        output.resize(slice);
        for(int i=start, j=0; i<end && i<BM_COUNT; i++, j++)
            output[j] = bm_list[i];
        return output;
    }

    // thread utility
    template <class Data = std::uint64_t, class Key = std::uint64_t>
    void run_bms(std::vector<BMtype<Data,Key>>& bm_list, 
            size_t thread_num, dataset::CollectionDS<Data>& collection, JsonOutput& writer) {
        // first of all, check thread compatibility
        if (thread_num > bm_list.size())
            thread_num = bm_list.size();
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
                for (BMtype<Data,Key> bm : slice) {
                    // run the function
                    bm(ds, writer);
                }
            }
        }
        // done!
    }

    // ----------------- benchmarks list ----------------- //
    // collision
    template <class HashFn = std::uint64_t, class Data = std::uint64_t, class Key = std::uint64_t>
    void collision_stats(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        HashFn fn;
        const std::string label = "Collisions:" + fn.name() + ":" + dataset_name;
   
        // ensure keys are sorted
        // std::sort(keys.begin(), keys.end(),
        //         [](const auto& a, const auto& b) { return a < b; });
        // start with the hash function

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
        std::vector<Key> keys;
        keys.resize(dataset_size, 0);

        HashCategories type = get_fn_type<HashFn>();
        if (type == HashCategories::UNKNOWN) {
            std::cerr << "Error: Hash function type is unknown. Failed :c" << std::endl;
            return;
        }

        // ====================== collision counters ====================== //
        size_t index;
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
            keys[index]++;
            tot_time += _end_ - _start_;
        }
        // count collisions
        for (auto k : keys) {
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

    // collision
    template <class HashFn = std::uint64_t, class Data = std::uint64_t, class Key = std::uint64_t>
    void gap_stats(const dataset::Dataset<Data>& ds_obj, JsonOutput& writer) {
        // Extract variables
        const size_t dataset_size = ds_obj.get_size();
        const std::string dataset_name = dataset::name(ds_obj.get_id());
        const std::vector<Data>& ds = ds_obj.get_ds();

        HashFn fn;
        const std::string label = "Gaps:" + fn.name() + ":" + dataset_name;
   
        // ensure keys are sorted
        // std::sort(keys.begin(), keys.end(),
        //         [](const auto& a, const auto& b) { return a < b; });
        // start with the hash function

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
}
