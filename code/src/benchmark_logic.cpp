#include <chrono>
#include <vector>
#include <omp.h>

#include "include/hash_categories.hpp"
#include "include/benchmark_logic.hpp"

namespace bm {
template <class Data = std::uint64_t, class Key = std::uint64_t>
std::vector<BMtype<Data,Key>> get_bm_slice(int threadID, size_t thread_num, const std::vector<BMtype<Data,Key>>& bm_list) {
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

template <class Data, class Key>
static void run_bms(const std::vector<BMtype<Data,Key>>& bm_list, 
    size_t thread_num, const dataset::CollectionDS<Data>& collection, JsonOutput& writer) {
    // first of all, check thread compatibility
    if (thread_num > bmList.size())
        thread_num = bm_list.size();
    // begin parallel computation
    #pragma omp parallel num_threads(thread_num)
    {
        int threadID = omp_get_thread_num();
        // get slice of benchmarks
        auto slice = get_bm_slice(threadID, thread_num, bm_list);
        // for each ds
        auto all_ds = collection.get_collection();
        for (dataset::Dataset<Data> ds : all_ds) {
            // for each function
            for (BMtype<Data,Key> bm : slice) {
                // run the function
                bm(ds, writer);
            }
        }
    }
    // done!
}

template <class HashFn, class Data, class Key>
static void collision_stats(const dataset::Dataset<Data>& ds, JsonOutput& writer) {
    // Extract variables
    const size_t dataset_size = ds.get_size();
    const std::string dataset_name = dataset::name(ds.get_id());
    #if PRINT
    std::cout << "performing setup... " << std::endl;
    auto start = std::chrono::steady_clock::now();
    #endif    
    // ensure keys are sorted
    // std::sort(keys.begin(), keys.end(),
    //         [](const auto& a, const auto& b) { return a < b; });
    // start with the hash function
    HashFn fn;

    #if PRINT
    std::cout << "has_train<HashFn>: " << has_train_method<HashFn>::value << std::endl;
    std::cout << "has_construct<HashFn>: " << has_construct_method<HashFn>::value << std::endl;
    #endif

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

    #if PRINT
    // measure time elapsed
    const auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "succeeded in " << std::setw(9) << diff.count() << " seconds"
                << std::endl;
    #endif

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
                throw std::runtime_error("Index is out of boundaries ("+std::to_string(index)+")");
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
    }

    json benchmark;

    benchmark["data_elem_count"] = dataset_size;
    benchmark["tot_time_s"] = tot_time.count();
    benchmark["collisions"] = collisions_count;
    benchmark["dataset_name"] = dataset_name;
    //benchmark["extra"] = extra;
    benchmark["label"] = "Collisions:" + std::string(typeid(HashFn).name()) + ":" + dataset_name;
    //    + dataset::name(did) + ":" + dataset::name(probing_dist) + ":" + std::to_string(extra);

    writer.add_data(benchmark);
}
}   // namespace bm 

