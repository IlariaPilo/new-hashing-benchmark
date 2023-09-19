#pragma once

#include <cstdint>
#include "output_json.hpp"
#include "datasets.hpp"

namespace bm {
    // bm function pointer type
    template <class Data = std::uint64_t, class Key = std::uint64_t>
    using BMtype = void(*)(const dataset::Dataset<Data>&, JsonOutput&);

    // thread utility
    template <class Data = std::uint64_t, class Key = std::uint64_t>
    static void run_bms(const std::vector<BMtype<Data>>& bm_list, 
        size_t thread_num, const dataset::CollectionDS<Data>& collection, JsonOutput& writer);

    // ----------------- benchmarks list ----------------- //
    // collision
    template <class HashFn, class Data = std::uint64_t, class Key = std::uint64_t>
    static void collision_stats(const dataset::Dataset<Data>& ds, JsonOutput& writer);
}
