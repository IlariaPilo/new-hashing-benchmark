#pragma once

#include <vector>
#include <chrono>
#include <utility>
#include <optional>
#include <tuple>

#include "generic_function.hpp"
#include "sort_indices.hpp"
#include "thirdparty/perfevent/PerfEvent.hpp"

// A simple wrapper to implement the Non Partitioned Hash Join (NPJ)

namespace join {
    // perf
    bool is_first = true;
    /**
     * Computes a classic inner join between a small table and a big one. 
     * To use our table implementations, we assume the small table does not have duplicates.
     * @param small_keys keys belonging to the smaller table
     * @param small_payloads payloads of the smaller table
     * @param big_keys keys belonging to the bigger table
     * @param big_payloads payloads of the bigger table
     * @param output_keys the keys resulting from the join
     * @param output_payloads the payloads resulting from the join
     * @return an optional storing the sort time, build time and the join time. 
     * If the optional is empty, the insertion in the hash table failed.
    */
    template <class Key, class Payload, class HashFn, class HashTable, size_t LoadPerc>
    std::optional<std::tuple<std::chrono::duration<double>,std::chrono::duration<double>,std::chrono::duration<double>>>
        npj_hash(
            std::vector<Key>& small_keys, std::vector<Payload>& small_payloads, /* table 1 */
            std::vector<Key>& big_keys, std::vector<Payload>& big_payloads,     /* table 2 */ 
            std::vector<Key>& output_keys, std::vector<std::pair<Payload,Payload>>& output_payloads,
            /* perf things */ bool is_perf = false, std::string perf_config = "", std::ostream& perf_out = std::cout) {

        // reserve space for output arrays
        output_keys.reserve(big_keys.size());
        output_payloads.reserve(big_keys.size());

        // ==================== time counters ==================== //
        std::chrono::high_resolution_clock::time_point start, end;
        std::chrono::duration<double> tot_build(0), tot_join(0), tot_sort(0);
        PerfEvent e_sort(!is_perf), e_insert(!is_perf), e_probe(!is_perf);
        // ======================================================= //

        // build the table for the smaller relation
        const size_t capacity = small_keys.size()*100/LoadPerc;

        if (_generic_::GenericFn<HashFn>::needs_sorted_samples()) {
            // sort samples
            if (is_perf)
                e_sort.startCounters();
            start = std::chrono::high_resolution_clock::now();
            sort_indices(small_keys, small_payloads);
            end = std::chrono::high_resolution_clock::now();
            if (is_perf)
                e_sort.stopCounters();
            tot_sort = end-start;
        }

        if (is_perf)
            e_insert.startCounters();
        start = std::chrono::high_resolution_clock::now();
        HashFn fn;
        _generic_::GenericFn<HashFn>::init_fn(fn,small_keys.begin(),small_keys.end(),capacity);
        HashTable table(capacity, fn);
        // insert in the table
        for (size_t i=0; i<small_keys.size(); i++) {
            try {
                table.insert(small_keys[i], small_payloads[i]);
            } catch(std::runtime_error& e) {
                // if we are here, we failed the insertion
                return std::nullopt;
            }
        }
        end = std::chrono::high_resolution_clock::now();
        if (is_perf)
            e_insert.stopCounters();
        tot_build = end - start;

        if (is_perf)
            e_probe.startCounters();
        start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<big_keys.size(); i++) {
            // look for the element
            std::optional<Payload> small_payload = table.lookup(big_keys[i]);
            if (small_payload.has_value()) {
                output_keys.push_back(big_keys[i]);
                output_payloads.push_back(std::make_pair(small_payload.value(), big_payloads[i]));
            }
        }
        end = std::chrono::high_resolution_clock::now();
        if (is_perf)
            e_probe.stopCounters();
        tot_join = end - start;

        output_keys.shrink_to_fit();
        output_payloads.shrink_to_fit();

        if (is_perf) {
            // print data
            perf_out << perf_config+ "sort,";
            e_sort.printReport(perf_out, small_keys.size(), /*printHeader*/ false, /*printData*/ true);
            perf_out << perf_config + "insert,";
            e_insert.printReport(perf_out, small_keys.size(), /*printHeader*/ false, /*printData*/ true);
            perf_out << perf_config + "join,";
            e_probe.printReport(perf_out, big_keys.size(), /*printHeader*/ false, /*printData*/ true);
        }

        return std::make_optional(std::make_tuple(tot_sort, tot_build, tot_join));
    }
    
}   // namespace join  
