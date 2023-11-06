#pragma once

#include <vector>
#include <chrono>
#include <utility>
#include <optional>
#include <tuple>
#include <omp.h>

#include "generic_function.hpp"
#include "sort_indices.hpp"

// A simple wrapper to implement the Non Partitioned Hash Join (NPJ)

namespace join {

    /**
     * Computes a classic inner join between a small table and a big one. 
     * To use our table implementations, we assume the small table does not have duplicates.
     * @param small_keys keys belonging to the smaller table
     * @param small_payloads payloads of the smaller table
     * @param big_keys keys belonging to the bigger table
     * @param big_payloads payloads of the bigger table
     * @param output the keys-payloads resulting from the join
     * @return an optional storing the sort time, build time and the join time. 
     * If the optional is empty, the insertion in the hash table failed.
    */
    template <class Key, class Payload, class HashFn, class HashTable, size_t LoadPerc>
    std::optional<std::tuple<std::chrono::duration<double>,std::chrono::duration<double>,std::chrono::duration<double>>>
        npj_hash(
            std::vector<Key>& small_keys, std::vector<Payload>& small_payloads, /* table 1 */
            std::vector<Key>& big_keys, std::vector<Payload>& big_payloads,     /* table 2 */ 
            std::vector<std::pair<Key,std::pair<Payload,Payload>>>& output,    /* output */
            size_t THREADS = 1) {

        // custom merge reduction
        #pragma omp declare reduction (merge : std::vector<std::pair<Key,std::pair<Payload,Payload>>> : \
                    omp_out.insert(omp_out.end(), omp_in.begin(), omp_in.end()))
    
        // reserve space for output arrays
        output.reserve(big_keys.size());

        bool insert_fail = false;

        // ==================== time counters ==================== //
        std::chrono::high_resolution_clock::time_point start, end;
        std::chrono::duration<double> tot_build(0), tot_join(0), tot_sort(0);
        // ======================================================= //

        // build the table for the smaller relation
        const size_t capacity = small_keys.size()*100/LoadPerc;

        if (_generic_::GenericFn<HashFn>::needs_sorted_samples()) {
            // sort samples
            start = std::chrono::high_resolution_clock::now();
            sort_indices(small_keys, small_payloads);
            end = std::chrono::high_resolution_clock::now();
            tot_sort = end-start;
        }

        start = std::chrono::high_resolution_clock::now();
        HashFn fn;
        _generic_::GenericFn<HashFn>::init_fn(fn,small_keys.begin(),small_keys.end(),capacity);
        HashTable table(capacity, fn);
        // insert in the table
        #pragma omp parallel for num_threads(THREADS)
        for (size_t i=0; i<small_keys.size(); i++) {
            #pragma omp cancellation point for
            try {
                table.insert(small_keys[i], small_payloads[i]);
            } catch(std::runtime_error& e) {
                // if we are here, we failed the insertion
                #pragma omp cancel for
                insert_fail = true;
            }
        }
        end = std::chrono::high_resolution_clock::now();
        tot_build = end - start;

        if (insert_fail)
            return std::nullopt;

        start = std::chrono::high_resolution_clock::now();
        #pragma omp parallel for reduction(merge: output) num_threads(THREADS)
        for (size_t i=0; i<big_keys.size(); i++) {
            // look for the element
            std::optional<Payload> small_payload = table.lookup(big_keys[i]);
            if (small_payload.has_value()) {
                output.push_back(std::make_pair(big_keys[i],std::make_pair(small_payload.value(), big_payloads[i])));
            }
        }
        end = std::chrono::high_resolution_clock::now();
        tot_join = end - start;

        output.shrink_to_fit();

        return std::make_optional(std::make_tuple(tot_sort, tot_build, tot_join));
    }
    
}   // namespace join  
