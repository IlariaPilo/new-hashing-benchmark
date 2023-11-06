#pragma once

/* Taken from https://stackoverflow.com/questions/1577475/c-sorting-and-keeping-track-of-indexes */

#include <vector>
#include <numeric>      // std::iota
#include <algorithm>    // std::sort, std::stable_sort
#include "omp_qsort.hpp"

template <typename T, typename V>
void sort_indices(std::vector<T> &v, std::vector<V> &payload) {

    // initialize original index locations
    std::vector<size_t> idx(v.size());
    std::iota(idx.begin(), idx.end(), 0);

    // v[i1]<v[i2] ==> true
    // v[i1]>=v[i2] ==> false
    auto comparator = [&v](size_t i1, size_t i2) { return v[i1] < v[i2]; };
    // parallel sort idx
    par_q_sort_tasks<size_t>(idx, comparator);

    std::vector<T> copy_v = v;
    std::vector<V> copy_payload = payload;
    #pragma omp parallel for
    for (size_t i=0; i<v.size(); i++) {
        v[i] = copy_v[idx[i]];
        payload[i] = copy_payload[idx[i]];
    }
    
    return;
}