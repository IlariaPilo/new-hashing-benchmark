#pragma once

/* Taken from https://stackoverflow.com/questions/1577475/c-sorting-and-keeping-track-of-indexes */

#include <vector>
#include <numeric>      // std::iota
#include <algorithm>    // std::sort, std::stable_sort

template <typename T, typename V>
void sort_indices(std::vector<T> &v, std::vector<V> &payload) {

    // initialize original index locations
    std::vector<size_t> idx(v.size());
    std::iota(idx.begin(), idx.end(), 0);

    // sort indexes based on comparing values in v
    // using std::stable_sort instead of std::sort
    // to avoid unnecessary index re-orderings
    // when v contains elements of equal values 
    std::stable_sort(idx.begin(), idx.end(),
        [&v](size_t i1, size_t i2) {return v[i1] < v[i2];});

    std::vector<T> copy_v = v;
    std::vector<V> copy_payload = payload;
    for (size_t i=0; i<v.size(); i++) {
        v[i] = copy_v[idx[i]];
        payload[i] = copy_payload[idx[i]];
    }
    
    return;
}