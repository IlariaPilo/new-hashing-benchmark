/* Taken from github.com/Michael-Beukman/OpenMPQuicksort */
#pragma once

#include <omp.h>
#include <vector>
#include <functional>

#define TASK_LIMIT 1000

template <class T>
int partition(int l, int r, std::vector<T>& data, std::function<bool(T, T)> comparator){
    T pivot = data[l];
    int left = l;
    int right = r;
    while (left < right){
        // increase until you get to a point where the element is greater that the pivot
        while (left < r && !comparator(pivot, data[left])) ++left;
        // decrease until you get to a point where the element is less or equal to the pivot
        while (right >= 0 && comparator(pivot, data[right])) --right;
        // swap if within bounds
        if (left < right && left < r && right >= 0){
            std::swap(data[left], data[right]);
        }
    }
    // swap at the end
    if (left < right && left <r && right >= 0){
        std::swap(data[left], data[right]);
    }
    data[l] = data[right];
    data[right] = pivot;
    return right;
}

template <class T>
void seq_qsort(int l, int r,  std::vector<T>& data, std::function<bool(T, T)> comparator) {
    if (l < r) {
        int q = partition<T>(l, r, data, comparator);
        seq_qsort(l, q - 1, data, comparator);
        seq_qsort(q + 1, r, data, comparator);
    }
}
template <class T>
void q_sort_tasks(int l, int r,  std::vector<T>& data, int low_limit, std::function<bool(T, T)> comparator) {
    if (l < r) {
        if (r - l < low_limit) {
            // small list => sequential.
            return seq_qsort<T>(l, r, data, comparator);
        } else {
            int q = partition<T>(l, r, data, comparator);
            // create two tasks
            #pragma omp task shared(data)
                q_sort_tasks<T>(l, q - 1, data, low_limit, comparator);
            #pragma omp task shared(data)
                q_sort_tasks<T>(q + 1, r, data, low_limit, comparator);
        }
    } 
}

template <class T>
void par_q_sort_tasks(std::vector<T>& data, std::function<bool(T, T)> comparator){
    int l = 0;
    int r = data.size()-1;
    #pragma omp parallel
    {
        #pragma omp single
        q_sort_tasks<T>(l, r, data, TASK_LIMIT - 1, comparator);
        #pragma omp taskwait
    }
}