#pragma once

#include <cstdint>
// Functions
#include <learned_hashing.hpp>
#include <hashing.hpp>
#include <exotic_hashing.hpp>
// Tables
#include <hashtable.hpp>
#include "rmi_sort.hpp"
// Datasets
#include "datasets.hpp"

// ********************* CONFIGS ********************* //

// The maximum dataset size
#define MAX_DS_SIZE 100000000      // 10^8, 100M

// ---- Probe Experiments ---- //
// [Cuckoo Table] Bias chance that element is kicked from second bucket in percent (i.e., value of 10 -> 10%)
#define KICK_BIAS_CHANCE 5
// [Linear Probing Table] maximum amount of probing steps
#define MAX_PROBING_STEPS 1000000
// load factors for each table
constexpr size_t chained_lf[] = {25,50,75,100,125,150,200};
constexpr size_t linear_lf[] = {25,35,45,55,65,75};
constexpr size_t cuckoo_lf[] = {75,80,85,90,95};
// datasets (when changing them, take a look at the benchmarks.cpp file too!)
constexpr dataset::ID probe_insert_ds[] = {dataset::ID::GAP_10,dataset::ID::NORMAL,dataset::ID::WIKI,dataset::ID::FB,dataset::ID::OSM};

// ---- Range Experiments ---- //
// load factor
#define RANGE_LOAD_PERC 50
// number of buckets
#define RANGE_BUCKETS 8
// percentage of point queries
constexpr size_t point_queries_perc[] = {0,10,20,30,40,50,60,70,80,90,100};
// size of the query range
constexpr size_t range_len[] = {1,2,4,8,16,32,64,128,256,512,1024};
// datasets
constexpr dataset::ID range_ds[] = {dataset::ID::WIKI,dataset::ID::FB};

// ---- Distribution Experiments ---- //
// load factors
constexpr size_t collisions_vs_gaps_lf[] = {10,25,50,70,100};
// datasets
constexpr dataset::ID collisions_vs_gaps_ds[] = {dataset::ID::UNIFORM,dataset::ID::VAR_x2,dataset::ID::VAR_x4,dataset::ID::VAR_HALF,dataset::ID::VAR_QUART};

// ---- Build Experiments ---- //
// number of entries
constexpr size_t build_entries[] = {MAX_DS_SIZE/100, MAX_DS_SIZE/20, MAX_DS_SIZE/10, MAX_DS_SIZE/2, MAX_DS_SIZE};
// datasets
constexpr dataset::ID build_time_ds[] = {dataset::ID::UNIFORM};

// ---- Everything Else ---- //
// datasets for remaining experiments
constexpr dataset::ID collisions_ds[] = {dataset::ID::GAP_10,dataset::ID::UNIFORM,dataset::ID::NORMAL,dataset::ID::WIKI,dataset::ID::FB};
constexpr dataset::ID gaps_ds[] = {dataset::ID::GAP_10,dataset::ID::UNIFORM,dataset::ID::NORMAL,dataset::ID::WIKI,dataset::ID::FB,dataset::ID::OSM};

// ********************* DATA TYPES ********************* //

// Data - the size of every database entry
using Data = std::uint64_t;
// Key - the size of the result of the hash function
using Key = std::uint64_t;
// Payload - the value associated to the Data
using Payload = std::uint64_t;

// ********************* HASH FUNCTIONS ********************* //
// learned_hashing::RMIHash<Data, size_t MaxSecondLevelModelCount>
using RMIHash_2 = learned_hashing::RMIHash<Data, 2>;
using RMIHash_10 = learned_hashing::RMIHash<Data, 10>;
using RMIHash_100 = learned_hashing::RMIHash<Data, 100>;
using RMIHash_1k = learned_hashing::RMIHash<Data, 1000>;
using RMIHash_10k = learned_hashing::RMIHash<Data, 10000>;
using RMIHash_100k = learned_hashing::RMIHash<Data, 100000>;
using RMIHash_1M = learned_hashing::RMIHash<Data, 1000000>;
using RMIHash_10M = learned_hashing::RMIHash<Data, 10000000>;
using RMIHash_100M = learned_hashing::RMIHash<Data, 100000000>;
//
using RMIMonotone = learned_hashing::MonotoneRMIHash<Data, 10000000>;

// learned_hashing::RadixSplineHash<Data, size_t NumRadixBits, size_t MaxError>
// notice that MaxError is the parameter controlling the number of models
using RadixSplineHash_4 = learned_hashing::RadixSplineHash<Data, 18, 4>;
using RadixSplineHash_16 = learned_hashing::RadixSplineHash<Data, 18, 16>;
using RadixSplineHash_128 = learned_hashing::RadixSplineHash<Data, 18, 128>;
using RadixSplineHash_1k = learned_hashing::RadixSplineHash<Data, 18, 1024>;
using RadixSplineHash_100k = learned_hashing::RadixSplineHash<Data, 18, 100000>;

// learned_hashing::PGMHash<Key, size_t Epsilon, size_t EpsilonRecursive, size_t MaxModels>
using PGMHash_100k = learned_hashing::PGMHash<Data, 100000, 100000, 500000000>;
using PGMHash_1k = learned_hashing::PGMHash<Data, 1024, 1024, 500000000>;
using PGMHash_100 = learned_hashing::PGMHash<Data, 128, 128, 500000000>;
using PGMHash_32 = learned_hashing::PGMHash<Data, 32, 32, 500000000>;
using PGMHash_2 = learned_hashing::PGMHash<Data, 2, 2, 500000000>;

using MURMUR = hashing::MurmurFinalizer<Data>;
using MultPrime64 = hashing::MultPrime64;
using FibonacciPrime64 = hashing::FibonacciPrime64;
using AquaHash = hashing::AquaHash<Data>;
using XXHash3 = hashing::XXHash3<Data>;
using MWHC = exotic_hashing::MWHC<Data>;
using BitMWHC = exotic_hashing::BitMWHC<Data>;
using RecSplit = exotic_hashing::RecSplit<Data>;


// ********************* HASH TABLES ********************* //
using FastModulo = hashing::reduction::FastModulo<Key>;

template <class HashFn, class ReductionFn = FastModulo>
using ChainedTable = hashtable::Chained<Key, Payload, 1 /*BucketSize*/, HashFn, ReductionFn>;

template <class HashFn, class ReductionFn = FastModulo>
using LinearTable = hashtable::Probing<Key, Payload, HashFn, ReductionFn, hashtable::LinearProbingFunc, MAX_PROBING_STEPS /*BucketSize = 1 by default*/>;

template <class HashFn, class ReductionFn = FastModulo>
using CuckooTable = hashtable::Cuckoo<Key, Payload, 4 /*BucketSize*/, HashFn, XXHash3, ReductionFn, FastModulo, 
    hashtable::BiasedKicking<KICK_BIAS_CHANCE>>;

// Chained table for range experiments
template <class HashFn>
using ChainedRange = hashtable::Chained<Key, Payload, RANGE_BUCKETS /*BucketSize*/, HashFn, FastModulo>;
template <class HashFn>
using RMISortRange = hashtable::RMISort<Key, Payload, HashFn>;