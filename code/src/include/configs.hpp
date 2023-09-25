#pragma once

#include <cstdint>
// Functions
#include <learned_hashing.hpp>
#include <hashing.hpp>
#include <exotic_hashing.hpp>
// Tables
#include <hashtable.hpp>

// ********************* CONFIGS ********************* //

#define MAX_SIZE 100000000      // 10^8, 100M
// Bias chance that element is kicked from second bucket in percent (i.e., value of 10 -> 10%)
#define KICK_BIAS_CHANCE 5

// ********************* DATA TYPES ********************* //

// Data - the size of every database entry
using Data = std::uint64_t;
// Key - the size of the result of the hash function
using Key = std::uint64_t;
// Payload - the value associated to the Data
using Payload = std::uint64_t;

// ********************* HASH FUNCTIONS ********************* //
using RMIHash_2 = learned_hashing::RMIHash<std::uint64_t, 2>;
using RMIHash_10 = learned_hashing::RMIHash<std::uint64_t, 10>;
using RMIHash_100 = learned_hashing::RMIHash<std::uint64_t, 100>;
using RMIHash_1k = learned_hashing::RMIHash<std::uint64_t, 1000>;
using RMIHash_10k = learned_hashing::RMIHash<std::uint64_t, 10000>;
using RMIHash_100k = learned_hashing::RMIHash<std::uint64_t, 100000>;
using RMIHash_1M = learned_hashing::RMIHash<std::uint64_t, 1000000>;
using RMIHash_10M = learned_hashing::RMIHash<std::uint64_t, 10000000>;
using RMIHash_100M = learned_hashing::RMIHash<std::uint64_t, 100000000>;

using RadixSplineHash_4 = learned_hashing::RadixSplineHash<std::uint64_t, 18, 4>;
using RadixSplineHash_16 = learned_hashing::RadixSplineHash<std::uint64_t, 18, 16>;
using RadixSplineHash_128 = learned_hashing::RadixSplineHash<std::uint64_t, 18, 128>;
using RadixSplineHash_1k = learned_hashing::RadixSplineHash<std::uint64_t, 18, 1024>;
using RadixSplineHash_100k = learned_hashing::RadixSplineHash<std::uint64_t, 18, 100000>;

using PGMHash_100k = learned_hashing::PGMHash<std::uint64_t, 100000, 100000, 500000000, float>;
using PGMHash_1k = learned_hashing::PGMHash<std::uint64_t, 1024, 1024, 500000000, float>;
using PGMHash_100 = learned_hashing::PGMHash<std::uint64_t, 128, 128, 500000000, float>;
using PGMHash_32 = learned_hashing::PGMHash<std::uint64_t, 32, 32, 500000000, float>;
using PGMHash_2 = learned_hashing::PGMHash<std::uint64_t, 2, 2, 500000000, float>;

using MURMUR = hashing::MurmurFinalizer<Key>;
using MultPrime64 = hashing::MultPrime64;
using FibonacciPrime64 = hashing::FibonacciPrime64;
using AquaHash = hashing::AquaHash<Key>;
using XXHash3 = hashing::XXHash3<Key>;
using MWHC = exotic_hashing::MWHC<Key>;
using BitMWHC = exotic_hashing::BitMWHC<Key>;
using RecSplit = exotic_hashing::RecSplit<std::uint64_t>;


// ********************* HASH TABLES ********************* //
using ReductionFn = hashing::reduction::DoNothing<Key>;
using FastModulo = hashing::reduction::FastModulo<Key>;

template <class HashFn>
using ChainedTable = hashtable::Chained<Key, Payload, 1 /*BucketSize*/, HashFn, ReductionFn>;

template <class HashFn>
using LinearTable = hashtable::Probing<Key, Payload, HashFn, ReductionFn, hashtable::LinearProbingFunc /*BucketSize = 1 by default*/>;

template <class HashFn>
using CuckooHashing = hashtable::Cuckoo<Key, Payload, 4 /*BucketSize*/, HashFn, XXHash3, ReductionFn, FastModulo, 
    hashtable::BiasedKicking<KICK_BIAS_CHANCE>>;
