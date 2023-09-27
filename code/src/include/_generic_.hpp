#pragma once

#include <vector>

#include "configs.hpp"

// _generic_.hpp - a file storing a wrapper for a generic hash function and a generic hash table
namespace _generic_ {
    // Define a helper type trait to check if 'train' member function exists with the desired signature
    // Thanks to https://blog.quasar.ai/2015/04/12/sfinae-hell-detecting-template-methods
    template <typename T>
    struct has_train_method {
        struct dummy { /* something */ };

        template <typename C, typename P>
        static auto test(P * p) -> decltype(std::declval<C>().train(
            std::declval<P>(), std::declval<P>(), std::declval<size_t>()),
            std::true_type());
        
        template <typename, typename>
        static std::false_type test(...);

        typedef decltype(test<T, dummy>(nullptr)) type;
        static const bool value = std::is_same<std::true_type, decltype(test<T, dummy>(nullptr))>::value;
    };

    // Define a helper type trait to check if 'construct' member function exists
    // Thanks to https://blog.quasar.ai/2015/04/12/sfinae-hell-detecting-template-methods
    template <typename T>
    struct has_construct_method {
        struct dummy { /* something */ };

        template <typename C, typename P>
        static auto test(P * p) -> decltype(std::declval<C>().construct(
            std::declval<P>(), std::declval<P>()), std::true_type());
        
        template <typename, typename>
        static std::false_type test(...);

        typedef decltype(test<T, dummy>(nullptr)) type;
        static const bool value = std::is_same<std::true_type, decltype(test<T, dummy>(nullptr))>::value;
    };

    template <class HashFn, class ReductionFn = FastModulo>
    class GenericFn {
        public:
            GenericFn(size_t max_value, const std::vector<Data>& ds = {}) : 
                    max_value(max_value), reduction(ReductionFn(max_value)) {
                init_fn(this->fn, max_value, ds);
            }
            Key operator()(const Data &data) const {
                if constexpr (!has_train_method<HashFn>::value && !has_construct_method<HashFn>::value)
                    return reduction(fn(data));
                return fn(data);
            }
            static std::string name() {
                return HashFn::name();
            }
            static void init_fn(HashFn& fn, size_t max_value, const std::vector<Data>& ds = {}) {
                // LEARNED FN
                if constexpr (has_train_method<HashFn>::value) {
                    // train model on sorted data
                    fn.train(ds.begin(), ds.end(), max_value);
                }
                // PERFECT FN
                else if constexpr (has_construct_method<HashFn>::value) {
                    // construct perfect hash table
                    fn.construct(ds.begin(), ds.end());
                }
            }

        private:
            size_t max_value;
            HashFn fn;
            ReductionFn reduction;
    };

    template <class HashTable, class HashFn>
    class GenericTable {
        public:
            GenericTable(size_t capacity, const std::vector<Data>& ds = {}) {
                // initialize function
                GenericFn<HashFn>::init_fn(this->fn, capacity, ds);
                // initialize table
                table = new HashTable(capacity, this->fn);
            }
            // Destructor to clean up dynamically allocated memory
            ~GenericTable() {
                delete table;
            }
            void insert(const Data& data, const Payload& value) {
                table->insert(fn(data), value);
            }
            std::optional<Payload> lookup(const Data& data) const {
                return table->lookup(fn(data));
            }
            static std::string name() {
                return HashTable::name();
            }

        private:
            HashTable* table;
            HashFn fn;
    };
}



