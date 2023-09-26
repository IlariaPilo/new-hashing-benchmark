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

    template <class HashFn>
    class GenericFn {
        public:
            GenericFn() : max_value(0) {};
            GenericFn(size_t max_value, const std::vector<Data>& ds = {}) : max_value(max_value) {
                // check if HashFn requires initialization
                // LEARNED FN
                if constexpr (has_train_method<HashFn>::value) {
                    // train model on sorted data
                    fn.train(ds.begin(), ds.end(), max_value);
                }
                // PERFECT FN
                if constexpr (has_construct_method<HashFn>::value) {
                    // construct perfect hash table
                    fn.construct(ds.begin(), ds.end());
                }
            }
            void init(size_t max_value, const std::vector<Data>& ds = {}) {
                if (this->max_value != 0)
                    // no init needed
                    return;
                this->max_value = max_value;
                // LEARNED FN
                if constexpr (has_train_method<HashFn>::value) {
                    // train model on sorted data
                    fn.train(ds.begin(), ds.end(), max_value);
                }
                // PERFECT FN
                if constexpr (has_construct_method<HashFn>::value) {
                    // construct perfect hash table
                    fn.construct(ds.begin(), ds.end());
                }
            }
            Key operator()(const Data &data) const {
                return fn(data) % this->max_value;
            }
            std::string name() {
                return fn.name();
            }
            HashFn& get_fn() {
                return fn;
            }

        private:
            size_t max_value;
            HashFn fn;
    };

    template <class HashTable, class HashFn>
    class GenericTable {
        public:
            GenericTable(size_t capacity, const std::vector<Data>& ds_fn = {}) {
                // initialize function
                fn.init(capacity, ds_fn);
                // initialize table
                table = new HashTable(capacity, fn.get_fn());
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
            std::string name() {
                return table->name();
            }

        private:
            HashTable* table;
            GenericFn<HashFn> fn;
    };
}



