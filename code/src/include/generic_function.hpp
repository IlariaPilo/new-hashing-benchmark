#pragma once

#include <vector>

#include "configs.hpp"

// generic_function.hpp - a file storing a wrapper for a generic hash function
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
            template <class RandomIt>
            GenericFn(const RandomIt &sample_begin, const RandomIt &sample_end, const size_t max_value) : 
                    max_value(max_value), reduction(ReductionFn(max_value)) {
                init_fn(this->fn, sample_begin, sample_end, max_value);
            }
            inline Key operator()(const Data &data) const {
                if constexpr (has_train_method<HashFn>::value) {
                    // is learned
                    return fn(data);
                }
                return reduction(fn(data));
            }
            inline static std::string name() {
                return HashFn::name();
            }
            template <class RandomIt>
            inline static void init_fn(HashFn& fn, const RandomIt &sample_begin, const RandomIt &sample_end, const size_t max_value) {
                // LEARNED FN
                if constexpr (has_train_method<HashFn>::value) {
                    // train model on sorted data
                    fn.train(sample_begin, sample_end, max_value);
                }
                // PERFECT FN
                else if constexpr (has_construct_method<HashFn>::value) { 
                    // construct perfect hash table
                    fn.construct(sample_begin, sample_end);
                }
            }
            inline static bool needs_sorted_samples() {
                // LEARNED FN
                if constexpr (has_train_method<HashFn>::value) {
                    return true;
                }
                // // PERFECT FN
                // else if constexpr (has_construct_method<HashFn>::value) { 
                // }
                return false;
            }

        private:
            size_t max_value;
            HashFn fn;
            ReductionFn reduction;
    };

}



