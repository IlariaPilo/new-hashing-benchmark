// Add definition for the tree types of functions
#pragma once

#include <unordered_map>
#include <string>
#include <iostream>

enum class HashCategories {
    LEARNED,
    CLASSIC,
    PERFECT,
    UNKNOWN
};

const std::unordered_map<std::string, HashCategories> HASH_FN_TYPES = {
    {"RMIHash", HashCategories::LEARNED},
    {"RadixSplineHash", HashCategories::LEARNED},
    {"PGMHash", HashCategories::LEARNED},
    {"MurmurFinalizer", HashCategories::CLASSIC},
    {"MultiplicationHash", HashCategories::CLASSIC},
    {"AquaHash", HashCategories::CLASSIC},
    {"XXHash3", HashCategories::CLASSIC},
    {"MWHC", HashCategories::PERFECT},
    {"BitMWHC", HashCategories::PERFECT},
    {"RecSplit", HashCategories::PERFECT}
};

HashCategories get_category(std::string full_name) {
    for (const auto& pair : HASH_FN_TYPES) {
        std::string name = pair.first;
        size_t pos = full_name.find(name);
        if (pos != std::string::npos) {
            return pair.second;
        }
    }
    // nothing was found
    std::cout << full_name << std::endl;
    return HashCategories::UNKNOWN;
}

// Function to check if a type is "learned"
// To be called as : if (is_learned<RMIHash>()) ...
template <typename T>
bool is_learned() {
    std::string full_name = typeid(T).name();
    HashCategories type = get_category(full_name);
    return (type == HashCategories::LEARNED);
}
template <typename T>
bool is_classic() {
    std::string full_name = typeid(T).name();
    HashCategories type = get_category(full_name);
    return (type == HashCategories::CLASSIC);
}
template <typename T>
bool is_perfect() {
    std::string full_name = typeid(T).name();
    HashCategories type = get_category(full_name);
    return (type == HashCategories::PERFECT);
}
template <typename T>
HashCategories get_fn_type() {
    std::string full_name = typeid(T).name();
    return get_category(full_name);
}

// Define a helper type trait to check if 'train' member function exists with the desired signature
// Thanks to https://blog.quasar.ai/2015/04/12/sfinae-hell-detecting-template-methods
template <typename T>
struct has_train_method {
    struct dummy { /* something */ };

    // Modify this line to check for 'train' instead of 'bam'
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

    // Modify this line to check for 'train' instead of 'bam'
    template <typename C, typename P>
    static auto test(P * p) -> decltype(std::declval<C>().construct(
        std::declval<P>(), std::declval<P>()), std::true_type());
    
    template <typename, typename>
    static std::false_type test(...);

    typedef decltype(test<T, dummy>(nullptr)) type;
    static const bool value = std::is_same<std::true_type, decltype(test<T, dummy>(nullptr))>::value;
};
