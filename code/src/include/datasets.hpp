// ============================================================== //
// extended from https://github.com/DominikHorn/hashing-benchmark //
// ============================================================== //
#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <omp.h>

namespace dataset {

// Defines all IDs of the datasets.
enum class ID {
  SEQUENTIAL = 0,
  GAPPED_10 = 1,
  UNIFORM = 2,
  FB = 3,
  OSM = 4,
  WIKI = 5,
  NORMAL = 6,
  // to count the number of elements
  COUNT
};
constexpr int ID_COUNT = static_cast<int>(ID::COUNT);
// Define the reverse ID
const std::unordered_map<int, ID> REVERSE_ID = {
    {0, ID::SEQUENTIAL},
    {1, ID::GAPPED_10},
    {2, ID::UNIFORM},
    {3, ID::FB},
    {4, ID::OSM},
    {5, ID::WIKI},
    {6, ID::NORMAL}
};

/**
 * Loads the datasets values into memory
 * @return a sorted and deduplicated list of all members of the dataset
 */
template <class Data = std::uint64_t>
std::vector<Data> load_cached(const ID& id, const size_t& dataset_size, std::string dataset_directory = "");

// Returns the dataset name, given the ID
inline std::string name(ID id);

// =============================== CollectionDS class =============================== //
template <class Data = std::uint64_t>
class CollectionDS {
public:
    CollectionDS(size_t dataset_size, std::string dataset_directory, size_t thread_num);

    // Declare other member functions if needed.

private:
    // Declare private members or methods here.
};

// =============================== Dataset class =============================== //
template <class Data = std::uint64_t>
class Dataset {
  public:
    Dataset(ID id, size_t dataset_size, std::string dataset_directory = "") : id(id) {
      ds = load_cached(id, dataset_size, dataset_directory);
      dataset_size = ds.size();
    }
    ID get_id() {
      return id;
    }
    size_t get_size() {
      return dataset_size;
    }
    const std::vector<Data>& get_ds() {
      return ds;
    }

  private:
    const ID id;
    const size_t dataset_size;
    const std::vector<Data> ds;
};
// ============================================================================= //
}
