// ============================================================== //
// extended from https://github.com/DominikHorn/hashing-benchmark //
// ============================================================== //
#pragma once

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <omp.h>

#include "builtins.hpp"

namespace dataset {

// Defines all IDs of the datasets.
enum class ID {
  SEQUENTIAL = 0,
  GAP_10 = 1,
  UNIFORM = 2,
  FB = 3,
  OSM = 4,
  WIKI = 5,
  NORMAL = 6,
  // now we begin with the variance datasets
  VAR_x2 = 7,
  VAR_x4 = 8,
  VAR_HALF = 9,
  VAR_QUART = 10,
  _NONE_ = 11
};
constexpr int ID_COUNT = 7;
constexpr int ID_ALL_COUNT = 11;
// Define the reverse ID
const std::unordered_map<int, ID> REVERSE_ID = {
    {0, ID::SEQUENTIAL},
    {1, ID::GAP_10},
    {2, ID::UNIFORM},
    {3, ID::FB},
    {4, ID::OSM},
    {5, ID::WIKI},
    {6, ID::NORMAL},
    {7, ID::VAR_x2},
    {8, ID::VAR_x4},
    {9, ID::VAR_HALF},
    {10, ID::VAR_QUART},
    {11, ID::_NONE_}
};

// ------------------ utility things ------------------ //
template <class T>
static void deduplicate_and_sort(std::vector<T>& vec) {
  std::sort(vec.begin(), vec.end());
  vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
  vec.shrink_to_fit();
}

template <class Key>
std::vector<Key> load(const std::string& filepath) {

  // parsing helper functions
  auto read_little_endian_8 = [](const std::vector<unsigned char>& buffer,
                                 uint64_t offset) {
    return static_cast<uint64_t>(buffer[offset + 0]) |
           (static_cast<uint64_t>(buffer[offset + 1]) << 8) |
           (static_cast<uint64_t>(buffer[offset + 2]) << (2 * 8)) |
           (static_cast<uint64_t>(buffer[offset + 3]) << (3 * 8)) |
           (static_cast<uint64_t>(buffer[offset + 4]) << (4 * 8)) |
           (static_cast<uint64_t>(buffer[offset + 5]) << (5 * 8)) |
           (static_cast<uint64_t>(buffer[offset + 6]) << (6 * 8)) |
           (static_cast<uint64_t>(buffer[offset + 7]) << (7 * 8));
  };
  auto read_little_endian_4 = [](const std::vector<unsigned char>& buffer,
                                 uint64_t offset) {
    return buffer[offset + 0] | (buffer[offset + 1] << 8) |
           (buffer[offset + 2] << (2 * 8)) | (buffer[offset + 3] << (3 * 8));
  };

  // Read file into memory from disk. Directly map file for more performance
  std::ifstream input(filepath, std::ios::binary | std::ios::ate);
  std::streamsize size = input.tellg();
  input.seekg(0, std::ios::beg);
  if (!input.is_open()) {
    std::cerr << "file '" + filepath + "' does not exist" << std::endl;
    return {};
  }

  const auto max_num_elements = (size - sizeof(std::uint64_t)) / sizeof(Key);
  std::vector<uint64_t> dataset(max_num_elements, 0);
  {
    std::vector<unsigned char> buffer(size);
    if (!input.read(reinterpret_cast<char*>(buffer.data()), size))
      throw std::runtime_error("Failed to read dataset '" + filepath + "'");

    // Parse file
    uint64_t num_elements = read_little_endian_8(buffer, 0);

    if (num_elements > max_num_elements) {
      throw std::runtime_error("\033[1;91mAssertion failed\033[0m num_elements<=max_num_elements\n           [num_elements] " + std::to_string(num_elements) + "\n           [max_num_elements] " + std::to_string(max_num_elements) + "\n");
    }
    switch (sizeof(Key)) {
      case sizeof(std::uint64_t):
        for (uint64_t i = 0; i < num_elements; i++) {
          // 8 byte header, 8 bytes per entry
          uint64_t offset = i * 8 + 8;
          dataset[i] = read_little_endian_8(buffer, offset);
        }
        break;
      case sizeof(std::uint32_t):
        for (uint64_t i = 0; i < num_elements; i++) {
          // 8 byte header, 4 bytes per entry
          uint64_t offset = i * 4 + 8;
          dataset[i] = read_little_endian_4(buffer, offset);
        }
        break;
      default:
        throw std::runtime_error(
            "unimplemented amount of bytes per value in dataset: " +
            std::to_string(sizeof(Key)));
    }
  }

  // remove duplicates from dataset and put it into random order
  deduplicate_and_sort(dataset);

  return dataset;
}

std::vector<ID> get_id_slice(int threadID, size_t thread_num, size_t how_many = ID_COUNT);


// ------------------ functions to be called from outside ------------------ //
/**
 * Loads the datasets values into memory
 * @return a sorted and deduplicated list of all members of the dataset
 */
template <class Data>
std::vector<Data> load_ds(const ID& id, const size_t& dataset_size, std::string dataset_directory) {
  /*static*/ std::random_device rd;
  /*static*/ std::default_random_engine rng(rd());

  // cache generated & sampled datasets to speed up repeated benchmarks
  /*static std::unordered_map<ID, std::unordered_map<size_t, std::vector<Data>>>
      datasets;

  // cache sosd dataset files to avoid expensive load operations
  static std::vector<Data> ds_fb, ds_osm, ds_wiki;

  // return cached (if available)
  const auto id_it = datasets.find(id);
  if (id_it != datasets.end()) {
    const auto ds_it = id_it->second.find(dataset_size);
    if (ds_it != id_it->second.end()) return ds_it->second;
  }*/

  // generate (or random sample) in appropriate size
  std::vector<Data> ds(dataset_size, 0);
  std::vector<Data> ds_sosd;
  switch (id) {
    case ID::SEQUENTIAL: {
      for (size_t i = 0; i < ds.size(); i++) ds[i] = i*10 + 20000;
      break;
    }
    case ID::GAP_10: {
      std::uniform_int_distribution<size_t> dist(0, 99999);
      for (size_t i = 0, num = 0; i < ds.size(); i++) {
        do num+=10;
        while (dist(rng) < 10000);
        ds[i] = num;
      }
      break;
    }
    case ID::UNIFORM: {
      std::uniform_int_distribution<Data> dist(0, std::pow(2, 40));
      for (size_t i = 0; i < ds.size(); i++) ds[i] = dist(rng);
      break;
    }
    case ID::NORMAL: {
      const auto mean = 100.0;
      const auto std_dev = 20.0;
      std::normal_distribution<> dist(mean, std_dev);
      for (size_t i = 0; i < ds.size(); i++) {
        // cutoff after 3 * std_dev
        const auto rand_val = std::max(mean - 3 * std_dev,
                                       std::min(mean + 3 * std_dev, dist(rng)));
        if (rand_val < mean - 3 * std_dev) {
          throw std::runtime_error("\033[1;91mAssertion failed\033[0m rand_val>=mean-3*std_dev\n           [rand_val] " + std::to_string(rand_val) + "\n           [mean] " + std::to_string(mean) + "\n           [std_dev] " + std::to_string(std_dev) + "\n");
        }
        if (rand_val > mean + 3 * std_dev) {
          throw std::runtime_error("\033[1;91mAssertion failed\033[0m rand_val<=mean+3*std_dev\n           [rand_val] " + std::to_string(rand_val) + "\n           [mean] " + std::to_string(mean) + "\n           [std_dev] " + std::to_string(std_dev) + "\n");
        }

        // rescale to [0, 2^50)
        const auto rescaled =
            (rand_val - (mean - 3 * std_dev)) * std::pow(2, 40);

        // round
        ds[i] = std::floor(rescaled);
      }
      break;
    }
    case ID::FB: {
      ds_sosd = load<Data>(dataset_directory+"/fb_200M_uint64");
      std::shuffle(ds_sosd.begin(), ds_sosd.end(),rng);
      // ds file does not exist
      if (ds_sosd.empty()) return {};
      size_t j=0;
      size_t i = 0;

      // sampling this way is only valid since ds_fb is shuffled!
      for (; j < ds_sosd.size() && i < ds.size(); j++) {
        if(log2(ds_sosd[j])<35.01 || log2(ds_sosd[j])>35.99){
          continue;
        }
        ds[i] = ds_sosd[j]-pow(2,35);
        i++;
      }
      break;
    }
    case ID::OSM: {
      ds_sosd = load<Data>(dataset_directory+"/osm_cellids_200M_uint64");
      std::shuffle(ds_sosd.begin(), ds_sosd.end(),rng);
      // ds file does not exist
      if (ds_sosd.empty()) return {};
      size_t j=0;
      size_t i = 0;
      // sampling this way is only valid since ds_osm is shuffled!
      for (; j < ds_sosd.size() && i < ds.size(); j++) {
          if(log2(ds_sosd[j])<62.01 || log2(ds_sosd[j])>62.99){
            continue;
          }
          ds[i] = ds_sosd[j]-pow(2,62);
          i++;
        }
      break;
    }
    case ID::WIKI: {
      ds_sosd = load<Data>(dataset_directory+"/wiki_ts_200M_uint64");
      std::shuffle(ds_sosd.begin(), ds_sosd.end(),rng);
      // ds file does not exist
      if (ds_sosd.empty()) return {};
       size_t j=0;
       size_t i = 0;
      // sampling this way is only valid since ds_wiki is shuffled!
      for (; j < ds_sosd.size() && i < ds.size(); j++) {
        ds[i] = ds_sosd[j];
        i++;
      }
      break;
    }
    case ID::VAR_x2: {
      std::uniform_int_distribution<Data> dist(0, std::pow(2, 40));
      for (size_t i = 0; i < ds.size(); i++) ds[i] = dist(rng);
      std::sort(ds.begin(),ds.end());
      double constant=1.414;
      for(size_t i=0;i<ds.size();i++) {
        uint64_t temp=i*std::pow(2, 40)/ds.size();
        uint64_t diff=0;
        if(temp>ds[i]) {
          diff=temp-ds[i];
          ds[i]=temp-(diff*constant);
        }
        else {
          diff=ds[i]-temp;
          ds[i]=temp+(diff*constant);
        }
      }
      break;
    }
    case ID::VAR_x4: {
      std::uniform_int_distribution<Data> dist(0, std::pow(2, 40));
      for (size_t i = 0; i < ds.size(); i++) ds[i] = dist(rng);
      std::sort(ds.begin(),ds.end());
      double constant=2;
      for(size_t i=0;i<ds.size();i++) {
        uint64_t temp=i*std::pow(2, 40)/ds.size();
        uint64_t diff=0;
        if(temp>ds[i]) {
          diff=temp-ds[i];
          ds[i]=temp-(diff*constant);
        }
        else {
          diff=ds[i]-temp;
          ds[i]=temp+(diff*constant);
        }
      }
      break;
    }
    case ID::VAR_HALF: {
      std::uniform_int_distribution<Data> dist(0, std::pow(2, 40));
      for (size_t i = 0; i < ds.size(); i++) ds[i] = dist(rng);
      std::sort(ds.begin(),ds.end());
      double constant=1.414;
      for(size_t i=0;i<ds.size();i++) {
        uint64_t temp=i*std::pow(2, 40)/ds.size();
        uint64_t diff=0;
        if(temp>ds[i]) {
          diff=temp-ds[i];
          ds[i]=temp-(diff/constant);
        }
        else {
          diff=ds[i]-temp;
          ds[i]=temp+(diff/constant);
        }
      }
      break;
    }
    case ID::VAR_QUART: {
      std::uniform_int_distribution<Data> dist(0, std::pow(2, 40));
      for (size_t i = 0; i < ds.size(); i++) ds[i] = dist(rng);
      std::sort(ds.begin(),ds.end());
      double constant=2;
      for(size_t i=0;i<ds.size();i++) {
        uint64_t temp=i*std::pow(2, 40)/ds.size();
        uint64_t diff=0;
        if(temp>ds[i]) {
          diff=temp-ds[i];
          ds[i]=temp-(diff/constant);
        }
        else {
          diff=ds[i]-temp;
          ds[i]=temp+(diff/constant);
        }
      }
      break;
    }

    default:
      throw std::runtime_error(
          "invalid datastet id " +
          std::to_string(static_cast<std::underlying_type<ID>::type>(id)));
  }

  // since std::numeric_limits<Data> is special (e.g., used as Sentinel),
  // systematically remove this from datasets with minimal impact on the
  // underlying distribution.
  for (auto& key : ds)
    if (key == std::numeric_limits<Data>::max()) key--;

  // deduplicate, sort before caching to avoid additional work in the future
  deduplicate_and_sort(ds);

  // cache dataset for future use
  /*
  const auto it = datasets.find(id);
  if (it == datasets.end()) {
    std::unordered_map<size_t, std::vector<Data>> map;
    map.insert({dataset_size, ds});
    datasets.insert({id, map});
  } else {
    it->second.insert({dataset_size, ds});
  }
  */
  return ds;
}

// Returns the dataset name, given the ID
inline std::string name(ID id) {
  switch (id) {
    case ID::SEQUENTIAL:
      return "seq";
    case ID::GAP_10:
      return "gap_10";
    case ID::UNIFORM:
      return "uniform";
    case ID::NORMAL:
      return "normal";
    case ID::FB:
      return "fb";
    case ID::OSM:
      return "osm";
    case ID::WIKI:
      return "wiki";
    case ID::VAR_x2:
      return "variance_x2";
    case ID::VAR_x4:
      return "variance_x4";
    case ID::VAR_HALF:
      return "variance_half";
    case ID::VAR_QUART:
      return "variance_quarter";
    case ID::_NONE_:
      return "no dataset"; 
  }
  return "unnamed";
};


// =============================== Dataset class =============================== //
template <class Data = std::uint64_t>
class Dataset {
  public:
    Dataset(ID id, size_t dataset_size, std::string dataset_directory = "") : id(id) {
      this->ds = load_ds<Data>(id, dataset_size, dataset_directory);
      this->dataset_size = ds.size();
    }
    ID get_id() const {
      return id;
    }
    size_t get_size() const {
      return dataset_size;
    }
    const std::vector<Data>& get_ds() const {
      return ds;
    }
    // Default constructor
    Dataset() :
        id(ID::_NONE_), dataset_size(0) {
      ds.clear();
    }
    // Destructor
    ~Dataset() {
    }
    // Copy constructor
    Dataset(const Dataset& other) : 
        id(other.id), dataset_size(other.dataset_size), ds(other.ds) {
    }
    // Copy assignment operator
    Dataset& operator=(const Dataset& other) {
      if (this != &other) { // Check for self-assignment
        id = other.id;
        dataset_size = other.dataset_size;
        ds = other.ds; // Copy the ds vector
      }
      return *this;
    }
    // Move constructor
    Dataset(Dataset&& other) noexcept : 
        id(other.id), dataset_size(other.dataset_size), ds(std::move(other.ds)) {
    }
    // Move assignment operator
    Dataset& operator=(Dataset&& other) noexcept {
      if (this != &other) {  // Check for self-assignment
        id = other.id;
        dataset_size = other.dataset_size;
        // Move the ds vector and reset the source object
        ds = std::move(other.ds);
      }
      return *this;
    }
    // two print utility functions
    void print_ds(size_t entries = 10) const {
      std::cout << "\nDataset " << name(id) << " | size " << dataset_size << std::endl;
      for (size_t i=0; i<entries && i<dataset_size; i++)
        std::cout << ds[i] << std::endl;
      std::cout << "------------------------\n";
    }
    template <class HashFn>
    void print_hash(const HashFn& fn, size_t entries = 10) const {
      std::cout << "\nDataset " << name(id) << " | size " << dataset_size << std::endl;
      std::cout << "Hash function " << HashFn::name() << std::endl;
      for (size_t i=0; i<entries && i<dataset_size; i++)
        std::cout << fn(ds[i]) << std::endl;
      std::cout << "------------------------\n";
    }

  private:
    ID id;
    size_t dataset_size;
    std::vector<Data> ds;
};

// =============================== CollectionDS class =============================== //
template <class Data = std::uint64_t>
class CollectionDS {
public:
    CollectionDS(size_t dataset_size, std::string dataset_directory, size_t thread_num, size_t how_many = ID_COUNT) : collection(how_many){
      // remove useless threads
      if (thread_num > how_many)
          thread_num = how_many;
      // start parallel computation
      #pragma omp parallel num_threads(thread_num)
      {
          int threadID = omp_get_thread_num();
          std::vector<ID> ids = get_id_slice(threadID, thread_num, how_many);

          for (auto id : ids) {
            // put the object into the array
            int i = static_cast<int>(id);
            collection[i] = Dataset<Data>(id, dataset_size, dataset_directory);
          }
      }
    }
    const Dataset<Data>& get_ds(ID id) {
      int i = static_cast<int>(id);
      return collection[i];
    }
    const Dataset<Data>& get_ds(int i) {
      return collection[i];
    }
    const std::vector<Dataset<Data>>& get_collection() {
      return collection;
    }

private:
    std::vector<Dataset<Data>> collection;
};
// ============================================================================= //

}
