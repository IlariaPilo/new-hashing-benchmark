#pragma once

// An implementation of the RMI-Hash by Dominik Horn, 
// using coroutines to prefetch from the submodel array.

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <string>
#include <vector>

#include "../builtins.hpp"

// ------ coro things ------ //
#include "cppcoro/coroutine.hpp"

#include "prefetch.hpp"
#include "scheduler.hpp"
#include "throttler.hpp"
// ------------------------- //

#include "chained-coro.hpp"

namespace rmi_coro
{
  template <class X, class Y>
  struct DatapointImpl
  {
    X x;
    Y y;

    DatapointImpl(const X x, const Y y) : x(x), y(y) {}
  };

  template <class Key, class Precision>
  struct LinearImpl
  {
  protected:
    Precision slope = 0, intercept = 0;

  private:
    using Datapoint = DatapointImpl<Key, Precision>;

    static forceinline Precision compute_slope(const Datapoint &min,
                                               const Datapoint &max)
    {
      if (min.x == max.x)
        return 0;

      // slope = delta(y)/delta(x)
      return ((max.y - min.y) / (max.x - min.x));
    }

    static forceinline Precision compute_intercept(const Datapoint &min,
                                                   const Datapoint &max)
    {
      // f(min.x) = min.y <=> slope * min.x + intercept = min.y <=> intercept =
      // min.y - slope * min.x
      return (min.y - compute_slope(min, max) * min.x);
    }

    static forceinline Precision compute_slope(const Key &minX,
                                               const Precision &minY,
                                               const Key &maxX,
                                               const Precision &maxY)
    {
      if (minX == maxX)
        return 0;

      // slope = delta(y)/delta(x)
      return ((maxY - minY) / (maxX - minX));
    }

    static forceinline Precision compute_intercept(const Key &minX,
                                                   const Precision &minY,
                                                   const Key &maxX,
                                                   const Precision &maxY)
    {
      // f(min.x) = min.y <=> slope * min.x + intercept = min.y <=> intercept =
      // min.y - slope * min.x
      return (minY - compute_slope(minX, minY, maxX, maxY) * minX);
    }

    explicit LinearImpl(const Key &minX, const Precision &minY, const Key &maxX,
                        const Precision &maxY)
        : slope(compute_slope(minX, minY, maxX, maxY)),
          intercept(compute_intercept(minX, minY, maxX, maxY)) {}

  public:
    explicit LinearImpl(Precision slope = 0, Precision intercept = 0)
        : slope(slope), intercept(intercept) {}

    /**
     * Performs trivial linear regression on the datapoints (i.e., computing
     * max->min spline)
     *
     * @param datapoints *sorted* datapoints to 'train' on
     * @param output_range outputs will be in range [0, output_range]
     */
    explicit LinearImpl(const std::vector<Datapoint> &datapoints)
        : slope(compute_slope(datapoints.front(), datapoints.back())),
          intercept(compute_intercept(datapoints.front(), datapoints.back()))
    {
      assert(slope != NAN);
      assert(intercept != NAN);
    }

    /**
     * Performs trivial linear regression on the datapoints (i.e., computing
     * max->min spline).
     *
     * @param keys sorted key array
     * @param begin first key contained in the training bucket
     * @param end last key contained in the training bucket
     */
    template <class It>
    LinearImpl(const It &dataset_begin, const It &dataset_end, size_t begin,
               size_t end)
        : LinearImpl(
              *(dataset_begin + begin),
              static_cast<Precision>(begin) /
                  static_cast<Precision>(
                      std::distance(dataset_begin, dataset_end)),
              *(dataset_begin + end),
              static_cast<Precision>(end) / static_cast<Precision>(std::distance(
                                                dataset_begin, dataset_end))) {}

    template <class It>
    LinearImpl(const It &dataset_begin, const It &dataset_end, size_t /*begin*/,
               size_t end, Key prev_max_x, Precision prev_max_y)
        : LinearImpl(
              prev_max_x, prev_max_y,
              std::max(prev_max_x, *(dataset_begin + end)),
              std::max(prev_max_y,
                       static_cast<Precision>(end) /
                           static_cast<Precision>(
                               std::distance(dataset_begin, dataset_end) - 1))) {}

    /**
     * computes y \in [0, 1] given a certain x
     */
    forceinline Precision normalized(const Key &k) const
    {
      const auto res = slope * k + intercept;
      if (res > 1.0)
        return 1.0;
      if (res < 0.0)
        return 0.0;
      return res;
    }

    /**
     * computes x (rounded up) given a certain y in normalized space:
     * (y \in [0, 1]).
     */
    forceinline Key normalized_inverse(const Precision y) const
    {
      // y = ax + b <=> x = (y-b)/a
      // +0.5 to round up (TODO(dominik): is this necessary?)
      return 0.5 + (y - intercept) / slope;
    }

    /**
     * Extrapolates an index for the given key to the range [0, max_value]
     *
     * @param k key value to extrapolate for
     * @param max_value output indices are \in [0, max_value]. Defaults to
     * std::numeric_limits<Precision>::max()
     */
    forceinline size_t
    operator()(const Key &k, const Precision &max_value =
                                 std::numeric_limits<Precision>::max()) const
    {
      // +0.5 as a quick&dirty ceil trick
      const size_t pred = max_value * normalized(k) + 0.5;
      assert(pred >= 0);
      assert(pred <= max_value);
      return pred;
    }

    /**
     * Two LinearImpl are equal if their slope & intercept match *exactly*
     */
    bool operator==(const LinearImpl<Key, Precision> other) const
    {
      return slope == other.slope && intercept == other.intercept;
    }

    forceinline Precision get_slope() const { return slope; }
    forceinline Precision get_intercept() const { return intercept; }
  };

  template <class Key, size_t MaxSecondLevelModelCount,
            size_t MinAvgDatapointsPerModel = 2, class Precision = double,
            class RootModel = LinearImpl<Key, Precision>,
            class SecondLevelModel = LinearImpl<Key, Precision>>
  class RMIHash
  {
    using Datapoint = DatapointImpl<Key, Precision>;
    
    // declare friendship with ChainedRMICoro
    template <class T1, class T2, size_t T3, class T4, class T5, T1 T6>
    friend struct hashtable_coro::ChainedRMICoro;

    /// Root model
    RootModel root_model;

    /// Second level models
    std::vector<SecondLevelModel> second_level_models;

    /// output range is scaled from [0, 1] to [0, max_output] = [0, full_size)
    size_t max_output = 0;

  public:
    // A class representing the hash task [coroutines]
    template <typename Scheduler>
    class HashTask;
    // A class representing the hash result [coroutines]
    template <class Result = size_t>
    class HashResult;

    /**
     * Constructs an empty, untrained RMI. to train, manually
     * train by invoking the train() function
     */
    RMIHash() = default;

    /**
     * Builds rmi on an already sorted (!) sample
     * @tparam RandomIt
     * @param sample_begin
     * @param sample_end
     * @param full_size operator() will extrapolate to [0, full_size)
     * @param faster_construction whether or not to use the faster construction
     *    algorithm without intermediate allocations etc. Around 100x speedup
     *    while end result is the same (! tested on various datasets)
     */
    template <class RandomIt>
    RMIHash(const RandomIt &sample_begin, const RandomIt &sample_end,
            const size_t full_size, bool faster_construction = true)
    {
      train(sample_begin, sample_end, full_size, faster_construction);
    }

    /**
     * trains rmi on an already sorted sample
     *
     * @tparam RandomIt
     * @param sample_begin
     * @param sample_end
     * @param full_size operator() will extrapolate to [0, full_size)
     * @param faster_construction whether or not to use the faster construction
     *    algorithm without intermediate allocations etc. Around 100x speedup
     *    while end result is the same (! tested on various datasets)
     */
    template <class RandomIt>
    void train(const RandomIt &sample_begin, const RandomIt &sample_end,
               const size_t full_size, bool faster_construction = true)
    {
      this->max_output = full_size - 1;
      const size_t sample_size = std::distance(sample_begin, sample_end);
      if (sample_size == 0)
        return;

      root_model =
          decltype(root_model)(sample_begin, sample_end, 0, sample_size - 1);
      if (MaxSecondLevelModelCount == 0)
        return;

      // ensure that there is at least MinAvgDatapointsPerModel datapoints per
      // model on average to not waste space/resources
      const auto second_level_model_cnt = std::min(
          MaxSecondLevelModelCount, sample_size / MinAvgDatapointsPerModel);
      second_level_models = decltype(second_level_models)(second_level_model_cnt);

      if (faster_construction)
      {
        // convenience function for training (code deduplication)
        size_t previous_end = 0, finished_end = 0, last_index = 0;
        const auto train_until = [&](const size_t i)
        {
          while (last_index < i)
          {
            second_level_models[last_index++] = SecondLevelModel(
                sample_begin, sample_end, finished_end, previous_end);
            finished_end = previous_end;
          }
        };

        for (auto it = sample_begin; it < sample_end; it++)
        {
          // Predict second level model using root model and put
          // sample datapoint into corresponding training bucket
          const auto key = *it;
          const auto current_second_level_index =
              root_model(key, second_level_models.size() - 1);
          assert(current_second_level_index >= 0);
          assert(current_second_level_index < second_level_models.size());

          // bucket is finished, train all affected models
          if (last_index < current_second_level_index)
            train_until(current_second_level_index);

          // last consumed datapoint
          previous_end = std::distance(sample_begin, it);
        }

        // train all remaining models
        train_until(second_level_models.size());
      }
      else
      {
        // Assign each sample point into a training bucket according to root model
        std::vector<std::vector<Datapoint>> training_buckets(
            second_level_models.size());

        for (auto it = sample_begin; it < sample_end; it++)
        {
          const auto i = std::distance(sample_begin, it);

          // Predict second level model using root model and put
          // sample datapoint into corresponding training bucket
          const auto key = *it;
          const auto second_level_index =
              root_model(key, second_level_models.size() - 1);
          auto &bucket = training_buckets[second_level_index];

          // The following works because the previous training bucket has to be
          // completed, because the sample is sorted: Each training bucket's min
          // is the previous training bucket's max (except for first bucket)
          if (bucket.empty() && second_level_index > 0)
          {
            size_t j = second_level_index - 1;
            while (j < second_level_index && j >= 0 &&
                   training_buckets[j].empty())
              j--;
            assert(!training_buckets[j].empty());
            bucket.push_back(training_buckets[j].back());
          }

          // Add datapoint at the end of the bucket
          bucket.push_back(Datapoint(
              key,
              static_cast<Precision>(i) / static_cast<Precision>(sample_size)));
        }

        // Edge case: First model does not have enough training data -> add
        // artificial datapoints
        assert(training_buckets[0].size() >= 1);
        while (training_buckets[0].size() < 2)
          training_buckets[0].insert(training_buckets[0].begin(),
                                     Datapoint(0, 0));

        // Train each second level model on its respective bucket
        for (size_t model_idx = 0; model_idx < second_level_models.size();
             model_idx++)
        {
          auto &training_bucket = training_buckets[model_idx];

          // Propagate datapoints from previous training bucket if necessary
          while (training_bucket.size() < 2)
          {
            assert(model_idx - 1 >= 0);
            assert(!training_buckets[model_idx - 1].empty());
            training_bucket.insert(training_bucket.begin(),
                                   training_buckets[model_idx - 1].back());
          }
          assert(training_bucket.size() >= 2);

          // Train model on training bucket & add it
          second_level_models[model_idx] = SecondLevelModel(training_bucket);
        }
      }
    }

    static std::string name()
    {
      return "coro_rmi_hash_" + std::to_string(MaxSecondLevelModelCount);
    }

    size_t byte_size() const
    {
      return sizeof(decltype(this)) +
             sizeof(SecondLevelModel) * second_level_models.size();
    }

    size_t model_count() const { return 1 + second_level_models.size(); }

    /**
     * Compute hash value for key
     *
     * @tparam Result result data type. Defaults to size_t
     * @param key
     * @return The hash value of key.
     */
    template <class Result = size_t>
    forceinline Result operator()(const Key &key) const
    {
      if (MaxSecondLevelModelCount == 0)
        return root_model(key, max_output);

      const auto second_level_index =
          root_model(key, second_level_models.size() - 1);
      assert(second_level_index < second_level_models.size());
      // this is the line we can add coroutines
      const auto result =
          second_level_models[second_level_index](key, max_output);

      assert(result <= max_output);
      return result;
    }

    /**
     * Compute hash value for key.
     *
     * @tparam Result result data type. Defaults to size_t
     * @param key
     * @return The pair <key, hash>.
     */
    template <class Result = size_t>
    HashResult<Result> hash(Key const &key)
    {
      auto hash_value = (*this)(key);
      return HashResult<Result>{key, hash_value};
    }

    /**
     * Compute hash value for key, using coroutines and prefetching.
     *
     * @param key
     * @param scheduler The scheduler that is handling the different streams.
     * @param append The lambda function that is called once the hash value is found.
     * @return The pair <key, hash>.
     */
    template <
        typename Scheduler,
        typename Append>
    HashTask<Scheduler> hash_task (
        Key const key,
        Scheduler const &scheduler,
        Append append)
    {
      if (MaxSecondLevelModelCount == 0) {
        const auto hash_value = root_model(key, max_output);
        co_return append(key, hash_value);
      }

      const auto second_level_index =
          root_model(key, second_level_models.size() - 1);
      assert(second_level_index < second_level_models.size());

      auto *second_level_model = co_await prefetch_and_schedule_on(second_level_models.data() + second_level_index, scheduler);

      const auto result = (*second_level_model)(key, max_output);
      assert(result <= max_output);

      co_return append(key, result);
    }

    /**
     * Compute hash values for multiple keys, using coroutines and prefetching.
     *
     * @tparam Result result data type. Defaults to size_t
     * @param begin_keys Begin iterator for the keys collection.
     * @param end_keys End iterator for the keys collection.
     * @param begin_results Begin iterator for the result array [this will contain the output]
     * @param n_streams The number of streams to be used [minimum 1, maximum MAX_CORO]
     * @return Fills the `begin_results` array with `HashResult` objects (that is, <key,hash> pairs)
     */
    template <
        typename BeginInputIter,
        typename EndInputIter,
        typename OutputIter,
        class Result = size_t>
    forceinline void interleaved_multihash(
        BeginInputIter begin_keys,
        EndInputIter end_keys,
        OutputIter begin_results,
        std::size_t const n_streams)
    {
      using FunType = RMIHash<Key, MaxSecondLevelModelCount, MinAvgDatapointsPerModel, Precision, RootModel, SecondLevelModel>;
      using ResultType = typename FunType::HashResult<Result>;

      StaticQueueScheduler<MAX_CORO+1> scheduler{};

      // instantiate a throttler for this multilookup
      Throttler throttler{scheduler, n_streams};

      for (auto key_iter = begin_keys; key_iter != end_keys; ++key_iter)
      {
        throttler.spawn(
            hash_task(
                *key_iter,
                scheduler,
                [&begin_results](Key const &k, Result const &v) mutable
                {
                  *begin_results = ResultType{k, v};
                  ++begin_results;
                }));
      }

      // run until all lookup tasks complete
      throttler.run();
    }

    /**
     * Compute hash values for multiple keys, in a classic sequential manner.
     *
     * @tparam Result result data type. Defaults to size_t
     * @param begin_keys Begin iterator for the keys collection.
     * @param end_keys End iterator for the keys collection.
     * @param begin_results Begin iterator for the result array [this will contain the output]
     * @return Fills the `begin_results` array with `HashResult` objects (that is, <key,hash> pairs)
     */
    template <
        typename BeginInputIter,
        typename EndInputIter,
        typename OutputIter,
        class Result = size_t>
    forceinline void sequential_multihash(
        BeginInputIter begin_keys,
        EndInputIter end_keys,
        OutputIter begin_results)
    {
      for (auto iter = begin_keys; iter != end_keys; ++iter)
      {
        *begin_results = hash<Result>(*iter);
        ++begin_results;
      }
    }

    bool operator==(const RMIHash &other) const
    {
      if (other.root_model != root_model)
        return false;
      if (other.second_level_models.size() != second_level_models.size())
        return false;

      for (size_t i = 0; i < second_level_models.size(); i++)
        if (other.second_level_models[i] != second_level_models[i])
          return false;

      return true;
    }
  };

  // ------------------------------------------------------------------- //

  // ========================================================= //
  // Adapted from https://github.com/turingcompl33t/coroutines //
  // ========================================================= //

  // The task type used to represent interleaved () operations.
  template <class Key, size_t MaxSecondLevelModelCount,
            size_t MinAvgDatapointsPerModel, class Precision,
            class RootModel,
            class SecondLevelModel>
  template <typename Scheduler>
  class RMIHash<
      Key,
      MaxSecondLevelModelCount,
      MinAvgDatapointsPerModel,
      Precision,
      RootModel,
      SecondLevelModel>::HashTask
  {
  public:
    struct promise_type;
    using CoroHandle = stdcoro::coroutine_handle<promise_type>;

    HashTask() = delete;

    // IMPT: not an RAII type; the throttler instance takes
    // ownership of the HashTask immediately upon spawning it.
    ~HashTask() = default;

    HashTask(HashTask const &) = delete;

    HashTask(HashTask &&rhs)
        : handle{rhs.handle}
    {
      rhs.handle = nullptr;
    }

    struct promise_type
    {
      // pointer to the owning throttler instance for this promise
      Throttler<Scheduler> *owning_throttler{nullptr};

      // utilize the custom recycling allocator
      void *operator new(std::size_t n)
      {
        // NOTE: in multithtreaded interleaved run this gives segfaults
        // in fact timewise on a single thread there is no difference
        // not tested memory usage...
        // return inline_recycling_allocator.alloc(n);
        return std::malloc(n);
      }

      // utilize the custom recycling allocator
      void operator delete(void *ptr /*, std::size_t n*/)
      {
        // inline_recycling_allocator.free(ptr, n);
        // NOTE: same as right above
        std::free(ptr);
      }

      HashTask get_return_object()
      {
        return HashTask{*this};
      }

      auto initial_suspend()
      {
        return std::suspend_always{};
      }

      auto final_suspend() noexcept
      {
        return std::suspend_never{};
      }

      void return_void()
      {
        owning_throttler->on_task_complete();
      }

      void unhandled_exception() noexcept
      {
        std::terminate();
      }
    };

    // make the throttler the "owner" of this coroutine
    auto set_owner(Throttler<Scheduler> *owner)
    {
      auto result = handle;

      // modify the promise to point to the owning throttler
      handle.promise().owning_throttler = owner;

      // reset our handle
      handle = nullptr;

      return result;
    }

  private:
    CoroHandle handle;

    HashTask(promise_type &p)
        : handle{CoroHandle::from_promise(p)} {}
  };

  // ------------------------------------------------------------------- //

  // ========================================================= //
  // Adapted from https://github.com/turingcompl33t/coroutines //
  // ========================================================= //

  // The type returned by () operations.
  template <class Key, size_t MaxSecondLevelModelCount,
            size_t MinAvgDatapointsPerModel, class Precision,
            class RootModel,
            class SecondLevelModel>
  template <class Result>
  class RMIHash<
      Key,
      MaxSecondLevelModelCount,
      MinAvgDatapointsPerModel,
      Precision,
      RootModel,
      SecondLevelModel>::HashResult
  {

    Key const *key;
    Result const value;

  public:
    HashResult()
        : key{nullptr}, value{} {}

    HashResult(Key const &key_, Result const &value_)
        : key{&key_}, value{value_} {}

    Key const &get_key() const
    {
      return *key;
    }

    Result const &get_value() const
    {
      return value;
    }

    explicit operator bool() const
    {
      return (key != nullptr);
    }
  };

} // namespace learned_hashing
