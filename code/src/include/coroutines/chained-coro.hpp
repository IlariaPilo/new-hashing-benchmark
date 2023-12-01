#pragma once

// An implementation of the Bucket chaining hash table by Dominik Horn, 
// using coroutines to prefetch buckets from linked lists.

#include <array>
#include <cassert>
#include <cmath>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "../builtins.hpp"

// ------ coro things ------ //
#include "cppcoro/coroutine.hpp"

#include "prefetch.hpp"
#include "scheduler.hpp"
#include "throttler.hpp"
// ------------------------- //

namespace hashtable_coro
{
    template <class Key, class Payload, size_t BucketSize, class HashFn, class ReductionFn,
              Key Sentinel = std::numeric_limits<Key>::max()>
    struct Chained
    {
    public:
        using KeyType = Key;
        using PayloadType = Payload;

        // A class representing the lookup task [coroutines]
        template <typename Scheduler>
        class LookupTask;
        // A class representing the lookup result [coroutines]
        class LookupResult;

    private:
        const HashFn hashfn;
        const ReductionFn reductionfn;
        const size_t capacity;

    public:
        explicit Chained(const size_t &capacity, const HashFn hashfn = HashFn())
            : hashfn(hashfn), reductionfn(ReductionFn(directory_address_count(capacity))), capacity(capacity),
              slots(directory_address_count(capacity)){};

        Chained(Chained &&) noexcept = default;

        /**
         * Inserts a key, value/payload pair into the hashtable
         *
         * @param key
         * @param payload
         * @return whether or not the key, payload pair was inserted. Insertion will fail
         *    iff the same key already exists or if key == Sentinel value
         */
        bool insert(const Key &key, const Payload &payload)
        {
            if (unlikely(key == Sentinel))
            {
                assert(false); // TODO(unknown): this must never happen in practice
                return false;
            }

            // Using template functor should successfully inline actual hash computation
            FirstLevelSlot &slot = slots[reductionfn(hashfn(key))];

            // Store directly in slot if possible
            if (slot.key == Sentinel)
            {
                slot.key = key;
                slot.payload = payload;
                return true;
            }

            // Initialize bucket chain if empty
            Bucket *bucket = slot.buckets;
            if (bucket == nullptr)
            {
                auto b = new Bucket();
                b->slots[0] = {.key = key, .payload = payload};
                slot.buckets = b;
                return true;
            }

            // Go through existing buckets and try to insert there if possible
            for (;;)
            {
                // Find suitable empty entry place. Note that deletions with holes will require
                // searching entire bucket to deal with duplicate keys!
                for (size_t i = 0; i < BucketSize; i++)
                {
                    if (bucket->slots[i].key == Sentinel)
                    {
                        bucket->slots[i] = {.key = key, .payload = payload};
                        return true;
                    }
                    else if (bucket->slots[i].key == key)
                    {
                        // key already exists
                        return false;
                    }
                }

                if (bucket->next == nullptr)
                    break;
                bucket = bucket->next;
            }

            // Append a new bucket to the chain and add element there
            auto b = new Bucket();
            b->slots[0] = {.key = key, .payload = payload};
            bucket->next = b;
            return true;
        }

        /**
         * Retrieves the associated payload/value for a given key.
         *
         * @param key
         * @return a pair <key,value>, where `value` is the payload or nullptr, if `key` was not found.
         */
        LookupResult lookup(const Key &key) const
        {
            if (unlikely(key == Sentinel))
            {
                assert(false); // TODO(unknown): this must never happen in practice
                return LookupResult{};
            }

            // Using template functor should successfully inline actual hash computation
            const FirstLevelSlot &slot = slots[reductionfn(hashfn(key))];

            if (slot.key == key)
            {
                return LookupResult{slot.key,slot.payload};
            }

            Bucket *bucket = slot.buckets;
            while (bucket != nullptr)
            {
                for (size_t i = 0; i < BucketSize; i++)
                {
                    if (bucket->slots[i].key == key)
                    {
                        return LookupResult{bucket->slots[i].key,bucket->slots[i].payload};
                    }

                    if (bucket->slots[i].key == Sentinel)
                        return LookupResult{};;
                }
                bucket = bucket->next;
            }

            return LookupResult{};;
        }

        /**
         * Retrieves the associated payload/value for a given key.
         *
         * @param key
         * @param scheduler The scheduler that is handling the different streams.
         * @param on_found The lambda function that is called when the key is found.
         * @param on_not_found The lambda function that is called when the key is NOT found.
         * @return a pair <key,value>, where `value` is the payload or nullptr, if `key` was not found.
         */
        template <
            typename Scheduler,
            typename OnFound,
            typename OnNotFound>
        LookupTask<Scheduler> lookup_task(
            Key const key,
            Scheduler const &scheduler,
            OnFound on_found,
            OnNotFound on_not_found)
        {
            if (unlikely(key == Sentinel))
            {
                assert(false); // TODO(unknown): this must never happen in practice
                co_return on_not_found();
            }

            // Using template functor should successfully inline actual hash computation
            // TODO - is it interesting to prefetch this one too?
            const FirstLevelSlot &slot = slots[reductionfn(hashfn(key))];

            if (slot.key == key)
            {
                co_return on_found(slot.key, slot.payload);
            }

            // Bucket* bucket = slot.buckets;
            Bucket *bucket = co_await prefetch_and_schedule_on(slot.buckets, scheduler);
            while (bucket != nullptr)
            {
                for (size_t i = 0; i < BucketSize; i++)
                {
                    if (bucket->slots[i].key == key)
                    {
                        co_return on_found(bucket->slots[i].key, bucket->slots[i].payload);
                    }

                    if (bucket->slots[i].key == Sentinel)
                        co_return on_not_found();
                }
                // bucket = bucket->next;
                bucket = co_await prefetch_and_schedule_on(bucket->next, scheduler);
            }

            co_return on_not_found();
        }

        /**
         * Retrieves the associated payload/value for multiple keys, using coroutines and prefetching.
         *
         * @param begin_keys Begin iterator for the keys collection.
         * @param end_keys End iterator for the keys collection.
         * @param begin_results Begin iterator for the result array [this will contain the output]
         * @param n_streams The number of streams to be used [minimum 1, maximum MAX_CORO]
         * @return Fills the `begin_results` array with `LookupResult` objects (that is, <key,value> pairs)
         */
        template <
            typename BeginInputIter,
            typename EndInputIter,
            typename OutputIter>
        forceinline void interleaved_multilookup(
            BeginInputIter begin_keys,
            EndInputIter end_keys,
            OutputIter begin_results,
            std::size_t const n_streams)
        {
            using TableType = Chained<Key, Payload, BucketSize, HashFn, ReductionFn, Sentinel>;
            using ResultType = typename TableType::LookupResult;

            StaticQueueScheduler<MAX_CORO + 1> scheduler{};

            // instantiate a throttler for this multilookup
            Throttler throttler{scheduler, n_streams};

            for (auto key_iter = begin_keys; key_iter != end_keys; ++key_iter)
            {
                throttler.spawn(
                    lookup_task(
                        *key_iter,
                        scheduler,
                        [&begin_results](Key const &k, Payload const &v) mutable
                        {
                            *begin_results = ResultType{k, v};
                            ++begin_results;
                        },
                        []() mutable
                        {
                            // do nothing
                        }));
            }
            // run until all lookup tasks complete
            throttler.run();
        }

        /**
         * Retrieves the associated payload/value for multiple keys, in a classic sequential manner.
         *
         * @param begin_keys Begin iterator for the keys collection.
         * @param end_keys End iterator for the keys collection.
         * @param begin_results Begin iterator for the result array [this will contain the output]
         * @return Fills the `begin_results` array with `LookupResult` objects (that is, <key,value> pairs)
         */
        template <
            typename BeginInputIter,
            typename EndInputIter,
            typename OutputIter>
        forceinline void sequential_multilookup(
            BeginInputIter begin_keys,
            EndInputIter end_keys,
            OutputIter begin_results)
        {
            for (auto iter = begin_keys; iter != end_keys; ++iter)
            {
                *begin_results = lookup(*iter);
                ++begin_results;
            }
        }

        size_t byte_size() const
        {
            size_t size = sizeof(decltype(*this)) + slots.size() * slot_byte_size();
            for (const auto &slot : slots)
                size += slot.buckets == nullptr ? 0 : slot.buckets->byte_size();

            return size;
        }

        static constexpr forceinline size_t bucket_byte_size()
        {
            return sizeof(Bucket);
        }

        static constexpr forceinline size_t slot_byte_size()
        {
            return sizeof(FirstLevelSlot);
        }

        static forceinline std::string name()
        {
            return "chained_" + hash_name() + "_" + reducer_name() + "_" + std::to_string(bucket_size());
        }

        static forceinline std::string hash_name()
        {
            return HashFn::name();
        }

        static forceinline std::string reducer_name()
        {
            return ReductionFn::name();
        }

        static constexpr forceinline size_t bucket_size()
        {
            return BucketSize;
        }

        static constexpr forceinline size_t directory_address_count(const size_t &capacity)
        {
            return capacity;
        }

        /**
         * Clears all keys from the hashtable. Note that payloads are technically
         * still in memory (i.e., might leak if sensitive).
         */
        void clear()
        {
            for (auto &slot : slots)
            {
                slot.key = Sentinel;

                auto bucket = slot.buckets;
                slot.buckets = nullptr;

                while (bucket != nullptr)
                {
                    auto next = bucket->next;
                    delete bucket;
                    bucket = next;
                }
            }
        }

        ~Chained()
        {
            clear();
        }

    protected:
        struct Bucket
        {
            struct Slot
            {
                Key key = Sentinel;
                Payload payload;
            } packit;

            std::array<Slot, BucketSize> slots /*__attribute((aligned(sizeof(Key) * 8)))*/;
            Bucket *next = nullptr;

            size_t byte_size() const
            {
                return sizeof(decltype(*this)) + (next == nullptr ? 0 : next->byte_size());
            }
        } packit;

        struct FirstLevelSlot
        {
            Key key = Sentinel;
            Payload payload;
            Bucket *buckets = nullptr;
        } packit;

        // First bucket is always inline in the slot
        std::vector<FirstLevelSlot> slots;
    };

    // ------------------------------------------------------------------- //

    // ========================================================= //
    // Adapted from https://github.com/turingcompl33t/coroutines //
    // ========================================================= //

    // The task type used to represent interleaved lookup operations.
    template <class Key, class Payload, size_t BucketSize, class HashFn, class ReductionFn, Key Sentinel>
    template <typename Scheduler>
    class Chained<
        Key,
        Payload,
        BucketSize,
        HashFn,
        ReductionFn,
        Sentinel>::LookupTask
    {
    public:
        struct promise_type;
        using CoroHandle = stdcoro::coroutine_handle<promise_type>;

        LookupTask() = delete;

        // IMPT: not an RAII type; the throttler instance takes
        // ownership of the LookupTask immediately upon spawning it.
        ~LookupTask() = default;

        LookupTask(LookupTask const &) = delete;

        LookupTask(LookupTask &&rhs)
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

            LookupTask get_return_object()
            {
                return LookupTask{*this};
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

        LookupTask(promise_type &p)
            : handle{CoroHandle::from_promise(p)} {}
    };

    // ------------------------------------------------------------------- //

    // ========================================================= //
    // Adapted from https://github.com/turingcompl33t/coroutines //
    // ========================================================= //

    // The type returned by lookup operations.
    template <class Key, class Payload, size_t BucketSize, class HashFn, class ReductionFn, Key Sentinel>
    class Chained<
        Key,
        Payload,
        BucketSize,
        HashFn,
        ReductionFn,
        Sentinel>::LookupResult
    {

        Key const *key;
        Payload const *value;

    public:
        LookupResult()
            : key{nullptr}, value{nullptr} {}

        LookupResult(Key const &key_, Payload const &value_)
            : key{&key_}, value{&value_} {}

        Key const &get_key() const
        {
            return *key;
        }

        Payload const &get_value() const
        {
            return *value;
        }

        explicit operator bool() const
        {
            return (key != nullptr);
        }
    };

} // namespace hashtable_coro
