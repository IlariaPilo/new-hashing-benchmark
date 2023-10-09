#pragma once

#include <vector>

// A simple wrapper to implement the RMI-Sort structure
// Such a structure simply builds a RMI index on top of a sorted dataset

namespace hashtable {

    template <class Key, class Payload, class RMIHash, Key Sentinel = std::numeric_limits<Key>::max()>
    class RMISort {

        struct Slot {
            Key key = Sentinel;
            Payload payload;
        };

        private:

        const RMIHash hashfn;           // the RMI index
        const size_t capacity;          // the number of elements in the dataset
        std::vector<Slot> slots;        // the table
        size_t filled;                  // the number of occupied positions
        size_t max_error;               // to do binary search
        bool finalized;

        public:

        /**
         * The "hashtable" compatible constructor for this class.
         * @param capacity the number of elements in the dataset
         * @param hashfn the RMI hash function (already trained) that will be used on top of the structure
        */
        explicit RMISort(const size_t& capacity, const RMIHash hashfn)
         : hashfn(hashfn), capacity(capacity), filled(0), max_error(0), finalized(false) {
            slots.reserve(capacity);
        };

        RMISort(RMISort&&) noexcept = default;

        /**
         * Inserts a pair (key, value) in the structure.
         * @param key the key (i.e., the entry in the dataset)
         * @param payload the correspondent value
         * @return "true" if there is still space to store the pair, "false" otherwise.
        */
        bool insert(const Key& key, const Payload& payload) {
            if (filled >= capacity)
                return false;
            // put in the array
            slots.push_back({key,payload});
            // update filled
            filled++;
            if (filled == capacity) {
                // we are done, call finalize
                finalize();
            }
            return true;
        }

        /**
         * Looks for a single entry in the structure, by using a bounded binary search.
         * @param key the value we are looking for
         * @return the correspondent payload if the key was found, `std::nullopt` otherwise.
        */
        std::optional<Payload> lookup(const Key& key) const {
            if (!finalized) {
                // structure is not completely filled (YET)
                std::cerr << "\033[1;93m [warning]\033[0m structure is not completely filled yet.\n";
                return std::nullopt;
            }
                 
            size_t m, l, r;
            Slot guess_slot;

            m = hashfn(key);
            // set up l and r for the bounded binary search
            l = std::max((size_t)0, m-max_error);
            r = std::min(m+max_error, capacity-1);
            // check in the keyv array
            while (l <= r) {
                guess_slot = slots[m];
                // if it's the same, done
                if (guess_slot.key == key)
                    return std::make_optional(guess_slot.payload);
                // else, do binary search
                if (guess_slot.key < key) {
                    l = m + 1;
                } else {
                    r = m - 1;
                }
                // update guess_pos
                m = l + (r-l)/2;
            }
            // not found :(
            return std::nullopt;
        }

        /**
         * Looks for all keys in the structure in the range [min,max].
         * @param min the lower bound of the interval (included)
         * @param max the upper bound of the interval (also included)
         * @return an array storing all payloads whose key is included in the interval.
        */
        std::vector<Payload> lookup_range(const Key& min, const Key& max) {
            std::vector<Payload> output;
            if (!finalized) {
                // structure is not completely filled (YET)
                std::cerr << "\033[1;93m [warning]\033[0m structure is not completely filled yet.\n";
                return output;
            }
            size_t left_idx = search_range(true, min);
            size_t right_idx = search_range(true, max);
            for (size_t i=left_idx; i<=right_idx; i++) 
                output.push_back(slots[i].payload);
            return output;
        }

        static std::string name() {
            return "sort_" + RMIHash::name();
        }

        private:
        /**
         * An utility function which sorts the `slots` array and compute the maximum error of the RMI function.
        */
        void finalize() {
            // sort the slots
            std::sort(slots.begin(), slots.end(), [](Slot lhs, Slot rhs) {
                return lhs.key < rhs.key;
            });
            // call the function for every key
            for (size_t i=0; i<capacity; i++) {
                Slot& s = slots[i];
                // predicted index
                size_t p_idx = hashfn(s.key);
                size_t diff = i>p_idx? i-p_idx:p_idx-i;
                if (diff > max_error)
                    max_error = diff;
            }
            finalized = true;
        }
        /**
         * An utility function to perform a bounded binary search of an interval endpoint.
         * @param left controls which endpoint of the interval we are looking for (left or right):
         * - if left==true, we are looking for the *first* value >= target
         * - if left==false, we are looking for the *last* value <= target
         * @param key the target we are looking for
         * @return the index of the slot that matches the search criteria.
        */
        size_t search_range(bool left, Key key) {
            size_t m, l, r;
            Key guess_key;

            m = hashfn(key);
            // set up l and r for the bounded binary search
            l = std::max((size_t)0, m-max_error);
            r = std::min(m+max_error, capacity-1);
            // check in the keyv array
            while (l <= r) {
                guess_key = slots[m].key;
                // if it's the same, done
                if (guess_key == key)
                    return m;
                // else, do binary search
                if (guess_key < key) {
                    l = m + 1;
                } else {
                    r = m - 1;
                }
                // update guess_pos
                m = l + (r-l)/2;
            }
            if (guess_key < key)
                return m + left;
            return m - !left;
        }
    };

}   // namespace hashtable
