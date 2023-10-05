#include"include/datasets.hpp"

namespace dataset {

// This function has to stay here, as it is the only one without template
std::vector<ID> get_id_slice(int threadID, size_t thread_num, size_t how_many) {
    /* The first thread_num % ID_COUNT elements get thread_num/ID_COUNT + 1 IDs
       All the others threads get thread_num/ID_COUNT
     */
    int mod = how_many % thread_num;
    int div = how_many / thread_num;
    size_t slice = (size_t)div;
    /* We also take a look in the past to set the starting point! */
    int past_mod_threads = -1;      // the ones with +1 in the slice
    int past_threads = -1;          // all the other ones
    if (threadID < mod) {
        slice++;
        past_mod_threads = threadID;
        past_threads = 0;
    } else {
        past_mod_threads = mod;
        past_threads = threadID - mod;
    }
    /* Define start and end */
    size_t start = past_mod_threads*(div+1) + past_threads*div;
    size_t end = start+slice;

    /* Create the vector */
    std::vector<ID> output;
    output.resize(slice);
    for(size_t i=start, j=0; i<end && i<how_many; i++, j++)
        output[j] = REVERSE_ID.at(i);
    return output;
}

}