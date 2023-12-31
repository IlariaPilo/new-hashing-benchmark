# ===================================================== #
# from https://github.com/DominikHorn/hashing-benchmark #
# ===================================================== #

include(FetchContent)

set(HASHTABLE_LIBRARY hashtable)
FetchContent_Declare(
  ${HASHTABLE_LIBRARY}
  GIT_REPOSITORY https://github.com/IlariaPilo/hashtable
  GIT_TAG 75e70aa
  )
FetchContent_MakeAvailable(${HASHTABLE_LIBRARY})
