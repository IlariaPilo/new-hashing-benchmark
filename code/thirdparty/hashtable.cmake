# ===================================================== #
# from https://github.com/DominikHorn/hashing-benchmark #
# ===================================================== #

include(FetchContent)

set(HASHTABLE_LIBRARY hashtable)
FetchContent_Declare(
  ${HASHTABLE_LIBRARY}
  GIT_REPOSITORY https://github.com/IlariaPilo/hashtable
  GIT_TAG f9564a6
  )
FetchContent_MakeAvailable(${HASHTABLE_LIBRARY})
