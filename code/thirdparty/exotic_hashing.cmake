# ===================================================== #
# from https://github.com/DominikHorn/hashing-benchmark #
# ===================================================== #

include(FetchContent)

set(EXOTIC_HASHING_LIBRARY exotic-hashing)
FetchContent_Declare(
  ${EXOTIC_HASHING_LIBRARY}
  GIT_REPOSITORY https://github.com/IlariaPilo/exotic-hashing.git
  GIT_TAG d08bdd3 
  )
FetchContent_MakeAvailable(${EXOTIC_HASHING_LIBRARY})
