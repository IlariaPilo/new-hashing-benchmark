# ===================================================== #
# from https://github.com/DominikHorn/hashing-benchmark #
# ===================================================== #

include(FetchContent)

set(LEARNED_HASHING_LIBRARY learned-hashing)
FetchContent_Declare(
  ${LEARNED_HASHING_LIBRARY}
  GIT_REPOSITORY https://github.com/IlariaPilo/learned-hashing.git
  GIT_TAG 0b2c3fb
  )
FetchContent_MakeAvailable(${LEARNED_HASHING_LIBRARY})
