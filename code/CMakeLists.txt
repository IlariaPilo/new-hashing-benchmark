# ===================================================== #
# from https://github.com/DominikHorn/hashing-benchmark #
# ===================================================== #

cmake_minimum_required(VERSION 3.19)
project("new-hashing-benchmark" VERSION 1.0
  DESCRIPTION "TODO"
  HOMEPAGE_URL "https://github.com/IlariaPilo/new-hashing-benchmark"
  LANGUAGES CXX)

# Declare library & directories to include. See
# http://mariobadr.com/creating-a-header-only-library-with-cmake.html for more
# info/install instructions
add_library(${PROJECT_NAME} INTERFACE)
target_include_directories(${PROJECT_NAME} INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
target_compile_features(${PROJECT_NAME} INTERFACE cxx_std_20)

#target_link_libraries(${PROJECT_NAME} INTERFACE )
#target_sources(${PROJECT_NAME} INTERFACE masters_thesis.hpp include/)
target_sources(${PROJECT_NAME} INTERFACE include/)

# Benchmark and test code
get_directory_property(hasParent PARENT_DIRECTORY)
if (NOT hasParent)
  add_subdirectory(src)
endif()
