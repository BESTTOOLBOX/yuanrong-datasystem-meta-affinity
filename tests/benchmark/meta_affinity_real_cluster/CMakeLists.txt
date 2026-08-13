cmake_minimum_required(VERSION 3.14)
project(meta_affinity_real_cluster_benchmark LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT DATASYSTEM_SDK_ROOT)
    message(FATAL_ERROR "Set DATASYSTEM_SDK_ROOT to the C++ SDK directory")
endif()

find_path(DATASYSTEM_INCLUDE_DIR
    NAMES datasystem/object_client.h
    PATHS "${DATASYSTEM_SDK_ROOT}/include"
    NO_DEFAULT_PATH)
find_library(DATASYSTEM_LIBRARY
    NAMES datasystem
    PATHS "${DATASYSTEM_SDK_ROOT}/lib" "${DATASYSTEM_SDK_ROOT}/lib64"
    NO_DEFAULT_PATH)

if(NOT DATASYSTEM_INCLUDE_DIR OR NOT DATASYSTEM_LIBRARY)
    message(FATAL_ERROR
        "Could not find include/datasystem/object_client.h and libdatasystem.so under ${DATASYSTEM_SDK_ROOT}")
endif()

add_executable(meta_affinity_benchmark meta_affinity_benchmark.cpp)
target_include_directories(meta_affinity_benchmark PRIVATE "${DATASYSTEM_INCLUDE_DIR}")
target_link_libraries(meta_affinity_benchmark PRIVATE "${DATASYSTEM_LIBRARY}" pthread)
get_filename_component(DATASYSTEM_LIBRARY_DIR "${DATASYSTEM_LIBRARY}" DIRECTORY)
set_target_properties(meta_affinity_benchmark PROPERTIES
    BUILD_RPATH "${DATASYSTEM_LIBRARY_DIR}"
    INSTALL_RPATH "${DATASYSTEM_LIBRARY_DIR}")
