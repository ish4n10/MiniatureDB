cmake_minimum_required(VERSION 3.10)
project(AdvanceStorage)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/include)

# Storage sources
file(GLOB_RECURSE STORAGE_SOURCES 
    "src/storage/*.cpp"
    "src/storage/*.c"
)

# Create storage library
add_library(storage STATIC ${STORAGE_SOURCES} "include/parser/parser.hpp" "include/parser/command.hpp")

# Create test executable for storage engine tests
add_executable(storage_engine_test "tests/storage_engine_test.cpp" "include/parser/parser.hpp" "include/parser/command.hpp")
target_link_libraries(storage_engine_test PRIVATE storage)

# MSVC Specific settings
if(MSVC)
    target_compile_options(storage PRIVATE /W4 /WX /D_CRT_SECURE_NO_WARNINGS)
    target_compile_options(storage_engine_test PRIVATE /W4 /WX /D_CRT_SECURE_NO_WARNINGS)
    # Ensure symbols are exported if we ever switch to SHARED
    set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
else()
    target_compile_options(storage PRIVATE -Wall -Wextra -Werror)
    target_compile_options(storage_engine_test PRIVATE -Wall -Wextra -Werror)
endif()

# Output directory
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
