cmake_minimum_required(VERSION 3.15 FATAL_ERROR)

project(QBTable 
    VERSION 1.0.0 
    DESCRIPTION "Quckbase interview demo"
    LANGUAGES CXX
)

# C++ Standard Configuration
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build type defaults
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose build type" FORCE)
endif()

message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")

# ============================================================================
# Platform-specific compiler settings
# ============================================================================

if(MSVC)
    # Visual Studio (MSVC) - Ensure C++20 support
    # MSVC 2022+ has full C++20 support; MSVC 2019 has partial support
    add_compile_options(/std:c++latest)  # Use latest C++ standard available
    add_compile_options(/W4)  # Warning level 4
    add_compile_options(/WX:OFF)  # Don't treat warnings as errors
    
    # Optimization flags
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        add_compile_options(/O2 /Ob2)
    elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
        add_compile_options(/ZI /RTC1)  # Debug info and runtime checks
    endif()
    
    # MSVC-specific defines
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
    add_compile_definitions(NOMINMAX)  # Avoid min/max macro conflicts
    
    message(STATUS "Compiler: MSVC (Visual Studio)")
    message(STATUS "MSVC C++ Standard: /std:c++latest (C++20)")
    
else()
    # GCC and Clang (macOS, Linux)
    
    # Common warnings for both GCC and Clang
    add_compile_options(
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wsign-compare
    )
    
    # GCC-specific flags
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        add_compile_options(-Wlogical-op)
        message(STATUS "Compiler: GCC")
    endif()
    
    # Clang-specific flags
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
        add_compile_options(
            -fcolor-diagnostics
            -fansi-escape-codes
        )
        
        if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
            message(STATUS "Compiler: Apple Clang (macOS)")
        else()
            message(STATUS "Compiler: Clang")
        endif()
    endif()
    
    # Optimization flags
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        add_compile_options(-O3 -DNDEBUG)
    elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
        add_compile_options(-g -O0)
    endif()
    
endif()

# ============================================================================
# Optional features
# ============================================================================

option(USE_ASAN "Enable AddressSanitizer (Linux/macOS Debug only)" OFF)

if(USE_ASAN AND NOT MSVC AND CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
    message(STATUS "AddressSanitizer: ENABLED")
else()
    message(STATUS "AddressSanitizer: DISABLED")
endif()

# ============================================================================
# Project structure
# ============================================================================

# Add the main executable
add_executable(qbtable_main
    main.cpp
    Quickbase.cpp
)

# Set output directory
set_target_properties(qbtable_main PROPERTIES
    OUTPUT_NAME "qucikbase_interview_demo"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Target properties for strict compilation
target_compile_features(qbtable_main PRIVATE cxx_std_20)

# Include directories (public headers)
target_include_directories(qbtable_main PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# ============================================================================
# Summary
# ============================================================================

message(STATUS "")
message(STATUS "QBTable Configuration Summary:")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Compiler: ${CMAKE_CXX_COMPILER_ID}")
if(CMAKE_CXX_COMPILER_VERSION)
    message(STATUS "  Compiler Version: ${CMAKE_CXX_COMPILER_VERSION}")
endif()
message(STATUS "")
message(STATUS "Build instructions:")
message(STATUS "  make                                (on Unix-like systems)")
message(STATUS "  cmake --build .                     (cross-platform)")
message(STATUS "  cmake --build . --config Release    (on Windows)")
message(STATUS "")
message(STATUS "Run the benchmark:")
message(STATUS "  ./bin/qucikbase_interview_demo             (Unix-like)")
message(STATUS "  .\\bin\\qucikbase_interview_demo.exe        (Windows)")
message(STATUS "")
