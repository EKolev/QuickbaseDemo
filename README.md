# QBTable: An In-Memory Database with Adaptive Indexing

QBTable is a solution written for the take home assignment from Quickbase.

## Build Instructions

### Prerequisites
- **C++20** or later (required)
- CMake 3.15+
- Clang, GCC, or MSVC compiler

### Build with CMake

```bash
# Clone or navigate to the project directory
cd QuickbaseDemo

# Create and enter build directory
mkdir build
cd build

# Configure and build
cmake ..
make

# Run tests and benchmarks
./bin/qucikbase_interview_demo
```

### Build on Windows (MSVC 2022 - Recommended)
```bash
cd cmake_build
cmake .. -G "Visual Studio 17 2022"
cmake --build . --config Release
.\bin\qucikbase_interview_demo.exe
```

### Build on Windows (MSVC 2019)
```bash
cd cmake_build
cmake .. -G "Visual Studio 16 2019"
cmake --build . --config Release
.\bin\qucikbase_interview_demo.exe
```
**Note:** MSVC 2019 has partial C++20 support. If you encounter compilation errors, upgrade to MSVC 2022.

### Platform Support
- **macOS**: Clang with C++20 support (AppleClang 13+)
- **Linux**: GCC 11+ or Clang 13+
- **Windows**: **MSVC 2022** (recommended) or MSVC 2019 with C++20 support
  - MSVC 2022 has full C++20 support
  - MSVC 2019 has partial C++20 support; some edge cases may fail

## Running Tests

The binary qucikbase_interview_demo runs benchmark unit tests
