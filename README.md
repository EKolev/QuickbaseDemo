# QBTable: An In-Memory Database with Adaptive Indexing

QBTable is a high-performance, thread-safe in-memory database library written in modern C++. It provides a `QBRecord`-based table with adaptive indexing—automatically manage primary key indexes (O(1) lookups) and create/drop secondary indexes on demand for optimized query performance. The design supports soft deletion with automatic compaction, record updates, and fall back to linear scan for non-indexed columns. Ideal for scenarios requiring fast exact-match queries over in-memory datasets with minimal memory overhead when indexes are not needed.

## Build Instructions

### Prerequisites
- **C++20** or later (required)
- CMake 3.15+
- Clang, GCC, or MSVC compiler

### Build with CMake

```bash
# Clone or navigate to the project directory
cd QuckBase_Demo

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

The binary runs comprehensive unit tests covering:
- Basic record operations (add, find, delete)
- Query error handling (parse errors vs. empty results)
- Soft deletion and automatic compaction
- Record updates with index maintenance
- Index creation, dropping, and performance verification
- Edge cases (empty tables, invalid columns)

Followed by performance benchmarks comparing baseline linear scan implementations against indexed lookups.
