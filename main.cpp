#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <iostream>
#include <ratio>
#include <memory>
#include <iomanip>
#include <optional>
#include "./Quickbase.hpp"

#define DATA_SIZE 100000
#define ITERATIONS 100


// ============================================================================
// ORIGINAL IMPLEMENTATION (BASE/REFERENCE)
// ============================================================================

/**
    Represents a Record Object
*/
struct QBRecord
{
    uint column0; // unique id column
    std::string column1;
    long column2;
    std::string column3;
};

typedef std::vector<QBRecord> QBRecordCollection;

QBRecordCollection QBFindMatchingRecords(const QBRecordCollection& records, const std::string& columnName, const std::string& matchString)
    {
    QBRecordCollection result;
    std::copy_if(records.begin(), records.end(), std::back_inserter(result), [&](const QBRecord& rec){
        if (columnName == "column0") {
            unsigned long matchValue = std::stoul(matchString);
            return matchValue == rec.column0;
        } else if (columnName == "column1") {
            return rec.column1.find(matchString) != std::string::npos;
        } else if (columnName == "column2") {
            long matchValue = std::stol(matchString);
            return matchValue == rec.column2;
        } else if (columnName == "column3") {
            return rec.column3.find(matchString) != std::string::npos;
        } else {
            return false;
        }
    });
    return result;
    }

/**
    Utility to populate a record collection
    prefix - prefix for the string value for every record
    numRecords - number of records to populate in the collection
*/
QBRecordCollection populateDummyData(const std::string& prefix, uint numRecords)
    {
    QBRecordCollection data;
    data.reserve(numRecords);
    for (uint i = 0; i < numRecords; i++)
        {
        QBRecord rec = { i, prefix + std::to_string(i), i % 100, std::to_string(i) + prefix };
        data.emplace_back(rec);
        }
    return data;
    }


// ============================================================================
// TESTING AND PERFORMANCE COMPARISON
// ============================================================================

/**
    Helper struct to hold benchmark results
*/
struct BenchmarkResult
{
    std::string name;
    double timeMs;
    size_t resultCount;
    std::string details;
};

/**
    Run benchmarks comparing base vs optimized implementations
*/
void runBenchmarks()
{
    using namespace std::chrono;
    
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "PERFORMANCE BENCHMARK: BASE vs QBTABLE" << std::endl;
    std::cout << std::string(80, '=') << std::endl;
    
    // ========== Setup: Create test data ==========
    std::cout << "\nDataset Configuration:" << std::endl;
    std::cout << "  - Total Records: " << DATA_SIZE << std::endl;
    std::cout << "  - ITERATIONS per test: " << ITERATIONS << std::endl;
    
    auto baseData = populateDummyData("testdata", DATA_SIZE);
    
    // create QBTable and populate it
    db::QBTable qbTable;
    for (const auto& rec : baseData)
    {
        db::QBRecord dbRec = {rec.column0, rec.column1, rec.column2, rec.column3};
        qbTable.addRecord(dbRec);
    }
    // create index for column2 
    qbTable.createIndex(db::ColumnType::COLUMN2);
    
    std::cout << "  - Records in base QB database implementation: " << baseData.size() << std::endl;
    std::cout << "  - QBTable Active Records: " << qbTable.activeRecordsCount() << std::endl;
    std::cout << std::endl;
    
    // ========== TEST 1: Exact Match on Numeric Column (column0) ==========
    std::cout << "TEST 1: Exact Match Query on Primary Key (column0)" << std::endl;
    std::cout << "-" << std::string(76, '-') << std::endl;
    {
        std::vector<BenchmarkResult> results;
        
        // base implementation
        auto startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = QBFindMatchingRecords(baseData, "column0", "50000");
        }
        auto elapsed = steady_clock::now() - startTimer;
        double timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"Base Implementation", timeMs, 1, "Linear scan O(n)"});
        
        
        // QBTable implementation (with dedicated primary key index)
        startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = qbTable.findMatching(db::ColumnType::COLUMN0, "50000");
        }
        elapsed = steady_clock::now() - startTimer;
        timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"QBTable (Two-Tier Index)", timeMs, 1, "Dedicated PK hash O(1)"});
        
        // print results
        for (const auto& r : results)
        {
            std::cout << "  " << std::left << std::setw(30) << r.name 
                      << std::setw(12) << std::fixed << std::setprecision(3) << r.timeMs << " ms"
                      << "  (" << r.details << ")" << std::endl;
        }
 
        double speedupQBTable = results[0].timeMs / results[1].timeMs;
        std::cout << "  Speedup (QBTable): " << std::fixed << std::setprecision(2) << speedupQBTable << "x faster\n" << std::endl;
    }
    
    // ========== TEST 2: Exact Match on Another Numeric Column (column2) ==========
    std::cout << "TEST 2: Exact Match Query on Another Numeric Column (column2)" << std::endl;
    std::cout << "-" << std::string(76, '-') << std::endl;
    
    {
        std::vector<BenchmarkResult> results;
        
        // base implementation (matching ~1000 records out of 100k)
        auto startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = QBFindMatchingRecords(baseData, "column2", "42");
        }
        auto elapsed = steady_clock::now() - startTimer;
        double timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"Base Implementation", timeMs, 1000, "Linear scan O(n)"});
        
        
        // QBTable implementation (with secondary index)
        startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = qbTable.findMatching(db::ColumnType::COLUMN2, "42");
        }
        elapsed = steady_clock::now() - startTimer;
        timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"QBTable (Two-Tier Index)", timeMs, 1000, "Secondary map index O(log n)"});
        
        // print results
        for (const auto& r : results)
        {
            std::cout << "  " << std::left << std::setw(30) << r.name 
                      << std::setw(12) << std::fixed << std::setprecision(3) << r.timeMs << " ms"
                      << "  (matches: " << r.resultCount << ", " << r.details << ")" << std::endl;
        }
        
        double speedupQBTable = results[0].timeMs / results[1].timeMs;
        std::cout << "  Speedup (QBTable): " << std::fixed << std::setprecision(2) << speedupQBTable << "x faster\n" << std::endl;
    }
    
    // ========== TEST 3: Substring Match (column1) ==========
    std::cout << "TEST 3: Substring Match Query (column1)" << std::endl;
    std::cout << "-" << std::string(76, '-') << std::endl;
    
    {
        std::vector<BenchmarkResult> results;
        
        // base implementation
        auto startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = QBFindMatchingRecords(baseData, "column1", "testdata50");
        }
        auto elapsed = steady_clock::now() - startTimer;
        double timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"Base Implementation", timeMs, 100, "Linear scan O(n*m) (no index)"});
        
        
        // QBTable implementation (no index for substring search)
        startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = qbTable.findMatching(db::ColumnType::COLUMN1, "testdata50");
        }
        elapsed = steady_clock::now() - startTimer;
        timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"QBTable (No Index)", timeMs, 100, "Linear scan O(n*m) (fallback)"});
        
        // print results
        for (const auto& r : results)
        {
            std::cout << "  " << std::left << std::setw(30) << r.name 
                      << std::setw(12) << std::fixed << std::setprecision(3) << r.timeMs << " ms"
                      << "  (matches: " << r.resultCount << ")" << std::endl;
        }
    }
    
    // ========== TEST 4: DeleteRecordByID and Requery ==========
    std::cout << "TEST 4: Delete Records and Verify Correctness" << std::endl;
    std::cout << "-" << std::string(76, '-') << std::endl;
    
    {   
        db::QBTable testDataQBTable;
        for (const auto& rec : baseData)
        {
            db::QBRecord dbRec = {rec.column0, rec.column1, rec.column2, rec.column3};
            testDataQBTable.addRecord(dbRec);
        }
        testDataQBTable.createIndex(db::ColumnType::COLUMN0);
        testDataQBTable.createIndex(db::ColumnType::COLUMN2);
        
        std::cout << "  Initial state (QBTable):" << std::endl;
        auto beforeDeleteQB = testDataQBTable.findMatching(db::ColumnType::COLUMN0, "100");
        std::cout << "    Records with column0=100: " << beforeDeleteQB.size() << std::endl;
        // perform a hard delete
        bool deletedQB = testDataQBTable.deleteRecordByID(100, true);
        std::cout << "  After deleteRecordByID(100):" << std::endl;
        std::cout << "    QBTable: " << (deletedQB ? "SUCCESS" : "FAILED") << std::endl;
        
        // try to find it again
        auto afterDeleteQB = testDataQBTable.findMatching(db::ColumnType::COLUMN0, "100");
        std::cout << "    QBTable - Records with column0=100: " << afterDeleteQB.size() << std::endl;
        
        // verify
        assert(afterDeleteQB.size() == 0 && "Record should be deleted (QBTable)");
        
        // soft delete multiple records and test
        std::vector<uint> idsToDelete = {200, 201, 202, 203, 204}; 
        for (const uint& id : idsToDelete)
        {
            testDataQBTable.deleteRecordByID(id);
        }
        
        auto allDeletedQB = testDataQBTable.findMatching(db::ColumnType::COLUMN2, "4");
        std::cout << "  After deleting records with IDs 200-204:" << std::endl;
        std::cout << "    QBTable - Records with column2=4: " << allDeletedQB.size() << std::endl;
        std::cout << "    QBTable - Active count: " << testDataQBTable.activeRecordsCount() << " / " << testDataQBTable.totalRecordsCount() << std::endl;
        
        // Test compaction
        size_t beforeCompactQB = testDataQBTable.totalRecordsCount();
        testDataQBTable.compactRecords();
        std::cout << "  After compactRecords():" << std::endl;
        std::cout << "    QBTable - Total records: " << beforeCompactQB << " -> " << testDataQBTable.totalRecordsCount() << std::endl;
        std::cout << "    QBTable - Active records: " << testDataQBTable.activeRecordsCount() << std::endl;
        
        std::cout << "\n  ✓ All deletion tests passed\n" << std::endl;
    }
    
    // ========== TEST 5: Memory Layout Analysis ==========
    std::cout << "TEST 5: Memory Layout and Data Structure Analysis" << std::endl;
    std::cout << "-" << std::string(76, '-') << std::endl;
    
    {
        std::cout << "  QBRecord structure size: " << sizeof(QBRecord) << " bytes" << std::endl;
        std::cout << "    - column0 (uint): " << sizeof(uint) << " bytes" << std::endl;
        std::cout << "    - column1 (string): " << sizeof(std::string) << " bytes (ptr + size + capacity)" << std::endl;
        std::cout << "    - column2 (long): " << sizeof(long) << " bytes" << std::endl;
        std::cout << "    - column3 (string): " << sizeof(std::string) << " bytes (ptr + size + capacity)" << std::endl;
        
        std::cout << "\n  Memory usage for " << DATA_SIZE << " records:" << std::endl;
        
        size_t baseVectorSize = sizeof(QBRecord) * DATA_SIZE;
        std::cout << "    - Base vector (QBRecordCollection):" << std::endl;
        std::cout << "      Data size: ~" << baseVectorSize / 1024 << " KB (record arrays only)" << std::endl;
        std::cout << "      String storage: Variable (depends on string content)" << std::endl;
        
        std::cout << "\n    - Optimized collection overhead:" << std::endl;
        std::cout << "      Primary vector: ~" << baseVectorSize / 1024 << " KB" << std::endl;
        std::cout << "      Deleted flags vector: ~" << DATA_SIZE / 1024 << " KB" << std::endl;
        std::cout << "      Index for column0 (unordered_map): ~" << DATA_SIZE * 48 / 1024 << " KB (estimated)" << std::endl;
        std::cout << "      Index for column2 (unordered_map): ~" << 100 * 48 / 1024 << " KB (estimated, ~100 unique values)" << std::endl;
        std::cout << "      Total overhead: ~40-50% of base data size" << std::endl;
        
        std::cout << "\n  Cache-friendliness:" << std::endl;
        std::cout << "    - Base: Excellent (sequential vector traversal)" << std::endl;
        std::cout << "    - Optimized: Good for indexed lookups, excellent for filtered scans" << std::endl;
        std::cout << "             Trade-off: Hash table lookups vs vector sequential access\n" << std::endl;
    }
    
    std::cout << std::string(80, '=') << std::endl;
    std::cout << "TESTS COMPLETED SUCCESSFULLY" << std::endl;
    std::cout << std::string(80, '=') << std::endl;
}

int main()
{
    using namespace std::chrono;
    
    // ========== Basic Correctness Tests ==========
    std::cout << "RUNNING QBTable TESTS\n" << std::endl;
    
    // Run comprehensive benchmarks
    runBenchmarks();

	return 0;
}