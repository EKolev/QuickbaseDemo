#include <string>
#include <vector>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <iostream>
#include <iomanip>
#include "../include/Quickbase.hpp"
#include "../include/Quickbase_dynamic.hpp"

#define DATA_SIZE 100000
#define ITERATIONS 100

// ============================================================================
// ORIGINAL IMPLEMENTATION (BASE/REFERENCE)
// ============================================================================

typedef std::vector<db::QBRecord> QBRecordCollection;

QBRecordCollection QBFindMatchingRecords(const QBRecordCollection &records, const std::string &columnName, const std::string &matchString)
{
    QBRecordCollection result;
    std::copy_if(records.begin(), records.end(), std::back_inserter(result), [&](const db::QBRecord &rec)
                 {
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
        } });
    return result;
}

/**
    Utility to populate a record collection
    prefix - prefix for the string value for every record
    numRecords - number of records to populate in the collection
*/
QBRecordCollection populateDummyData(const std::string &prefix, uint numRecords)
{
    QBRecordCollection data;
    data.reserve(numRecords);
    for (uint i = 0; i < numRecords; i++)
    {
        db::QBRecord rec = {i, prefix + std::to_string(i), i % 100, std::to_string(i) + prefix};
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

    std::cout << "\n"
              << std::string(80, '=') << std::endl;
    std::cout << "PERFORMANCE BENCHMARK: BASE vs QBTABLE vs QBTABLE DYNAMIC" << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    // ========== Setup: Create test data ==========
    std::cout << "\nDataset Configuration:" << std::endl;
    std::cout << "  - Total Records: " << DATA_SIZE << std::endl;
    std::cout << "  - ITERATIONS per test: " << ITERATIONS << std::endl;
    // populate base data collection
    auto baseData = populateDummyData("testdata", DATA_SIZE);

    // create QBTable and populate it
    db::QBTable qbTable;
    for (const auto &rec : baseData)
    {
        db::QBRecord dbRec = {rec.column0, rec.column1, rec.column2, rec.column3};
        qbTable.addRecord(dbRec);
    }
    // create index for column2
    qbTable.createIndex(db::ColumnType::COLUMN2);

    // create qbTableDynamic and populate it
    db::QBTableDynamic qbTableDynamic;
    // add dynamic columns
    qbTableDynamic.addColumn("column1", std::string{});
    qbTableDynamic.addColumn("column2", 0L);
    qbTableDynamic.addColumn("column3", std::string{});
    // create index for column2
    qbTableDynamic.createIndex("column2");
    // copy records from baseData to qbTableDynamic
    for (const auto &rec : baseData)
    {
        qbTableDynamic.addRecord({rec.column0, {{"column1", rec.column1}, {"column2", rec.column2}, {"column3", rec.column3}}});
    }
    std::cout << "  - Records in base QB database implementation: " << baseData.size() << std::endl;
    std::cout << "  - QBTable Active Records: " << qbTable.activeRecordsCount() << std::endl;
    std::cout << "  - QBTableDynamic Active Records: " << qbTableDynamic.activeRecordsCount() << std::endl;
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
        results.push_back({"Base Implementation", timeMs, 1, "\t(queries: 100)"});

        // QBTable implementation (with dedicated primary key index)
        startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = qbTable.findMatching(db::ColumnType::COLUMN0, "50000");
        }
        elapsed = steady_clock::now() - startTimer;
        timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"QBTable", timeMs, 1, "\t(queries: 100)"});

        // QBTableDynamic implementation (primary key)
        startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = qbTableDynamic.findMatching("id", 50000u);
        }
        elapsed = steady_clock::now() - startTimer;
        timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"QBTableDynamic", timeMs, 1, "\t(queries: 100)"});
        // print results
        for (const auto &r : results)
        {
            std::cout << "  " << std::left << std::setw(30) << r.name
                      << std::setw(12) << std::fixed << std::setprecision(3) << r.timeMs << " ms"
                      << r.details << std::endl;
        }

        double speedupQBTable = results[0].timeMs / results[1].timeMs;
        double speedupQBTableDynamic = results[0].timeMs / results[2].timeMs;
        std::cout << "  Speedup (QBTable): " << std::fixed << std::setprecision(2) << speedupQBTable << "x faster\n"
                  << "  Speedup (QBTableDynamic): " << std::fixed << std::setprecision(2) << speedupQBTableDynamic << "x faster\n"
                  << std::endl;
    }

    // ========== TEST 2: Exact Match on Another Numeric Column (column2) ==========
    std::cout << "TEST 2: Exact Match Query on Secondary Indexed Numeric Column (column2)" << std::endl;
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
        results.push_back({"Base Implementation", timeMs, ITERATIONS, ""});

        // QBTable implementation (with secondary index)
        startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = qbTable.findMatching(db::ColumnType::COLUMN2, "42");
        }
        elapsed = steady_clock::now() - startTimer;
        timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"QBTable", timeMs, ITERATIONS, ""});

        // QBTableDynamic implementation (secondary index)
        startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = qbTableDynamic.findMatching("column2", 42);
        }
        elapsed = steady_clock::now() - startTimer;
        timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"QBTableDynamic", timeMs, ITERATIONS, ""});

        // print results
        for (const auto &r : results)
        {
            std::cout << "  " << std::left << std::setw(30) << r.name
                      << std::setw(12) << std::fixed << std::setprecision(3) << r.timeMs << " ms"
                      << "  (queries: " << r.resultCount << r.details << ")" << std::endl;
        }

        double speedupQBTable = results[0].timeMs / results[1].timeMs;
        double speedupQBTableDynamic = results[0].timeMs / results[2].timeMs;
        std::cout << "  Speedup (QBTable): " << std::fixed << std::setprecision(2) << speedupQBTable << "x faster\n"
                  << "  Speedup (QBTableDynamic): " << std::fixed << std::setprecision(2) << speedupQBTableDynamic << "x faster\n"
                  << std::endl;
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
        results.push_back({"Base Implementation", timeMs, 100, ""});

        // QBTable implementation (no index for substring search)
        startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = qbTable.findMatching(db::ColumnType::COLUMN1, "testdata50");
        }
        elapsed = steady_clock::now() - startTimer;
        timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"QBTable", timeMs, 100, ""});

        // QBTableDynamic implementation (no index for substring search)
        startTimer = steady_clock::now();
        for (int i = 0; i < ITERATIONS; ++i)
        {
            auto filtered = qbTableDynamic.findMatching("column1", std::string("testdata50"));
        }
        elapsed = steady_clock::now() - startTimer;
        timeMs = double(elapsed.count()) * steady_clock::period::num / steady_clock::period::den * 1000;
        results.push_back({"QBTableDynamic", timeMs, 100, ""});

        // print results
        for (const auto &r : results)
        {
            std::cout << "  " << std::left << std::setw(30) << r.name
                      << std::setw(12) << std::fixed << std::setprecision(3) << r.timeMs << " ms"
                      << "  (queries: " << r.resultCount << ")" << std::endl;
        }
        std::cout << std::endl;
    }

    // ========== TEST 4: DeleteRecordByID and Requery ==========
    std::cout << "TEST 4: Delete Records and Verify Correctness" << std::endl;
    std::cout << "-" << std::string(76, '-') << std::endl;

    {
        // delete on QBTable
        std::cout << "  Initial state (QBTable):" << std::endl;
        auto beforeDeleteQB = qbTable.findMatching(db::ColumnType::COLUMN0, "100");
        std::cout << "    Records with column0=100: " << beforeDeleteQB.size() << std::endl;
        // perform a hard delete
        bool deletedQB = qbTable.deleteRecordByID(100, true);
        std::cout << "  After deleteRecordByID(100):" << std::endl;
        std::cout << "    QBTable: " << (deletedQB ? "SUCCESS" : "FAILED") << std::endl;

        // try to find it again
        auto afterDeleteQB = qbTable.findMatching(db::ColumnType::COLUMN0, "100");
        std::cout << "    QBTable - Records with column0=100: " << afterDeleteQB.size() << std::endl;

        // verify
        assert(afterDeleteQB.size() == 0 && "Record should be deleted (QBTable)");

        // soft delete multiple records and test
        std::vector<uint> idsToDelete = {200, 201, 202, 203, 204};
        for (const uint &id : idsToDelete)
        {
            qbTable.deleteRecordByID(id);
        }

        auto allDeletedQB = qbTable.findMatching(db::ColumnType::COLUMN2, "4");
        std::cout << "  After deleting records with IDs 200-204:" << std::endl;
        std::cout << "    QBTable - Records with column2=4: " << allDeletedQB.size() << std::endl;
        std::cout << "    QBTable - Active count: " << qbTable.activeRecordsCount() << " / " << qbTable.totalRecordsCount() << std::endl;
        // Test compaction
        size_t beforeCompactQB = qbTable.totalRecordsCount();
        qbTable.compactRecords();
        std::cout << "  After compactRecords():" << std::endl;
        std::cout << "    QBTable - Total records (before compact) -> (after compact): " << beforeCompactQB << " -> " << qbTable.totalRecordsCount() << std::endl;
        std::cout << "    QBTable - Active records: " << qbTable.activeRecordsCount() << "\n" << std::endl;

        // delete on QBTableDynamic
        std::cout << "  Initial state (QBTableDynamic):" << std::endl;
        auto beforeDeleteQBD = qbTableDynamic.findMatching("id", 100u);
        std::cout << "    Records with id=100: " << beforeDeleteQBD.size() << std::endl;
        // perform a hard delete
        bool deletedQBD = qbTableDynamic.deleteRecordByID(100, true);
        std::cout << "  After deleteRecordByID(100):" << std::endl;
        std::cout << "    QBTableDynamic: " << (deletedQBD ? "SUCCESS" : "FAILED") << std::endl;

        // try to find it again
        auto afterDeleteQBD = qbTableDynamic.findMatching("id", 100u);
        std::cout << "    QBTableDynamic - Records with id=100: " << afterDeleteQBD.size() << std::endl;

        // verify
        assert(afterDeleteQBD.size() == 0 && "Record should be deleted (QBTableDynamic)");
        // soft delete multiple records and test
        std::vector<uint> idsToDeleteKeys = {200, 201, 202, 203, 204};
        for (const uint &id : idsToDeleteKeys)
        {
            qbTableDynamic.deleteRecordByID(id);
        }

        auto allDeletedQBD = qbTableDynamic.findMatching("column2", 4L);
        std::cout << "  After deleting records with IDs 200-204:" << std::endl;
        std::cout << "    QBTableDynamic - Records with column2=4: " << allDeletedQBD.size() << std::endl;
        std::cout << "    QBTableDynamic - Active count: " << qbTableDynamic.activeRecordsCount() << " / " << qbTableDynamic.totalRecordsCount() << std::endl;

        // Test compaction
        size_t beforeCompactQBD = qbTableDynamic.totalRecordsCount();
        qbTableDynamic.compactRecords();
        std::cout << "  After compactRecords():" << std::endl;
        std::cout << "    QBTableDynamic - Total records (before compact) -> (after compact): " << beforeCompactQBD << " -> " << qbTableDynamic.totalRecordsCount() << std::endl;
        std::cout << "    QBTableDynamic - Active records: " << qbTableDynamic.activeRecordsCount() << std::endl;

        std::cout << "\n  ✓ All deletion tests passed\n"
                  << std::endl;
    }

    std::cout << std::string(80, '=') << std::endl;
    std::cout << "TESTS COMPLETED SUCCESSFULLY" << std::endl;
    std::cout << std::string(80, '=') << std::endl;
}

int main()
{
    using namespace std::chrono;

    // ========== Basic Correctness Tests ==========
    std::cout << "RUNNING QBTable TESTS\n"
              << std::endl;

    // Run comprehensive benchmarks
    runBenchmarks();

    return 0;
}