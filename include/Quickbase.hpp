#pragma once
#include <vector>
#include <unordered_map>
#include <set>
#include <map>
#include <string>
#include <string_view>
#include <variant>
#include "./Quickbase_types.hpp"

// Quickbase static database declarations
namespace db
{

    // QBTable class represents a collection of records with optimized indexing and deletion handling
    class QBTable
    {
    private:
        // container memebers
        std::vector<db::QBRecord> records_;
        // deleted_ - parallel vector to records_ for soft deletion tracking
        std::vector<bool> deleted_;

        // table indexing members
        // pkIndex_ - primary key  indexing
        std::unordered_map<db::uint, size_t> pkIndex_;
        // secondaryIndexedColumns_ - track which non-pk columns are indexed
        std::set<db::ColumnType> secondaryIndexedColumns_;
        // secondaryIndexes_ - index for secondary-indexed columns: (columnID, FieldType) -> record indices
        std::map<std::pair<db::ColumnType, db::FieldType>, std::vector<size_t>> secondaryIndexes_;

        // helper methods for indexing
        db::FieldType getColumnField(size_t recordIdx, db::ColumnType columnID) const;
        void rebuildPrimaryKeyIndex();
        void rebuildSecondaryIndexForColumn(db::ColumnType columnID);
        void removeSecondaryIndexForColumn(db::ColumnType columnID);
        // kept private to prevent accidental linear scans - only used internally for non-indexed queries
        std::vector<db::QBRecord> linearScan(db::ColumnType columnID, std::string_view matchString) const;

    public:
        QBTable() = default;
        // allow only move operations on QBTables objects, forbid copying to prevent expensive deep copies
        QBTable(const QBTable &) = delete;
        QBTable &operator=(const QBTable &) = delete;
        // move operations
        QBTable(QBTable &&) noexcept = default;
        QBTable &operator=(QBTable &&) noexcept = default;
        ~QBTable() = default;

        // index management - create/drop indexes on demand
        void createIndex(db::ColumnType columnID);
        void dropIndex(db::ColumnType columnID);
        bool isColumnIndexed(db::ColumnType columnID) const;

        // core operations
        void addRecord(const QBRecord &record);
        bool deleteRecordByID(db::uint id, bool hardDelete = false);
        void compactRecords();
        std::vector<QBRecord> findMatching(db::ColumnType column, std::string_view matchString) const;

        // get record counts
        size_t activeRecordsCount() const noexcept;
        size_t totalRecordsCount() const noexcept;
    };

}