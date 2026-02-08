#pragma once
#include <vector>
#include <unordered_map>
#include <set>
#include <map>
#include <string>
#include <variant>
#include <functional>
#include <stdexcept>
#include "./Quickbase_types.hpp"

// Quickbase dynamic database declarations
namespace db
{
    class QBTableDynamic
    {
    private:
        // container memebers
        std::vector<db::QBRecordDynamic> records_;
        // deleted_ - parallel vector to records_ for soft deletion tracking
        std::vector<bool> deleted_;

        // table schema and indexing members
        // columns_ track physical columns in the table, used for schema enforcement
        std::set<std::string> columns_;
        // derivedColumns_ track virtual columns computed from other columns
        std::unordered_map<std::string, db::DerivedFunc> derivedColumns_;
        // pkIndex_ - primary key  indexing
        std::unordered_map<db::uint, size_t> pkIndex_;
        // secondaryIndexedColumns_ - track which non-pk columns are indexed
        std::set<std::string> secondaryIndexedColumns_;
        // secondaryIndexes_ - index for secondary-indexed columns: (column, FieldType) -> record indices
        std::map<std::pair<std::string, db::FieldType>, std::vector<size_t>> secondaryIndexes_;

        // helper methods for indexing
        void rebuildPrimaryIndex();
        void rebuildSecondaryIndex(const std::string& column);
        db::FieldType getField(size_t recordIdx, const std::string& column) const;

    public:
        QBTableDynamic() = default;
        // allow only move operations on QBTableDynamic objects, forbid copying to prevent expensive deep copies
        QBTableDynamic(const db::QBTableDynamic&) = delete;
        QBTableDynamic& operator=(const db::QBTableDynamic&) = delete;
        // move operations
        QBTableDynamic(db::QBTableDynamic&&) noexcept = default;
        QBTableDynamic& operator=(db::QBTableDynamic&&) noexcept = default;
        ~QBTableDynamic() = default;

        // schema operations
        bool addColumn(const std::string& name, db::FieldType defaultValue = std::string{});
        void removeColumn(const std::string& name);
        bool addDerivedColumn(const std::string& name, db::DerivedFunc func);

        // index management - create/drop indexes on demand
        void createIndex(const std::string& column);
        void dropIndex(const std::string& column);

        // core operations
        bool addRecord(const db::QBRecordDynamic& record);
        bool deleteRecordByID(db::uint id, bool hardDelete = false);
        void compactRecords();
        std::vector<db::QBRecordDynamic> findMatching(std::string column, db::FieldType value) const;

        // get record counts
        size_t activeRecordsCount() const noexcept;
        size_t totalRecordsCount() const noexcept;
        
    };
}
