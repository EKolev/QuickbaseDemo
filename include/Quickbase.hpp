#pragma once
#include <vector>
#include <unordered_map>
#include <set>
#include <map>
#include <string>
#include <string_view>
#include <variant>
#include <optional>
#include <limits>
// Quickbase database declarations
namespace db
{
    using uint = unsigned int;
    // QBTable record
    struct QBRecord
    {
        uint column0; // unique id column
        std::string column1;
        long column2;
        std::string column3;
    };

    // table column identifier enum
    enum class ColumnType : uint8_t
    {
        COLUMN0, // assumed to be the pk column
        COLUMN1,
        COLUMN2,
        COLUMN3
    };

    // QBTable class represents a collection of records with optimized indexing and deletion handling
    class QBTable
    {
    private:
        // container memebers
        std::vector<QBRecord> records_;
        // deleted_ - parallel vector to records_ for soft deletion tracking
        std::vector<bool> deleted_;
        // pkIndex_ - pk indexing
        std::unordered_map<uint, size_t> pkIndex_;
        // secondaryIndexedColumns_ - track which non-pk columns are indexed
        std::set<ColumnType> secondaryIndexedColumns_;
        // IndexKey - typed key for secondary indexed columns, supports multiple types of columns
        using IndexKey = std::variant<uint, long, std::string>;
        // secondaryIndexes_ - index for secondary-indexed columns: (columnID, IndexKey) -> record indices
        std::map<std::pair<ColumnType, IndexKey>, std::vector<size_t>> secondaryIndexes_;

        // helper methods for indexing
        IndexKey getColumnIndexKey(size_t recordIdx, ColumnType columnID) const;
        void rebuildPrimaryKeyIndex();
        void rebuildSecondaryIndexForColumn(ColumnType columnID);
        void removeSecondaryIndexForColumn(ColumnType columnID);
        // kept private to prevent accidental linear scans - only used internally for non-indexed queries
        std::vector<QBRecord> linearScan(ColumnType columnID, std::string_view matchString) const;

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
        void createIndex(ColumnType columnID);
        void dropIndex(ColumnType columnID);
        bool isColumnIndexed(ColumnType columnID) const;

        // core operations
        void addRecord(const QBRecord &record);
        bool deleteRecordByID(uint id, bool hardDelete = false);
        void compactRecords();
        std::vector<QBRecord> findMatching(ColumnType column, std::string_view matchString) const;
        // backward-compatible version using string column name
        std::vector<QBRecord> findMatching(std::string_view columnName, std::string_view matchString) const;

        // query info
        size_t activeRecordsCount() const noexcept;
        size_t totalRecordsCount() const noexcept;

        // direct access for testing
        const std::vector<QBRecord> &getRecords() const;
        const std::vector<bool> &getDeletedFlags() const;
    };

    /**
    * Convert string column name to ColumnType enum
    * only used to support backward-compatible findMatching with string column names, not intended for general use
    */
    inline std::optional<ColumnType> stringToColumnType(std::string_view columnName) noexcept
    {
        if (columnName.size() <= 7 || columnName.compare(0, 6, "column") != 0)
            return std::nullopt;
        switch (columnName[6])
        {
        case '0':
            return ColumnType::COLUMN0;
        case '1':
            return ColumnType::COLUMN1;
        case '2':
            return ColumnType::COLUMN2;
        case '3':
            return ColumnType::COLUMN3;
        default:
            return std::nullopt;
        }
    }
}