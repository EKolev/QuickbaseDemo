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

    /**
        Convert string column name to ColumnType enum
        @param columnName: Column name as string ("column0", "column1", etc.)
        @return: ColumnType enum value, or std::nullopt if invalid
    */
    inline std::optional<ColumnType> stringToColumnType(std::string_view columnName) noexcept
    {
        if (columnName == "column0")
            return ColumnType::COLUMN0;
        if (columnName == "column1")
            return ColumnType::COLUMN1;
        if (columnName == "column2")
            return ColumnType::COLUMN2;
        if (columnName == "column3")
            return ColumnType::COLUMN3;
        return std::nullopt;
    }
    // QBTable class represents a collection of records with optimized indexing and deletion handling
    class QBTable
    {
    private:
        // container memebers
        std::vector<QBRecord> records_;
        std::vector<bool> deleted_;
        // pkIndex_ - pk indexing
        std::unordered_map<uint, size_t> pkIndex_;
        // secondaryIndexedColumns_ - track which non-pk columns are indexed
        std::set<ColumnType> secondaryIndexedColumns_;
        // IndexKey - typed key for secondary indexes
        using IndexKey = std::variant<uint, long, std::string>;
        // secondaryIndexes_ - generic index for secondary columns: (columnID, IndexKey) -> record indices
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
        bool deleteRecordByID(const uint &id, const bool hardDelete = false);
        void compactRecords();
        std::vector<QBRecord> findMatching(ColumnType column, std::string_view matchString) const;
        // backward-compatible version using string column name
        std::vector<QBRecord> findMatching(std::string_view columnName, std::string_view matchString) const;

        // query info
        size_t activeRecordCount() const noexcept;
        size_t totalRecordCount() const noexcept;
        

        // direct access for testing
        const std::vector<QBRecord> &getRecords() const;
        const std::vector<bool> &getDeletedFlags() const;
    };
}