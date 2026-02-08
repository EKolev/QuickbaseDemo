#include "../include/Quickbase.hpp"
#include <algorithm>
#include <charconv>

// Quickbase database definitions
namespace db
{
    /**
     * Find matching records by column type and value
     * Uses primary key index for COLUMN0, secondary indexes for other columns,
     * or falls back to linear scan for non-indexed columns
     */
    std::vector<QBRecord> QBTable::findMatching(db::ColumnType columnID, std::string_view matchString) const
    {
        std::vector<QBRecord> result;
        // handle queries on primary key
        if (columnID == db::ColumnType::COLUMN0)
        {
            db::uint matchValue = 0;
            auto convResult = std::from_chars(matchString.data(), matchString.data() + matchString.size(), matchValue);
            // check if no error and entire string was consumed
            if (convResult.ec != std::errc{} || convResult.ptr != matchString.data() + matchString.size())
                return {}; // Invalid conversion

            auto it = pkIndex_.find(matchValue);
            if (it == pkIndex_.end())
                return {}; // No matches

            result.push_back(records_[it->second]);

            return result;
        }

        // handle queries on non-pk columns - secondery indexed
        if (secondaryIndexedColumns_.contains(columnID))
        {
            // create typed field from matchString depending on column type
            db::FieldType field;
            switch (columnID)
            {
            case db::ColumnType::COLUMN1:
            case db::ColumnType::COLUMN3:
                field = std::string(matchString);
                break;
            case db::ColumnType::COLUMN2:
            {
                long val = 0;
                auto convResult = std::from_chars(matchString.data(), matchString.data() + matchString.size(), val);
                // check if no error and entire string was consumed
                if (convResult.ec != std::errc{} || convResult.ptr != matchString.data() + matchString.size())
                    return {};
                field = val;
            }
            break;
            default:
                // could not convert to a valid field type for indexing - return empty result
                return {};
            }

            auto it = secondaryIndexes_.find({columnID, field});
            if (it == secondaryIndexes_.end())
                return {}; // No match found

            for (size_t idx : it->second)
            {
                if (!deleted_[idx])
                    result.push_back(records_[idx]);
            }
            return result;
        }
        else
        {
            // fall back to linear scan for non-indexed columns
            return linearScan(columnID, matchString);
        }
    }

    /**
     * Delete a record by its unique ID - primary key column0
     */
    bool QBTable::deleteRecordByID(db::uint id, bool hardDelete)
    {
        // lookup record index via primary key
        auto pkIt = pkIndex_.find(id);
        if (pkIt == pkIndex_.end())
            return false;

        size_t recordIdx = pkIt->second;

        // already deleted
        if (!hardDelete && deleted_[recordIdx])
            return false;

        // soft delete
        if (!hardDelete)
        {
            deleted_[recordIdx] = true;
            // remove from PK index
            pkIndex_.erase(pkIt);

            // remove from secondary indexes
            for (auto it = secondaryIndexes_.begin(); it != secondaryIndexes_.end();)
            {
                auto &indices = it->second;
                indices.erase(std::remove(indices.begin(), indices.end(), recordIdx), indices.end());
                if (indices.empty())
                    it = secondaryIndexes_.erase(it);
                else
                    ++it;
            }
        }
        else // hard delete
        {
            size_t lastIdx = records_.size() - 1;

            // swap the record to delete with the last record
            std::swap(records_[recordIdx], records_[lastIdx]);
            std::swap(deleted_[recordIdx], deleted_[lastIdx]);

            // remove last record
            records_.pop_back();
            deleted_.pop_back();

            // rebuild all indexes for safety
            rebuildPrimaryKeyIndex();
            for (const db::ColumnType colID : secondaryIndexedColumns_)
                rebuildSecondaryIndexForColumn(colID);
        }

        return true;
    }

    /**
     * Convert column value to FieldType for generic indexing
     * This allows us to use a single index for all column types
     */
    db::FieldType QBTable::getColumnField(size_t recordIdx, db::ColumnType columnID) const
    {
        // bounds check
        if (recordIdx >= records_.size())
            return std::string{};

        if (deleted_[recordIdx])
            return std::string{};

        const db::QBRecord &rec = records_[recordIdx];
        switch (columnID)
        {
        case db::ColumnType::COLUMN1:
            return rec.column1;
        case db::ColumnType::COLUMN2:
            return rec.column2;
        case db::ColumnType::COLUMN3:
            return rec.column3;
        case db::ColumnType::COLUMN0:
            throw std::logic_error("COLUMN0 should not be used in getColumnField");
        }
        return std::string{};
    }

    /**
     * Rebuild the primary key index - column0
     * Used during construction, compact, and addRecord operations
     */
    void QBTable::rebuildPrimaryKeyIndex()
    {
        pkIndex_.clear();
        for (size_t i = 0; i < records_.size(); ++i)
        {
            if (!deleted_[i])
            {
                pkIndex_[records_[i].column0] = i;
            }
        }
    }

    /**
     * Rebuild the index for a specific secondary column
     * Called when createIndex() is invoked for non-PK columns
     */
    void QBTable::rebuildSecondaryIndexForColumn(db::ColumnType columnID)
    {
        // remove any existing entries for this column
        removeSecondaryIndexForColumn(columnID);

        // rebuild index from scratch
        for (size_t i = 0; i < records_.size(); ++i)
        {
            if (!deleted_[i])
            {
                db::FieldType key = getColumnField(i, columnID);
                secondaryIndexes_[{columnID, key}].push_back(i);
            }
        }
    }

    /*
     * Remove all secondary index entries for a specific column
     */
    void QBTable::removeSecondaryIndexForColumn(ColumnType columnID)
    {
        auto it = secondaryIndexes_.begin();
        while (it != secondaryIndexes_.end())
        {
            if (it->first.first == columnID)
                it = secondaryIndexes_.erase(it);
            else
                ++it;
        }
    }

    /*
     * Linear scan fallback for non-indexed columns
     */
    std::vector<db::QBRecord> QBTable::linearScan(db::ColumnType columnID, std::string_view matchString) const
    {
        std::vector<db::QBRecord> result;

        for (size_t i = 0; i < records_.size(); ++i)
        {
            if (deleted_[i])
                continue;

            const db::QBRecord &rec = records_[i];
            bool matches = false;

            switch (columnID)
            {
            case db::ColumnType::COLUMN1:
                matches = (rec.column1.find(matchString) != std::string::npos);
                break;

            case db::ColumnType::COLUMN2:
            {
                long matchValue = 0;
                auto convResult = std::from_chars(matchString.data(), matchString.data() + matchString.size(), matchValue);
                // check if no error and entire string was consumed
                if (convResult.ec != std::errc{} || convResult.ptr != matchString.data() + matchString.size())
                    return {};
                matches = (rec.column2 == matchValue);
            }
            break;

            case db::ColumnType::COLUMN3:
                matches = (rec.column3.find(matchString) != std::string::npos);
                break;
            case db::ColumnType::COLUMN0:
                // Should never reach here - COLUMN0 is always indexed
                break;
            }

            if (matches)
                result.push_back(rec);
        }

        return result;
    }

    /*
     * Create an index on a specific column
     * Important!!! - Column0 (primary key) is always indexed for O(1) performance
     * Other columns can be optionally indexed for faster queries
     */
    void QBTable::createIndex(db::ColumnType columnID)
    {
        // if column is already indexed, or is primary key exit
        if (columnID == ColumnType::COLUMN0 || secondaryIndexedColumns_.contains(columnID))
            return;
        else
        {
            secondaryIndexedColumns_.insert(columnID);
            rebuildSecondaryIndexForColumn(columnID);
        }
    }

    /**
     * Drop an index on a specific column
     * Important!!! - Column0 (PK) cannot be dropped - it's always indexed
     * Frees memory for secondary indexes
     */
    void QBTable::dropIndex(db::ColumnType columnID)
    {
        // Column0 is always indexed - can't drop it
        if (columnID == db::ColumnType::COLUMN0)
            return;

        secondaryIndexedColumns_.erase(columnID);
        removeSecondaryIndexForColumn(columnID);
    }

    /**
     * Check if a column is indexed
     */
    bool QBTable::isColumnIndexed(db::ColumnType columnID) const
    {
        if (columnID == db::ColumnType::COLUMN0)
            return true;
        return secondaryIndexedColumns_.contains(columnID);
    }

    /**
     * Add a new record to the collection
     * Updates both primary key index and secondary indexes
     */
    void QBTable::addRecord(const db::QBRecord &record)
    {
        size_t idx = records_.size();
        records_.push_back(record);
        deleted_.push_back(false);

        // update primary key index
        pkIndex_[record.column0] = idx;

        // update secondary indexes
        for (db::ColumnType columnID : secondaryIndexedColumns_)
        {
            db::FieldType key = getColumnField(idx, columnID);
            secondaryIndexes_[{columnID, key}].push_back(idx);
        }
    }

    /**
     * Get count of active records
     */
    size_t QBTable::activeRecordsCount() const noexcept
    {
        // static_cast to size_t to avoid signed/unsigned comparison warnings
        return static_cast<size_t>(std::count_if(deleted_.begin(), deleted_.end(), [](bool del) -> bool
                             { return !del; }));
    }

    /**
     * Get total record count including deleted records
     */
    size_t QBTable::totalRecordsCount() const noexcept
    {
        return records_.size();
    }

    /**
     * Compact the collection by removing deleted records
     * Rebuilds primary and secondary indexes with new positions
     */
    void QBTable::compactRecords()
    {
        // count active records
        size_t activeCount = activeRecordsCount();
        std::vector<QBRecord> compacted;
        // only allocate what's needed for active records
        compacted.reserve(activeCount);

        // move only active records
        size_t i = 0;
        std::for_each(records_.begin(), records_.end(), [&](QBRecord &rec)
                      { 
                        if (!deleted_[i])
                        compacted.push_back(std::move(rec));
                        ++i; });

        records_ = std::move(compacted);
        deleted_.assign(records_.size(), false); // reset deleted flags

        // rebuild all indexes from scratch
        rebuildPrimaryKeyIndex();
        secondaryIndexes_.clear();
        for (const db::ColumnType colID : secondaryIndexedColumns_)
            rebuildSecondaryIndexForColumn(colID);
    }
}