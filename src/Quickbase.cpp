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
    std::vector<QBRecord> QBTable::findMatching(ColumnType columnID, std::string_view matchString) const
    {
        std::vector<QBRecord> result;
        // handle queries on primary key
        if (columnID == ColumnType::COLUMN0)
        {
            uint matchValue = 0;
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
            // Build typed key from matchString depending on column type
            IndexKey lookupKey;
            switch (columnID)
            {
            case ColumnType::COLUMN1:
            case ColumnType::COLUMN3:
                lookupKey = std::string(matchString);
                break;
            case ColumnType::COLUMN2:
            {
                long v = 0;
                auto convResult = std::from_chars(matchString.data(), matchString.data() + matchString.size(), v);
                // check if no error and entire string was consumed
                if (convResult.ec != std::errc{} || convResult.ptr != matchString.data() + matchString.size())
                    return {};
                lookupKey = v;
            }
            break;
            default:
                lookupKey = std::string(matchString);
                break;
            }

            auto it = secondaryIndexes_.find({columnID, lookupKey});
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
    bool QBTable::deleteRecordByID(uint id, bool hardDelete)
    {
        // lookup record index via primary key
        auto pkIt = pkIndex_.find(id);
        if (pkIt == pkIndex_.end())
            return false;

        size_t recordIdx = pkIt->second;

        // already deleted (for soft delete)
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
            for (const ColumnType colID : secondaryIndexedColumns_)
                rebuildSecondaryIndexForColumn(colID);
        }

        return true;
    }

    /**
     * Convert column value to IndexKey for generic indexing
     * This allows us to use a single index for all column types
     */
    QBTable::IndexKey QBTable::getColumnIndexKey(size_t recordIdx, ColumnType columnID) const
    {
        // bounds check
        if (recordIdx >= records_.size())
            return std::string{};

        if (deleted_[recordIdx])
            return std::string{};

        const QBRecord &rec = records_[recordIdx];
        switch (columnID)
        {
        case ColumnType::COLUMN1:
            return rec.column1;
        case ColumnType::COLUMN2:
            return rec.column2;
        case ColumnType::COLUMN3:
            return rec.column3;
        case ColumnType::COLUMN0:
            throw std::logic_error("COLUMN0 should not be used in getColumnIndexKey");
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
    void QBTable::rebuildSecondaryIndexForColumn(ColumnType columnID)
    {
        // remove any existing entries for this column
        removeSecondaryIndexForColumn(columnID);

        // rebuild index from scratch
        for (size_t i = 0; i < records_.size(); ++i)
        {
            if (!deleted_[i])
            {
                IndexKey key = getColumnIndexKey(i, columnID);
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
    std::vector<QBRecord> QBTable::linearScan(ColumnType columnID, std::string_view matchString) const
    {
        std::vector<QBRecord> result;

        for (size_t i = 0; i < records_.size(); ++i)
        {
            if (deleted_[i])
                continue;

            const QBRecord &rec = records_[i];
            bool matches = false;

            switch (columnID)
            {
            case ColumnType::COLUMN1:
                matches = (rec.column1.find(matchString) != std::string::npos);
                break;

            case ColumnType::COLUMN2:
            {
                long matchValue = 0;
                auto convResult = std::from_chars(matchString.data(), matchString.data() + matchString.size(), matchValue);
                // check if no error and entire string was consumed
                if (convResult.ec != std::errc{} || convResult.ptr != matchString.data() + matchString.size())
                    return {};
                matches = (rec.column2 == matchValue);
            }
            break;

            case ColumnType::COLUMN3:
                matches = (rec.column3.find(matchString) != std::string::npos);
                break;
            case ColumnType::COLUMN0:
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
    void QBTable::createIndex(ColumnType columnID)
    {
        // if column is already indexed, or is primary key
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
    void QBTable::dropIndex(ColumnType columnID)
    {
        // Column0 is always indexed - can't drop it
        if (columnID == ColumnType::COLUMN0)
            return;

        secondaryIndexedColumns_.erase(columnID);
        removeSecondaryIndexForColumn(columnID);
    }

    /**
     * Check if a column is indexed
     */
    bool QBTable::isColumnIndexed(ColumnType columnID) const
    {
        if (columnID == ColumnType::COLUMN0)
            return true;
        return secondaryIndexedColumns_.contains(columnID);
    }

    /**
     * Add a new record to the collection
     * Updates both primary key index and secondary indexes
     */
    void QBTable::addRecord(const QBRecord &record)
    {
        size_t idx = records_.size();
        records_.push_back(record);
        deleted_.push_back(false);

        // Update primary key index
        pkIndex_[record.column0] = idx;

        // Update secondary indexes
        for (ColumnType columnID : secondaryIndexedColumns_)
        {
            IndexKey key = getColumnIndexKey(idx, columnID);
            secondaryIndexes_[{columnID, key}].push_back(idx);
        }
    }

    /**
     * Get count of active records
     */
    size_t QBTable::activeRecordsCount() const noexcept
    {
        size_t count = 0;
        for (bool del : deleted_)
            if (!del)
                count++;
        return count;
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
        std::vector<QBRecord> compacted;
        std::vector<bool> newDeleted;
        std::map<size_t, size_t> oldToNewIndex; // Map old indices to new

        // deep copy only non-deleted records
        for (size_t i = 0; i < records_.size(); ++i)
        {
            if (!deleted_[i])
            {
                oldToNewIndex[i] = compacted.size();
                compacted.push_back(records_[i]);
                newDeleted.push_back(false);
            }
        }

        records_ = std::move(compacted);
        deleted_ = std::move(newDeleted);

        // Rebuild primary key index
        rebuildPrimaryKeyIndex();

        // Rebuild secondary indexes
        secondaryIndexes_.clear();
        for (const ColumnType columnID : secondaryIndexedColumns_)
        {
            rebuildSecondaryIndexForColumn(columnID);
        }
    }
}