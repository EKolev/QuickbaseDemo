#include "../include/Quickbase_dynamic.hpp"
#include <algorithm>

namespace db
{
    /**
     * Find matching records by column name and corresponding value
     * Uses primary key index for column "id", secondary indexes for other columns,
     * or falls back to linear scan for non-indexed columns
     */
    std::vector<db::QBRecordDynamic> QBTableDynamic::findMatching(std::string column, db::FieldType value) const
    {
        std::vector<db::QBRecordDynamic> result;
        // handle queries on primary key
        if (column == "id")
        {
            auto it = pkIndex_.find(std::get<db::uint>(value));
            if (it != pkIndex_.end() && !deleted_[it->second])
                result.push_back(records_[it->second]);
            return result;
        }
        // handle queries on non-pk columns - secondery indexed
        auto idxIt = secondaryIndexes_.find({column, value});
        if (idxIt != secondaryIndexes_.end())
        {
            for (size_t i : idxIt->second)
                if (!deleted_[i])
                    result.push_back(records_[i]);
            return result;
        }

        // Linear scan fallback
        for (size_t i = 0; i < records_.size(); ++i)
        {
            if (deleted_[i])
                continue;
            auto fIt = records_[i].fields.find(column);
            if (fIt != records_[i].fields.end() && fIt->second == value)
                result.push_back(records_[i]);
        }

        return result;
    }
    /**
     * Delete a record by its unique ID - id
     */
    bool QBTableDynamic::deleteRecordByID(db::uint id, bool hardDelete)
    {
        auto pkIt = pkIndex_.find(id);
        if (pkIt == pkIndex_.end())
            return false;

        const size_t idx = pkIt->second;
        // already deleted
        if (!hardDelete && deleted_[idx])
            return false;

        // soft delete
        if (!hardDelete)
        {
            deleted_[idx] = true;
            // remove from PK index
            pkIndex_.erase(pkIt);

            // remove index entries for this record only
            for (const auto &column : secondaryIndexedColumns_)
            {

                FieldType value = getField(idx, column);
                auto key = std::make_pair(column, value);

                auto it = secondaryIndexes_.find(key);
                if (it == secondaryIndexes_.end())
                    continue;
                auto &vec = it->second;
                vec.erase(std::remove(vec.begin(), vec.end(), idx), vec.end());

                if (vec.empty())
                    secondaryIndexes_.erase(it);
            }

            return true;
        }
        else // hard delete
        {
            const size_t lastIdx = records_.size() - 1;

            if (idx != lastIdx)
            {
                std::swap(records_[idx], records_[lastIdx]);
                std::swap(deleted_[idx], deleted_[lastIdx]);
            }

            records_.pop_back();
            deleted_.pop_back();

            // Rebuild indexes for correctness and simplicity
            rebuildPrimaryIndex();
            secondaryIndexes_.clear();
            for (const auto &column : secondaryIndexedColumns_)
            {
                if (column == "id")
                    continue;

                rebuildSecondaryIndex(column);
            }

            return true;
        }
    }
    /**
     * Add a new column to the table schema
     * Updates all existing records with the default value for the type if not provided
     */
    bool QBTableDynamic::addColumn(const std::string &name, db::FieldType defaultValue)
    {
        if (!columns_.insert(name).second)
            return false;

        // add a new column/field to each record with a default value if not provided
        for (auto &r : records_)
            r.fields.emplace(name, defaultValue);
        return true;
    }
    /**
     * Remove a column from the table schema
      * Deletes the column/field from all records and removes any associated indexes
      * Note: This is a destructive operation and should be used with caution as it permanently removes
     */
    void QBTableDynamic::removeColumn(const std::string &name)
    {
        columns_.erase(name);
        secondaryIndexedColumns_.erase(name);
        secondaryIndexes_.erase({name, {}}); // full cleanup

        for (auto &r : records_)
            r.fields.erase(name);
    }
    /**
     * Add a derived column with a custom computation function
     * The function takes a record and computes the value for the derived column based on other fields
     */
    bool QBTableDynamic::addDerivedColumn(const std::string &name, db::DerivedFunc func)
    {
        if (columns_.contains(name))
            return false; // name conflicts with physical column

        derivedColumns_[name] = func;
        return true;
    }
    /**
     * Get the value of a column for a specific record, handling both physical and derived columns
      * This is used internally for indexing and querying
      * Throws an exception if the column is unknown
      * Note: For simplicity, this implementation does not cache derived column values - they are computed on demand
     */
    FieldType QBTableDynamic::getField(size_t recordIdx, const std::string &column) const
    {
        const auto &rec = records_[recordIdx];

        if (auto it = rec.fields.find(column); it != rec.fields.end())
            return it->second;

        if (auto dit = derivedColumns_.find(column); dit != derivedColumns_.end())
            return dit->second(rec);

        throw std::runtime_error("Unknown column: " + column);
    }
    /**
     * Rebuild the index for a specific secondary column
     * Called when createIndex() is invoked for non-PK columns
     */
    void QBTableDynamic::rebuildSecondaryIndex(const std::string &column)
    {
        for (size_t i = 0; i < records_.size(); ++i)
        {
            if (!deleted_[i])
            {
                db::FieldType v = getField(i, column);
                secondaryIndexes_[{column, v}].push_back(i);
            }
        }
    }
   /**
     * Rebuild the primary key index - id
     * Used during compact, hard delete
     */
    void QBTableDynamic::rebuildPrimaryIndex()
    {
        pkIndex_.clear();
        for (size_t i = 0; i < records_.size(); ++i)
        {
            if (!deleted_[i])
                pkIndex_[records_[i].id] = i;
        }
    }
    /**
     * Add a new record to the table
     * Updates both primary key index and secondary indexes
     */
    bool QBTableDynamic::addRecord(const db::QBRecordDynamic &record)
    {
        // Enforce schema
        for (const auto &[key, _] : record.fields)
            if (!columns_.contains(key))
                return false; // invalid column

        size_t idx = records_.size();
        records_.push_back(record);
        deleted_.push_back(false);

        pkIndex_[record.id] = idx;

        for (const auto &col : secondaryIndexedColumns_)
        {
            db::FieldType v = getField(idx, col);
            secondaryIndexes_[{col, v}].push_back(idx);
        }
        return true;
    }
    /*
     * Create an index on a specific column
     */
    void QBTableDynamic::createIndex(const std::string &column)
    {
        if (!columns_.contains(column) && !derivedColumns_.contains(column))
            throw std::runtime_error("Cannot index unknown column");

        if (!secondaryIndexedColumns_.insert(column).second)
            return;

        rebuildSecondaryIndex(column);
    }
    /*
     * Remove all secondary index entries for a specific column
     */
    void QBTableDynamic::dropIndex(const std::string &column)
    {
        if (column == "id")
            throw std::runtime_error("Cannot drop index on primary key column");
        secondaryIndexedColumns_.erase(column);
        auto it = secondaryIndexes_.begin();
        while (it != secondaryIndexes_.end())
        {
            if (it->first.first == column)
                it = secondaryIndexes_.erase(it);
            else
                ++it;
        }
    }
    /**
     * Get count of active records
     */
    size_t QBTableDynamic::activeRecordsCount() const noexcept
    {
        // static_cast to size_t to avoid signed/unsigned comparison warnings
        return static_cast<size_t>(std::count_if(deleted_.begin(), deleted_.end(), [](bool del) -> bool
                                                 { return !del; }));
    }
    /**
     * Get total record count including deleted records
     */
    size_t QBTableDynamic::totalRecordsCount() const noexcept
    {
        return records_.size();
    }

    /**
     * Compact the collection by removing deleted records
     * Rebuilds primary and secondary indexes with new positions
     */
    void QBTableDynamic::compactRecords()
    {
        // Count active records
        size_t activeCount = activeRecordsCount();

        std::vector<QBRecordDynamic> compacted;
        compacted.reserve(activeCount); // only allocate needed space

        // move active records
        size_t i = 0;
        std::for_each(records_.begin(), records_.end(), [&](QBRecordDynamic &rec)
                      { 
                        if (!deleted_[i])
                        compacted.push_back(std::move(rec));
                        ++i; });

        records_ = std::move(compacted);
        deleted_.assign(records_.size(), false);

        // rebuild all indexes
        rebuildPrimaryIndex();
        secondaryIndexes_.clear();
        for (const auto &column : secondaryIndexedColumns_)
            rebuildSecondaryIndex(column);
    }

}
