#pragma once
#include <cstdint>
#include <variant>
#include <functional>
#include <string>

namespace db {
    using uint = unsigned int;
    // FieldType - typed field, supports multiple types for columns
    using FieldType = std::variant<uint, long, std::string>;
    // DerivedFunc - a function that takes a record and computes a value for a derived column from physical columns
    using DerivedFunc = std::function<FieldType(const struct QBRecordDynamic&)>;
    // QBTable - static record
    struct QBRecord
    {
        uint column0; // primary key
        std::string column1;
        long column2;
        std::string column3;
    };
    // ColumnType - static table column identifier enum
    enum class ColumnType : uint8_t
    {
        COLUMN0, // assumed to be the primary key column
        COLUMN1,
        COLUMN2,
        COLUMN3
    };
    // QBRecordDynamic  - record can hold an arbitrary number of fields in a map
    struct QBRecordDynamic
    {
        uint id; // primary key
        std::unordered_map<std::string, FieldType> fields;
    };
}