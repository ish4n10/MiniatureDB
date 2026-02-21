#pragma once
#include <vector>
#include <string>
#include <unordered_map>
#include <optional>
#include <memory>

namespace Relational {
    enum class ColumnType {
        INT,
        FLOAT,
        DOUBLE,
        STRING,
        BOOLEAN,
        DATETIME,
    };

    struct ColumnDef {
        std::string name;
        ColumnType type;
    };

    struct TableSchema {
        int pk_index;
        std::vector<ColumnDef> columns;
    };

    class Catalog {
    private:
        std::unordered_map<std::string, TableSchema> tables;
    public:
        Catalog(); 
        ~Catalog();

        bool register_table(const std::string& table_name, const TableSchema& schema);
        std::optional<const TableSchema*> get_schema(const std::string& table_name) const;
        bool has_table(const std::string& table_name) const;
        bool drop_table(const std::string& table_name);
    };
}