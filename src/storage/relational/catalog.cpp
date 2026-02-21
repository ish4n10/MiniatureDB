#include "storage/relational/catalog.hpp"

namespace Relational {

Catalog::Catalog() = default;

Catalog::~Catalog() = default;

bool Catalog::register_table(const std::string& table_name, const TableSchema& schema) {
    if (tables.find(table_name) != tables.end()) {
        return false;
    }
    tables.insert(std::make_pair(table_name, schema));
    return true;
}

std::optional<const TableSchema*> Catalog::get_schema(const std::string& table_name) const {
    auto found_pair = tables.find(table_name);
    if (found_pair == tables.end()) {
        return std::nullopt;
    }

    return &found_pair->second;
}

bool Catalog::has_table(const std::string& table_name) const {
    return tables.find(table_name) != tables.end();
}

bool Catalog::drop_table(const std::string& table_name) {
    auto found_pair = tables.find(table_name);
    if (found_pair == tables.end()) {
        return false;
    }

    tables.erase(found_pair);
    return true;
}

}

