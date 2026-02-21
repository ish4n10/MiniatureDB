#pragma once

#include <string>
#include <cstdint>
#include <vector>
#include <memory>
#include <unordered_map>
#include "storage/relational/catalog.hpp"
#include "storage/relational/row_codec.hpp"
struct TableHandle;


class StorageEngine {
public:
    StorageEngine();
    ~StorageEngine();

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;

    bool create_table(const std::string& table_name);
    bool create_table(const std::string& table_name, const Relational::TableSchema& schema);
    bool drop_table(const std::string& table_name);
    TableHandle* open_table(const std::string& table_name);
    void close_table(TableHandle* handle);

    bool insert_record(TableHandle* handle, const std::vector<uint8_t>& key, const std::vector<uint8_t>& value);
    bool get_record(TableHandle* handle, const std::vector<uint8_t>& key, std::vector<uint8_t>& out_value);
    bool delete_record(TableHandle* handle, const std::vector<uint8_t>& key);
    bool update_record(TableHandle* handle, const std::vector<uint8_t>& key, const std::vector<uint8_t>& new_value);

    using ScanCallback = void (*)(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value, void* ctx);
    void scan_table(TableHandle* handle, ScanCallback callback, void* ctx);
    void range_scan(TableHandle* handle, const std::vector<uint8_t>& start_key, const std::vector<uint8_t>& end_key, ScanCallback callback, void* ctx);

    void flush_all();

    bool insert(const std::string& table_name, const Relational::Tuple& row);
    std::vector<Relational::Tuple> scan(const std::string& table_name);
    bool has_table(const std::string& table_name) const;
    const Relational::TableSchema* get_schema(const std::string& table_name) const;

private:
    std::unordered_map<std::string, std::unique_ptr<TableHandle>> open_tables_;
    Relational::Catalog catalog_;
    TableHandle* get_or_open_table(const std::string& table_name);
};
