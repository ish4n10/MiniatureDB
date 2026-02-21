#include "storage/interface/storage_engine.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <cstring>

void test_basic_operations() {
    std::cout << "\n=== StorageEngine Basic Operations Test ===\n";

    StorageEngine se;

    const std::string table_name = "test_storage_api";
    std::string path = "data/" + table_name + ".db";
    std::remove(path.c_str());

    assert(se.create_table(table_name) && "create_table failed");
    std::cout << "[OK] Created table: " << table_name << "\n";

    TableHandle* th = se.open_table(table_name);
    assert(th != nullptr && "open_table failed");
    std::cout << "[OK] Opened table\n";

    std::vector<uint8_t> key1 = { 'k', 'e', 'y', '1' };
    std::vector<uint8_t> value1 = { 'v', 'a', 'l', 'u', 'e', '1' };

    assert(se.insert_record(th, key1, value1) && "insert_record failed");
    std::cout << "[OK] Inserted record\n";

    std::vector<uint8_t> out_value;
    assert(se.get_record(th, key1, out_value) && "get_record failed");
    assert(out_value == value1 && "value mismatch");
    std::cout << "[OK] Retrieved record correctly\n";

    std::vector<uint8_t> new_value = { 'v', 'a', 'l', 'u', 'e', '2' };
    assert(se.update_record(th, key1, new_value) && "update_record failed");
    assert(se.get_record(th, key1, out_value) && "get_record after update failed");
    assert(out_value == new_value && "updated value mismatch");
    std::cout << "[OK] Updated record correctly\n";

    assert(se.delete_record(th, key1) && "delete_record failed");
    assert(!se.get_record(th, key1, out_value) && "record should not exist after delete");
    std::cout << "[OK] Deleted record correctly\n";

    se.close_table(th);
    std::cout << "[OK] Closed table\n";

    assert(se.drop_table(table_name) && "drop_table failed");
    std::cout << "[OK] Dropped table\n";

    std::cout << "\n=== Basic Operations Test PASSED ===\n";
}

void test_multiple_records() {
    std::cout << "\n=== StorageEngine Multiple Records Test ===\n";

    StorageEngine se;
    const std::string table_name = "test_multi";
    std::string path = "data/" + table_name + ".db";
    std::remove(path.c_str());

    assert(se.create_table(table_name) && "create_table failed");
    TableHandle* th = se.open_table(table_name);
    assert(th != nullptr && "open_table failed");

    const int num_records = 20;
    for (int i = 0; i < num_records; i++) {
        std::string key_str = "key" + std::to_string(i);
        std::string value_str = "value" + std::to_string(i);
        std::vector<uint8_t> key(key_str.begin(), key_str.end());
        std::vector<uint8_t> value(value_str.begin(), value_str.end());

        assert(se.insert_record(th, key, value) && ("insert failed for " + key_str).c_str());
    }
    std::cout << "[OK] Inserted " << num_records << " records\n";

    for (int i = 0; i < num_records; i++) {
        std::string key_str = "key" + std::to_string(i);
        std::string value_str = "value" + std::to_string(i);
        std::vector<uint8_t> key(key_str.begin(), key_str.end());
        std::vector<uint8_t> out_value;

        assert(se.get_record(th, key, out_value) && ("get failed for " + key_str).c_str());
        std::string out_str(out_value.begin(), out_value.end());
        assert(out_str == value_str && ("value mismatch for " + key_str).c_str());
    }
    std::cout << "[OK] Retrieved all " << num_records << " records correctly\n";

    se.close_table(th);
    se.drop_table(table_name);
    std::cout << "\n=== Multiple Records Test PASSED ===\n";
}

int scan_count = 0;
void scan_callback(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value, void* ctx) {
    scan_count++;
    int* expected_count = static_cast<int*>(ctx);
    (void)expected_count;
    if (scan_count <= 5) {
        std::string key_str(key.begin(), key.end());
        std::string value_str(value.begin(), value.end());
        std::cout << "  [" << scan_count << "] key=" << key_str << ", value=" << value_str << "\n";
    }
}

static void test_scan_table() {
    std::cout << "\n=== StorageEngine Scan Table Test ===\n";

    StorageEngine se;
    const std::string table_name = "test_scan";
    std::string path = "data/" + table_name + ".db";
    std::remove(path.c_str());

    assert(se.create_table(table_name) && "create_table failed");
    TableHandle* th = se.open_table(table_name);
    assert(th != nullptr && "open_table failed");

    const int num_records = 15;
    for (int i = 0; i < num_records; i++) {
        std::string key_str = "key" + std::to_string(i);
        std::string value_str = "value" + std::to_string(i);
        std::vector<uint8_t> key(key_str.begin(), key_str.end());
        std::vector<uint8_t> value(value_str.begin(), value_str.end());
        se.insert_record(th, key, value);
    }
    std::cout << "[OK] Inserted " << num_records << " records\n";

    scan_count = 0;
    int expected_count = num_records;
    se.scan_table(th, scan_callback, &expected_count);
    assert(scan_count == num_records && "scan count mismatch");
    std::cout << "[OK] Scanned all " << scan_count << " records\n";

    se.close_table(th);
    se.drop_table(table_name);
    std::cout << "\n=== Scan Table Test PASSED ===\n";
}

static void test_range_scan() {
    std::cout << "\n=== StorageEngine Range Scan Test ===\n";

    StorageEngine se;
    const std::string table_name = "test_range";
    std::string path = "data/" + table_name + ".db";
    std::remove(path.c_str());

    assert(se.create_table(table_name) && "create_table failed");
    TableHandle* th = se.open_table(table_name);
    assert(th != nullptr && "open_table failed");

    for (int i = 0; i < 10; i++) {
        std::string key_str = "key" + std::to_string(i);
        std::string value_str = "val" + std::to_string(i);
        std::vector<uint8_t> key(key_str.begin(), key_str.end());
        std::vector<uint8_t> value(value_str.begin(), value_str.end());
        se.insert_record(th, key, value);
    }
    std::cout << "[OK] Inserted 10 records\n";

    std::vector<uint8_t> start_key = { 'k', 'e', 'y', '2' };
    std::vector<uint8_t> end_key = { 'k', 'e', 'y', '7' };

    scan_count = 0;
    se.range_scan(th, start_key, end_key, scan_callback, nullptr);
    assert(scan_count == 6 && "range scan count should be 6 (key2-key7)");
    std::cout << "[OK] Range scan found " << scan_count << " records (key2-key7)\n";

    se.close_table(th);
    se.drop_table(table_name);
    std::cout << "\n=== Range Scan Test PASSED ===\n";
}

int main() {
    try {
        test_basic_operations();
        test_multiple_records();
        test_scan_table();
        test_range_scan();

        std::cout << "\n\n=== ALL STORAGE ENGINE TESTS PASSED ===\n";
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}