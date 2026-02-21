#pragma once
#include <cstdint>
#include "storage/table_handle.hpp"
#include "storage/page.hpp"
#include <vector>
#include <cstring>
#include <string_view>

class Key {
private:
    std::vector<uint8_t> owned_data_;
    const uint8_t* data_ = nullptr;
    uint16_t size_ = 0;

public:
    Key() = default;
    
    Key(const uint8_t* d, uint16_t s) : data_(d), size_(s) {}
    
    Key(std::string_view sv) : data_(reinterpret_cast<const uint8_t*>(sv.data())), size_(static_cast<uint16_t>(sv.size())) {}
    
    static Key owned(const uint8_t* src, uint16_t len) {
        Key k;
        k.owned_data_.assign(src, src + len);
        k.data_ = k.owned_data_.data();
        k.size_ = len;
        return k;
    }
    
    void assign(const uint8_t* src, uint16_t len) {
        owned_data_.assign(src, src + len);
        data_ = owned_data_.data();
        size_ = len;
    }
    
    [[nodiscard]] const uint8_t* data() const { return data_; }
    [[nodiscard]] uint16_t size() const { return size_; }
    [[nodiscard]] bool empty() const { return size_ == 0; }
};

class Value {
private:
    std::vector<uint8_t> owned_data_;
    const uint8_t* data_ = nullptr;
    uint16_t size_ = 0;

public:
    Value() = default;
    
    Value(const uint8_t* d, uint16_t s) : data_(d), size_(s) {}
    
    static Value owned(const uint8_t* src, uint16_t len) {
        Value v;
        v.owned_data_.assign(src, src + len);
        v.data_ = v.owned_data_.data();
        v.size_ = len;
        return v;
    }
    
    void assign(const uint8_t* src, uint16_t len) {
        owned_data_.assign(src, src + len);
        data_ = owned_data_.data();
        size_ = len;
    }
    
    [[nodiscard]] const uint8_t* data() const { return data_; }
    [[nodiscard]] uint16_t size() const { return size_; }
    [[nodiscard]] bool empty() const { return size_ == 0; }
};

struct SplitLeafResult {
    uint32_t new_page;
    Key seperator_key;
    Page left_page;
    Page right_page;
};

using SplitInternalResult = SplitLeafResult;

// B+Tree operations
bool btree_search(TableHandle& th, const Key& key, Value& value);
bool btree_insert(TableHandle& th, const Key& key, const Value& value);
bool btree_delete(TableHandle& th, const Key& key);

using BTreeRangeScanCallback = void (*)(const Key& key, const Value& value, void* ctx);
void btree_range_scan(TableHandle& th, const Key& start_key, const Key& end_key,
                     BTreeRangeScanCallback callback, void* ctx);

#pragma pack(push, 1)
struct InternalEntry {
    uint16_t key_size;
    uint32_t child_page;
    uint8_t key[1];
};
#pragma pack(pop)


uint16_t write_raw_record(Page& page, const uint8_t* raw, uint16_t size);

uint32_t find_leaf_page(TableHandle& th, const Key& key, Page& out_page);
uint32_t find_leftmost_leaf_page(TableHandle& th, Page& out_page);
bool btree_insert_leaf_no_split(TableHandle& th, uint32_t page_id, Page& page, const Key& key, const Value& value);
SplitLeafResult split_leaf_page(TableHandle& th, Page& page);

uint32_t internal_find_child(Page& page, const Key& key);
bool insert_internal_no_split(Page& page, const Key& key, uint32_t child);
SplitInternalResult split_internal_page(TableHandle& th, Page& page);
void create_new_root(TableHandle& th, uint32_t left, const Key& key, uint32_t right);
void insert_into_parent(TableHandle& th, uint32_t left, const Key& key, uint32_t right);