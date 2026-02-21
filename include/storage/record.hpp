#pragma once
#include <cstdint>

struct Page;

#pragma pack(push, 1)
struct RecordHeader {
    uint8_t flags;
    uint16_t key_size;
    uint16_t value_size;
};
#pragma pack(pop)

struct BSearchResult {
    bool found;
    uint16_t index;
};

inline uint16_t record_size(uint16_t key_size, uint16_t value_size) {
    return sizeof(RecordHeader) + key_size + value_size;
}

uint16_t write_record(Page& page, const uint8_t* key, uint16_t key_len, const uint8_t* value, uint16_t value_len);
const uint8_t* slot_key(Page& page, uint16_t slot_index, uint16_t& key_len);
const uint8_t* slot_value(Page& page, uint16_t slot_index, uint16_t& value_len);
int compare_keys(const uint8_t* first, uint16_t first_size, const uint8_t* second, uint16_t second_size);
BSearchResult search_record(Page& page, const uint8_t* key, uint16_t key_len);
bool can_insert(Page& page, uint16_t record_size);
bool page_insert(Page& page, const uint8_t* key, uint16_t key_size, const uint8_t* value, uint16_t value_size);
bool page_delete(Page& page, const uint8_t* key, uint16_t key_len);