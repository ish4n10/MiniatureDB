#include "storage/page.hpp"
#include "storage/record.hpp"
#include <cstring>
#include <algorithm>

bool can_insert(Page& page, uint16_t record_size) {
    PageHeader* page_header = get_header(page);
    uint16_t slot_space = (page_header->cell_count + 1) * sizeof(uint16_t);
    return page_header->free_start + record_size + slot_space <= page_header->free_end;
}

int compare_keys(const uint8_t* first, uint16_t first_size, const uint8_t* second, uint16_t second_size) {
    int min = std::min(first_size, second_size);
    int res = std::memcmp(first, second, min);
    if (res != 0) return res;
    return static_cast<int>(first_size) - static_cast<int>(second_size);
}

uint16_t write_record(Page& page, const uint8_t* key, uint16_t key_len, const uint8_t* value, uint16_t value_len) {
    PageHeader* page_header = get_header(page);
    uint16_t offset = page_header->free_start;
    
    if (offset + sizeof(RecordHeader) + key_len + value_len > page_header->free_end) {
        return 0;
    }
    
    uint8_t* ptr = page.data + offset;
    RecordHeader* rh = reinterpret_cast<RecordHeader*>(ptr);
    rh->flags = 0;
    rh->key_size = key_len;
    rh->value_size = value_len;
    ptr += sizeof(RecordHeader);

    std::memcpy(ptr, key, key_len);
    ptr += key_len;
    std::memcpy(ptr, value, value_len);

    uint16_t total_size = record_size(key_len, value_len);
    page_header->free_start = offset + total_size;
    return offset;
}

BSearchResult search_record(Page& page, const uint8_t* key, uint16_t key_len) {
    PageHeader* header = get_header(page);
    uint16_t left = 0;
    uint16_t right = header->cell_count;

    while (left < right) {
        uint16_t mid = left + (right - left) / 2;
        uint16_t mid_key_len = 0;
        const uint8_t* mid_key = slot_key(page, mid, mid_key_len);
        if (mid_key == nullptr) {
            return {false, left};
        }
        int cmp = compare_keys(mid_key, mid_key_len, key, key_len);
        if (cmp < 0) {
            left = mid + 1;
        } else if (cmp > 0) {
            right = mid;
        } else {
            return {true, mid};
        }
    }
    return {false, left};
}

bool page_insert(Page& page, const uint8_t* key, uint16_t key_size, const uint8_t* value, uint16_t value_size) {
    PageHeader* header = get_header(page);
    
    BSearchResult result = search_record(page, key, key_size);
    if (result.found) {
        return false;
    }
    
    uint16_t rsize = record_size(key_size, value_size);
    if (!can_insert(page, rsize)) {
        return false;
    }
    
    uint16_t old_free_start = header->free_start;
    uint16_t old_free_end = header->free_end;
    uint16_t old_cell_count = header->cell_count;
    
    uint16_t roffset = write_record(page, key, key_size, value, value_size);
    if (roffset == 0) {
        header->free_start = old_free_start;
        return false;
    }
    
    if (header->free_start > header->free_end - sizeof(uint16_t)) {
        header->free_start = old_free_start;
        return false;
    }
    
    try {
        insert_slot(page, result.index, roffset);
    } catch (...) {
        header->free_start = old_free_start;
        header->free_end = old_free_end;
        header->cell_count = old_cell_count;
        return false;
    }
    
    if (header->free_start > header->free_end) {
        header->free_start = old_free_start;
        header->free_end = old_free_end;
        header->cell_count = old_cell_count;
        return false;
    }
    
    return true;
}

bool page_delete(Page& page, const uint8_t* key, uint16_t key_len) {
    BSearchResult sr = search_record(page, key, key_len);
    if (!sr.found) return false;
    uint16_t* slot = slot_ptr(page, sr.index);
    if (slot == nullptr) return false;
    uint32_t record_offset = *slot;
    RecordHeader* rh = reinterpret_cast<RecordHeader*>(page.data + record_offset);
    rh->flags |= RECORD_DELETED;
    remove_slot(page, sr.index);
    return true;
}
