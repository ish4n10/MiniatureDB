#include <storage/page.hpp>
#include <storage/record.hpp>
#include <stdexcept>
#include <cassert>
#include <vector>
#include <cstring>

uint16_t* slot_ptr(Page& page, uint16_t index) {
    PageHeader* header = get_header(page);
    if (index >= header->cell_count) {
        return nullptr;
    }
    uint16_t slot_offset = header->free_end + (index * sizeof(uint16_t));
    if (slot_offset + sizeof(uint16_t) > PAGE_SIZE) {
        return nullptr;
    }
    return reinterpret_cast<uint16_t*>(page.data + slot_offset);
}

const uint8_t* slot_key(Page& page, uint16_t slot_index, uint16_t& key_len) {
    PageHeader* header = get_header(page);
    if (slot_index >= header->cell_count) {
        key_len = 0;
        return nullptr;
    }
    uint16_t* slot = slot_ptr(page, slot_index);
    if (slot == nullptr) {
        key_len = 0;
        return nullptr;
    }
    uint16_t record_offset = *slot;
    if (record_offset < sizeof(PageHeader) || record_offset >= header->free_start) {
        key_len = 0;
        return nullptr;
    }
    RecordHeader* record_header = reinterpret_cast<RecordHeader*>(page.data + record_offset);
    if (record_header->key_size == 0 || record_header->key_size > PAGE_SIZE) {
        key_len = 0;
        return nullptr;
    }
    if (record_offset + sizeof(RecordHeader) + record_header->key_size > header->free_start) {
        key_len = 0;
        return nullptr;
    }
    key_len = record_header->key_size;
    return page.data + record_offset + sizeof(RecordHeader);
}

const uint8_t* slot_value(Page& page, uint16_t slot_index, uint16_t& value_len) {
    PageHeader* header = get_header(page);
    if (slot_index >= header->cell_count) {
        value_len = 0;
        return nullptr;
    }
    uint16_t* slot = slot_ptr(page, slot_index);
    if (slot == nullptr) {
        value_len = 0;
        return nullptr;
    }
    uint16_t record_offset = *slot;
    if (record_offset < sizeof(PageHeader) || record_offset >= header->free_start) {
        value_len = 0;
        return nullptr;
    }
    RecordHeader* record_header = reinterpret_cast<RecordHeader*>(page.data + record_offset);
    if (record_header->key_size == 0 || record_header->key_size > PAGE_SIZE ||
        record_header->value_size == 0 || record_header->value_size > PAGE_SIZE) {
        value_len = 0;
        return nullptr;
    }
    uint16_t key_end = record_offset + sizeof(RecordHeader) + record_header->key_size;
    if (key_end + record_header->value_size > header->free_start) {
        value_len = 0;
        return nullptr;
    }
    value_len = record_header->value_size;
    return page.data + key_end;
}

void insert_slot(Page& page, uint16_t index, uint16_t record_offset) {
    PageHeader* header = get_header(page);
    
    if (index > header->cell_count) {
        throw std::runtime_error("Invalid slot index");
    }
    
    uint16_t old_free_end = header->free_end;
    uint16_t current_count = header->cell_count;
    uint16_t new_free_end = old_free_end - sizeof(uint16_t);
    
    if (new_free_end < header->free_start) {
        throw std::runtime_error("Slot directory would overlap with records");
    }
    
    if (new_free_end + (current_count + 1) * sizeof(uint16_t) > PAGE_SIZE) {
        throw std::runtime_error("Slot directory would exceed page size");
    }
    
    header->free_end = new_free_end;
    
    std::vector<uint16_t> temp_slots(current_count);
    for (uint16_t i = 0; i < current_count; i++) {
        temp_slots[i] = *reinterpret_cast<uint16_t*>(page.data + old_free_end + i * sizeof(uint16_t));
    }
    
    for (uint16_t i = 0; i < index; i++) {
        *reinterpret_cast<uint16_t*>(page.data + new_free_end + i * sizeof(uint16_t)) = temp_slots[i];
    }
    
    *reinterpret_cast<uint16_t*>(page.data + new_free_end + index * sizeof(uint16_t)) = record_offset;
    
    for (uint16_t i = index; i < current_count; i++) {
        *reinterpret_cast<uint16_t*>(page.data + new_free_end + (i + 1) * sizeof(uint16_t)) = temp_slots[i];
    }
    
    header->cell_count += 1;
}

void remove_slot(Page& page, uint16_t index) {
    PageHeader* header = get_header(page);
    if (index >= header->cell_count || header->cell_count == 0) {
        throw std::runtime_error("Could not remove an invalid slot");
    }
    
    uint16_t old_free_end = header->free_end;
    uint16_t current_count = header->cell_count;
    
    std::vector<uint16_t> slot_values(current_count);
    for (uint16_t i = 0; i < current_count; i++) {
        slot_values[i] = *reinterpret_cast<uint16_t*>(page.data + old_free_end + i * sizeof(uint16_t));
    }
    
    header->free_end += sizeof(uint16_t);
    uint16_t new_free_end = header->free_end;

    for(uint16_t i = 0; i < index; ++i) {
        *reinterpret_cast<uint16_t*>(page.data + new_free_end + i * sizeof(uint16_t)) = slot_values[i];
    }
    
    for(uint16_t i = index + 1; i < current_count; ++i) {
        *reinterpret_cast<uint16_t*>(page.data + new_free_end + (i - 1) * sizeof(uint16_t)) = slot_values[i];
    }

    header->cell_count -= 1;
}
