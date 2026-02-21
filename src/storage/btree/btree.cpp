#include <cstdint>
#include "storage/page.hpp"
#include "storage/table_handle.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/record.hpp"
#include "storage/btree.hpp"
#include "storage/constants.hpp"
#include <cstring>
#include <cassert>
#include <vector>
#include <climits>

extern uint32_t find_leaf_page(TableHandle& th, const Key& key, Page& out_page);
extern uint32_t find_leftmost_leaf_page(TableHandle& th, Page& out_page);
extern bool btree_insert_leaf_no_split(TableHandle& th, uint32_t page_id, Page& page, const Key& key, const Value& value);
extern SplitLeafResult split_leaf_page(TableHandle& th, Page& page);
extern void insert_into_parent(TableHandle& th, uint32_t left, const Key& key, uint32_t right);
extern uint16_t write_raw_record(Page& page, const uint8_t* raw, uint16_t size);

void btree_range_scan(TableHandle& th, const Key& start_key, const Key& end_key,
                     BTreeRangeScanCallback callback, void* ctx) {
    if (th.root_page == 0 || callback == nullptr) {
        return;
    }
    Page page;
    uint32_t page_id;
    uint16_t start_index;
    if (start_key.empty()) {
        page_id = find_leftmost_leaf_page(th, page);
        if (page_id == UINT32_MAX) {
            return;
        }
        start_index = 0;
    } else {
        page_id = find_leaf_page(th, start_key, page);
        if (page_id == UINT32_MAX) {
            return;
        }
        BSearchResult sr = search_record(page, start_key.data(), start_key.size());
        start_index = sr.index;
    }
    PageHeader* ph = get_header(page);
    while (true) {
        for (uint16_t i = start_index; i < ph->cell_count; i++) {
            uint16_t key_len = 0;
            const uint8_t* key_data = slot_key(page, i, key_len);
            uint16_t value_len = 0;
            const uint8_t* value_data = slot_value(page, i, value_len);
            if (key_data == nullptr || value_data == nullptr) {
                continue;
            }
            if (!end_key.empty() && compare_keys(key_data, key_len, end_key.data(), end_key.size()) > 0) {
                return;
            }
            Key k(key_data, key_len);
            Value v;
            v.assign(value_data, value_len);
            callback(k, v, ctx);
        }
        page_id = ph->next_page_id;
        if (page_id == 0) {
            return;
        }
        if (!th.bpm) {
            return;
        }
        Page* next = th.bpm->fetch_page(page_id);
        if (!next) {
            return;
        }
        std::memcpy(page.data, next->data, PAGE_SIZE);
        th.bpm->unpin_page(page_id, false);
        ph = get_header(page);
        start_index = 0;
    }
}

bool btree_search(TableHandle& th, const Key& key, Value& value) {
    if (th.root_page == 0) {
        return false;
    }

    Page leaf_page;
    uint32_t leaf_page_id = find_leaf_page(th, key, leaf_page);
    if (leaf_page_id == UINT32_MAX) {
        return false;
    }
    
    BSearchResult result = search_record(leaf_page, key.data(), key.size());
    if (!result.found) {
        return false;
    }
    
    uint16_t value_len;
    const uint8_t* value_data = slot_value(leaf_page, result.index, value_len);
    if (value_data == nullptr || value_len == 0) {
        return false;
    }
    
    value.assign(value_data, value_len);
    return true;
}

bool btree_insert(TableHandle& th, const Key& key, const Value& value) {
    if (!th.bpm) {
        return false;
    }
    if (th.root_page == 0) {
        uint32_t root_page_id = allocate_page(th);
        if (root_page_id == INVALID_PAGE_ID) {
            return false;
        }
        Page* root = th.bpm->new_page(root_page_id, PageType::DATA, PageLevel::LEAF);
        if (!root) {
            return false;
        }
        th.root_page = root_page_id;

        Page* meta = th.bpm->fetch_page(0);
        if (meta) {
            get_header(*meta)->root_page = root_page_id;
            th.bpm->unpin_page(0, true);
        }

        page_insert(*root, key.data(), key.size(), value.data(), value.size());
        th.bpm->unpin_page(root_page_id, true);
        return true;
    }

    Page leaf_page;
    uint32_t leaf_page_id = find_leaf_page(th, key, leaf_page);

    BSearchResult search_result = search_record(leaf_page, key.data(), key.size());
    if (search_result.found) {
        return false;
    }

    if (btree_insert_leaf_no_split(th, leaf_page_id, leaf_page, key, value)) {
        return true;
    }

    Page* leaf_bp = th.bpm->fetch_page(leaf_page_id);
    if (!leaf_bp) {
        return false;
    }
    std::memcpy(leaf_page.data, leaf_bp->data, PAGE_SIZE);
    th.bpm->unpin_page(leaf_page_id, false);

    SplitLeafResult split_result = split_leaf_page(th, leaf_page);
    
    Key sep_key;
    sep_key.assign(split_result.seperator_key.data(), split_result.seperator_key.size());
    
    int cmp = compare_keys(key.data(), key.size(), sep_key.data(), sep_key.size());
    
    if (cmp < 0) {
        if (!can_insert(split_result.left_page, record_size(key.size(), value.size()))) {
            assert(false && "Left page doesn't have space after split");
            return false;
        }
        if (!page_insert(split_result.left_page, key.data(), key.size(), value.data(), value.size())) {
            assert(false && "page_insert failed for left page");
            return false;
        }
        Page* left_bp = th.bpm->fetch_page(leaf_page_id);
        if (left_bp) {
            std::memcpy(left_bp->data, split_result.left_page.data, PAGE_SIZE);
            th.bpm->unpin_page(leaf_page_id, true);
        }
    } else {
        if (!can_insert(split_result.right_page, record_size(key.size(), value.size()))) {
            assert(false && "Right page doesn't have space after split");
            return false;
        }
        if (!page_insert(split_result.right_page, key.data(), key.size(), value.data(), value.size())) {
            assert(false && "page_insert failed for right page");
            return false;
        }
        Page* right_bp = th.bpm->fetch_page(split_result.new_page);
        if (right_bp) {
            std::memcpy(right_bp->data, split_result.right_page.data, PAGE_SIZE);
            th.bpm->unpin_page(split_result.new_page, true);
        }
    }
    
    insert_into_parent(th, leaf_page_id, sep_key, split_result.new_page);
    
    return true;
}

struct SiblingInfo {
    uint32_t left_sibling;
    uint32_t right_sibling;
    Key separator_key;        // Key that points to current page (for merge with left)
    Key right_separator_key;  // Key that points to right sibling (for merge with right)
    bool is_leftmost;
    bool is_rightmost;
};

SiblingInfo find_leaf_siblings(TableHandle& th, uint32_t leaf_page_id, Page& leaf_page) {
    SiblingInfo info = {0, 0, Key(), Key(), false, false};

    PageHeader* ph = get_header(leaf_page);
    if (ph->parent_page_id == 0) {
        info.is_leftmost = true;
        info.is_rightmost = true;
        return info;
    }

    if (!th.bpm) {
        return info;
    }
    Page* parent = th.bpm->fetch_page(ph->parent_page_id);
    if (!parent) {
        return info;
    }
    PageHeader* parent_ph = get_header(*parent);
    
    if (parent_ph->page_level != PageLevel::INTERNAL) {
        return info;
    }
    
    uint32_t leftmost = *reinterpret_cast<uint32_t*>(parent_ph->reserved);
    if (leftmost == leaf_page_id) {
        info.is_leftmost = true;
        if (parent_ph->cell_count > 0) {
            uint16_t entry_offset = *slot_ptr(*parent, 0);
            InternalEntry* entry = reinterpret_cast<InternalEntry*>(parent->data + entry_offset);
            info.right_sibling = entry->child_page;
            
            uint16_t sep_len = entry->key_size;
            if (sep_len > 256) {
                assert(false && "Key too large");
                return info;
            }
            // For leftmost page, entry[0]'s key IS the right separator
            info.right_separator_key.assign(entry->key, sep_len);
        }
        th.bpm->unpin_page(ph->parent_page_id, false);
        return info;
    }

    for (uint16_t i = 0; i < parent_ph->cell_count; i++) {
        uint16_t entry_offset = *slot_ptr(*parent, i);
        InternalEntry* entry = reinterpret_cast<InternalEntry*>(parent->data + entry_offset);

        if (entry->child_page == leaf_page_id) {
            if (i == 0) {
                info.left_sibling = leftmost;
            } else {
                uint16_t prev_offset = *slot_ptr(*parent, i - 1);
                InternalEntry* prev_entry = reinterpret_cast<InternalEntry*>(parent->data + prev_offset);
                info.left_sibling = prev_entry->child_page;
            }
            
            if (i + 1 < parent_ph->cell_count) {
                uint16_t next_offset = *slot_ptr(*parent, i + 1);
                InternalEntry* next_entry = reinterpret_cast<InternalEntry*>(parent->data + next_offset);
                info.right_sibling = next_entry->child_page;
                // Right separator is the key of entry[i+1]
                info.right_separator_key.assign(next_entry->key, next_entry->key_size);
            } else {
                info.is_rightmost = true;
            }
            
            // Current page's separator key (for merging with left)
            uint16_t sep_len = entry->key_size;
            if (sep_len > 256) {
                assert(false && "Key too large");
                return info;
            }
            info.separator_key.assign(entry->key, sep_len);

            th.bpm->unpin_page(ph->parent_page_id, false);
            return info;
        }
    }

    th.bpm->unpin_page(ph->parent_page_id, false);
    assert(false && "Leaf page not found in parent");
    return info;
}

static int16_t find_internal_entry_index(Page& parent, const Key& key_to_remove) {
    PageHeader* ph = get_header(parent);
    for (uint16_t i = 0; i < ph->cell_count; i++) {
        uint16_t offset = *slot_ptr(parent, i);
        InternalEntry* entry = reinterpret_cast<InternalEntry*>(parent.data + offset);
        const uint8_t* entry_key = parent.data + offset + sizeof(InternalEntry);
        if (entry->key_size == key_to_remove.size() &&
            memcmp(entry_key, key_to_remove.data(), key_to_remove.size()) == 0) {
            return static_cast<int16_t>(i);
        }
    }
    return -1;
}

void remove_from_internal(TableHandle& th, uint32_t parent_id, const Key& key_to_remove, uint32_t deleted_child_page) {
    if (!th.bpm) {
        return;
    }
    Page* parent = th.bpm->fetch_page(parent_id);
    if (!parent) {
        return;
    }
    PageHeader* ph = get_header(*parent);

    if (ph->page_level != PageLevel::INTERNAL) {
        th.bpm->unpin_page(parent_id, false);
        return;
    }

    uint32_t* leftmost_ptr = reinterpret_cast<uint32_t*>(ph->reserved);
    if (deleted_child_page != 0 && *leftmost_ptr == deleted_child_page) {
        if (ph->cell_count > 0) {
            uint16_t first_offset = *slot_ptr(*parent, 0);
            InternalEntry* first_entry = reinterpret_cast<InternalEntry*>(parent->data + first_offset);
            *leftmost_ptr = first_entry->child_page;
            remove_slot(*parent, 0);
        } else {
            *leftmost_ptr = 0;
        }
        th.bpm->unpin_page(parent_id, true);
        return;
    }

    int16_t idx = find_internal_entry_index(*parent, key_to_remove);
    if (idx >= 0) {
        remove_slot(*parent, static_cast<uint16_t>(idx));
        th.bpm->unpin_page(parent_id, true);
    } else {
        th.bpm->unpin_page(parent_id, false);
    }
}

static bool is_page_underutilized(Page& page) {
    PageHeader* ph = get_header(page);
    if (ph->cell_count == 0) {
        return true;
    }
    
    // Calculate actual space used by remaining records (not free_start which doesn't shrink)
    uint16_t actual_records_size = 0;
    for (uint16_t i = 0; i < ph->cell_count; i++) {
        uint16_t* slot = slot_ptr(page, i);
        if (slot == nullptr) continue;
        uint16_t offset = *slot;
        RecordHeader* rh = reinterpret_cast<RecordHeader*>(page.data + offset);
        actual_records_size += record_size(rh->key_size, rh->value_size);
    }
    
    uint16_t slots_space = ph->cell_count * sizeof(uint16_t);
    uint16_t total_used = actual_records_size + slots_space;
    uint16_t available_space = PAGE_SIZE - sizeof(PageHeader);
    uint16_t utilization_percent = (total_used * 100) / available_space;
    
    return utilization_percent < MERGE_THRESHOLD_PERCENT;
}

static uint16_t calculate_total_records_size(Page& page) {
    PageHeader* ph = get_header(page);
    uint16_t total_size = 0;
    for (uint16_t i = 0; i < ph->cell_count; i++) {
        uint16_t* slot = slot_ptr(page, i);
        if (slot == nullptr) continue;
        uint16_t offset = *slot;
        RecordHeader* rh = reinterpret_cast<RecordHeader*>(page.data + offset);
        total_size += record_size(rh->key_size, rh->value_size);
    }
    return total_size;
}

static bool can_merge_pages(Page& left_page, Page& right_page) {
    PageHeader* left_ph = get_header(left_page);
    PageHeader* right_ph = get_header(right_page);
    
    uint16_t left_records_size = calculate_total_records_size(left_page);
    uint16_t right_records_size = calculate_total_records_size(right_page);
    uint16_t total_records_size = left_records_size + right_records_size;
    
    uint16_t total_slots = left_ph->cell_count + right_ph->cell_count;
    uint16_t slots_space = total_slots * sizeof(uint16_t);
    
    uint16_t total_needed = sizeof(PageHeader) + total_records_size + slots_space;
    return total_needed <= PAGE_SIZE;
}

static void update_leaf_links_on_free(TableHandle& th, uint32_t freed_page_id, Page& freed_page) {
    (void)freed_page_id;
    if (!th.bpm) {
        return;
    }
    PageHeader* ph = get_header(freed_page);
    if (ph->prev_page_id != 0) {
        Page* prev_page = th.bpm->fetch_page(ph->prev_page_id);
        if (prev_page) {
            get_header(*prev_page)->next_page_id = ph->next_page_id;
            th.bpm->unpin_page(ph->prev_page_id, true);
        }
    }
    if (ph->next_page_id != 0) {
        Page* next_page = th.bpm->fetch_page(ph->next_page_id);
        if (next_page) {
            get_header(*next_page)->prev_page_id = ph->prev_page_id;
            th.bpm->unpin_page(ph->next_page_id, true);
        }
    }
}

static void merge_leaf_pages(TableHandle& th, uint32_t left_page_id, Page& left_page, 
                             uint32_t right_page_id, Page& right_page) {
    PageHeader* left_ph = get_header(left_page);
    PageHeader* right_ph = get_header(right_page);
    uint32_t saved_prev = left_ph->prev_page_id;
    uint32_t right_next = right_ph->next_page_id;
    
    // Extract all records from both pages (left page may have holes from deletions)
    struct RecordData {
        std::vector<uint8_t> data;
    };
    std::vector<RecordData> all_records;
    
    // Extract from left page
    for (uint16_t i = 0; i < left_ph->cell_count; i++) {
        uint16_t offset = *slot_ptr(left_page, i);
        RecordHeader* rh = reinterpret_cast<RecordHeader*>(left_page.data + offset);
        uint16_t rec_size = record_size(rh->key_size, rh->value_size);
        RecordData rd;
        rd.data.assign(left_page.data + offset, left_page.data + offset + rec_size);
        all_records.push_back(std::move(rd));
    }
    
    // Extract from right page
    for (uint16_t i = 0; i < right_ph->cell_count; i++) {
        uint16_t offset = *slot_ptr(right_page, i);
        RecordHeader* rh = reinterpret_cast<RecordHeader*>(right_page.data + offset);
        uint16_t rec_size = record_size(rh->key_size, rh->value_size);
        RecordData rd;
        rd.data.assign(right_page.data + offset, right_page.data + offset + rec_size);
        all_records.push_back(std::move(rd));
    }
    
    // Reinitialize left page (compacts it, removes holes)
    uint32_t parent_id = left_ph->parent_page_id;
    init_page(left_page, left_page_id, PageType::DATA, PageLevel::LEAF);
    left_ph = get_header(left_page);
    left_ph->parent_page_id = parent_id;
    left_ph->prev_page_id = saved_prev;
    left_ph->next_page_id = right_next;
    if (right_next != 0 && th.bpm) {
        Page* next_page = th.bpm->fetch_page(right_next);
        if (next_page) {
            get_header(*next_page)->prev_page_id = left_page_id;
            th.bpm->unpin_page(right_next, true);
        }
    }

    // Write all records back
    for (size_t i = 0; i < all_records.size(); i++) {
        uint16_t new_offset = write_raw_record(left_page, all_records[i].data.data(), 
                                                static_cast<uint16_t>(all_records[i].data.size()));
        left_ph = get_header(left_page);
        insert_slot(left_page, left_ph->cell_count, new_offset);
    }
    
    if (th.bpm) {
        Page* left_bp = th.bpm->fetch_page(left_page_id);
        if (left_bp) {
            std::memcpy(left_bp->data, left_page.data, PAGE_SIZE);
            th.bpm->unpin_page(left_page_id, true);
        }
    }
    free_page(th, right_page_id);
}

bool btree_delete(TableHandle& th, const Key& key) {
    if (th.root_page == 0) {
        return false;
    }
    
    Page leaf_page;
    uint32_t leaf_page_id = find_leaf_page(th, key, leaf_page);
    
    if (leaf_page_id == UINT32_MAX) {
        return false;
    }
    
    BSearchResult result = search_record(leaf_page, key.data(), key.size());
    if (!result.found) {
        return false;
    }
    
    if (!th.bpm) {
        return false;
    }
    Page* leaf_bp = th.bpm->fetch_page(leaf_page_id);
    if (!leaf_bp) {
        return false;
    }
    bool deleted = page_delete(*leaf_bp, key.data(), key.size());
    if (!deleted) {
        th.bpm->unpin_page(leaf_page_id, false);
        return false;
    }
    th.bpm->unpin_page(leaf_page_id, true);

    leaf_bp = th.bpm->fetch_page(leaf_page_id);
    if (!leaf_bp) {
        return false;
    }
    std::memcpy(leaf_page.data, leaf_bp->data, PAGE_SIZE);
    PageHeader* ph = get_header(leaf_page);
    th.bpm->unpin_page(leaf_page_id, false);

    if (ph->parent_page_id == 0) {
        if (ph->cell_count == 0) {
            th.root_page = 0;
            Page* meta = th.bpm->fetch_page(0);
            if (meta) {
                get_header(*meta)->root_page = 0;
                th.bpm->unpin_page(0, true);
            }
            update_leaf_links_on_free(th, leaf_page_id, leaf_page);
            free_page(th, leaf_page_id);
        }
        return true;
    }

    if (is_page_underutilized(leaf_page)) {
        SiblingInfo siblings = find_leaf_siblings(th, leaf_page_id, leaf_page);
        uint32_t parent_id = ph->parent_page_id;

        if (siblings.left_sibling != 0) {
            Page* left_sibling = th.bpm->fetch_page(siblings.left_sibling);
            if (left_sibling && can_merge_pages(*left_sibling, leaf_page)) {
                merge_leaf_pages(th, siblings.left_sibling, *left_sibling, leaf_page_id, leaf_page);
                th.bpm->unpin_page(siblings.left_sibling, false);
                remove_from_internal(th, parent_id, siblings.separator_key, leaf_page_id);
                return true;
            }
            if (left_sibling) {
                th.bpm->unpin_page(siblings.left_sibling, false);
            }
        }
        if (siblings.right_sibling != 0) {
            Page* right_sibling = th.bpm->fetch_page(siblings.right_sibling);
            if (right_sibling && can_merge_pages(leaf_page, *right_sibling)) {
                merge_leaf_pages(th, leaf_page_id, leaf_page, siblings.right_sibling, *right_sibling);
                th.bpm->unpin_page(siblings.right_sibling, false);
                remove_from_internal(th, parent_id, siblings.right_separator_key, siblings.right_sibling);
                leaf_bp = th.bpm->fetch_page(leaf_page_id);
                if (leaf_bp) {
                    std::memcpy(leaf_page.data, leaf_bp->data, PAGE_SIZE);
                    th.bpm->unpin_page(leaf_page_id, false);
                    ph = get_header(leaf_page);
                }
            } else if (right_sibling) {
                th.bpm->unpin_page(siblings.right_sibling, false);
            }
        }
    }

    if (ph->cell_count == 0) {
        SiblingInfo siblings = find_leaf_siblings(th, leaf_page_id, leaf_page);
        uint32_t parent_id = ph->parent_page_id;

        if (siblings.is_leftmost) {
            if (siblings.right_sibling != 0) {
                Page* right_sibling = th.bpm->fetch_page(siblings.right_sibling);
                if (right_sibling) {
                    th.bpm->unpin_page(siblings.right_sibling, false);
                    merge_leaf_pages(th, leaf_page_id, leaf_page, siblings.right_sibling, *right_sibling);
                }
                remove_from_internal(th, parent_id, siblings.right_separator_key, siblings.right_sibling);
            } else {
                Page* parent = th.bpm->fetch_page(parent_id);
                if (parent) {
                    PageHeader* parent_ph = get_header(*parent);
                    uint32_t* leftmost_ptr = reinterpret_cast<uint32_t*>(parent_ph->reserved);
                    *leftmost_ptr = 0;
                    th.bpm->unpin_page(parent_id, true);
                }
                update_leaf_links_on_free(th, leaf_page_id, leaf_page);
                free_page(th, leaf_page_id);
            }
        } else if (siblings.left_sibling != 0) {
            Page* left_sibling = th.bpm->fetch_page(siblings.left_sibling);
            if (left_sibling) {
                th.bpm->unpin_page(siblings.left_sibling, false);
                merge_leaf_pages(th, siblings.left_sibling, *left_sibling, leaf_page_id, leaf_page);
            }
            remove_from_internal(th, parent_id, siblings.separator_key, leaf_page_id);
        } else if (siblings.right_sibling != 0) {
            Page* right_sibling = th.bpm->fetch_page(siblings.right_sibling);
            if (right_sibling) {
                th.bpm->unpin_page(siblings.right_sibling, false);
                merge_leaf_pages(th, leaf_page_id, leaf_page, siblings.right_sibling, *right_sibling);
            }
            remove_from_internal(th, parent_id, siblings.right_separator_key, siblings.right_sibling);
        } else {
            update_leaf_links_on_free(th, leaf_page_id, leaf_page);
            free_page(th, leaf_page_id);
        }
    }
    
    return true;
}
