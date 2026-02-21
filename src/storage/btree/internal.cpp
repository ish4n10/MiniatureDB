#include <cstdint>
#include <cassert>
#include "storage/page.hpp"
#include "storage/btree.hpp"
#include "storage/table_handle.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/record.hpp"
#include <cstring>

static const uint8_t* internal_slot_key(Page& page, uint16_t index, uint16_t& key_len) {
    PageHeader* ph = get_header(page);
    if (index >= ph->cell_count) {
        key_len = 0;
        return nullptr;
    }
    uint16_t* slot = slot_ptr(page, index);
    if (slot == nullptr) {
        key_len = 0;
        return nullptr;
    }
    uint16_t offset = *slot;
    InternalEntry* entry = reinterpret_cast<InternalEntry*>(page.data + offset);
    key_len = entry->key_size;
    return page.data + offset + sizeof(InternalEntry);
}

uint32_t internal_find_child(Page& page, const Key& key) {
    PageHeader* ph = get_header(page);
    assert(ph->page_level == PageLevel::INTERNAL);

    int left = 0;
    int right = ph->cell_count - 1;
    int pos = ph->cell_count;

    while(left <= right) {
        int mid = (right + left) / 2;
        uint16_t mid_key_len;
        const uint8_t* mid_key = internal_slot_key(page, static_cast<uint16_t>(mid), mid_key_len);
        if (mid_key == nullptr) {
            break;
        }
        auto cmp = compare_keys(key.data(), key.size(), mid_key, mid_key_len);
        if (cmp < 0) {
            pos = mid;
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    if (pos == 0) {
        uint32_t leftmost_child = *reinterpret_cast<uint32_t*>(ph->reserved);
        if (leftmost_child != 0 && leftmost_child < 1000000 && leftmost_child != INVALID_PAGE_ID) {
            return leftmost_child;
        }
        if (ph->cell_count > 0) {
            InternalEntry* entry = reinterpret_cast<InternalEntry*>(page.data + *slot_ptr(page, 0));
            if (entry->child_page != 0 && entry->child_page < 1000000) {
                return entry->child_page;
            }
        }
        return 0;
    }
    
    if (pos == ph->cell_count) {
        if (ph->cell_count == 0) {
            return 0;
        }
        InternalEntry* last_entry = reinterpret_cast<InternalEntry*>(page.data + *slot_ptr(page, ph->cell_count - 1));
        return last_entry->child_page;
    }
    
    if (pos > 0 && pos <= ph->cell_count) {
        InternalEntry* entry = reinterpret_cast<InternalEntry*>(page.data + *slot_ptr(page, static_cast<uint16_t>(pos - 1)));
        return entry->child_page;
    }
    
    return 0;
}

uint16_t write_internal_entry(Page& page, const Key& key, uint32_t child) {
    PageHeader* ph = get_header(page);
    assert(ph->page_level == PageLevel::INTERNAL);

    uint16_t offset = static_cast<uint16_t>(ph->free_start);
    InternalEntry ieheader;
    ieheader.key_size = key.size();
    ieheader.child_page = child;

    memcpy(page.data + offset, &ieheader, sizeof(ieheader));
    memcpy(page.data + offset + sizeof(ieheader), key.data(), key.size());

    ph->free_start += sizeof(ieheader) + key.size();
    return offset;
}

static BSearchResult internal_search_record(Page& page, const uint8_t* key, uint16_t key_len) {
    PageHeader* header = get_header(page);
    uint16_t left = 0;
    uint16_t right = header->cell_count;

    while (left < right) {
        uint16_t mid = left + (right - left) / 2;
        uint16_t mid_key_len = 0;
        const uint8_t* mid_key = internal_slot_key(page, mid, mid_key_len);
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

bool insert_internal_no_split(Page& page, const Key& key, uint32_t child) {
    PageHeader* ph = get_header(page);
    assert(ph->page_level == PageLevel::INTERNAL);

    uint16_t rec_size = sizeof(InternalEntry) + key.size();
    if (!can_insert(page, rec_size)) return false;

    BSearchResult sr = internal_search_record(page, key.data(), key.size());
    if (sr.found) return false;
    
    uint16_t offset = write_internal_entry(page, key, child);
    insert_slot(page, sr.index, offset);
    return true;
}

SplitInternalResult split_internal_page(TableHandle& th, Page& page) {
    auto* ph = get_header(page);
    assert(ph->page_level == PageLevel::INTERNAL);

    uint32_t new_pid = allocate_page(th);
    Page new_page;
    init_page(new_page, new_pid, PageType::INDEX, PageLevel::INTERNAL);

    uint16_t total = ph->cell_count;
    if (total < 2) {
        assert(false && "Cannot split internal page with less than 2 elements");
        return {0, Key()};
    }
    uint16_t mid = total / 2;

    uint16_t sep_len;
    const uint8_t* sep_data = internal_slot_key(page, mid, sep_len);
    if (sep_len > 256 || sep_data == nullptr) {
        assert(false && "Key too large or null");
        return {0, Key()};
    }
    Key sep;
    sep.assign(sep_data, sep_len);

    uint32_t new_leftmost_child = 0;
    if (mid < total) {
        uint16_t mid_entry_offset = *slot_ptr(page, mid);
        auto* mid_entry = reinterpret_cast<InternalEntry*>(page.data + mid_entry_offset);
        new_leftmost_child = mid_entry->child_page;
    }
    
    for (uint16_t i = mid + 1; i < total; i++) {
        uint16_t offset = *slot_ptr(page, i);
        auto* ieentry = reinterpret_cast<InternalEntry*>(page.data + offset);
        uint16_t size = sizeof(InternalEntry) + ieentry->key_size;

        uint8_t buf[PAGE_SIZE];
        memcpy(buf, page.data + offset, size);

        uint16_t new_off = write_raw_record(new_page, buf, size);
        insert_slot(new_page, get_header(new_page)->cell_count, new_off);

        uint32_t child_page_id = ieentry->child_page;
        if (th.bpm) {
            Page* child_page = th.bpm->fetch_page(child_page_id);
            if (child_page) {
                get_header(*child_page)->parent_page_id = new_pid;
                th.bpm->unpin_page(child_page_id, true);
            }
        }
    }
    
    if (new_leftmost_child != 0) {
        *reinterpret_cast<uint32_t*>(get_header(new_page)->reserved) = new_leftmost_child;
    }

    uint16_t num_to_remove = total - mid;
    for(uint16_t j = 0; j < num_to_remove; j++) {
        uint16_t last_index = get_header(page)->cell_count - 1;
        if (last_index >= mid) {
            remove_slot(page, last_index);
        } else {
            break;
        }
    }

    auto* new_ph = get_header(new_page);
    new_ph->parent_page_id = ph->parent_page_id;

    if (th.bpm) {
        Page* new_bp = th.bpm->new_page(new_pid, PageType::INDEX, PageLevel::INTERNAL);
        if (new_bp) {
            memcpy(new_bp->data, new_page.data, PAGE_SIZE);
            th.bpm->unpin_page(new_pid, true);
        }
    }

    return { new_pid, sep, page, new_page };
}

void create_new_root(TableHandle& th, uint32_t left, const Key& key, uint32_t right) {
    if (!th.bpm) {
        return;
    }
    uint32_t new_root_id = allocate_page(th);
    Page* root = th.bpm->new_page(new_root_id, PageType::INDEX, PageLevel::INTERNAL);
    if (!root) {
        return;
    }

    auto* root_ph = get_header(*root);
    *reinterpret_cast<uint32_t*>(root_ph->reserved) = left;
    root_ph->root_page = left;

    uint16_t offset = write_internal_entry(*root, key, right);
    insert_slot(*root, 0, offset);

    th.root_page = new_root_id;

    Page* meta = th.bpm->fetch_page(0);
    if (meta) {
        get_header(*meta)->root_page = new_root_id;
        th.bpm->unpin_page(0, true);
    }

    th.bpm->unpin_page(new_root_id, true);

    Page* left_page = th.bpm->fetch_page(left);
    if (left_page) {
        get_header(*left_page)->parent_page_id = new_root_id;
        th.bpm->unpin_page(left, true);
    }

    Page* right_page = th.bpm->fetch_page(right);
    if (right_page) {
        get_header(*right_page)->parent_page_id = new_root_id;
        th.bpm->unpin_page(right, true);
    }
}

void insert_into_parent(TableHandle& th, uint32_t left, const Key& key, uint32_t right) {
    if (!th.bpm) {
        return;
    }
    Page* left_page = th.bpm->fetch_page(left);
    if (!left_page) {
        return;
    }
    auto* lh = get_header(*left_page);
    uint32_t parent_pid = lh->parent_page_id;
    th.bpm->unpin_page(left, false);

    if (parent_pid == 0 || parent_pid == INVALID_PAGE_ID) {
        create_new_root(th, left, key, right);
        return;
    }

    Page* parent = th.bpm->fetch_page(parent_pid);
    if (!parent) {
        return;
    }
    auto* ph = get_header(*parent);
    if (ph->page_level != PageLevel::INTERNAL) {
        th.bpm->unpin_page(parent_pid, false);
        create_new_root(th, left, key, right);
        return;
    }

    BSearchResult sr = internal_search_record(*parent, key.data(), key.size());
    if (sr.found) {
        th.bpm->unpin_page(parent_pid, false);
        create_new_root(th, left, key, right);
        return;
    }

    if (sr.index == 0) {
        *reinterpret_cast<uint32_t*>(ph->reserved) = left;
    }

    if (insert_internal_no_split(*parent, key, right)) {
        th.bpm->unpin_page(parent_pid, true);
        return;
    }

    auto split = split_internal_page(th, *parent);
    th.bpm->unpin_page(parent_pid, true);
    insert_into_parent(th, parent_pid, split.seperator_key, split.new_page);
}
