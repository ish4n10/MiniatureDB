#pragma once
#include <cstdint>
#include "storage/constants.hpp"

enum class PageType : uint16_t {
    HEADER = 0,
    META = 1,
    INDEX = 2,
    DATA = 3,
    FREE = 4
};

enum class PageLevel : uint16_t {
    NONE = 0,
    LEAF = 1,
    INTERNAL = 2
};

#pragma pack(push, 1)
struct PageHeader {
    uint32_t page_id;
    PageType page_type;
    PageLevel page_level;

    uint32_t root_page;
    uint8_t reserved[4];
    uint16_t flags;
    uint16_t cell_count; 
    uint16_t free_start;
    uint16_t free_end;

    uint32_t parent_page_id;
    uint32_t lsn;

    uint32_t prev_page_id;
    uint32_t next_page_id;
};
#pragma pack(pop)


struct Page {
    uint8_t data[PAGE_SIZE];
};


static_assert(sizeof(PageHeader) == 40, "PageHeader size must be 40 bytes");

inline PageHeader* get_header(Page& page);
void init_page(Page& page, uint32_t page_id, PageType page_type, PageLevel page_level);
uint16_t* slot_ptr(Page& page, uint16_t index);
void insert_slot(Page& page, uint16_t index, uint16_t record_offset);
void remove_slot(Page& page, uint16_t index);

inline PageHeader* get_header(Page& page) {
    return reinterpret_cast<PageHeader*>(page.data);
}
