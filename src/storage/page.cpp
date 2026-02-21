#include "storage/page.hpp"
#include "storage/record.hpp"
#include <cstring>
#include <algorithm>

void init_page(Page& page, uint32_t page_id, PageType page_type, PageLevel page_level) {
    PageHeader* page_header = get_header(page);
    std::memset(page.data, 0, PAGE_SIZE);
    
    page_header->page_id = page_id;
    page_header->page_type = page_type;
    page_header->page_level = page_level;
    std::fill_n(page_header->reserved, sizeof(page_header->reserved) / sizeof(page_header->reserved[0]), static_cast<uint8_t>(0));
    page_header->flags = 0;
    page_header->cell_count = 0;
    page_header->free_start = sizeof(PageHeader);
    page_header->free_end = PAGE_SIZE;
    page_header->parent_page_id = 0;
    page_header->lsn = 0;
    page_header->prev_page_id = 0;
    page_header->next_page_id = 0;
}
