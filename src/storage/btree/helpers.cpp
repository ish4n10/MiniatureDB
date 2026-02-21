#include <cstdint>
#include "storage/page.hpp"
#include "storage/btree.hpp"
#include "storage/table_handle.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/record.hpp"
#include <cstring>

uint16_t write_raw_record(Page& page, const uint8_t* raw, uint16_t size) {
    PageHeader* ph = get_header(page);
    uint16_t offset = ph->free_start;
    
    if (offset + size > ph->free_end) {
        return 0;
    }
    
    if (offset < sizeof(PageHeader)) {
        return 0;
    }
    
    std::memcpy(page.data + offset, raw, size);
    ph->free_start = offset + size;
    
    return offset;
}
