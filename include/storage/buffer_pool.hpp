#pragma once

#include "storage/page.hpp"
#include "storage/disk_manager.hpp"
#include "storage/constants.hpp"
#include <unordered_map>
#include <vector>
#include <list>
#include <cstdint>

class BufferPoolManager {
public:
    explicit BufferPoolManager(DiskManager& disk_manager, size_t pool_size = BUFFER_POOL_SIZE);
    ~BufferPoolManager();

    BufferPoolManager(const BufferPoolManager&) = delete;
    BufferPoolManager& operator=(const BufferPoolManager&) = delete;

    Page* fetch_page(uint32_t page_id);
    bool unpin_page(uint32_t page_id, bool dirty);
    Page* new_page(uint32_t page_id, PageType page_type = PageType::DATA, PageLevel page_level = PageLevel::LEAF);
    bool delete_page(uint32_t page_id);
    bool flush_page(uint32_t page_id);
    void flush_all();
    size_t get_pinned_count() const;
    size_t get_free_frame_count() const;

private:
    struct Frame {
        uint32_t page_id;
        uint32_t pin_count;
        bool dirty;
        Page page;
        Frame() : page_id(INVALID_PAGE_ID), pin_count(0), dirty(false) {}
    };

    size_t find_or_evict_frame();
    bool evict_frame(size_t frame_id);
    void mark_frame_used(size_t frame_id);
    void remove_from_lru(size_t frame_id);

    DiskManager& disk_manager_;
    std::vector<Frame> frames_;
    std::unordered_map<uint32_t, size_t> page_table_;
    std::list<size_t> lru_list_;
    size_t pool_size_;
};
