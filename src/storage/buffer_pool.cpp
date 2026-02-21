#include "storage/buffer_pool.hpp"
#include "storage/page.hpp"
#include <algorithm>
#include <stdexcept>
#include <cstring>

BufferPoolManager::BufferPoolManager(DiskManager& disk_manager, size_t pool_size)
    : disk_manager_(disk_manager), pool_size_(pool_size) {
    frames_.resize(pool_size_);
    
    for (size_t i = 0; i < pool_size_; ++i) {
        lru_list_.push_back(i);
    }
}

BufferPoolManager::~BufferPoolManager() {
    flush_all();
}

Page* BufferPoolManager::fetch_page(uint32_t page_id) {
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        size_t frame_id = it->second;
        Frame& frame = frames_[frame_id];
        frame.pin_count++;
        mark_frame_used(frame_id);
        return &frame.page;
    }

    size_t frame_id = find_or_evict_frame();
    if (frame_id == SIZE_MAX) {
        return nullptr;
    }

    Frame& frame = frames_[frame_id];
    
    if (frame.page_id != INVALID_PAGE_ID) {
        if (!evict_frame(frame_id)) {
            return nullptr;
        }
    }

    try {
        disk_manager_.read_page(static_cast<int>(page_id), frame.page.data);
    } catch (const std::exception&) {
        return nullptr;
    }

    frame.page_id = page_id;
    frame.pin_count = 1;
    frame.dirty = false;
    page_table_[page_id] = frame_id;
    mark_frame_used(frame_id);

    return &frame.page;
}

bool BufferPoolManager::unpin_page(uint32_t page_id, bool dirty) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }

    size_t frame_id = it->second;
    Frame& frame = frames_[frame_id];

    if (frame.pin_count == 0) {
        return false;
    }

    frame.pin_count--;
    if (dirty) {
        frame.dirty = true;
    }

    if (frame.pin_count == 0) {
        auto lru_it = std::find(lru_list_.begin(), lru_list_.end(), frame_id);
        if (lru_it == lru_list_.end()) {
            lru_list_.push_back(frame_id);
        }
    }

    return true;
}

Page* BufferPoolManager::new_page(uint32_t page_id, PageType page_type, PageLevel page_level) {
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        size_t frame_id = it->second;
        Frame& frame = frames_[frame_id];
        frame.pin_count++;
        mark_frame_used(frame_id);
        return &frame.page;
    }

    size_t frame_id = find_or_evict_frame();
    if (frame_id == SIZE_MAX) {
        return nullptr;
    }

    Frame& frame = frames_[frame_id];
    
    if (frame.page_id != INVALID_PAGE_ID) {
        if (!evict_frame(frame_id)) {
            return nullptr;
        }
    }

    init_page(frame.page, page_id, page_type, page_level);
    frame.page_id = page_id;
    frame.pin_count = 1;
    frame.dirty = true;
    page_table_[page_id] = frame_id;
    mark_frame_used(frame_id);

    return &frame.page;
}

bool BufferPoolManager::delete_page(uint32_t page_id) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }

    size_t frame_id = it->second;
    Frame& frame = frames_[frame_id];

    if (frame.pin_count > 0) {
        return false;
    }

    page_table_.erase(it);
    remove_from_lru(frame_id);
    frame.page_id = INVALID_PAGE_ID;
    frame.pin_count = 0;
    frame.dirty = false;

    return true;
}

bool BufferPoolManager::flush_page(uint32_t page_id) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }

    size_t frame_id = it->second;
    Frame& frame = frames_[frame_id];

    if (frame.dirty) {
        try {
            disk_manager_.write_page(static_cast<int>(page_id), frame.page.data);
            frame.dirty = false;
        } catch (const std::exception&) {
            return false;
        }
    }
    return true;
}

void BufferPoolManager::flush_all() {
    for (auto& [page_id, frame_id] : page_table_) {
        Frame& frame = frames_[frame_id];
        if (frame.dirty) {
            try {
                disk_manager_.write_page(static_cast<int>(page_id), frame.page.data);
                frame.dirty = false;
            } catch (const std::exception&) {
            }
        }
    }
}

size_t BufferPoolManager::get_pinned_count() const {
    size_t count = 0;
    for (const auto& frame : frames_) {
        if (frame.pin_count > 0) {
            count++;
        }
    }
    return count;
}

size_t BufferPoolManager::get_free_frame_count() const {
    size_t count = 0;
    for (const auto& frame : frames_) {
        if (frame.page_id == INVALID_PAGE_ID) {
            count++;
        }
    }
    return count;
}

size_t BufferPoolManager::find_or_evict_frame() {
    for (size_t i = 0; i < pool_size_; ++i) {
        if (frames_[i].page_id == INVALID_PAGE_ID) {
            return i;
        }
    }

    while (!lru_list_.empty()) {
        size_t frame_id = lru_list_.back();
        Frame& frame = frames_[frame_id];

        if (frame.pin_count == 0) {
            lru_list_.pop_back();
            return frame_id;
        }
        lru_list_.pop_back();
    }

    return SIZE_MAX;
}

bool BufferPoolManager::evict_frame(size_t frame_id) {
    Frame& frame = frames_[frame_id];
    
    if (frame.page_id == INVALID_PAGE_ID) {
        return true;
    }

    if (frame.dirty) {
        try {
            disk_manager_.write_page(static_cast<int>(frame.page_id), frame.page.data);
        } catch (const std::exception&) {
            return false;
        }
    }

    page_table_.erase(frame.page_id);
    remove_from_lru(frame_id);
    frame.page_id = INVALID_PAGE_ID;
    frame.pin_count = 0;
    frame.dirty = false;

    return true;
}

void BufferPoolManager::mark_frame_used(size_t frame_id) {
    remove_from_lru(frame_id);
    lru_list_.push_front(frame_id);
}

void BufferPoolManager::remove_from_lru(size_t frame_id) {
    auto it = std::find(lru_list_.begin(), lru_list_.end(), frame_id);
    if (it != lru_list_.end()) {
        lru_list_.erase(it);
    }
}
