#pragma once
#include <string>
#include <cstdint>

class DiskManager {
public:
    DiskManager(const std::string& file_path);
    ~DiskManager();

    DiskManager(DiskManager&& other) noexcept;
    DiskManager& operator=(DiskManager&& other) noexcept;

    DiskManager(const DiskManager&) = delete;
    DiskManager& operator=(const DiskManager&) = delete;

    void read_page(int page_id, uint8_t* page_data);
    void write_page(int page_id, const void* page_data); // void as pointer can be anything for now
    void flush();

private: 
    int file_descriptor{-1};
};