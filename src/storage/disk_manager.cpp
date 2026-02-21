#include "storage/disk_manager.hpp"
#include "storage/constants.hpp"
#include "storage/page.hpp"
#include <fcntl.h>
#include <stdexcept>
#include <cstring>
#include <algorithm>

#ifdef _WIN32
#include <io.h>
#include <sys/stat.h>
#include <sys/types.h>
#define open _open
#define read _read
#define write _write
#define lseek _lseek
#define close _close
#ifndef ssize_t
typedef intptr_t ssize_t;
#endif
#else
#include <unistd.h>
#endif

DiskManager::DiskManager(const std::string& file_path) {
    #ifdef _WIN32
    file_descriptor = open(file_path.c_str(), O_RDWR | O_CREAT | O_BINARY, _S_IREAD | _S_IWRITE);
    #else
    file_descriptor = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    #endif

    if (file_descriptor < 0) {
        throw std::runtime_error("Failed to open or create file");
    }
}

DiskManager::DiskManager(DiskManager&& other) noexcept {
    file_descriptor = other.file_descriptor;
    other.file_descriptor = -1;
}

DiskManager& DiskManager::operator=(DiskManager&& other) noexcept {
    if (this == &other) return *this;
    if (file_descriptor >= 0) {
        close(file_descriptor);
    }
    file_descriptor = other.file_descriptor;
    other.file_descriptor = -1;
    return *this;
}

DiskManager::~DiskManager() {
    if (file_descriptor >= 0) {
        close(file_descriptor);
    }
}

void DiskManager::read_page(int page_id, uint8_t* page_data) {
    long offset = static_cast<long>(page_id * PAGE_SIZE);
    if (lseek(file_descriptor, offset, SEEK_SET) < 0) {
        throw std::runtime_error("Failed to seek to the correct position for reading");
    }
    
    ssize_t total_read = 0;
    ssize_t bytes_read = 0;
    uint8_t* ptr = page_data;
    
    while (total_read < static_cast<ssize_t>(PAGE_SIZE)) {
        bytes_read = read(file_descriptor, ptr + total_read, static_cast<unsigned int>(PAGE_SIZE - total_read));
        if (bytes_read < 0) {
            throw std::runtime_error("Failed to read page data");
        }
        if (bytes_read == 0) {
            break;
        }
        total_read += bytes_read;
    }
    
    if (total_read < static_cast<ssize_t>(PAGE_SIZE)) {
        std::fill_n(page_data + total_read, PAGE_SIZE - total_read, static_cast<uint8_t>(0));
    }
}

void DiskManager::write_page(int page_id, const void* page_data) {
    long offset = static_cast<long>(page_id * PAGE_SIZE);
    long required_size = offset + PAGE_SIZE;
    
    long current_size = lseek(file_descriptor, 0, SEEK_END);
    if (current_size < 0) {
        throw std::runtime_error("Failed to get file size");
    }
    
    if (current_size < required_size) {
        if (lseek(file_descriptor, required_size - 1, SEEK_SET) < 0) {
            throw std::runtime_error("Failed to seek to extend file");
        }
        char zero = 0;
        ssize_t extend_bytes = write(file_descriptor, &zero, 1);
        if (extend_bytes != 1) {
            throw std::runtime_error("Failed to extend file");
        }
        #ifdef _WIN32
        _commit(file_descriptor);
        #endif
    }

    if (lseek(file_descriptor, offset, SEEK_SET) < 0) {
        throw std::runtime_error("Failed to seek to the correct position for writing");
    }
    
    ssize_t bytes_written = write(file_descriptor, page_data, static_cast<unsigned int>(PAGE_SIZE)); 
    
    if (bytes_written != static_cast<ssize_t>(PAGE_SIZE)) {
        throw std::runtime_error("Failed to write the complete page");
    }
    
    #ifdef _WIN32
    _commit(file_descriptor);
    #endif
}

void DiskManager::flush() {
    #ifdef _WIN32
    if (_commit(file_descriptor) < 0) {
        throw std::runtime_error("Failed to flush data to disk");
    }
    #else
    // fsync(file_descriptor); // If we wanted POSIX flush
    #endif
}