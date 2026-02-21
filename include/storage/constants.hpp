#pragma once
#include <cstdint>

inline constexpr uint32_t PAGE_SIZE = 2048;
inline constexpr uint32_t INVALID_PAGE_ID = static_cast<uint32_t>(-1);
inline constexpr uint32_t BUFFER_POOL_SIZE = 128;  // Default buffer pool size (can be overridden)
inline constexpr uint32_t MAX_FILE_PATH_LENGTH = 255;

inline constexpr uint8_t RECORD_DELETED = 1 << 0;
inline constexpr uint16_t MERGE_THRESHOLD_PERCENT = 50;