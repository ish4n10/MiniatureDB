#include "storage/relational/row_codec.hpp"
#include <cstring>

namespace Relational {

RowCodec::RowCodec(const TableSchema& _schema) : schema(_schema) {}

namespace {
    constexpr uint8_t TAG_INT = 0;
    constexpr uint8_t TAG_FLOAT = 1;
    constexpr uint8_t TAG_DOUBLE = 2;
    constexpr uint8_t TAG_STRING = 3;
    constexpr uint8_t TAG_BOOLEAN = 4;
    constexpr uint8_t TAG_DATETIME = 5;

void append_column(std::vector<uint8_t>& result, Relational::ColumnType type, const Relational::Value& v) {
    switch (type) {
        case Relational::ColumnType::INT: {
            result.push_back(TAG_INT);
            int32_t x = std::get<int>(v);
            size_t pos = result.size();
            result.resize(pos + 4);
            std::memcpy(result.data() + pos, &x, 4);
            break;
        }
        case ColumnType::FLOAT: {
            result.push_back(TAG_FLOAT);
            float x = std::get<float>(v);
            size_t pos = result.size();
            result.resize(pos + 4);
            std::memcpy(result.data() + pos, &x, 4);
            break;
        }
        case ColumnType::DOUBLE: {
            result.push_back(TAG_DOUBLE);
            double x = std::get<double>(v);
            size_t pos = result.size();
            result.resize(pos + 8);
            std::memcpy(result.data() + pos, &x, 8);
            break;
        }
        case ColumnType::STRING: {
            result.push_back(TAG_STRING);
            const std::string& s = std::get<std::string>(v);
            uint16_t len = static_cast<uint16_t>(s.size());
            size_t pos = result.size();
            result.resize(pos + 2 + len);
            std::memcpy(result.data() + pos, &len, 2);
            std::memcpy(result.data() + pos + 2, s.data(), len);
            break;
        }
        case ColumnType::BOOLEAN: {
            result.push_back(TAG_BOOLEAN);
            result.push_back(std::get<bool>(v) ? 1 : 0);
            break;
        }
        case ColumnType::DATETIME: {
            result.push_back(TAG_DATETIME);
            result.resize(result.size() + 8, 0);
            break;
        }
    }
}

}

std::vector<uint8_t> RowCodec::encode_key(const Tuple& tuple) const {
    std::vector<uint8_t> result;
    if (schema.pk_index < 0 || static_cast<size_t>(schema.pk_index) >= schema.columns.size()) return result;
    if (tuple.size() <= static_cast<size_t>(schema.pk_index)) return result;
    append_column(result, schema.columns[static_cast<size_t>(schema.pk_index)].type, tuple[static_cast<size_t>(schema.pk_index)]);
    return result;
}

std::vector<uint8_t> RowCodec::encode_value(const Tuple& tuple) const {
    return encode(tuple);
}

std::vector<uint8_t> RowCodec::encode(const Tuple& tuple) const {
    std::vector<uint8_t> result;
    if (tuple.size() != schema.columns.size()) return result;

    for (size_t i = 0; i < schema.columns.size(); ++i) {
        append_column(result, schema.columns[i].type, tuple[i]);
    }
    return result;
}

Tuple RowCodec::decode(const std::vector<uint8_t>& data) const {
    Tuple result;
    const uint8_t* p = data.data();
    const uint8_t* end = data.data() + data.size();

    for (const auto& col : schema.columns) {
        if (p >= end) return {};

        uint8_t tag = *p++;
        switch (col.type) {
            case ColumnType::INT: {
                if (tag != TAG_INT || p + 4 > end) return {};
                int32_t x;
                std::memcpy(&x, p, 4);
                p += 4;
                result.push_back(static_cast<int>(x));
                break;
            }
            case ColumnType::FLOAT: {
                if (tag != TAG_FLOAT || p + 4 > end) return {};
                float x;
                std::memcpy(&x, p, 4);
                p += 4;
                result.push_back(x);
                break;
            }
            case ColumnType::DOUBLE: {
                if (tag != TAG_DOUBLE || p + 8 > end) return {};
                double x;
                std::memcpy(&x, p, 8);
                p += 8;
                result.push_back(x);
                break;
            }
            case ColumnType::STRING: {
                if (tag != TAG_STRING || p + 2 > end) return {};
                uint16_t len;
                std::memcpy(&len, p, 2);
                p += 2;
                if (p + len > end) return {};
                result.emplace_back(std::string(reinterpret_cast<const char*>(p), len));
                p += len;
                break;
            }
            case ColumnType::BOOLEAN: {
                if (tag != TAG_BOOLEAN || p >= end) return {};
                result.push_back(*p++ != 0);
                break;
            }
            case ColumnType::DATETIME: {
                if (tag != TAG_DATETIME || p + 8 > end) return {};
                p += 8;
                result.push_back(0);
                break;
            }
        }
    }
    return result;
}

}
