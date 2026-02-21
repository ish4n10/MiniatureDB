#pragma once
#include "storage/relational/catalog.hpp"
#include <cstdint>
#include <variant>

namespace Relational {
    using Value = std::variant<int, float, double, std::string, bool>;
    using Tuple = std::vector<Value>;

    class RowCodec {
    private:
        const TableSchema& schema;
    public:
        RowCodec(const TableSchema& schema);
        ~RowCodec() = default;
        std::vector<uint8_t> encode(const Tuple& tuple) const;
        std::vector<uint8_t> encode_key(const Tuple& tuple) const;
        std::vector<uint8_t> encode_value(const Tuple& tuple) const;
        Tuple decode(const std::vector<uint8_t>& data) const;
    };
}
