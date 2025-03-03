#pragma once

#include <cstdint>
#include <vector>

std::vector<uint8_t> cxx_rlp_buffer(size_t reserved) {
    std::vector<uint8_t> buf {};
    buf.reserve(reserved);
    return buf;
}
