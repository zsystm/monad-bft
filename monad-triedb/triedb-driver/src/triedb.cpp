#include <cassert>
#include <filesystem>
#include <iostream>
#include <limits>
#include <optional>
#include <vector>

#include <monad/core/byte_string.hpp>
#include <monad/core/nibble.h>
#include <monad/mpt/db.hpp>
#include <monad/mpt/traverse.hpp>
#include <monad/mpt/ondisk_db_config.hpp>

#include "triedb.h"

struct triedb
{
    explicit triedb(std::vector<std::filesystem::path> dbname_paths)
        : db_{monad::mpt::ReadOnlyOnDiskDbConfig{
              .disable_mismatching_storage_pool_check = true,
              .dbname_paths = std::move(dbname_paths)}}
    {
    }

    monad::mpt::Db db_;
};

int triedb_open(char const *dbdirpath, triedb **db)
{
    if (*db != nullptr) {
        return -1;
    }

    std::vector<std::filesystem::path> paths;
    if (std::filesystem::is_block_file(dbdirpath)) {
        paths.emplace_back(dbdirpath);
    }
    else {
        std::error_code ec;
        for (auto const &file :
             std::filesystem::directory_iterator(dbdirpath, ec)) {
            paths.emplace_back(file.path());
        }
        if (ec) {
            return -2;
        }
    }

    try {
        *db = new triedb{std::move(paths)};
    } catch (const std::exception &e) {
        std::cerr << e.what();
        return -3;
    }
    return 0;
}

int triedb_close(triedb *db)
{
    delete db;
    return 0;
}

int triedb_read(
    triedb *db, bytes key, uint8_t key_len_nibbles, bytes *value,
    uint64_t block_id)
{
    if (!db->db_.is_latest()) {
        db->db_.load_latest();
    }
    auto result =
        db->db_.get(monad::mpt::NibblesView{0, key_len_nibbles, key}, block_id);
    if (!result.has_value()) {
        return -1;
    }

    auto const &value_view = result.value();
    if ((value_view.size() >> std::numeric_limits<int>::digits) != 0) {
        // value length doesn't fit in return type
        return -2;
    }
    size_t const value_len = value_view.size();
    *value = new uint8_t[value_len];
    memcpy((void *)*value, value_view.data(), value_len);
    return (int) value_len;
}

int triedb_read_data(
    triedb *db, bytes key, uint8_t key_len_nibbles, bytes *value,
    uint64_t block_id)
{
    if (!db->db_.is_latest()) {
        db->db_.load_latest();
    }
    auto result =
        db->db_.get_data(monad::mpt::NibblesView{0, key_len_nibbles, key}, block_id);
    if (!result.has_value()) {
        return -1;
    }

    auto const &value_view = result.value();
    if ((value_view.size() >> std::numeric_limits<int>::digits) != 0) {
        // value length doesn't fit in return type
        return -2;
    }
    size_t const value_len = value_view.size();
    *value = new uint8_t[value_len];
    memcpy((void *)*value, value_view.data(), value_len);
    return (int) value_len;
}

void triedb_async_read(
    triedb *db, bytes key, uint8_t key_len_nibbles, uint64_t block_id,
    void (*completed)(bytes value, int length, void *user), void *user)
{
    if (!db->db_.is_latest()) {
        db->db_.load_latest();
    }

    struct receiver_t
    {
        void (*completed_)(bytes value, int length, void *user);
        void *user_;

        void set_value(
            monad::async::erased_connected_operation *state,
            monad::async::result<monad::byte_string> result)
        {
            bytes value = nullptr;
            int length = 0;
            auto completed = completed_;
            auto user = user_;
            if (!result) {
                length = -1;
            }
            else {
                auto const &value_view = result.value();
                if ((value_view.size() >> std::numeric_limits<int>::digits) !=
                    0) {
                    // value length doesn't fit in return type
                    length = -2;
                }
                else {
                    value = new uint8_t[value_view.size()];
                    memcpy((void *)value, value_view.data(), value_view.size());
                    length = (int)value_view.size();
                }
            }
            delete state;
            completed(value, length, user);
        }
    };

    auto *state = new auto(monad::async::connect(
        monad::mpt::make_get_sender(
            db->db_,
            monad::mpt::NibblesView{0, key_len_nibbles, key},
            block_id),
        receiver_t{completed, user}));
    state->initiate();
}

void triedb_traverse_state(triedb *db, bytes key, uint8_t key_len_nibbles, uint64_t block_id, state_callback callback)
{
    auto cursor =
        db->db_.find(monad::mpt::NibblesView{0, key_len_nibbles, key}, block_id);
    if (!cursor.has_value()) {
        return;
    }

    class Traverse final : public monad::mpt::TraverseMachine
    {
        state_callback callback_;
        monad::mpt::Nibbles path_;
        monad::mpt::NibblesView const root_;

    public:
        explicit Traverse(
            state_callback callback, monad::mpt::NibblesView const root = {})
            : callback_(std::move(callback))
            , path_(root)
            , root_(root)
        {
        }

        virtual bool
        down(unsigned char const branch, monad::mpt::Node const &node) override
        {
            if (branch == monad::mpt::INVALID_BRANCH) {
                MONAD_ASSERT(path_ == root_);
                return true;
            }
            path_ =
                monad::mpt::concat(monad::mpt::NibblesView{path_}, branch, node.path_nibble_view());

            const bool account_node = (path_.nibble_size() == (KECCAK256_SIZE * 2));
            const bool state_node = (path_.nibble_size() == ((KECCAK256_SIZE + KECCAK256_SIZE) * 2));
            if (account_node || state_node) {
                uint8_t path_bytes[64];
                for (unsigned n = 0; n < (unsigned) path_.nibble_size(); ++n)
                {
                    set_nibble(path_bytes, n, path_.get(n));
                }
                auto const value = node.data();
                size_t path_size = account_node ? KECCAK256_SIZE : KECCAK256_SIZE+KECCAK256_SIZE;
                callback_(path_bytes, path_size, value.data(), value.size());
            }
            return true;
        }

        virtual void
        up(unsigned char const branch, monad::mpt::Node const &node) override
        {
            auto const path_view = monad::mpt::NibblesView{path_};
            auto const rem_size = [&] {
                if (branch == monad::mpt::INVALID_BRANCH) {
                    return 0;
                }
                int const rem_size = path_view.nibble_size() - 1 -
                                        node.path_nibble_view().nibble_size();
                return rem_size;
            }();
            path_ = path_view.substr(0, static_cast<unsigned>(rem_size));
        }

        virtual std::unique_ptr<TraverseMachine> clone() const override
        {
            return std::make_unique<Traverse>(*this);
        }

    } machine(callback, cursor.value().node->path_nibble_view());
    db->db_.traverse(cursor.value(), machine, block_id);
}

size_t triedb_poll(triedb *db, bool blocking, size_t count)
{
    return db->db_.poll(blocking, count);
}

int triedb_finalize(bytes value)
{
    delete value;
    return 0;
}

uint64_t triedb_latest_block(triedb *db)
{
    if (!db->db_.is_latest()) {
        db->db_.load_latest();
    }

    std::optional<uint64_t> latest_block_id = db->db_.get_latest_block_id();

    if (latest_block_id.has_value()) {
        return latest_block_id.value();
    }
    else {
        // no block has been produced
        return 0;
    }
}
