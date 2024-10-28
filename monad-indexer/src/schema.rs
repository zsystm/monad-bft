// @generated automatically by Diesel CLI.

diesel::table! {
    block_header (block_id) {
        block_id -> Bytea,
        timestamp -> Timestamp,
        author -> Bytea,
        epoch -> Int8,
        round -> Int8,
        state_root -> Bytea,
        seq_num -> Int8,
        beneficiary -> Bytea,
        randao_reveal -> Bytea,
        payload_id -> Nullable<Bytea>,
        parent_block_id -> Bytea,
    }
}

diesel::table! {
    block_payload (payload_id) {
        payload_id -> Bytea,
        num_tx -> Int4,
        payload_size -> Int4,
    }
}

diesel::table! {
    key (node_id) {
        node_id -> Bytea,
        dns -> Text,
    }
}

diesel::table! {
    validator_set (epoch, node_id) {
        epoch -> Int8,
        round -> Nullable<Int8>,
        node_id -> Bytea,
        certificate_key -> Bytea,
        stake -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    block_header,
    block_payload,
    key,
    validator_set,
);
