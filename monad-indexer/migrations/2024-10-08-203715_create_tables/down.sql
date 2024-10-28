DROP INDEX ix_validator_set_round;
DROP INDEX ix_validator_set_epoch;
DROP TABLE validator_set;

DROP TABLE key;

DROP TABLE block_payload;

DROP INDEX ix_block_header_epoch;
DROP INDEX ix_block_header_round;
DROP INDEX ix_block_header_timestamp;
DROP TABLE block_header;

DROP DOMAIN bytes32;
