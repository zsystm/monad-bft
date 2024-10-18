CREATE DOMAIN bytes32 BYTEA CHECK (octet_length(VALUE) = 32);
CREATE TABLE block_header (
	-- unique hash used to identify the block
	block_id bytes32 NOT NULL PRIMARY KEY,

	-- timestamp in block header
	timestamp TIMESTAMP NOT NULL,

	-- proposer of this block
	author BYTEA NOT NULL,

	-- epoch this block was proposed in
	epoch BIGINT NOT NULL,

	-- round this block was proposed in
	round BIGINT NOT NULL,

	---- BEGIN ExecutionProtocol
	state_root bytes32 NOT NULL,
	seq_num BIGINT NOT NULL,
	beneficiary BYTEA NOT NULL,
	randao_reveal BYTEA NOT NULL,
	---- END ExecutionProtocol

	-- identifier for the transaction payload of this block
	-- null if this is not an executable block
	payload_id bytes32,

	---- BEGIN QC
	parent_block_id bytes32 NOT NULL
	---- END QC
);

CREATE INDEX ix_block_header_timestamp ON block_header(timestamp);

CREATE TABLE block_payload (
	-- unique hash used to identify the payload
	payload_id bytes32 PRIMARY KEY NOT NULL,

	num_tx INT NOT NULL,

	payload_size INT NOT NULL
);
