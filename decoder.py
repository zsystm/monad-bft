from dataclasses import dataclass
from typing import List, Optional
import rlp
from eth_utils import decode_hex

class ValidationError(Exception):
    pass

def validate_length(value: bytes, expected_length: int, field_name: str):
    if len(value) != expected_length:
        raise ValidationError(f"{field_name} must be {expected_length} bytes, got {len(value)}")

@dataclass
class SignerMap:
    num_bits: int
    bitmap: bytes

    def validate(self):
        expected_bitmap_length = (self.num_bits + 7) // 8
        if len(self.bitmap) != expected_bitmap_length:
            raise ValidationError(f"Bitmap length mismatch. Expected {expected_bitmap_length}, got {len(self.bitmap)}")

    @classmethod
    def from_list(cls, decoded_list):
        return cls(
            num_bits=int.from_bytes(decoded_list[0], 'big'),
            bitmap=decoded_list[1]
        )

@dataclass
class Signatures:
    signers: SignerMap
    aggregate_signature: bytes

    def validate(self):
        validate_length(self.aggregate_signature, 96, "aggregate_signature")
        self.signers.validate()

    @classmethod
    def from_list(cls, decoded_list):
        return cls(
            signers=SignerMap.from_list(decoded_list[0]),
            aggregate_signature=decoded_list[1]
        )

@dataclass
class Vote:
    id: bytes
    round: int
    epoch: int
    parent_id: bytes
    parent_round: int

    def validate(self):
        validate_length(self.id, 32, "id")
        validate_length(self.parent_id, 32, "parent_id")
        if self.round < 0 or self.epoch < 0 or self.parent_round < 0:
            raise ValidationError("Round and epoch values must be non-negative")

    @classmethod
    def from_list(cls, decoded_list):
        return cls(
            id=decoded_list[0],
            round=int.from_bytes(decoded_list[1], 'big'),
            epoch=int.from_bytes(decoded_list[2], 'big'),
            parent_id=decoded_list[3],
            parent_round=int.from_bytes(decoded_list[4], 'big')
        )

@dataclass
class QuorumCertificate:
    vote: Vote
    signatures: Signatures

    def validate(self):
        self.vote.validate()
        self.signatures.validate()

    @classmethod
    def from_list(cls, decoded_list):
        return cls(
            vote=Vote.from_list(decoded_list[0]),
            signatures=Signatures.from_list(decoded_list[1])
        )

@dataclass
class FinalizedEthHeader:
    parent_hash: bytes
    ommers_hash: bytes
    beneficiary: bytes
    state_root: bytes
    transactions_root: bytes
    receipts_root: bytes
    logs_bloom: bytes
    difficulty: bytes
    number: int
    gas_limit: int
    gas_used: int
    timestamp: int
    extra_data: bytes
    mix_hash: bytes
    nonce: bytes
    base_fee_per_gas: Optional[int]
    withdrawals_root: Optional[bytes]
    blob_gas_used: Optional[int]
    excess_blob_gas: Optional[int]
    parent_beacon_block_root: Optional[bytes]
    requests_hash: Optional[bytes]
    target_blobs_per_block: Optional[int]

    def validate(self):
        validate_length(self.parent_hash, 32, "parent_hash")
        validate_length(self.ommers_hash, 32, "ommers_hash")
        validate_length(self.beneficiary, 20, "beneficiary")
        validate_length(self.state_root, 32, "state_root")
        validate_length(self.transactions_root, 32, "transactions_root")
        validate_length(self.receipts_root, 32, "receipts_root")
        validate_length(self.logs_bloom, 256, "logs_bloom")
        # validate_length(self.difficulty, 32, "difficulty")
        # validate_length(self.extra_data, 32, "extra_data")
        validate_length(self.mix_hash, 32, "mix_hash")
        validate_length(self.nonce, 8, "nonce")

    @classmethod
    def from_list(cls, decoded_list):
        return cls(
            parent_hash=decoded_list[0],
            ommers_hash=decoded_list[1],
            beneficiary=decoded_list[2],
            state_root=decoded_list[3],
            transactions_root=decoded_list[4],
            receipts_root=decoded_list[5],
            logs_bloom=decoded_list[6],
            difficulty=decoded_list[7],
            number=int.from_bytes(decoded_list[8], 'big'),
            gas_limit=int.from_bytes(decoded_list[9], 'big'),
            gas_used=int.from_bytes(decoded_list[10], 'big'),
            timestamp=int.from_bytes(decoded_list[11], 'big'),
            extra_data=decoded_list[12],
            mix_hash=decoded_list[13],
            nonce=decoded_list[14],
            base_fee_per_gas=int.from_bytes(decoded_list[15], 'big') if decoded_list[15] else None,
            withdrawals_root=decoded_list[16] if len(decoded_list) > 16 else None,
            blob_gas_used=int.from_bytes(decoded_list[17], 'big') if len(decoded_list) > 17 else None,
            excess_blob_gas=int.from_bytes(decoded_list[18], 'big') if len(decoded_list) > 18 else None,
            parent_beacon_block_root=decoded_list[19] if len(decoded_list) > 19 else None,
            requests_hash=decoded_list[20] if len(decoded_list) > 20 else None,
            target_blobs_per_block=int.from_bytes(decoded_list[21], 'big') if len(decoded_list) > 21 else None
        )

@dataclass
class ProposedEthHeader:
    ommers_hash: bytes
    beneficiary: bytes
    transactions_root: bytes
    difficulty: int
    number: int
    gas_limit: int
    timestamp: int
    extra_data: bytes
    mix_hash: bytes
    nonce: bytes
    base_fee_per_gas: int
    withdrawals_root: bytes
    blob_gas_used: int
    excess_blob_gas: int
    parent_beacon_block_root: bytes

    def validate(self):
        validate_length(self.ommers_hash, 32, "ommers_hash")
        validate_length(self.beneficiary, 20, "beneficiary")
        validate_length(self.transactions_root, 32, "transactions_root")
        validate_length(self.extra_data, 32, "extra_data")
        validate_length(self.mix_hash, 32, "mix_hash")
        validate_length(self.nonce, 8, "nonce")
        validate_length(self.withdrawals_root, 32, "withdrawals_root")
        validate_length(self.parent_beacon_block_root, 32, "parent_beacon_block_root")

    @classmethod
    def from_list(cls, decoded_list):
        return cls(
            ommers_hash=decoded_list[0],
            beneficiary=decoded_list[1],
            transactions_root=decoded_list[2],
            difficulty=int.from_bytes(decoded_list[3], 'big'),
            number=int.from_bytes(decoded_list[4], 'big'),
            gas_limit=int.from_bytes(decoded_list[5], 'big'),
            timestamp=int.from_bytes(decoded_list[6], 'big'),
            extra_data=decoded_list[7],
            mix_hash=decoded_list[8],
            nonce=decoded_list[9],
            base_fee_per_gas=int.from_bytes(decoded_list[10], 'big'),
            withdrawals_root=decoded_list[11],
            blob_gas_used=int.from_bytes(decoded_list[12], 'big'),
            excess_blob_gas=int.from_bytes(decoded_list[13], 'big'),
            parent_beacon_block_root=decoded_list[14]
        )

@dataclass
class ConsensusBlockHeader:
    round: int
    epoch: int
    qc: QuorumCertificate
    author: bytes
    seq_num: int
    timestamp_ns: int
    round_signature: bytes
    delayed_execution_results: List[FinalizedEthHeader]
    execution_inputs: ProposedEthHeader
    block_body_id: bytes

    def validate(self):
        validate_length(self.author, 33, "author")
        validate_length(self.round_signature, 96, "round_signature")
        validate_length(self.block_body_id, 32, "block_body_id")
        
        if self.round < 0 or self.epoch < 0 or self.seq_num < 0 or self.timestamp_ns < 0:
            raise ValidationError("Numeric values must be non-negative")

        self.qc.validate()
        self.execution_inputs.validate()
        for result in self.delayed_execution_results:
            result.validate()

    @classmethod
    def from_list(cls, decoded_list):
        return cls(
            round=int.from_bytes(decoded_list[0], 'big'),
            epoch=int.from_bytes(decoded_list[1], 'big'),
            qc=QuorumCertificate.from_list(decoded_list[2]),
            author=decoded_list[3],
            seq_num=int.from_bytes(decoded_list[4], 'big'),
            timestamp_ns=int.from_bytes(decoded_list[5], 'big'),
            round_signature=decoded_list[6],
            delayed_execution_results=[FinalizedEthHeader.from_list(x) for x in decoded_list[7]],
            execution_inputs=ProposedEthHeader.from_list(decoded_list[8]),
            block_body_id=decoded_list[9]
        )

@dataclass
class TransactionSigned:
    raw_bytes: bytes

    @classmethod
    def from_list(cls, decoded_list):
        return cls(raw_bytes=decoded_list)

@dataclass
class Withdrawal:
    index: int
    validator_index: int
    address: bytes
    amount: int

    def validate(self):
        validate_length(self.address, 20, "address")
        if any(x < 0 for x in [self.index, self.validator_index, self.amount]):
            raise ValidationError("Numeric values must be non-negative")

    @classmethod
    def from_list(cls, decoded_list):
        return cls(
            index=int.from_bytes(decoded_list[0], 'big'),
            validator_index=int.from_bytes(decoded_list[1], 'big'),
            address=decoded_list[2],
            amount=int.from_bytes(decoded_list[3], 'big')
        )

@dataclass
class EthBlockBody:
    transactions: List[TransactionSigned]
    ommers: List[bytes]
    withdrawals: List[Withdrawal]

    def validate(self):
        if len(self.ommers) != 0:
            raise ValidationError("ommers must be empty")
        if len(self.withdrawals) != 0:
            raise ValidationError("withdrawals must be empty")

    @classmethod
    def from_list(cls, decoded_list):
        return cls(
            transactions=[TransactionSigned.from_list(tx) for tx in decoded_list[0]],
            ommers=[ommer for ommer in decoded_list[1]],
            withdrawals=[Withdrawal.from_list(w) for w in decoded_list[2]]
        )

@dataclass
class ConsensusBlockBody:
    execution_body: EthBlockBody

    def validate(self):
        self.execution_body.validate()

    @classmethod
    def from_list(cls, decoded_list):
        return cls(
            execution_body=EthBlockBody.from_list(decoded_list[0])
        )

def decode_consensus_block_header(rlp_bytes: bytes) -> ConsensusBlockHeader:
    """
    Decode an RLP-encoded consensus block header
    """
    decoded_list = rlp.decode(rlp_bytes)
    header = ConsensusBlockHeader.from_list(decoded_list)
    header.validate()
    return header

def decode_consensus_block_body(rlp_bytes: bytes) -> ConsensusBlockBody:
    """
    Decode an RLP-encoded consensus block body
    """
    decoded_list = rlp.decode(rlp_bytes)
    body = ConsensusBlockBody.from_list(decoded_list)
    body.validate()
    return body