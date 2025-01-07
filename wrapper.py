import os
from pathlib import Path
import rlp
from typing import Iterator, Tuple
from dataclasses import dataclass

# Import from our previous decoder - assuming it's in decoder.py
from decoder import (
    ConsensusBlockHeader,
    ConsensusBlockBody,
    decode_consensus_block_header,
    decode_consensus_block_body,
    ValidationError
)

@dataclass
class Block:
    number: int
    header: ConsensusBlockHeader
    body: ConsensusBlockBody
    header_id: bytes
    body_id: bytes

class LedgerReader:
    def __init__(self, ledger_path: str):
        self.ledger_path = Path(ledger_path)
        if not self.ledger_path.is_dir():
            raise ValueError(f"Ledger path {ledger_path} is not a directory")

    def _read_file(self, path: Path) -> bytes:
        """Read raw bytes from a file."""
        try:
            with open(path, 'rb') as f:
                return f.read()
        except IOError as e:
            raise IOError(f"Failed to read {path}: {e}")

    def _get_block_number_from_filename(self, filename: str) -> int:
        """Extract block number from filename (e.g., '1.header' -> 1)."""
        try:
            return int(filename.split('.')[0])
        except (IndexError, ValueError) as e:
            raise ValueError(f"Invalid filename format {filename}: {e}")

    def _find_block_files(self) -> Iterator[Tuple[int, Path, Path]]:
        """Find and pair header and body files by block number."""
        header_files = {self._get_block_number_from_filename(f.name): f 
                       for f in self.ledger_path.glob('*.header')}
        body_files = {self._get_block_number_from_filename(f.name): f 
                        for f in self.ledger_path.glob('*.body')}
        
        # Get sorted list of block numbers that have both header and body
        block_numbers = sorted(set(header_files.keys()) & set(body_files.keys()))
        
        for number in block_numbers:
            yield number, header_files[number], body_files[number]

    def read_block(self, header_path: Path, body_path: Path) -> Tuple[ConsensusBlockHeader, ConsensusBlockBody, bytes, bytes]:
        """Read and decode a single block's header and body."""
        try:
            header_bytes = self._read_file(header_path)
            body_bytes = self._read_file(body_path)
            
            header = decode_consensus_block_header(header_bytes)
            body = decode_consensus_block_body(body_bytes)

            import blake3
            computed_header_id = blake3.blake3(header_bytes).digest()
            computed_body_id = blake3.blake3(body_bytes).digest()
            
            return header, body, computed_header_id, computed_body_id
        except (IOError, ValidationError, rlp.exceptions.DecodingError) as e:
            raise RuntimeError(f"Failed to decode block {header_path.stem}: {e}")

    def read_blocks(self) -> Iterator[Block]:
        """Read and decode all blocks in the ledger directory."""
        for number, header_path, body_path in self._find_block_files():
            try:
                header, body, header_id, body_id = self.read_block(header_path, body_path)
                yield Block(number=number, header=header, body=body, header_id=header_id, body_id=body_id)
            except Exception as e:
                print(f"Error processing block {number}: {e}")
                continue

def main():
    # import argparse
    # parser = argparse.ArgumentParser(description='Read and decode consensus blocks from ledger directory')
    # parser.add_argument('ledger_path', help='Path to ledger directory')
    # parser.add_argument('--validate', action='store_true', help='Perform additional validation checks')
    # args = parser.parse_args()

    # reader = LedgerReader(args.ledger_path)
    reader = LedgerReader("ledger/")
    
    # print(f"Reading blocks from {args.ledger_path}")
    parent_id = None
    for block in reader.read_blocks():
        print(f"\nBlock {block.number}:")
        print(f"  Round: {block.header.round}")
        print(f"  Epoch: {block.header.epoch}")
        print(f"  Sequence Number: {block.header.seq_num}")
        print(f"  Timestamp: {block.header.timestamp_ns}")
        print(f"  Author: {block.header.author.hex()}")
        print(f"  Number of transactions: {len(block.body.execution_body.transactions)}")

        # Perform additional validation checks
        assert block.header.seq_num == block.number
        
        # Validate block body hash matches header
        assert block.body_id == block.header.block_body_id
        assert parent_id is None or parent_id == block.header.qc.vote.id
        parent_id = block.header_id

if __name__ == '__main__':
    main()
