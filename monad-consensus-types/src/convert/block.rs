use alloy_rlp::{Decodable, Encodable};
use bytes::BytesMut;
use monad_eth_types::EthAddress;
use monad_proto::{
    error::ProtoError,
    proto::{
        basic::{ProtoBloom, ProtoGas, ProtoPayloadId},
        block::*,
        blocksync::ProtoBlockRange,
    },
};
use reth_primitives::Header;

use crate::{
    block::{Block, BlockRange, FullBlock},
    payload::{
        Bloom, ExecutionProtocol, FullTransactionList, Gas, Payload, PayloadId, RandaoReveal,
    },
    signature_collection::SignatureCollection,
};

impl From<&BlockRange> for ProtoBlockRange {
    fn from(value: &BlockRange) -> Self {
        Self {
            last_block_id: Some((&value.last_block_id).into()),
            max_blocks: Some((&value.max_blocks).into()),
        }
    }
}

impl TryFrom<ProtoBlockRange> for BlockRange {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockRange) -> Result<Self, Self::Error> {
        Ok(Self {
            last_block_id: value
                .last_block_id
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockRange.last_block_id".to_owned(),
                ))?
                .try_into()?,
            max_blocks: value
                .max_blocks
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockRange.max_blocks".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl From<&PayloadId> for ProtoPayloadId {
    fn from(value: &PayloadId) -> Self {
        Self {
            pid: Some((&(value.0)).into()),
        }
    }
}

impl TryFrom<ProtoPayloadId> for PayloadId {
    type Error = ProtoError;

    fn try_from(value: ProtoPayloadId) -> Result<Self, Self::Error> {
        Ok(Self(
            value
                .pid
                .ok_or(Self::Error::MissingRequiredField(
                    "ProtoPayloadId.pid".to_owned(),
                ))?
                .try_into()?,
        ))
    }
}

impl<SCT: SignatureCollection> From<&Block<SCT>> for ProtoBlock {
    fn from(value: &Block<SCT>) -> Self {
        Self {
            author: Some((&value.author).into()),
            epoch: Some((&value.epoch).into()),
            round: Some((&value.round).into()),
            execution: Some((&value.execution).into()),
            payload_id: Some((&value.payload_id).into()),
            qc: Some((&value.qc).into()),
            timestamp: value.timestamp,
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoBlock> for Block<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoBlock) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value
                .author
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.author".to_owned(),
                ))?
                .try_into()?,
            value.timestamp,
            value
                .epoch
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.epoch".to_owned(),
                ))?
                .try_into()?,
            value
                .round
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.round".to_owned(),
                ))?
                .try_into()?,
            &value
                .execution
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.execution".to_owned(),
                ))?
                .try_into()?,
            value
                .payload_id
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.payload_id".to_owned(),
                ))?
                .try_into()?,
            &value
                .qc
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.qc".to_owned(),
                ))?
                .try_into()?,
        ))
    }
}

impl<SCT: SignatureCollection> From<&FullBlock<SCT>> for ProtoFullBlock {
    fn from(value: &FullBlock<SCT>) -> Self {
        Self {
            block: Some((&value.block).into()),
            payload: Some((&value.payload).into()),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoFullBlock> for FullBlock<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoFullBlock) -> Result<Self, Self::Error> {
        Ok(Self {
            block: value
                .block
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.block".to_owned(),
                ))?
                .try_into()?,
            payload: value
                .payload
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.payload".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl From<&Gas> for ProtoGas {
    fn from(value: &Gas) -> Self {
        Self { gas: value.0 }
    }
}

impl TryFrom<ProtoGas> for Gas {
    type Error = ProtoError;
    fn try_from(value: ProtoGas) -> Result<Self, Self::Error> {
        Ok(Self(value.gas))
    }
}

impl From<&Bloom> for ProtoBloom {
    fn from(value: &Bloom) -> Self {
        Self {
            bloom: value.0.to_vec().into(),
        }
    }
}

impl TryFrom<ProtoBloom> for Bloom {
    type Error = ProtoError;
    fn try_from(value: ProtoBloom) -> Result<Self, Self::Error> {
        Ok(Self(value.bloom.to_vec().try_into().map_err(
            |e: Vec<_>| Self::Error::WrongHashLen(format!("{}", e.len())),
        )?))
    }
}

impl From<&ExecutionProtocol> for ProtoExecutionProtocol {
    fn from(value: &ExecutionProtocol) -> Self {
        Self {
            delayed_execution_result: {
                let mut buf = BytesMut::new();
                value.delayed_execution_result.encode(&mut buf);
                buf.into()
            },
            seq_num: Some((&(value.seq_num)).into()),
            beneficiary: value.beneficiary.0.to_vec().into(),
            randao_reveal: value.randao_reveal.0.to_vec().into(),
        }
    }
}

impl TryFrom<ProtoExecutionProtocol> for ExecutionProtocol {
    type Error = ProtoError;
    fn try_from(value: ProtoExecutionProtocol) -> Result<Self, Self::Error> {
        Ok(Self {
            delayed_execution_result: Header::decode(&mut value.delayed_execution_result.as_ref())
                .map_err(|_err| {
                    Self::Error::DeserializeError(
                        "ExecutionProtocol.delayed_execution_result".to_owned(),
                    )
                })?,
            seq_num: value
                .seq_num
                .ok_or(Self::Error::MissingRequiredField(
                    "Payload.seq_num".to_owned(),
                ))?
                .try_into()?,
            beneficiary: EthAddress::from_bytes(
                value
                    .beneficiary
                    .to_vec()
                    .try_into()
                    .map_err(|_| Self::Error::WrongHashLen("Payload.beneficiary".to_owned()))?,
            ),
            randao_reveal: RandaoReveal(value.randao_reveal.to_vec()),
        })
    }
}

impl From<&Payload> for ProtoPayload {
    fn from(value: &Payload) -> Self {
        ProtoPayload {
            txns: value.txns.bytes().clone(),
        }
    }
}

impl TryFrom<ProtoPayload> for Payload {
    type Error = ProtoError;
    fn try_from(value: ProtoPayload) -> Result<Self, Self::Error> {
        Ok(Self {
            txns: FullTransactionList::new(value.txns),
        })
    }
}
