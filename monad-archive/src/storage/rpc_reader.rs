use std::str::FromStr;

use alloy_rlp::{Decodable, Encodable};
use alloy_rpc_client::{ClientBuilder, ReqwestClient};
use reth_primitives::Bytes;
use reth_rpc_types::{trace::geth::CallFrame as RethCallFrame, TransactionReceipt};
use serde::Deserialize;

use crate::LatestKind;
use eyre::{bail, Result};
use reth_primitives::{
    ruint::Uint, Address, Block, BlockHash, Receipt, ReceiptWithBloom, TxType, U256, U64, U8,
};

use super::BlockDataReader;

#[derive(Clone)]
pub struct RpcReader {
    client: ReqwestClient,
}

impl RpcReader {
    pub fn new(s: &str) -> Self {
        Self {
            client: ClientBuilder::default()
                .http(url::Url::from_str(s).expect("Url failed to parse")),
        }
    }
}

impl BlockDataReader for RpcReader {
    fn get_bucket(&self) -> &str {
        "rpc-reader"
    }

    async fn get_latest(&self, _latest_kind: LatestKind) -> Result<u64> {
        self.client
            .request::<&[u8], u64>("eth_blockNumber", &[])
            .await
            .map_err(Into::into)
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.client
            .request::<_, Block>("eth_getBlockByNumber", &[block_num])
            .await
            .map_err(Into::into)
    }

    async fn get_block_by_hash(&self, block_hash: BlockHash) -> Result<Block> {
        self.client
            .request::<_, Block>("eth_getBlockByHash", &[block_hash])
            .await
            .map_err(Into::into)
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptWithBloom>> {
        let rxs = self
            .client
            .request::<_, Vec<TransactionReceipt>>("eth_getBlockReceipts", &[block_number])
            .await?;

        let convert_tx_type = |ident: u8| {
            Ok(match ident {
                0 => TxType::Legacy,
                1 => TxType::EIP2930,
                2 => TxType::EIP1559,
                3 => TxType::EIP4844,
                _ => bail!("unexpected transaction type {}", ident),
            })
        };

        let convert_receipt = |rx: TransactionReceipt| -> Result<ReceiptWithBloom> {
            Ok(ReceiptWithBloom {
                bloom: rx.logs_bloom,
                receipt: Receipt {
                    tx_type: convert_tx_type(rx.transaction_type.to::<u8>())?,
                    success: rx.status_code == Some(Uint::from(1)),
                    cumulative_gas_used: rx.cumulative_gas_used.to::<u64>(),
                    logs: rx
                        .logs
                        .into_iter()
                        .map(|log| reth_primitives::Log {
                            address: log.address,
                            topics: log.topics,
                            data: log.data,
                        })
                        .collect(),
                },
            })
        };

        rxs.into_iter().map(convert_receipt).collect()
    }

    async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>> {
        let frames = self
            .client
            .request::<_, Vec<RethCallFrame>>("debug_traceBlockByNumber", &[block_number])
            .await?;

        frames
            .into_iter()
            .map(|frame| -> Result<Vec<_>> {
                let mut flattened = Vec::new();
                flatten_frame(frame, 0, &mut flattened)?;

                let mut buf = Vec::new();
                // TODO: verify this nested vec is correct
                vec![flattened].encode(&mut buf);

                Ok(buf)
            })
            .collect()
    }
}

fn convert(frame: &RethCallFrame, depth: u64) -> Result<CallFrame> {
    Ok(CallFrame {
        typ: serde_json::from_str(&frame.typ)?,
        flags: U64::from(if &frame.typ == "STATIC_CALL" { 1 } else { 0 }),
        from: frame.from,
        to: frame.to,
        value: frame.value.unwrap_or_default(),
        gas: frame.gas.to::<U64>(),
        gas_used: frame.gas_used.to::<U64>(),
        input: frame.input.clone(),
        output: frame.output.clone().unwrap_or_default(),
        status: U8::from(if frame.error.is_some() { 0 } else { 1 }),
        depth: U64::from(depth),
    })
}

fn flatten_frame(root: RethCallFrame, depth: u64, flattened: &mut Vec<CallFrame>) -> Result<()> {
    let converted = convert(&root, depth)?;
    flattened.push(converted);
    for child in root.calls {
        flatten_frame(child, depth + 1, flattened)?;
    }
    Ok(())
}

/* Based off monad-rpc trace_handlers.rs */

#[derive(Deserialize, Debug, Clone, schemars::JsonSchema, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
enum CallKind {
    Call,
    DelegateCall,
    CallCode,
    Create,
    Create2,
    SelfDestruct,
    StaticCall,
}

#[derive(Clone, Debug, PartialEq)]
struct CallFrame {
    typ: CallKind,
    flags: U64,
    from: Address,
    to: Option<Address>,
    value: U256,
    gas: U64,
    gas_used: U64,
    input: Bytes,
    output: Bytes,
    status: U8,
    depth: U64,
}

impl Decodable for CallFrame {
    fn decode(rlp_buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let typ: U8 = U8::decode(rlp_buf)?;
        let flags: U64 = U64::decode(rlp_buf)?;
        let from: Address = Address::decode(rlp_buf)?;

        // Decode the `to` field, handling the case where it's `None`.
        let to: Option<Address> = {
            let first_byte = rlp_buf.first().ok_or(alloy_rlp::Error::InputTooShort)?;
            if *first_byte == 0x80 {
                // If the first byte is 0x80, it represents an empty value (None for the Address).
                *rlp_buf = &rlp_buf[1..]; // Advance the buffer
                None
            } else {
                // Otherwise, decode it as a normal Address.
                Some(Address::decode(rlp_buf)?)
            }
        };

        let value: U256 = U256::decode(rlp_buf)?;
        let gas: U64 = U64::decode(rlp_buf)?;
        let gas_used: U64 = U64::decode(rlp_buf)?;
        let input = Bytes::decode(rlp_buf)?;
        let output = Bytes::decode(rlp_buf)?;
        let status: U8 = U8::decode(rlp_buf)?;
        let depth: U64 = U64::decode(rlp_buf)?;

        let typ = match typ.to::<u8>() {
            0 if flags == U64::from(1) => CallKind::StaticCall,
            0 => CallKind::Call,
            1 => CallKind::DelegateCall,
            2 => CallKind::CallCode,
            3 => CallKind::Create,
            4 => CallKind::Create2,
            5 => CallKind::SelfDestruct,
            _ => return Err(alloy_rlp::Error::Custom("Invalid call kind")),
        };

        Ok(Self {
            typ,
            flags,
            from,
            to,
            value,
            gas,
            gas_used,
            input,
            output,
            status,
            depth,
        })
    }
}

impl Encodable for CallFrame {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        // 1. Convert CallKind to u8 and encode
        let typ_value: u8 = match self.typ {
            CallKind::Call => 0,
            CallKind::StaticCall => 0, // Differentiated by flags
            CallKind::DelegateCall => 1,
            CallKind::CallCode => 2,
            CallKind::Create => 3,
            CallKind::Create2 => 4,
            CallKind::SelfDestruct => 5,
        };
        U8::from(typ_value).encode(out);

        // 2. Encode flags (note: flags=1 indicates StaticCall when typ=0)
        self.flags.encode(out);

        // 3. Encode from address
        self.from.encode(out);

        // 4. Encode optional to address
        match &self.to {
            None => {
                // Encode empty value (0x80) for None
                out.put_u8(0x80);
            }
            Some(addr) => {
                addr.encode(out);
            }
        }

        // 5. Encode remaining fields in order
        self.value.encode(out);
        self.gas.encode(out);
        self.gas_used.encode(out);
        self.input.encode(out);
        self.output.encode(out);
        self.status.encode(out);
        self.depth.encode(out);
    }
}
