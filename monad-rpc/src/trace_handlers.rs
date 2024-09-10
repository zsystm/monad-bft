use crate::{
    eth_json_types::{
        deserialize_fixed_data, EthAddress, EthHash, FixedData, MonadU256, Quantity,
        UnformattedData,
    },
    jsonrpc::{JsonRpcError, JsonRpcResult},
    triedb,
};
use alloy_primitives::{
    aliases::{B256, U256, U64, U8},
    Address, Bytes,
};
use alloy_rlp::Decodable;
use monad_blockdb::EthTxKey;
use monad_blockdb_utils::BlockDbEnv;
use monad_rpc_docs::rpc;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, rc::Rc};
use tracing::{debug, error, trace};
use triedb::{TriedbEnv, TriedbResult};

#[derive(Clone, Debug)]
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
            let first_byte = rlp_buf.get(0).ok_or(alloy_rlp::Error::InputTooShort)?;
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

#[derive(Deserialize, Debug, Default, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TracerObject {
    #[serde(default)]
    tracer: Tracer,
    only_top_call: Option<bool>,
}

#[derive(Deserialize, Debug, Default, schemars::JsonSchema)]
pub enum Tracer {
    #[default]
    #[serde(rename = "callTracer")]
    CallTracer,
    #[serde(rename = "prestateTracer")]
    PreStateTracer, // TODO: implement prestate tracer
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadDebugTraceTransactionParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
    #[serde(default)]
    tracer: TracerObject,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MonadCallFrame {
    #[serde(rename = "type")]
    typ: CallKind,
    from: EthAddress,
    to: Option<EthAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<MonadU256>,
    gas: Quantity,
    gas_used: Quantity,
    input: UnformattedData,
    #[serde(skip_serializing_if = "UnformattedData::is_empty")]
    output: UnformattedData,
    #[serde(skip)]
    depth: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revert_reason: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    calls: Vec<std::rc::Rc<std::cell::RefCell<MonadCallFrame>>>,
}

impl From<CallFrame> for MonadCallFrame {
    fn from(value: CallFrame) -> Self {
        // the “value” argument is not included for STATICALL
        let frame_value = if matches!(value.typ, CallKind::StaticCall) {
            None
        } else {
            Some(MonadU256(value.value))
        };

        Self {
            typ: value.typ.into(),
            from: value.from.into(),
            to: value.to.map(Into::into),
            value: frame_value,
            gas: Quantity(u64::from_le_bytes(value.gas.to_le_bytes())),
            gas_used: Quantity(u64::from_le_bytes(value.gas_used.to_le_bytes())),
            input: value.input.into(),
            output: value.output.into(),
            depth: value.depth.to::<usize>(),
            error: None, //TODO
            revert_reason: None,
            calls: Vec::new(),
        }
    }
}

#[derive(Serialize, Debug, Clone, schemars::JsonSchema)]
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

#[rpc(method = "debug_traceTransaction")]
#[allow(non_snake_case)]
pub async fn monad_debugTraceTransaction(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    params: MonadDebugTraceTransactionParams,
) -> JsonRpcResult<Option<MonadCallFrame>> {
    trace!("monad_eth_debugTraceTransaction: {params:?}");

    let tx_hash = params.tx_hash.0;
    let key = EthTxKey(B256::new(tx_hash));
    let Some(txn_value) = blockdb_env.get_txn(key).await else {
        debug!("transaction not found");
        return Ok(None);
    };
    let txn_index = txn_value.transaction_index;
    let block_key = txn_value.block_hash;
    let block = blockdb_env
        .get_block_by_hash(block_key)
        .await
        .expect("txn was found so its block should exist");
    let block_num = block.block.number;

    match triedb_env.get_call_frame(txn_index, block_num).await {
        TriedbResult::Null => return Ok(None),
        TriedbResult::CallFrame(rlp_call_frames) => {
            let mut call_frames = Vec::<Vec<CallFrame>>::decode(&mut rlp_call_frames.as_slice())
                .map_err(|e| JsonRpcError::custom(format!("Rlp Decode error: {e}")))?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

            match params.tracer.tracer {
                Tracer::CallTracer => {
                    if let Some(true) = params.tracer.only_top_call {
                        if call_frames.is_empty() {
                            Ok(None)
                        } else {
                            let mut frame = MonadCallFrame::from(call_frames.remove(0));
                            include_code_output(&mut frame, triedb_env, block_num).await?;
                            Ok(Some(frame))
                        }
                    } else {
                        Ok(build_call_tree(call_frames, triedb_env, block_num)
                            .await?
                            .and_then(|rc| {
                                Rc::try_unwrap(rc).ok().map(|refcell| refcell.into_inner())
                            }))
                    }
                }
                _ => Err(JsonRpcError::method_not_supported()),
            }
        }
        _ => return Err(JsonRpcError::custom("Not matched".to_string())),
    }
}

async fn include_code_output(
    frame: &mut MonadCallFrame,
    triedb_env: &TriedbEnv,
    block_num: u64,
) -> JsonRpcResult<()> {
    if matches!(frame.typ, CallKind::Create) || matches!(frame.typ, CallKind::Create2) {
        let Some(contract_addr) = &frame.to else {
            error!("expected contract address in call frame");
            return Err(JsonRpcError::internal_error());
        };

        let TriedbResult::Account(_, _, code_hash) = triedb_env
            .get_account(
                contract_addr.0,
                monad_blockdb_utils::BlockTags::Number(block_num),
            )
            .await
        else {
            error!("expected account {} in triedb", contract_addr);
            return Err(JsonRpcError::internal_error());
        };

        let TriedbResult::Code(code) = triedb_env
            .get_code(code_hash, monad_blockdb_utils::BlockTags::Number(block_num))
            .await
        else {
            error!("expected code {} in triedb", FixedData::<32>(code_hash));
            return Err(JsonRpcError::internal_error());
        };

        frame.output = UnformattedData(code);
    }

    Ok(())
}

async fn build_call_tree(
    nodes: Vec<CallFrame>,
    triedb_env: &TriedbEnv,
    block_num: u64,
) -> JsonRpcResult<Option<std::rc::Rc<std::cell::RefCell<MonadCallFrame>>>> {
    if nodes.is_empty() {
        return Ok(None);
    }

    let root = Rc::new(RefCell::new(MonadCallFrame::from(nodes[0].clone())));
    include_code_output(&mut root.borrow_mut(), triedb_env, block_num).await?;
    let mut stack = vec![Rc::clone(&root)];

    for value in nodes.into_iter().skip(1) {
        let depth = value.depth.to::<usize>();
        let new_node = Rc::new(RefCell::new(MonadCallFrame::from(value)));
        include_code_output(&mut new_node.borrow_mut(), triedb_env, block_num).await?;
        while let Some(last) = stack.last() {
            if last.borrow().depth < depth {
                last.borrow_mut().calls.push(Rc::clone(&new_node));
                break;
            }
            stack.pop();
        }

        stack.push(new_node);
    }

    Ok(Some(root))
}
