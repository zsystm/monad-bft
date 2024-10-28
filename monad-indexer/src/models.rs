use std::time::{Duration, SystemTime};

use alloy_rlp::Decodable;
use diesel::{
    deserialize::{self, FromSql, FromSqlRow},
    expression::AsExpression,
    pg::{Pg, PgValue},
    prelude::*,
    serialize::{self, Output, ToSql},
    sql_types::Bytea,
};
use monad_consensus_types::{
    block::{Block, BlockKind, BlockType},
    payload::{Payload, PayloadId, TransactionPayload},
    signature_collection::SignatureCollection,
    state_root_hash::StateRootHash,
    validator_data::ValidatorSetDataWithEpoch,
};
use monad_crypto::{certificate_signature::PubKey, hasher::Hash};
use monad_eth_tx::EthSignedTransaction;
use monad_node_config::NodeBootstrapPeerConfig;
use monad_types::BlockId;
use serde::{Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq, AsExpression, FromSqlRow)]
#[diesel(sql_type = Bytea)]
pub struct Bytes32(pub [u8; 32]);

impl Serialize for Bytes32 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let hex = hex::encode(self.0);
        serializer.serialize_str(&format!("0x{hex}"))
    }
}

impl From<&BlockId> for Bytes32 {
    fn from(block_id: &BlockId) -> Self {
        block_id.0.into()
    }
}

impl From<BlockId> for Bytes32 {
    fn from(block_id: BlockId) -> Self {
        block_id.0.into()
    }
}

impl From<&StateRootHash> for Bytes32 {
    fn from(state_root: &StateRootHash) -> Self {
        state_root.0.into()
    }
}

impl From<StateRootHash> for Bytes32 {
    fn from(state_root: StateRootHash) -> Self {
        state_root.0.into()
    }
}

impl From<&PayloadId> for Bytes32 {
    fn from(payload_id: &PayloadId) -> Self {
        payload_id.0.into()
    }
}

impl From<PayloadId> for Bytes32 {
    fn from(payload_id: PayloadId) -> Self {
        payload_id.0.into()
    }
}

impl From<&Hash> for Bytes32 {
    fn from(hash: &Hash) -> Self {
        Self(hash.0)
    }
}

impl From<Hash> for Bytes32 {
    fn from(hash: Hash) -> Self {
        Self(hash.0)
    }
}

impl FromSql<Bytea, Pg> for Bytes32 {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let bytes: Vec<u8> = <Vec<u8> as FromSql<Bytea, Pg>>::from_sql(bytes)?;
        if bytes.len() == 32 {
            let mut data = [0u8; 32];
            data.copy_from_slice(&bytes);
            Ok(Self(data))
        } else {
            Err("Invalid data for Bytes32: expected 32 bytes".into())
        }
    }
}

impl ToSql<Bytea, Pg> for Bytes32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        <[u8; 32] as ToSql<Bytea, Pg>>::to_sql(&self.0, out)
    }
}

#[derive(Insertable, Queryable, Selectable, Serialize)]
#[diesel(table_name = crate::schema::block_header)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BlockHeader {
    block_id: Bytes32,
    #[serde(serialize_with = "serialize_timestamp")]
    timestamp: SystemTime,
    #[serde(serialize_with = "serialize_hex")]
    author: Vec<u8>,
    epoch: i64,
    round: i64,
    state_root: Bytes32,
    seq_num: i64,
    #[serde(serialize_with = "serialize_hex")]
    beneficiary: Vec<u8>,
    #[serde(serialize_with = "serialize_hex")]
    randao_reveal: Vec<u8>,
    payload_id: Option<Bytes32>,
    parent_block_id: Bytes32,
}

fn serialize_hex<S: Serializer>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
    let hex = hex::encode(bytes);
    serializer.serialize_str(&format!("0x{hex}"))
}

fn serialize_timestamp<S: Serializer>(
    timestamp: &SystemTime,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let millis_since_epoch: i64 = timestamp
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    serializer.serialize_i64(millis_since_epoch)
}

impl<SCT: SignatureCollection> From<&Block<SCT>> for BlockHeader {
    fn from(block_header: &Block<SCT>) -> Self {
        Self {
            block_id: block_header.get_id().into(),
            timestamp: std::time::UNIX_EPOCH + Duration::from_millis(block_header.get_timestamp()),
            author: block_header.get_author().pubkey().bytes(),
            epoch: block_header
                .get_epoch()
                .0
                .try_into()
                .expect("epoch doesn't fit in i64"),
            round: block_header
                .get_round()
                .0
                .try_into()
                .expect("round doesn't fit in i64"),
            state_root: block_header.get_state_root().into(),
            seq_num: block_header
                .get_seq_num()
                .0
                .try_into()
                .expect("seq_num doesn't fit in i64"),
            beneficiary: block_header.execution.beneficiary.0.as_slice().to_vec(),
            randao_reveal: block_header.execution.randao_reveal.0.to_vec(),
            payload_id: match block_header.block_kind {
                BlockKind::Executable => Some(block_header.payload_id.into()),
                BlockKind::Null => None,
            },
            parent_block_id: block_header.get_parent_id().into(),
        }
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = crate::schema::block_payload)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BlockPayload {
    payload_id: Bytes32,
    num_tx: i32,
    payload_size: i32,
}

impl From<&Payload> for BlockPayload {
    fn from(block_payload: &Payload) -> Self {
        let TransactionPayload::List(payload) = &block_payload.txns else {
            unreachable!("tried to serialize null block")
        };

        let transactions =
            Vec::<EthSignedTransaction>::decode(&mut payload.bytes().as_ref()).unwrap();

        Self {
            payload_id: block_payload.get_id().into(),
            num_tx: transactions.len() as i32,
            payload_size: payload.bytes().len() as i32,
        }
    }
}

#[derive(Insertable, Queryable, Selectable, Serialize)]
#[diesel(table_name = crate::schema::key)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Key {
    #[serde(serialize_with = "serialize_hex")]
    node_id: Vec<u8>,
    dns: String,
}

impl From<&NodeBootstrapPeerConfig> for Key {
    fn from(config: &NodeBootstrapPeerConfig) -> Self {
        Self {
            node_id: config.secp256k1_pubkey.bytes(),
            dns: config.address.clone(),
        }
    }
}

#[derive(PartialEq, Eq, Insertable, Queryable, Selectable, Serialize)]
#[diesel(table_name = crate::schema::validator_set)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ValidatorSetMember {
    pub epoch: i64,
    pub round: Option<i64>,
    #[serde(serialize_with = "serialize_hex")]
    pub node_id: Vec<u8>,
    #[serde(serialize_with = "serialize_hex")]
    pub certificate_key: Vec<u8>,
    pub stake: i64,
}

pub fn validator_set_transform<SCT: SignatureCollection>(
    validator_set: &ValidatorSetDataWithEpoch<SCT>,
) -> Vec<ValidatorSetMember> {
    validator_set
        .validators
        .0
        .iter()
        .map(|validator| ValidatorSetMember {
            epoch: validator_set
                .epoch
                .0
                .try_into()
                .expect("epoch doesn't fit in i64"),
            round: validator_set
                .round
                .map(|round| round.0.try_into().expect("round doesn't fit in i64")),
            node_id: validator.node_id.pubkey().bytes(),
            certificate_key: validator.cert_pubkey.bytes(),
            stake: validator.stake.0,
        })
        .collect()
}
