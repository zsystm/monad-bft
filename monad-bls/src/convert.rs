use std::marker::PhantomData;

use monad_crypto::certificate_signature::PubKey;
use monad_proto::{
    error::ProtoError,
    proto::signing::{ProtoBlsSignatureCollection, ProtoScaledBlsSignatureCollection},
};

use crate::{
    aggregation_tree::BlsSignatureCollection,
    scaled_aggregation_tree::ScaledBlsSignatureCollection, BlsAggregateSignature,
};

impl<PT: PubKey> From<&BlsSignatureCollection<PT>> for ProtoBlsSignatureCollection {
    fn from(value: &BlsSignatureCollection<PT>) -> Self {
        Self {
            signers: serde_cbor::to_vec(&value.signers)
                .expect("serialization success")
                .into(),
            sig: value.sig.serialize().into(),
        }
    }
}

impl<PT: PubKey> TryFrom<ProtoBlsSignatureCollection> for BlsSignatureCollection<PT> {
    type Error = ProtoError;

    fn try_from(value: ProtoBlsSignatureCollection) -> Result<Self, Self::Error> {
        Ok(Self {
            signers: serde_cbor::from_slice(&value.signers)
                .map_err(|e| ProtoError::DeserializeError(format!("{}", e)))?,
            sig: BlsAggregateSignature::deserialize(&value.sig)
                .map_err(|e| ProtoError::CryptoError(format!("{}", e)))?,
            _phantom: PhantomData,
        })
    }
}

impl<PT: PubKey> From<&ScaledBlsSignatureCollection<PT>> for ProtoScaledBlsSignatureCollection {
    fn from(value: &ScaledBlsSignatureCollection<PT>) -> Self {
        Self {
            signers: serde_cbor::to_vec(&value.signers)
                .expect("serialization success")
                .into(),
            sig: value.sig.serialize().into(),
            seed: value.seed.clone(),
        }
    }
}

impl<PT: PubKey> TryFrom<ProtoScaledBlsSignatureCollection> for ScaledBlsSignatureCollection<PT> {
    type Error = ProtoError;

    fn try_from(value: ProtoScaledBlsSignatureCollection) -> Result<Self, Self::Error> {
        Ok(Self {
            signers: serde_cbor::from_slice(&value.signers)
                .map_err(|e| ProtoError::DeserializeError(format!("{}", e)))?,
            sig: BlsAggregateSignature::deserialize(&value.sig)
                .map_err(|e| ProtoError::CryptoError(format!("{}", e)))?,
            seed: value.seed,
            _phantom: PhantomData,
        })
    }
}
