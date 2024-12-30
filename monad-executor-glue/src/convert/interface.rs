use bytes::{Bytes, BytesMut};
use monad_consensus_types::{block::ExecutionProtocol, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_proto::{error::ProtoError, proto::event::ProtoMonadEvent};
use prost::Message;

use crate::MonadEvent;

pub fn serialize_event<ST, SCT, EPT>(event: &MonadEvent<ST, SCT, EPT>) -> Bytes
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    let proto_event: ProtoMonadEvent = event.into();
    let mut buf = BytesMut::new();
    proto_event
        .encode(&mut buf)
        .expect("event serialization shouldn't fail");
    buf.into()
}

pub fn deserialize_event<ST, SCT, EPT>(data: &[u8]) -> Result<MonadEvent<ST, SCT, EPT>, ProtoError>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    let event = ProtoMonadEvent::decode(data)?;
    event.try_into()
}
