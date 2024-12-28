use bytes::{Bytes, BytesMut};
use monad_consensus_types::{block::ExecutionProtocol, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_proto::{error::ProtoError, proto::message::ProtoMonadMessage};
use prost::Message;

use crate::{MonadMessage, VerifiedMonadMessage};

pub fn serialize_verified_monad_message<
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
>(
    msg: &VerifiedMonadMessage<ST, SCT, EPT>,
) -> Bytes {
    let proto_msg: ProtoMonadMessage = {
        let mut _convert_span = tracing::trace_span!("convert_span").entered();
        msg.into()
    };
    let mut _encode_span = tracing::trace_span!("encode_span").entered();
    // FIXME this copy can be avoided
    let mut buf = BytesMut::new();
    proto_msg
        .encode(&mut buf)
        .expect("message serialization shouldn't fail");
    buf.into()
}

pub fn deserialize_monad_message<ST, SCT, EPT>(
    data: Bytes,
) -> Result<MonadMessage<ST, SCT, EPT>, ProtoError>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    let message_len = data.len();
    let msg = {
        let mut _decode_span = tracing::trace_span!("decode_span", ?message_len).entered();
        ProtoMonadMessage::decode(data)?
    };
    let mut _convert_span = tracing::trace_span!("convert_span", ?message_len).entered();
    msg.try_into()
}
