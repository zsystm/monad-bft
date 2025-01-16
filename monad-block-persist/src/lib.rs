use std::{
    ffi::OsStr,
    fs::File,
    io::{Read, Write},
    marker::PhantomData,
    path::PathBuf,
};

use monad_consensus_types::{
    block::ConsensusBlockHeader,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_proto::proto::block::{ProtoBlockBody, ProtoBlockHeader};
use monad_types::{BlockId, ExecutionProtocol};
use prost::Message;

pub trait BlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn write_bft_header(&self, block: &ConsensusBlockHeader<ST, SCT, EPT>) -> std::io::Result<()>;
    fn write_bft_body(&self, payload: &ConsensusBlockBody<EPT>) -> std::io::Result<()>;

    fn read_bft_header(
        &self,
        block_id: &BlockId,
    ) -> std::io::Result<ConsensusBlockHeader<ST, SCT, EPT>>;
    fn read_bft_body(
        &self,
        payload_id: &ConsensusBlockBodyId,
    ) -> std::io::Result<ConsensusBlockBody<EPT>>;
}

#[derive(Clone)]
pub struct FileBlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    block_dir_path: PathBuf,
    payload_dir_path: PathBuf,

    _pd: PhantomData<(ST, SCT, EPT)>,
}

pub fn is_valid_bft_block_header_path(filepath: &OsStr) -> bool {
    filepath
        .to_str()
        .is_some_and(|filename| filename.ends_with(BLOCKDB_HEADER_EXTENSION))
}

impl<ST, SCT, EPT> FileBlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(block_dir_path: PathBuf, payload_dir_path: PathBuf) -> Self {
        Self {
            block_dir_path,
            payload_dir_path,

            _pd: PhantomData,
        }
    }

    pub fn read_bft_header_from_filepath(
        &self,
        filepath: &OsStr,
    ) -> std::io::Result<ConsensusBlockHeader<ST, SCT, EPT>> {
        assert!(is_valid_bft_block_header_path(filepath));
        let mut file = File::open(filepath)?;

        let size = file.metadata()?.len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf)?;

        // TODO maybe expect is too strict
        let proto_block =
            ProtoBlockHeader::decode(buf.as_slice()).expect("local protoblock decode");
        let block: ConsensusBlockHeader<ST, SCT, EPT> = proto_block
            .try_into()
            .expect("proto_block to block should not be invalid");

        Ok(block)
    }
}

pub const BLOCKDB_HEADER_EXTENSION: &str = ".header";
pub const BLOCKDB_PAYLOAD_EXTENSION: &str = ".body";

impl<ST, SCT, EPT> BlockPersist<ST, SCT, EPT> for FileBlockPersist<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn write_bft_header(&self, block: &ConsensusBlockHeader<ST, SCT, EPT>) -> std::io::Result<()> {
        let proto_block: ProtoBlockHeader = block.into();
        let encoded = proto_block.encode_to_vec();

        let filename = block.get_id().0.to_string();
        let mut file_path = PathBuf::from(&self.block_dir_path);
        file_path.push(format!("{}{}", filename, BLOCKDB_HEADER_EXTENSION));

        // FIXME if already exists, don't recreate + update timestamp meta
        let mut f = File::create(file_path).unwrap();
        f.write_all(&encoded).unwrap();

        Ok(())
    }

    fn write_bft_body(&self, payload: &ConsensusBlockBody<EPT>) -> std::io::Result<()> {
        let proto_payload: ProtoBlockBody = payload.into();
        let encoded = proto_payload.encode_to_vec();

        let filename = payload.get_id().0.to_string();
        let mut file_path = PathBuf::from(&self.payload_dir_path);
        file_path.push(format!("{}{}", filename, BLOCKDB_PAYLOAD_EXTENSION));

        // FIXME if already exists, don't recreate + update timestamp meta
        let mut f = File::create(file_path).unwrap();
        f.write_all(&encoded).unwrap();

        Ok(())
    }

    fn read_bft_header(
        &self,
        block_id: &BlockId,
    ) -> std::io::Result<ConsensusBlockHeader<ST, SCT, EPT>> {
        let filename = block_id.0.to_string();
        let mut file_path = PathBuf::from(&self.block_dir_path);
        file_path.push(format!("{}{}", filename, BLOCKDB_HEADER_EXTENSION));
        self.read_bft_header_from_filepath(file_path.as_os_str())
    }

    fn read_bft_body(
        &self,
        payload_id: &ConsensusBlockBodyId,
    ) -> std::io::Result<ConsensusBlockBody<EPT>> {
        let filename = payload_id.0.to_string();
        let mut file_path = PathBuf::from(&self.payload_dir_path);
        file_path.push(format!("{}{}", filename, BLOCKDB_PAYLOAD_EXTENSION));
        let mut file = File::open(file_path)?;
        let size = file.metadata()?.len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf)?;

        // TODO maybe expect is too strict
        let proto_body =
            ProtoBlockBody::decode(buf.as_slice()).expect("local protoblockbody decode");
        let body: ConsensusBlockBody<EPT> = proto_body
            .try_into()
            .expect("proto_body to body should not be invalid");

        Ok(body)
    }
}
