use monad_consensus_types::quorum_certificate::QuorumCertificate;
use monad_secp::PubKey;

fn main() {
    let qc: QuorumCertificate<monad_bls::BlsSignatureCollection<PubKey>> =
        QuorumCertificate::genesis_qc();
    println!("{}", toml::to_string(&qc).unwrap())
}
