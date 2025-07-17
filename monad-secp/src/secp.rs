use alloy_rlp::{Decodable, Encodable};
use k256::{
    elliptic_curve::hash2curve::{ExpandMsgXmd, GroupDigest},
    Secp256k1 as K256Secp256k1,
};
use monad_crypto::hasher::{Hasher, HasherType};
use secp256k1::Secp256k1;
use sha2::Sha256;
use zeroize::Zeroize;

/// secp256k1 public key
#[derive(Copy, Clone, PartialOrd, Ord)]
pub struct PubKey(secp256k1::PublicKey);
/// secp256k1 keypair
pub struct KeyPair(secp256k1::KeyPair);
/// secp256k1 ecdsa recoverable signature
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SecpSignature(secp256k1::ecdsa::RecoverableSignature);

/// wrapped secp256k1 library errors
#[derive(Debug, Clone)]
pub struct Error(secp256k1::Error);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for Error {}

impl std::fmt::Debug for PubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let ser = self.bytes_compressed();
        for byte in ser {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl std::cmp::PartialEq for PubKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_fast_unstable(&other.0)
    }
}

impl std::cmp::Eq for PubKey {}

impl std::hash::Hash for PubKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let slice = unsafe { std::mem::transmute::<Self, [u8; 64]>(*self) };
        slice.hash(state)
    }
}

fn msg_hash(msg: &[u8]) -> secp256k1::Message {
    let mut hasher = HasherType::new();
    hasher.update(msg);
    let hash = hasher.hash();

    secp256k1::Message::from_slice(&hash.0).expect("32 bytes")
}

impl KeyPair {
    /// Create a keypair from a secret key slice. The secret is zero-ized after
    /// use. The secret must be 32 byytes.
    pub fn from_bytes(secret: &mut [u8]) -> Result<Self, Error> {
        let keypair = secp256k1::KeyPair::from_seckey_slice(secp256k1::SECP256K1, secret)
            .map(Self)
            .map_err(Error);
        secret.zeroize();
        keypair
    }

    pub fn from_ikm(ikm: &[u8]) -> Result<Self, Error> {
        let dst = b"monad-ecdsa-keygen";
        let scalar =
            <K256Secp256k1 as GroupDigest>::hash_to_scalar::<ExpandMsgXmd<Sha256>>(&[ikm], &[dst])
                .map_err(|_| Error(secp256k1::Error::InvalidSecretKey))?;
        let mut scalar_bytes = scalar.to_bytes();
        let result = Self::from_bytes(scalar_bytes.as_mut_slice());
        scalar_bytes.zeroize();
        result
    }

    /// Create a SecpSignature over Hash(msg)
    pub fn sign(&self, msg: &[u8]) -> SecpSignature {
        SecpSignature(Secp256k1::sign_ecdsa_recoverable(
            secp256k1::SECP256K1,
            &msg_hash(msg),
            &self.0.secret_key(),
        ))
    }

    /// Get the pubkey
    pub fn pubkey(&self) -> PubKey {
        PubKey(self.0.public_key())
    }
}

impl PubKey {
    /// Deserialize public key from bytes
    /// Can be compressed OR uncompressed pubkey
    pub fn from_slice(pubkey: &[u8]) -> Result<Self, Error> {
        secp256k1::PublicKey::from_slice(pubkey)
            .map(Self)
            .map_err(Error)
    }

    /// Serialize public key
    pub fn bytes(&self) -> [u8; 65] {
        self.0.serialize_uncompressed()
    }

    pub fn bytes_compressed(&self) -> [u8; 33] {
        self.0.serialize()
    }

    /// Verify that the message is correctly signed
    pub fn verify(&self, msg: &[u8], signature: &SecpSignature) -> Result<(), Error> {
        Secp256k1::verify_ecdsa(
            secp256k1::SECP256K1,
            &msg_hash(msg),
            &signature.0.to_standard(),
            &self.0,
        )
        .map_err(Error)
    }
}

impl SecpSignature {
    /// Recover the pubkey from signature given the message
    pub fn recover_pubkey(&self, msg: &[u8]) -> Result<PubKey, Error> {
        Secp256k1::recover_ecdsa(secp256k1::SECP256K1, &msg_hash(msg), &self.0)
            .map(PubKey)
            .map_err(Error)
    }

    /// Serialize the signature. The signature itself is 64 bytes. An extra byte
    /// is used to store the RecoveryId to recover the pubkey
    pub fn serialize(&self) -> [u8; secp256k1::constants::COMPACT_SIGNATURE_SIZE + 1] {
        // recid is 0..3, fit in a single byte (see secp256k1 https://docs.rs/secp256k1/0.27.0/src/secp256k1/ecdsa/recovery.rs.html#39)
        let (recid, sig) = self.0.serialize_compact();
        assert!((0..=3).contains(&recid.to_i32()));
        let mut sig_vec = sig.to_vec();
        sig_vec.push(recid.to_i32() as u8);
        sig_vec.try_into().unwrap()
    }

    /// Deserialize the signature
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        if data.len() != secp256k1::constants::COMPACT_SIGNATURE_SIZE + 1 {
            return Err(Error(secp256k1::Error::InvalidSignature));
        }
        let sig_data = &data[..secp256k1::constants::COMPACT_SIGNATURE_SIZE];
        let recid = secp256k1::ecdsa::RecoveryId::from_i32(
            data[secp256k1::constants::COMPACT_SIGNATURE_SIZE] as i32,
        )
        .map_err(Error)?;
        Ok(SecpSignature(
            secp256k1::ecdsa::RecoverableSignature::from_compact(sig_data, recid).map_err(Error)?,
        ))
    }
}

impl Encodable for SecpSignature {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        self.serialize().encode(out);
    }
}

impl Decodable for SecpSignature {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let raw_bytes = <[u8; 65]>::decode(buf)?;

        match SecpSignature::deserialize(&raw_bytes) {
            Ok(sig) => Ok(sig),
            Err(_) => Err(alloy_rlp::Error::Custom("invalid secp signature")),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tiny_keccak::Hasher;

    use super::{KeyPair, PubKey, SecpSignature};

    #[test]
    fn test_pubkey_roundtrip() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let pubkey_bytes = keypair.pubkey().bytes();
        assert_eq!(
            pubkey_bytes,
            PubKey::from_slice(&pubkey_bytes).unwrap().bytes()
        );
        let pubkey_compressed_bytes = keypair.pubkey().bytes_compressed();
        assert_eq!(
            pubkey_bytes,
            PubKey::from_slice(&pubkey_compressed_bytes)
                .unwrap()
                .bytes()
        );
    }

    #[test]
    fn test_eth_address() {
        let mut privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let mut hasher = tiny_keccak::Keccak::v256();
        // pubkey() returns 65 bytes, ignore first one
        hasher.update(&keypair.pubkey().bytes()[1..]);
        let mut output = [0u8; 32];
        hasher.finalize(&mut output);

        let generated_eth_address = output[12..].to_vec();

        let expected_eth_address = hex::decode("ff7F1B7DbaaF35259dDa7cb42564CB7507C1D88d").unwrap();
        assert_eq!(generated_eth_address, expected_eth_address);
    }

    #[test]
    fn test_verify() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign(msg);

        assert!(keypair.pubkey().verify(msg, &signature).is_ok());
        assert!(keypair.pubkey().verify(b"bye world", &signature).is_err());
    }

    #[test]
    fn test_recovery() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign(msg);

        let recovered_key = signature.recover_pubkey(msg).unwrap();

        assert!(keypair.pubkey().bytes() == recovered_key.bytes());
    }

    #[test]
    fn test_signature_serde() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign(msg);

        let ser = signature.serialize();
        let deser = SecpSignature::deserialize(&ser);
        assert_eq!(signature, deser.unwrap());
    }

    #[test]
    fn test_signature_rlp() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign(msg);

        let rlp = alloy_rlp::encode(signature);
        let x: SecpSignature = alloy_rlp::decode_exact(rlp).unwrap();

        assert_eq!(signature, x);
    }

    #[test]
    fn test_from_ikm() {
        let ikm = b"test input keying material 32byt";
        let keypair = KeyPair::from_ikm(ikm).unwrap();

        let msg = b"test message";
        let signature = keypair.sign(msg);
        assert!(keypair.pubkey().verify(msg, &signature).is_ok());

        let keypair2 = KeyPair::from_ikm(ikm).unwrap();
        assert_eq!(keypair.pubkey().bytes(), keypair2.pubkey().bytes());
    }

    #[test]
    fn test_secp256k1_out_of_range_key_fails() {
        let mut zero_key = [0u8; 32];
        let result = KeyPair::from_bytes(&mut zero_key);
        assert!(result.is_err());

        let mut curve_order =
            hex::decode("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141")
                .unwrap();
        let result = KeyPair::from_bytes(&mut curve_order);
        assert!(result.is_err());

        let mut above_order =
            hex::decode("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364142")
                .unwrap();
        let result = KeyPair::from_bytes(&mut above_order);
        assert!(result.is_err());

        let mut max_value = [0xFFu8; 32];
        let result = KeyPair::from_bytes(&mut max_value);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_ikm_deterministic_profiles() {
        #[derive(Debug)]
        struct IkmProfile {
            ikm_hex: String,
            private_key_hex: String,
            pubkey_hex: String,
        }

        let test_ikm_hexes = [
            "0000000000000000000000000000000000000000000000000000000000000001",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "c0c1c2c3c4c5c6c7c8c9cacbcccdcecfc0c1c2c3c4c5c6c7c8c9cacbcccdcecf",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "2222222222222222222222222222222222222222222222222222222222222222",
            "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
        ];

        let profiles: Vec<_> = test_ikm_hexes
            .into_iter()
            .map(|ikm_hex| {
                let ikm = hex::decode(ikm_hex).unwrap();
                let keypair = KeyPair::from_ikm(&ikm).unwrap();
                let private_key_bytes = keypair.0.secret_key().secret_bytes();
                let pubkey_bytes = keypair.pubkey().bytes();

                IkmProfile {
                    ikm_hex: ikm_hex.to_string(),
                    private_key_hex: hex::encode(private_key_bytes),
                    pubkey_hex: hex::encode(pubkey_bytes),
                }
            })
            .collect();

        insta::assert_debug_snapshot!(profiles);
    }

    proptest! {
        #[test]
        fn proptest_from_ikm(ikm: [u8; 32]) {
            let keypair_result = KeyPair::from_ikm(&ikm);

            match keypair_result {
                Ok(keypair) => {
                    let msg = b"test message for proptest";
                    let signature = keypair.sign(msg);
                    prop_assert!(keypair.pubkey().verify(msg, &signature).is_ok());
                }
                Err(crate::secp::Error(e)) => {
                    panic!("key should be valid");
                }
            }
        }
    }
}
