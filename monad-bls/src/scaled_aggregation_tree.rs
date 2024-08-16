use std::{
    collections::{HashMap, HashSet, VecDeque,BTreeMap},
    marker::PhantomData,
};

use bitvec::prelude::*;
use monad_consensus_types::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    voting::ValidatorMapping,
};
use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hash, Hashable, Hasher, HasherType},
};
use monad_proto::proto::signing::ProtoBlsSignatureCollection;
use monad_types::{NodeId, Serializable};
use prost::Message;
use rand_chacha::{rand_core::{RngCore, SeedableRng}, ChaCha8Core};
use monad_crypto::hasher::{Blake3Hash };

use crate::{bls::BlsAggregatePubKey, BlsAggregateSignature, BlsKeyPair, BlsPubKey, BlsSignature};

#[derive(Debug)]
struct ScaledAggregationTree<PT: PubKey> {
    nodes: Vec<ScaledBlsSignatureCollection<PT>>,
    
}

impl<PT: PubKey> ScaledAggregationTree<PT> {
    fn new(
        scaled_sigs: &[(NodeId<PT>, BlsSignature)],
        validator_mapping: &ValidatorMapping<PT, BlsKeyPair>,
        validator_index: &HashMap<NodeId<PT>, usize>,
    ) -> Self {
        // build the binary heap represented as vector
        if scaled_sigs.is_empty() {
            return Self {
                nodes: vec![ScaledBlsSignatureCollection::with_capacity(
                    validator_mapping.map.len(),
                )],
            };
        }

        let n = scaled_sigs.len();
        let total_nodes = n * 2 - 1;
        let mut nodes =
            vec![ScaledBlsSignatureCollection::with_capacity(validator_mapping.map.len()); total_nodes];

        // copy all the signature as leaves
        for (i, (node_id, sig)) in scaled_sigs.iter().enumerate() {
            let cert = nodes.get_mut(n + i - 1).expect("node in range");
            cert.signers.set(
                *validator_index.get(node_id).expect("validator index"),
                true,
            );
            cert.sig.add_assign(sig).expect("bls add assign");
        }

        // build the tree bottom-up. A node is the aggregation of its two children
        if n > 1 {
            for i in (0..n - 1).rev() {
                let left = &nodes[2 * i + 1];
                let right = &nodes[2 * i + 2];

                *nodes.get_mut(i).unwrap() = merge_nodes(left, right);
            }
        }

        Self { nodes }
    }

    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<PT, BlsKeyPair>,
        msg: &[u8],
    ) -> Result<ScaledBlsSignatureCollection<PT>, Vec<(NodeId<PT>, BlsSignature)>> {
        // verify the certificate, if invalid, search the binary tree for invalid sig

        let mut unverified_certs: VecDeque<usize> = VecDeque::new();
        unverified_certs.push_back(0_usize);
        let mut invalid_sig = Vec::new();
        while !unverified_certs.is_empty() {
            let cert_idx: usize = unverified_certs.pop_front().unwrap();
            let cert = &self.nodes[cert_idx];
            match cert.verify(validator_mapping, msg) {
                Ok(_) => {}
                Err(_) => {
                    if 2 * cert_idx + 1 >= self.nodes.len() {
                        let signer_idx = cert
                            .signers
                            .first_one()
                            .expect("signer should be one-hot encoded");
                        let (node_id, _) = validator_mapping
                            .map
                            .iter()
                            .nth(signer_idx)
                            .expect("signer idx in range");

                        invalid_sig.push((*node_id, cert.sig.as_signature()));
                    } else {
                        unverified_certs.push_back(cert_idx * 2 + 1);
                        unverified_certs.push_back(cert_idx * 2 + 2);
                    }
                }
            }
        }

        if invalid_sig.is_empty() {
            Ok(self.nodes[0].clone())
        } else {
            Err(invalid_sig)
        }
    }
}

fn merge_nodes<PT: PubKey>(
    n1: &ScaledBlsSignatureCollection<PT>,
    n2: &ScaledBlsSignatureCollection<PT>,
) -> ScaledBlsSignatureCollection<PT> {
    assert_eq!(n1.signers.len(), n2.signers.len());

    let signers = n1.signers.clone() | n2.signers.clone();
    let mut sig = n1.sig;
    sig.add_assign_aggregate(&n2.sig);
    let cert = ScaledBlsSignatureCollection {
        signers,
        sig,
        _phantom: PhantomData,
    };
    // the signer sets should be disjoint
    assert_eq!(
        n1.num_signatures() + n2.num_signatures(),
        cert.num_signatures()
    );

    cert
}

#[derive(Clone, PartialEq, Eq)]
pub struct ScaledBlsSignatureCollection<PT: PubKey> {
    pub signers: BitVec<usize, Lsb0>,
    pub sig: BlsAggregateSignature,

    pub(crate) _phantom: PhantomData<PT>,
}

impl<PT: PubKey> std::fmt::Debug for ScaledBlsSignatureCollection<PT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScaledBlsSignatureCollection")
            .field("signers", &format_args!("{}", &self.signers))
            .field("sig", &self.sig)
            .finish()
    }
}

impl<PT: PubKey> Hashable for ScaledBlsSignatureCollection<PT> {
    fn hash(&self, state: &mut impl Hasher) {
        let bitvec_slice = self.signers.as_raw_slice();
        let mut u8_slice = Vec::new();
        for elem in bitvec_slice {
            let bytes = elem.to_le_bytes();
            u8_slice.extend_from_slice(&bytes);
        }
        state.update(u8_slice.as_slice());
        self.sig.hash(state);
    }
}

impl<PT: PubKey> ScaledBlsSignatureCollection<PT> {

    fn sig_scaler(validator_maping:&ValidatorMapping<PT, SignatureCollectionKeyPairType<Self>>, sigs:  &Vec<(NodeId<PT>, BlsSignature)> )->Vec< (NodeId<PT>, BlsSignature)>{
        let validator_scaler = ScaledBlsSignatureCollection::generate_scalers(validator_maping);
        sigs.iter().map(|(node_id,sig)|(*node_id, sig.mult_scalar(&validator_scaler[node_id])  )  ).collect()

    }
    fn generate_scalers(validator_mapping: &ValidatorMapping<PT, SignatureCollectionKeyPairType<Self>> ) ->  HashMap<NodeId<PT>, Vec<u8>> {
        let seed= [3u8;32];
        let mut rng = rand_chacha::ChaCha8Rng::from_seed(seed);
        let mut validator_scale: HashMap<NodeId<PT>, Vec<u8>>= HashMap::new();
        for node_id in validator_mapping.map.keys() {
            let mut bytes = vec![0u8; 32];
            rng.try_fill_bytes(&mut bytes[..]);
            validator_scale.insert(*node_id, bytes);
        }
        validator_scale
    }
    fn with_capacity(n: usize) -> Self {
        let signers = bitvec![usize, Lsb0; 0; n];
        let sig = BlsAggregateSignature::infinity();
        Self {
            signers,
            sig,
            _phantom: PhantomData,
        }
    }
}

impl<PT: PubKey> SignatureCollection for ScaledBlsSignatureCollection<PT> {
    type NodeIdPubKey = PT;
    type SignatureType = BlsSignature;

    fn new(
        sigs: impl IntoIterator<Item = (NodeId<PT>, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<PT, SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<PT, Self::SignatureType>> {

        let mut sigs_map = HashMap::new();
        let mut non_validator = Vec::new();
        let mut validator_index = HashMap::new();

        for (i, node_id) in validator_mapping.map.keys().enumerate() {
            validator_index.insert(*node_id, i);

        }
        let validator_scale = ScaledBlsSignatureCollection::generate_scalers(validator_mapping);
        for (node_id, sig) in sigs {
            if !validator_mapping.map.contains_key(&node_id) {
                non_validator.push((node_id, sig));
            }
            else {
                let bytes = &validator_scale[&node_id];
                BlsSignature::aggregate_sigs(&vec![sig], &vec![bytes.to_vec()], false);
                let scaled_sig = BlsAggregateSignature::as_signature(&(BlsSignature::aggregate_sigs(&vec![sig], &vec![bytes.to_vec()], false)))  ;
                let entry =sigs_map.entry(node_id).or_insert(scaled_sig);
                if *entry != scaled_sig {
                    return Err(SignatureCollectionError::ConflictingSignatures((
                        node_id, *entry, scaled_sig,
                    )));
                }
                
            }
        }
    
        if !non_validator.is_empty() {
            return Err(SignatureCollectionError::NodeIdNotInMapping(non_validator));
        }

        
        // use a binary tree to store intermediate aggregation result
        let tree = ScaledAggregationTree::new(
            &sigs_map.into_iter().collect::<Vec<_>>(),
            &validator_mapping,
            &validator_index,
        );


        tree.verify(&validator_mapping, msg)
            .map_err(SignatureCollectionError::InvalidSignaturesCreate)
    }

    fn get_hash(&self) -> Hash {
        HasherType::hash_object(self)
    }

    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<PT, SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Vec<NodeId<PT>>, SignatureCollectionError<PT, Self::SignatureType>> {
        assert_eq!(self.signers.len(), validator_mapping.map.len());
        let validator_scale = ScaledBlsSignatureCollection::generate_scalers(validator_mapping);
        let mut signers = Vec::new();
        let mut vec_scalers: Vec<Vec<u8>> = Vec::new();
        let mut vec_pubs = Vec::new();
       for (bit, (node_id, pubkey)) in self.signers.iter().zip(validator_mapping.map.iter()) {
            if *bit {
                let bytes = &validator_scale[node_id];
                vec_scalers.push((*bytes).clone());
                vec_pubs.push(*pubkey);
                signers.push(*node_id);
            }
        }

        let aggpk = BlsPubKey::aggregate_pubs(&vec_pubs, &vec_scalers, false);
        if signers.is_empty() && self.sig == BlsAggregateSignature::infinity() {
            return Ok(signers);
        }
        if self.sig.fast_verify(msg, &aggpk).is_err() {
            return Err(SignatureCollectionError::InvalidSignaturesVerify);
        }
        Ok(signers)
    }

    fn get_participants(
        &self,
        validator_mapping: &ValidatorMapping<PT, SignatureCollectionKeyPairType<Self>>,
        _msg: &[u8],
    ) -> HashSet<NodeId<PT>> {
        assert_eq!(self.signers.len(), validator_mapping.map.len());

        let mut signers = HashSet::new();
        for (bit, (node_id, _)) in self.signers.iter().zip(validator_mapping.map.iter()) {
            if *bit {
                signers.insert(*node_id);
            }
        }
        signers
    }

    fn num_signatures(&self) -> usize {
        self.signers.count_ones()
    }

    fn serialize(&self) -> Vec<u8> {
        let proto: ProtoBlsSignatureCollection = self.into();
        proto.encode_to_vec()
    }

    fn deserialize(data: &[u8]) -> Result<Self, SignatureCollectionError<PT, Self::SignatureType>> {
        let bls = ProtoBlsSignatureCollection::decode(data)
            .map_err(|e| SignatureCollectionError::DeserializeError(format!("{}", e)))?;
        bls.try_into()
            .map_err(|e| SignatureCollectionError::DeserializeError(format!("{}", e)))
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use monad_consensus_types::signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        },
        hasher::Hash,
        NopSignature,
    };
    use monad_testutil::{
        signing::{get_certificate_key, get_key},
        validators::create_keys_w_validators,
    };
    use monad_types::NodeId;
    use monad_validator::validator_set::ValidatorSetFactory;
    use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
    use test_case::test_case;

    use super::{merge_nodes, ScaledAggregationTree, ScaledBlsSignatureCollection};

    type SignatureType = NopSignature;
    type PubKey = CertificateSignaturePubKey<SignatureType>;
    type SignatureCollectionType = ScaledBlsSignatureCollection<PubKey>;

    fn get_sigs<'a>(
        msg: &[u8],
        iter: impl Iterator<
            Item = &'a (
                NodeId<PubKey>,
                SignatureCollectionKeyPairType<SignatureCollectionType>,
            ),
        >,
    ) -> Vec<(
        NodeId<PubKey>,
        <SignatureCollectionType as SignatureCollection>::SignatureType,
    )> {
        let mut sigs = Vec::new();
        for (node_id, key) in iter {
            let sig =
                <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg, key);
            sigs.push((*node_id, sig));
        }
        sigs
    }

    /// SUCCESSFUL SIGNATURE_COLLECTION TESTS
    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_creation(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());

        let sigcol = SignatureCollectionType::new(sigs, &valmap, msg_hash.as_ref());
        assert!(sigcol.is_ok());
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_creation_steal_signature(num_keys: u32) {
        if num_keys <= 1 {
            // skip test because we can't "steal" others signature
            return;
        }
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);
        let mut sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter().skip(1));
        let validator_scaler  = ScaledBlsSignatureCollection::generate_scalers(&valmap);
        // the first voter steals the signature of the second voter
        let first_voter_node_id = voting_keys[0].0;
        let invalid_sig = sigs.first().unwrap().1;
        sigs.push((first_voter_node_id, invalid_sig));
        let sigcol_err = SignatureCollectionType::new(sigs.clone(), &valmap, msg_hash.as_ref());
        assert!(matches!(
            sigcol_err,
            Err(SignatureCollectionError::InvalidSignaturesCreate(_))
        ));



        match sigcol_err.unwrap_err() {
            SignatureCollectionError::InvalidSignaturesCreate(sigs) => {
                let expected_set = vec![(first_voter_node_id, invalid_sig.mult_scalar(&validator_scaler[&first_voter_node_id]))]
                    .into_iter()
                    .collect::<HashSet<_>>();

                let test_set = sigs.into_iter().collect::<HashSet<_>>();


                assert_eq!(expected_set, test_set);
            }
            _ => unreachable!(),
        }
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_num_signatures(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();
        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());

        let sig_col = SignatureCollectionType::new(sigs, &valmap, msg_hash.as_ref()).unwrap();
        assert_eq!(sig_col.num_signatures(), num_keys as usize);
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_node_id_not_in_validator_mapping(num_keys: u32) {
        use monad_crypto::certificate_signature::CertificateKeyPair;
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let non_validator_node_id = NodeId::new(get_key::<SignatureType>(100).pubkey());
        let non_validator_cert_key = get_certificate_key::<SignatureCollectionType>(100);

        assert!(!valmap.map.contains_key(&non_validator_node_id));

        let msg_hash = Hash([129_u8; 32]);

        let mut sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());

        let non_validator_sig =
            <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(
                msg_hash.as_ref(),
                &non_validator_cert_key,
            );
        sigs.push((non_validator_node_id, non_validator_sig));
        assert_eq!(
            SignatureCollectionType::new(sigs, &valmap, msg_hash.as_ref()).unwrap_err(),
            SignatureCollectionError::NodeIdNotInMapping(vec![(
                non_validator_node_id,
                non_validator_sig
            )])
        );
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_duplicate_sig(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let mut sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());

        // duplicate the last signature
        sigs.push(*sigs.last().unwrap());
        assert_eq!(sigs[sigs.len() - 1], sigs[sigs.len() - 2]);
        let sigcol = SignatureCollectionType::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        // duplicate signature is removed
        assert_eq!(sigcol.num_signatures(), voting_keys.len());
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_verify(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());
        let sigcol = SignatureCollectionType::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        let signers = sigcol.verify(&valmap, msg_hash.as_ref()).unwrap();

        let signers_set = signers.iter().collect::<HashSet<_>>();
        let expected_set = valmap.map.keys().collect::<HashSet<_>>();
        assert_eq!(signers_set, expected_set);
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_get_participants(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());
        let sigcol = SignatureCollectionType::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        let signers = sigcol.get_participants(&valmap, msg_hash.as_ref());

        let expected_set = valmap.map.into_keys().collect::<HashSet<_>>();
        assert_eq!(signers, expected_set);
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_serialization_roundtrip(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());
        let sigcol = SignatureCollectionType::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        let sigcol_bytes = sigcol.serialize();
        assert_eq!(
            sigcol,
            SignatureCollectionType::deserialize(&sigcol_bytes).unwrap()
        );
    }

    #[test]
    fn test_invalid_deserialization() {
        let bytes = [127; 27];
        let sig_col_err = SignatureCollectionType::deserialize(&bytes).unwrap_err();

        assert!(matches!(
            sig_col_err,
            SignatureCollectionError::DeserializeError(_)
        ));
    }

    #[test]
    fn test_aggregation_tree() {
        // with 7 nodes, the tree looks like
        //                        (0123456)
        //                  /                  \
        //            (1234)                    (056)
        //          /        \                 /     \
        //     (12)          (34)          (56)       (0)
        //     /  \          /   \         /   \
        //  (1)    (2)    (3)    (4)    (5)    (6)

        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(7, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let mut validator_index = HashMap::new();

        for (i, node_id) in valmap.map.keys().enumerate() {
            validator_index.insert(*node_id, i);
        }

        let msg = b"hello world";
        let sigs: Vec<(NodeId<monad_crypto::NopPubKey>, crate::BlsSignature)> = get_sigs(msg, voting_keys.iter());
        let scaled_sigs = ScaledBlsSignatureCollection::sig_scaler(&valmap, &sigs);
        let tree = ScaledAggregationTree::new(&scaled_sigs, &valmap, &validator_index);

        let expected_n0 = SignatureCollectionType::new(sigs.clone(), &valmap, msg).unwrap();
        let expected_n1 =
            SignatureCollectionType::new(vec![sigs[1], sigs[2], sigs[3], sigs[4]], &valmap, msg)
                .unwrap();
        let expected_n2 =
            SignatureCollectionType::new(vec![sigs[0], sigs[5], sigs[6]], &valmap, msg).unwrap();
        let expected_n3 =
            SignatureCollectionType::new(vec![sigs[1], sigs[2]], &valmap, msg).unwrap();
        let expected_n4 =
            SignatureCollectionType::new(vec![sigs[3], sigs[4]], &valmap, msg).unwrap();
        let expected_n5 =
            SignatureCollectionType::new(vec![sigs[5], sigs[6]], &valmap, msg).unwrap();
        let expected_n6 = SignatureCollectionType::new(vec![sigs[0]], &valmap, msg).unwrap();
        let expected_n7 = SignatureCollectionType::new(vec![sigs[1]], &valmap, msg).unwrap();
        let expected_n8 = SignatureCollectionType::new(vec![sigs[2]], &valmap, msg).unwrap();
        let expected_n9 = SignatureCollectionType::new(vec![sigs[3]], &valmap, msg).unwrap();
        let expected_n10 = SignatureCollectionType::new(vec![sigs[4]], &valmap, msg).unwrap();
        let expected_n11 = SignatureCollectionType::new(vec![sigs[5]], &valmap, msg).unwrap();
        let expected_n12 = SignatureCollectionType::new(vec![sigs[6]], &valmap, msg).unwrap();

        let expected = vec![
            expected_n0,
            expected_n1,
            expected_n2,
            expected_n3,
            expected_n4,
            expected_n5,
            expected_n6,
            expected_n7,
            expected_n8,
            expected_n9,
            expected_n10,
            expected_n11,
            expected_n12,
        ];

        assert_eq!(tree.nodes, expected);
    }

    #[test_case(5,0; "5 signatures, 0-5 split")]
    #[test_case(5,1; "5 signatures, 1-4 split")]
    #[test_case(5,2; "5 signatures, 2-3 split")]
    fn test_merge_node(num_sigs: u32, first: usize) {
        assert!(num_sigs as usize >= first);
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_sigs, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg = b"hello world";

        let sigs1 = get_sigs(msg, voting_keys.iter().take(first));
        let sigs2 = get_sigs(msg, voting_keys.iter().skip(first));

        let sc1 = SignatureCollectionType::new(sigs1, &valmap, msg).unwrap();
        let sc2 = SignatureCollectionType::new(sigs2, &valmap, msg).unwrap();

        let sigs_all = get_sigs(msg, voting_keys.iter());

        let sc_all = SignatureCollectionType::new(sigs_all, &valmap, msg).unwrap();

        let sc_merged = merge_nodes(&sc1, &sc2);
        assert_eq!(sc_all, sc_merged);
    }

    #[test_case(7, 1, 1; "seed 7, 1/1 invalid")]
    #[test_case(7, 2, 1; "seed 7, 1/2 invalid")]
    #[test_case(7, 3, 1; "seed 7, 1/3 invalid")]
    #[test_case(7, 6, 1; "seed 7, 1/6 invalid")]
    #[test_case(7, 6, 3; "seed 7, 3/6 invalid")]
    #[test_case(7, 6, 6; "seed 7, 6/6 invalid")]
    #[test_case(7, 32, 1; "seed 7, 1/32 invalid")]
    #[test_case(7, 32, 10; "seed 7, 10/32 invalid")]
    #[test_case(7, 32, 32; "seed 7, 32/32 invalid")]

    fn test_creation_multiple_invalid(seed: u64, num_sigs: u32, num_invalid: u32) {
        assert!(num_invalid <= num_sigs);
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_sigs, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg = b"hello world";
        let wrong_msg = b"bye world";

        let invalid_sigs: Vec<(NodeId<monad_crypto::NopPubKey>, crate::BlsSignature)> = get_sigs(wrong_msg, voting_keys.iter().take(num_invalid as usize));
        let validator_scale = ScaledBlsSignatureCollection::generate_scalers(&valmap);
        let  mut scaled_invalid_sigs: Vec<(NodeId<monad_crypto::NopPubKey>, crate::BlsSignature)> = Vec::new();
        for (node_id, sig ) in &invalid_sigs{
        let scaled_sig = sig.mult_scalar(&validator_scale[&node_id]);
        scaled_invalid_sigs.push((*node_id ,scaled_sig));
        
        }

        let mut sigs = get_sigs(msg, voting_keys.iter().skip(num_invalid as usize));
        sigs.extend(invalid_sigs.clone());

        // shuffle the signature ordering
        sigs.shuffle(&mut StdRng::seed_from_u64(seed));

        let sig_col_err = SignatureCollectionType::new(sigs, &valmap, msg).unwrap_err();
        assert!(matches!(
            sig_col_err,
            SignatureCollectionError::InvalidSignaturesCreate(_)
        ));

        match sig_col_err {
            SignatureCollectionError::InvalidSignaturesCreate(sigs) => {
                let test_set = sigs.into_iter().collect::<HashSet<_>>();
                let invalid_set = scaled_invalid_sigs.into_iter().collect::<HashSet<_>>();

                assert_eq!(test_set, invalid_set);
            }
            _ => {
                unreachable!()
            }
        };
    }

    #[test]
    fn test_conflict_signatures() {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(5, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let validator_scalor = ScaledBlsSignatureCollection::generate_scalers(&valmap);
        let msg = b"hello world";
        let invalid_msg = b"bye world";
        let mut sigs = Vec::new();
        for (node_id, key) in voting_keys.iter().skip(1) {
            let sig =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg, key);
            sigs.push((*node_id, sig));
        }
        let expected_node_id = voting_keys[0].0;
        let valid_sig =  < <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg, &voting_keys[0].1);
        let invalid_sig = < <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(invalid_msg, &voting_keys[0].1);
        sigs.push((expected_node_id, valid_sig));
        sigs.push((expected_node_id, invalid_sig));
        let sig_col_err = SignatureCollectionType::new(sigs.clone(), &valmap, msg).unwrap_err();
        // resolves to ConflictingSignatures error because verification is deferred
        assert!(matches!(
            sig_col_err,
            SignatureCollectionError::ConflictingSignatures(_)
        ));

        let scaled_valid_sig = valid_sig.mult_scalar(&validator_scalor[&expected_node_id]);
        let scaled_invalid_sig = invalid_sig.mult_scalar(&validator_scalor[&expected_node_id]);

        match sig_col_err {
            SignatureCollectionError::ConflictingSignatures((node_id, s1, s2)) => {
                assert_eq!(node_id, expected_node_id);
                assert!(
                    (s1 == scaled_valid_sig && s2 == scaled_invalid_sig)
                        || (s1 == scaled_invalid_sig || s2 == scaled_valid_sig)
                );
            }
            _ => {
                unreachable!()
            }
        };
    }
}
