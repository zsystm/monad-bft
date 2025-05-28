#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// Message is signed by an author not in the validator set
    InvalidAuthor,
    /// Message does not contain the proper QC or TC values
    NotWellFormed,
    /// Bad signature
    InvalidSignature,
    /// Signature author doesn't match sender
    AuthorNotSender,
    /// The round signed by validators is larger than the TC round
    InvalidTcRound,
    /// The round signed by validators is larger than the NEC round
    InvalidNecRound,
    /// The recovering high tip doesn't match the TC high tip
    InvalidHighTip,
    /// The SignatureCollection doesn't have supermajority of the stake signed
    InsufficientStake,
    /// Required validator set (or cert pubkeys) not in validators epoch mapping
    ValidatorSetDataUnavailable,
    /// Vote does not contain a valid commit condition
    InvalidVote,
    /// Consensus Message version must match
    InvalidVersion,
    /// Epoch number in message doesn't match local records
    InvalidEpoch,
}
