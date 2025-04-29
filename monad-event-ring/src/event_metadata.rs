#[derive(Debug)]
pub struct EventMetadata {
    pub event_type: u16,
    pub c_name: &'static str,
    pub description: &'static str,
}

#[derive(Debug)]
pub struct EventDomainMetadata {
    pub metadata_hash: [u8; 32],
    pub events: &'static [EventMetadata],
}
