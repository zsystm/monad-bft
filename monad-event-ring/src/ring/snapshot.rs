use std::ffi::CString;

use super::{raw::RawEventRing, EventRing, TypedEventRing};
use crate::{reader::SnapshotEventReader, EventRingType};

#[derive(Debug)]
pub struct SnapshotEventRing<T>
where
    T: EventRingType,
{
    ring: EventRing<T>,
}

impl<T> SnapshotEventRing<T>
where
    T: EventRingType,
{
    pub fn new_from_zstd_bytes(bytes: &[u8], name: impl AsRef<str>) -> Result<Self, String> {
        let error_name_cstr = CString::new(name.as_ref()).unwrap();

        let snapshot_fd: libc::c_int =
            unsafe { libc::memfd_create(error_name_cstr.as_ptr(), libc::MFD_CLOEXEC) };
        assert_ne!(snapshot_fd, -1);

        let snapshot_off: libc::off_t = 0;

        let mut decompressed = Vec::new();

        zstd::stream::copy_decode(bytes, &mut decompressed)
            .expect(format!("could not decompress `{}`", name.as_ref()).as_str());

        let n_write = unsafe {
            libc::write(
                snapshot_fd,
                decompressed.as_ptr() as *const libc::c_void,
                decompressed.len() as libc::size_t,
            )
        };
        assert_eq!(n_write as usize, decompressed.len());

        let raw = RawEventRing::mmap_from_fd(
            libc::PROT_READ,
            0,
            snapshot_fd,
            snapshot_off,
            name.as_ref(),
        )?;

        Ok(Self {
            ring: EventRing::new(raw)?,
        })
    }
}

impl<T> TypedEventRing for SnapshotEventRing<T>
where
    T: EventRingType,
{
    type Reader<'ring>
        = SnapshotEventReader<'ring, T>
    where
        Self: 'ring;

    fn create_reader<'ring>(&'ring self) -> Self::Reader<'ring> {
        SnapshotEventReader::new(self.ring.create_reader())
    }
}
