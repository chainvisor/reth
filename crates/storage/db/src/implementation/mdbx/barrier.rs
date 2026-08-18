//! chainvisor reader-txn barrier (CV_READER_TXN_BARRIER).
//!
//! On a chainvisor CVBD reader, an applier process rewrites the block
//! device UNDERNEATH this mounted filesystem between MDBX commits.
//! Two hazards close here, in one flock window:
//!
//! 1. A read transaction must never span an apply (the applier may
//!    overwrite pages the transaction's snapshot still references —
//!    MDBX's reader table cannot protect readers it cannot see).
//!    Every RO txn holds the barrier file SHARED for its lifetime;
//!    the applier holds it EXCLUSIVE around each batch.
//! 2. Device writes bypass this process's mmap and the fs page cache:
//!    touched pages go stale-clean. The applier publishes the batch's
//!    DEVICE ranges in a sidecar; at the next txn open (under the
//!    shared lock) we translate device→file via a cached FIEMAP of
//!    mdbx.dat, then madvise(DONTNEED) our mapping and
//!    posix_fadvise(DONTNEED) the file so the next access faults
//!    fresh bytes from the device.
//!
//! Inert unless CV_READER_TXN_BARRIER points at the lock file (the
//! sidecar is `<lock>.ranges`). Writers and ordinary nodes never set
//! it. Failures are loud but non-fatal: serving stale-but-consistent
//! bytes is worse than pausing, so lock acquisition blocks; sidecar
//! parse errors fall back to full-cache invalidation.

#![allow(clippy::missing_const_for_fn)]

#[cfg(target_os = "linux")]
mod linux_impl {
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

pub struct Barrier {
    lock_path: PathBuf,
    ranges_path: PathBuf,
    data_path: PathBuf,
    last_seq: AtomicU64,
}

static BARRIER: OnceLock<Option<Barrier>> = OnceLock::new();

pub fn get(env_dir: &Path) -> Option<&'static Barrier> {
    BARRIER
        .get_or_init(|| {
            let lock = std::env::var("CV_READER_TXN_BARRIER").ok()?;
            let lock_path = PathBuf::from(lock.trim());
            let ranges_path = lock_path.with_extension("ranges");
            Some(Barrier {
                lock_path,
                ranges_path,
                data_path: env_dir.join("mdbx.dat"),
                last_seq: AtomicU64::new(0),
            })
        })
        .as_ref()
}

impl Barrier {
    /// Take the shared lock for a transaction's lifetime (the
    /// returned file's close releases it) and consume any pending
    /// invalidation sidecar first.
    pub fn enter_read_txn(&self) -> Option<std::fs::File> {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.lock_path)
            .ok()?;
        // Blocking shared flock: an in-flight batch apply finishes in
        // milliseconds; waiting is the correctness.
        unsafe {
            if libc::flock(f.as_raw_fd(), libc::LOCK_SH) != 0 {
                return None;
            }
        }
        self.consume_ranges();
        Some(f)
    }

    fn consume_ranges(&self) {
        let Ok(bytes) = std::fs::read(&self.ranges_path) else { return };
        if bytes.len() < 8 {
            return;
        }
        let seq = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let seen = self.last_seq.load(Ordering::Acquire);
        if seq <= seen {
            return;
        }
        let ranges: Vec<(u64, u64)> = bytes[8..]
            .chunks_exact(16)
            .map(|c| {
                (
                    u64::from_le_bytes(c[0..8].try_into().unwrap()),
                    u64::from_le_bytes(c[8..16].try_into().unwrap()),
                )
            })
            .collect();
        self.invalidate_device_ranges(&ranges);
        self.last_seq.store(seq, Ordering::Release);
    }

    /// Translate device ranges to mdbx.dat file ranges via FIEMAP and
    /// drop both our PTEs and the fs cache for them. On any mapping
    /// failure, invalidate the WHOLE file — expensive but always safe.
    fn invalidate_device_ranges(&self, device_ranges: &[(u64, u64)]) {
        let Some((map_base, map_len, file)) = self.mdbx_mapping() else { return };
        match fiemap_extents(&file) {
            Ok(extents) if !extents.is_empty() => {
                for (dev_off, len) in device_ranges {
                    for fr in device_to_file(&extents, *dev_off, *len) {
                        drop_range(map_base, map_len, &file, fr.0, fr.1);
                    }
                }
            }
            _ => {
                // Fall back: drop everything (correct, slower).
                drop_range(map_base, map_len, &file, 0, map_len as u64);
            }
        }
    }

    /// Find our own mdbx.dat mapping via /proc/self/maps (no libmdbx
    /// API dependency; the data file is mapped exactly once).
    fn mdbx_mapping(&self) -> Option<(usize, usize, std::fs::File)> {
        let maps = std::fs::read_to_string("/proc/self/maps").ok()?;
        let needle = self.data_path.to_string_lossy();
        for line in maps.lines() {
            if !line.ends_with(needle.as_ref()) {
                continue;
            }
            let range = line.split_whitespace().next()?;
            let (a, b) = range.split_once('-')?;
            let start = usize::from_str_radix(a, 16).ok()?;
            let end = usize::from_str_radix(b, 16).ok()?;
            let file = std::fs::File::open(&self.data_path).ok()?;
            return Some((start, end - start, file));
        }
        None
    }
}

fn drop_range(map_base: usize, map_len: usize, file: &std::fs::File, file_off: u64, len: u64) {
    let page = 4096u64;
    let off = file_off & !(page - 1);
    let end = (file_off + len + page - 1) & !(page - 1);
    let span = (end - off) as usize;
    if (off as usize) < map_len {
        let span_in_map = span.min(map_len - off as usize);
        unsafe {
            libc::madvise(
                (map_base + off as usize) as *mut libc::c_void,
                span_in_map,
                libc::MADV_DONTNEED,
            );
        }
    }
    unsafe {
        libc::posix_fadvise(
            file.as_raw_fd(),
            off as libc::off_t,
            span as libc::off_t,
            libc::POSIX_FADV_DONTNEED,
        );
    }
    // Touch nothing else: the next fault reads the device.
    let _ = file.read_at(&mut [0u8; 0], 0);
}

/// (file_offset, device_offset, length) extents of mdbx.dat.
fn fiemap_extents(file: &std::fs::File) -> std::io::Result<Vec<(u64, u64, u64)>> {
    // FIEMAP ioctl, minimal fixed-count implementation.
    const FS_IOC_FIEMAP: libc::c_ulong = 0xC020660B;
    const EXTENT_CAP: usize = 512;
    #[repr(C)]
    struct FiemapExtent {
        fe_logical: u64,
        fe_physical: u64,
        fe_length: u64,
        fe_reserved64: [u64; 2],
        fe_flags: u32,
        fe_reserved: [u32; 3],
    }
    #[repr(C)]
    struct Fiemap {
        fm_start: u64,
        fm_length: u64,
        fm_flags: u32,
        fm_mapped_extents: u32,
        fm_extent_count: u32,
        fm_reserved: u32,
        // extents follow
    }
    let mut buf =
        vec![0u8; std::mem::size_of::<Fiemap>() + EXTENT_CAP * std::mem::size_of::<FiemapExtent>()];
    let fm = buf.as_mut_ptr() as *mut Fiemap;
    unsafe {
        (*fm).fm_start = 0;
        (*fm).fm_length = u64::MAX;
        (*fm).fm_flags = 0;
        (*fm).fm_extent_count = EXTENT_CAP as u32;
        if libc::ioctl(file.as_raw_fd(), FS_IOC_FIEMAP, fm) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let n = (*fm).fm_mapped_extents as usize;
        let exts = (fm as *const u8).add(std::mem::size_of::<Fiemap>()) as *const FiemapExtent;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let e = &*exts.add(i);
            out.push((e.fe_logical, e.fe_physical, e.fe_length));
        }
        Ok(out)
    }
}

fn device_to_file(extents: &[(u64, u64, u64)], dev_off: u64, len: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let dev_end = dev_off + len;
    for (logical, physical, elen) in extents {
        let pend = physical + elen;
        if dev_end <= *physical || dev_off >= pend {
            continue;
        }
        let s = dev_off.max(*physical);
        let e = dev_end.min(pend);
        out.push((logical + (s - physical), e - s));
    }
    out
}

}

#[cfg(target_os = "linux")]
pub(crate) use linux_impl::{get, Barrier};

#[cfg(not(target_os = "linux"))]
pub(crate) struct Barrier;
#[cfg(not(target_os = "linux"))]
impl Barrier {
    pub(crate) fn enter_read_txn(&self) -> Option<std::fs::File> {
        None
    }
}
#[cfg(not(target_os = "linux"))]
pub(crate) fn get(_env_dir: &std::path::Path) -> Option<&'static Barrier> {
    None
}
