//! reth's static file database table import and access

use rayon::prelude::*;
use reth_nippy_jar::{NippyJar, NippyJarError};
use reth_static_file_types::{
    SegmentHeader, SegmentRangeInclusive, StaticFileMap, StaticFileSegment,
};
use std::path::{Path, PathBuf};

mod cursor;
pub use cursor::StaticFileCursor;

mod mask;
pub use mask::*;

mod masks;
pub use masks::*;

/// Alias type for a map of [`StaticFileSegment`] and sorted lists of existing static file ranges.
type SortedStaticFiles = StaticFileMap<Vec<(SegmentRangeInclusive, SegmentHeader)>>;

/// Given the `static_files` directory path, it returns a list over the existing `static_files`
/// organized by [`StaticFileSegment`]. Each segment has a sorted list of block ranges and
/// segment headers as presented in the file configuration.
pub fn iter_static_files(path: &Path) -> Result<SortedStaticFiles, NippyJarError> {
    if !path.exists() {
        reth_fs_util::create_dir_all(path).map_err(|err| NippyJarError::Custom(err.to_string()))?;
    }

    // chainvisor: a fresh diskless reader cold-opens reth over S3 (a virtual block device
    // backed by S3). Loading the static-file segment headers SERIALLY here was the dominant
    // cold-open cost — measured ~176s, because each `NippyJar::load` is an ~150ms S3-backed
    // read and there can be hundreds of segment files. Collect the directory entries first
    // (cheap), then load the headers IN PARALLEL so the reader's source dispatcher serves the
    // reads concurrently. This changes ONLY the read concurrency — the same headers are
    // loaded and the same `SortedStaticFiles` is produced (load errors are still propagated),
    // so there is no behavioural/integrity difference; on a local-NVMe datadir it is a
    // harmless speedup.
    let paths: Vec<PathBuf> = reth_fs_util::read_dir(path)
        .map_err(|err| NippyJarError::Custom(err.to_string()))?
        .filter_map(Result::ok)
        .filter(|entry| entry.metadata().is_ok_and(|metadata| metadata.is_file()))
        .map(|entry| entry.path())
        .collect();

    // The loads are I/O-bound (each blocks on a segment-header read), so oversubscribe
    // relative to CPU count; bounded at 64 to stay within the source dispatcher's budget.
    let num_threads = paths.len().clamp(1, 64);
    let loaded: Vec<(StaticFileSegment, SegmentRangeInclusive, SegmentHeader)> =
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .map_err(|err| NippyJarError::Custom(err.to_string()))?
            .install(|| {
                paths
                    .par_iter()
                    .map(
                        |entry_path| -> Result<
                            Option<(StaticFileSegment, SegmentRangeInclusive, SegmentHeader)>,
                            NippyJarError,
                        > {
                            let file_name = entry_path
                                .file_name()
                                .map(|name| name.to_string_lossy().into_owned())
                                .unwrap_or_default();
                            let Some((segment, _)) =
                                StaticFileSegment::parse_filename(&file_name)
                            else {
                                return Ok(None);
                            };
                            let jar = NippyJar::<SegmentHeader>::load(entry_path)?;
                            Ok(jar.user_header().block_range().map(|block_range| {
                                (segment, block_range, jar.user_header().clone())
                            }))
                        },
                    )
                    .collect::<Result<Vec<_>, NippyJarError>>()
            })?
            .into_iter()
            .flatten()
            .collect();

    let mut static_files = SortedStaticFiles::default();
    for (segment, block_range, header) in loaded {
        static_files.entry(segment).or_insert_with(Vec::new).push((block_range, header));
    }

    // Sort by block end range.
    for range_list in static_files.values_mut() {
        range_list.sort_unstable_by_key(|(block_range, _)| block_range.end());
    }

    Ok(static_files)
}
