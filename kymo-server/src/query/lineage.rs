//! Verify and echo cache lineages behind incremental chart responses.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::{Arc, OnceLock};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use prost::Message;
use sha2::{Digest, Sha256};

use super::{proto, MAX_CHART_REQUEST_RAW_SERIES};
use crate::series_cache::{LineageOrigin, SeriesSnapshot};

pub(super) const LINEAGE_VERSION_KEY: &str = "\0kymo:lineage-version";
const LINEAGE_VERSION: i64 = 2;
pub(super) const LINEAGE_PREFIX: &str = "\0kymo:r:";

/// Pin render parameters and each ref independently of other added inputs; distinguish duplicate refs by occurrence.
fn lineage_ids(req: &proto::ChartRequest) -> Vec<String> {
    let identity = proto::ChartRequest {
        y_series: Vec::new(),
        x_series: req.x_series.clone(),
        smoothing: req.smoothing,
        target_resolution: req.target_resolution,
        step_min: req.step_min,
        step_max: req.step_max,
        use_timestamp_axis: req.use_timestamp_axis,
        relative_time: req.relative_time,
        cache_state: None,
        log_buckets: req.log_buckets,
    };
    let parameters = identity.encode_to_vec();
    let mut occurrences = std::collections::HashMap::new();
    req.y_series
        .iter()
        .map(|series| {
            let bytes = series.encode_to_vec();
            let occurrence = occurrences.entry(bytes.clone()).or_insert(0u64);
            let mut hash = Sha256::new();
            for bytes in [parameters.as_slice(), bytes.as_slice()] {
                hash.update((bytes.len() as u64).to_le_bytes());
                hash.update(bytes);
            }
            hash.update(occurrence.to_le_bytes());
            *occurrence += 1;
            URL_SAFE_NO_PAD.encode(&hash.finalize()[..16])
        })
        .collect()
}

#[cfg(test)]
pub(super) fn lineage_id(req: &proto::ChartRequest, index: usize) -> String {
    lineage_ids(req).swap_remove(index)
}

#[derive(Clone, Copy, Debug)]
pub(super) struct LineageStamp {
    pub(super) maximum: i64,
    pub(super) digest: u64,
    pub(super) order: usize,
}

/// Consumers receive cutoffs only after the complete row-set proof succeeds; they cannot accidentally use the client's unverified map.
#[derive(Debug)]
pub(super) struct VerifiedLineages {
    cutoffs: Vec<Option<i64>>,
}

impl VerifiedLineages {
    pub(super) fn cutoff(&self, index: usize) -> Option<i64> {
        self.cutoffs.get(index).copied().flatten()
    }
}

#[derive(Default, Debug)]
pub(super) struct LineageState {
    pub(super) verification: Option<Result<VerifiedLineages, Rejection>>,
    current: Vec<(String, LineageStamp)>,
}

impl LineageState {
    pub(super) fn rejection(&self) -> Option<Rejection> {
        self.verification.as_ref()?.as_ref().err().copied()
    }
}

/// Authenticate one cache lineage and cutoff, bound to the requested input and its held order.
/// Constant work per ref; no row is hashed by the chart path. The process key invalidates pre-restart stamps.
fn lineage_stamp(snapshot: &SeriesSnapshot, id: &str, maximum: i64, order: usize) -> u64 {
    static KEY: OnceLock<RandomState> = OnceLock::new();
    let mut hash = KEY.get_or_init(RandomState::new).build_hasher();
    snapshot.lineage().hash(&mut hash);
    id.hash(&mut hash);
    maximum.hash(&mut hash);
    order.hash(&mut hash);
    hash.finish()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Rejection {
    State,
    Order,
    Digest(LineageOrigin),
}

impl Rejection {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::State => "state",
            Self::Order => "order",
            Self::Digest(LineageOrigin::Miss) => "digest_miss",
            Self::Digest(LineageOrigin::Rewrite) => "digest_rewrite",
            Self::Digest(LineageOrigin::Late) => "digest_late",
        }
    }
}

pub(super) fn parse_lineages(
    frontiers: &std::collections::HashMap<String, i64>,
) -> Option<std::collections::HashMap<&str, LineageStamp>> {
    (frontiers.get(LINEAGE_VERSION_KEY) == Some(&LINEAGE_VERSION)).then_some(())?;
    let mut rows = std::collections::HashMap::new();
    let mut orders = std::collections::HashSet::new();
    for (key, &maximum) in frontiers {
        let Some(encoded) = key.strip_prefix(LINEAGE_PREFIX) else {
            continue;
        };
        let mut parts = encoded.split(':');
        let (id, digest, order) = (parts.next()?, parts.next()?, parts.next()?);
        if parts.next().is_some() || id.len() != 22 || digest.len() != 11 || order.len() > 3 {
            return None;
        }
        (URL_SAFE_NO_PAD.decode(id).ok()?.len() == 16).then_some(())?;
        let digest = u64::from_le_bytes(URL_SAFE_NO_PAD.decode(digest).ok()?.try_into().ok()?);
        let order = order.parse::<usize>().ok()?;
        if order >= MAX_CHART_REQUEST_RAW_SERIES
            || !orders.insert(order)
            || rows
                .insert(
                    id,
                    LineageStamp {
                        maximum,
                        digest,
                        order,
                    },
                )
                .is_some()
        {
            return None;
        }
    }
    Some(rows)
}

/// Verify held rows from cache-maintained append lineage, in O(requested refs) time.
/// The lineage proves the complete series; the identity pins rendering and fetch/warmup ranges.
/// Full loads without verified recovery and non-monotone additions invalidate it, including changes outside the plotted range.
pub(super) fn inspect_lineages(
    req: &proto::ChartRequest,
    all_rows: &[Arc<SeriesSnapshot>],
    frontiers: Option<&std::collections::HashMap<String, i64>>,
) -> LineageState {
    // Custom-x uses independent per-gap anchors and has no standard-axis continuation proof.
    if req.x_series.is_some() {
        return LineageState::default();
    }
    let held = frontiers.and_then(parse_lineages);
    let mut verification = frontiers.map(|_| {
        if held.is_some() && all_rows.len() == req.y_series.len() {
            Ok(())
        } else {
            Err(Rejection::State)
        }
    });
    let mut cutoffs = vec![None; req.y_series.len()];
    let mut previous_order = None;
    let mut current = Vec::new();
    for (index, (rows, id)) in all_rows.iter().zip(lineage_ids(req)).enumerate() {
        if let Some(old) = held.as_ref().and_then(|held| held.get(id.as_str())) {
            if verification == Some(Ok(())) {
                verification = Some(if previous_order.is_some_and(|order| old.order <= order) {
                    Err(Rejection::Order)
                } else if rows.maximum().is_none_or(|maximum| old.maximum > maximum)
                    || old.digest != lineage_stamp(rows, &id, old.maximum, old.order)
                {
                    Err(Rejection::Digest(rows.origin()))
                } else {
                    Ok(())
                });
            }
            previous_order = Some(old.order);
            cutoffs[index] = Some(old.maximum);
        }
        if let Some(maximum) = rows.maximum() {
            let digest = lineage_stamp(rows, &id, maximum, index);
            current.push((
                id,
                LineageStamp {
                    maximum,
                    digest,
                    order: index,
                },
            ));
        }
    }
    LineageState {
        verification: verification.map(|result| result.map(|()| VerifiedLineages { cutoffs })),
        current,
    }
}

/// One compact entry per ref: identity, digest and order in the key; maximum insertion stamp in the value. Legacy watermark keys are deliberately absent, so older servers cannot treat this stronger state as permission for watermark-only deltas.
pub(super) fn stamp_lineages(
    frontiers: &mut std::collections::HashMap<String, i64>,
    lineages: &LineageState,
) {
    if lineages.current.is_empty() {
        return;
    }
    frontiers.insert(LINEAGE_VERSION_KEY.into(), LINEAGE_VERSION);
    for (id, row) in &lineages.current {
        let digest = URL_SAFE_NO_PAD.encode(row.digest.to_le_bytes());
        frontiers.insert(
            format!("{LINEAGE_PREFIX}{id}:{digest}:{}", row.order),
            row.maximum,
        );
    }
}
