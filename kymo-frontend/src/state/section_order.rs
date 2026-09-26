//! Live-anchor section ordering. All decisions here are pure and browser-independent.
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::layout_config::SectionConfig;
use crate::util::natural_cmp;

/// The default coordinate of a section, independent of its saved placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SectionKey {
    pub priority: i32,
    pub label: String,
    pub name: String,
}

impl From<&SectionConfig> for SectionKey {
    fn from(section: &SectionConfig) -> Self {
        Self {
            priority: section.priority,
            label: section.display_name().to_owned(),
            name: section.name.clone(),
        }
    }
}

impl Ord for SectionKey {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| natural_cmp(&self.label, &other.label))
            .then_with(|| natural_cmp(&self.name, &other.name))
    }
}

impl PartialOrd for SectionKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Anchor {
    Before(String),
    After(String),
}

impl Anchor {
    pub fn name(&self) -> &str {
        match self {
            Self::Before(name) | Self::After(name) => name,
        }
    }

    fn phase(&self) -> i8 {
        match self {
            Self::Before(_) => -1,
            Self::After(_) => 1,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    pub id: String,
    pub at: Anchor,
}

/// Stable within an anchor/side group; independent of discovery and renames.
pub fn canonical(mut entries: Vec<Placement>) -> Vec<Placement> {
    entries.sort_by(|a, b| {
        natural_cmp(a.at.name(), b.at.name()).then_with(|| a.at.phase().cmp(&b.at.phase()))
    });
    entries
}

/// Temporary field-local salvage retains unreadable values through unrelated saves.
/// Readable saves and actual ordering writes use canonical lists.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(transparent)]
pub struct SectionOrder(Value);

impl SectionOrder {
    pub fn is_empty(&self) -> bool {
        self.0.is_null() || self.0.as_array().is_some_and(Vec::is_empty)
    }

    pub fn parsed(&self, deleted: &[String]) -> Vec<Placement> {
        let mut seen = BTreeSet::new();
        let Some(raw) = self.0.as_array() else {
            return Vec::new();
        };
        canonical(
            raw.iter()
                .filter_map(|raw| serde_json::from_value::<Placement>(raw.clone()).ok())
                .filter(|entry| {
                    !deleted.contains(&entry.id)
                        && !deleted.iter().any(|name| name == entry.at.name())
                        && seen.insert(entry.id.clone())
                })
                .collect(),
        )
    }

    pub fn replace(&mut self, entries: Vec<Placement>) {
        self.0 = serde_json::to_value(canonical(entries)).expect("placements contain only strings");
    }

    /// Every readable write uses canonical groups. If even one raw entry is
    /// unreadable (including a duplicate or deleted name), preserve the whole
    /// original value until an actual ordering edit replaces it.
    pub fn for_save(&self, deleted: &[String]) -> Self {
        let Some(raw) = self.0.as_array() else {
            return self.clone();
        };
        let entries = self.parsed(deleted);
        if entries.len() != raw.len() {
            return self.clone();
        }
        let mut result = Self::default();
        result.replace(entries);
        result
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Coordinate {
    key: SectionKey,
    phase: i8,
    slot: usize,
}

impl Coordinate {
    fn automatic(key: SectionKey) -> Self {
        Self {
            key,
            phase: 0,
            slot: 0,
        }
    }
}

#[derive(Clone, Debug)]
struct Item {
    at: Coordinate,
    forced: bool,
    name: String,
    entry: Option<Placement>,
    chosen: bool,
    kept: bool,
}

impl Item {
    fn automatic(key: SectionKey, forced: bool) -> Self {
        Self {
            name: key.name.clone(),
            at: Coordinate::automatic(key),
            forced,
            entry: None,
            chosen: false,
            kept: false,
        }
    }
}

/// Resolved keys come from the freshly loaded document, applying every settings
/// patch in order. Live keys take precedence. Missing names use auto defaults.
pub struct OrderContext {
    live: BTreeMap<String, SectionKey>,
    resolved: BTreeMap<String, SectionKey>,
    previous: Vec<Placement>,
}

impl OrderContext {
    pub fn new(live: Vec<SectionKey>, previous: Vec<Placement>, resolved: Vec<SectionKey>) -> Self {
        Self {
            live: live
                .into_iter()
                .map(|key| (key.name.clone(), key))
                .collect(),
            resolved: resolved
                .into_iter()
                .map(|key| (key.name.clone(), key))
                .collect(),
            previous: canonical(previous),
        }
    }

    fn key(&self, name: &str) -> SectionKey {
        self.live
            .get(name)
            .or_else(|| self.resolved.get(name))
            .cloned()
            .unwrap_or_else(|| SectionKey::from(&SectionConfig::auto(name.to_owned(), Vec::new())))
    }

    fn placed(&self) -> BTreeSet<&str> {
        self.previous
            .iter()
            .map(|entry| entry.id.as_str())
            .collect()
    }

    fn coordinate(&self, entry: &Placement, slot: usize) -> Coordinate {
        Coordinate {
            key: self.key(entry.at.name()),
            phase: entry.at.phase(),
            slot,
        }
    }

    pub fn project(&self) -> Vec<String> {
        self.known_items(None)
            .into_iter()
            .filter(|item| self.live.contains_key(&item.name))
            .map(|item| item.name)
            .collect()
    }

    fn known_items(&self, skip: Option<&str>) -> Vec<Item> {
        let placed = self.placed();
        let mut items: Vec<_> = self
            .previous
            .iter()
            .enumerate()
            .filter(|(_, entry)| Some(entry.id.as_str()) != skip)
            .map(|(index, entry)| Item {
                at: self.coordinate(entry, index),
                forced: !self.live.contains_key(&entry.id)
                    || !self.live.contains_key(entry.at.name()),
                name: entry.id.clone(),
                entry: Some(entry.clone()),
                chosen: false,
                kept: false,
            })
            .collect();
        let hidden_anchors: BTreeSet<_> = self
            .previous
            .iter()
            .map(|entry| entry.at.name())
            .filter(|name| {
                Some(*name) != skip && !self.live.contains_key(*name) && !placed.contains(*name)
            })
            .collect();
        items.extend(
            hidden_anchors
                .into_iter()
                .map(|name| Item::automatic(self.key(name), true)),
        );
        items.extend(
            self.live
                .values()
                .filter(|key| {
                    Some(key.name.as_str()) != skip && !placed.contains(key.name.as_str())
                })
                .map(|key| Item::automatic(key.clone(), false)),
        );
        items.sort_by(|a, b| a.at.cmp(&b.at));
        items
    }

    /// `gap_index` indexes the fresh display with `source` removed. Invalid
    /// actions return None; a no-change action returns the unchanged list.
    pub fn rebuild(&self, source: &str, gap_index: usize) -> Option<Vec<Placement>> {
        let shown = self.project();
        let old_index = shown.iter().position(|name| name == source)?;
        if gap_index >= shown.len() {
            return None;
        }
        if old_index == gap_index {
            return Some(self.previous.clone());
        }
        let mut target = shown;
        target.remove(old_index);
        target.insert(gap_index, source.to_owned());
        let predecessor = gap_index.checked_sub(1).map(|index| target[index].as_str());
        let successor = target.get(gap_index + 1).map(String::as_str);
        let mut items = self.known_items(Some(source));
        let index_of = |items: &[Item], name: &str| {
            items
                .iter()
                .position(|item| item.name == name)
                .expect("visible neighbor is known")
        };
        let lo = predecessor.map_or(0, |name| index_of(&items, name) + 1);
        let hi = successor.map_or(items.len(), |name| index_of(&items, name));
        let dragged = Item::automatic(self.key(source), false);
        let insertion = lo
            + items[lo..hi]
                .iter()
                .filter(|item| item.at < dragged.at)
                .count();
        items.insert(insertion, dragged);
        let coordinates: Vec<_> = items.iter().map(|item| self.settled(item)).collect();
        let placed = self.placed();
        let weights: Vec<_> = items
            .iter()
            .map(|item| {
                if item.forced {
                    [1, 0, 0, 0]
                } else {
                    [
                        0,
                        1,
                        usize::from(!placed.contains(item.name.as_str())),
                        usize::from(item.name != source),
                    ]
                }
            })
            .collect();
        for index in increasing_chain(&coordinates, &weights) {
            items[index].chosen = true;
        }
        debug_assert!(items
            .iter()
            .filter(|item| item.forced)
            .all(|item| item.chosen));
        let source_index = index_of(&items, source);
        if !items[source_index].chosen {
            // Only X can cross protected hidden positions. Its visible neighbor
            // avoids introducing a hidden anchor merely because one is nearby.
            let dragged = items.remove(source_index);
            let insertion = predecessor.map_or_else(
                || {
                    index_of(
                        &items,
                        successor.expect("a real move has another visible row"),
                    )
                },
                |name| index_of(&items, name) + 1,
            );
            items.insert(insertion, dragged);
        }
        let mut lower = Vec::with_capacity(items.len());
        let mut boundary = None;
        for item in &items {
            lower.push(boundary.clone());
            if item.chosen {
                boundary = Some(self.settled(item));
            }
        }
        boundary = None;
        for (item, low) in items.iter_mut().rev().zip(lower.into_iter().rev()) {
            if !item.chosen && item.entry.is_some() {
                item.kept = low.as_ref().is_none_or(|low| low < &item.at)
                    && boundary.as_ref().is_none_or(|high| &item.at < high);
            }
            if item.chosen {
                boundary = Some(self.settled(item));
            }
        }
        Some(emit(items, &self.live))
    }

    fn settled(&self, item: &Item) -> Coordinate {
        if item.forced {
            item.at.clone()
        } else {
            Coordinate::automatic(self.key(&item.name))
        }
    }

    /// Call before removing settings or writing the deletion tombstone. This
    /// preserves all unrelated entries and re-anchors only direct followers.
    pub fn delete_anchor(&self, name: &str) -> Vec<Placement> {
        let mut items = self.known_items(Some(name));
        for item in &mut items {
            if item
                .entry
                .as_ref()
                .is_some_and(|entry| entry.at.name() == name)
            {
                item.entry = None;
            } else if item.entry.is_some() {
                item.kept = true;
            } else {
                item.chosen = true;
            }
        }
        emit(items, &self.live)
    }
}

/// O(m²) time, O(m) memory. Equal scores pick the lowest predecessor/end index,
/// giving the lexicographically smallest reversed chain of known-order indices.
fn increasing_chain(coordinates: &[Coordinate], weights: &[[usize; 4]]) -> Vec<usize> {
    let mut scores: Vec<[usize; 4]> = Vec::with_capacity(coordinates.len());
    let mut parents = Vec::with_capacity(coordinates.len());
    let mut end: Option<usize> = None;
    for i in 0..coordinates.len() {
        let mut parent: Option<usize> = None;
        for j in 0..i {
            if coordinates[j] < coordinates[i] && parent.is_none_or(|p| scores[j] > scores[p]) {
                parent = Some(j);
            }
        }
        scores.push(std::array::from_fn(|part| {
            weights[i][part] + parent.map_or(0, |p| scores[p][part])
        }));
        parents.push(parent);
        if end.is_none_or(|old| scores[i] > scores[old]) {
            end = Some(i);
        }
    }
    let mut chosen = Vec::new();
    while let Some(index) = end {
        chosen.push(index);
        end = parents[index];
    }
    chosen.reverse();
    chosen
}

fn group(item: &Item, before: bool) -> Anchor {
    match &item.entry {
        Some(entry) if !item.chosen || item.forced => entry.at.clone(),
        _ if before => Anchor::Before(item.name.clone()),
        _ => Anchor::After(item.name.clone()),
    }
}

/// Shared run construction for drops and deletion. Fixed rows keep their exact
/// value and within-group position, including hidden rows between visible rows.
fn emit(mut items: Vec<Item>, visible: &BTreeMap<String, SectionKey>) -> Vec<Placement> {
    let mut output = Vec::new();
    let mut i = 0;
    while i < items.len() {
        let item = &items[i];
        if item.chosen || item.kept {
            if (item.forced || item.kept) && item.entry.is_some() {
                output.push(item.entry.clone().expect("existing placement"));
            }
            i += 1;
            continue;
        }
        let mut end = i + 1;
        while end < items.len() && !(items[end].chosen || items[end].kept) {
            end += 1;
        }
        let left = i.checked_sub(1).map(|index| &items[index]);
        let right = items.get(end);
        let at = match (left, right) {
            (None, None) => {
                // Deletion left only followers: choose one automatic leader.
                let lead = (i..end)
                    .find(|index| visible.contains_key(&items[*index].name))
                    .unwrap_or(i);
                items[lead].chosen = true;
                items[lead].forced = false;
                items[lead].entry = None;
                continue;
            }
            (Some(left), _) if visible.contains_key(&left.name) => group(left, false),
            (_, Some(right)) if visible.contains_key(&right.name) => group(right, true),
            (Some(left), _) => group(left, false),
            (_, Some(right)) => group(right, true),
        };
        output.extend(items[i..end].iter().map(|item| Placement {
            id: item.name.clone(),
            at: at.clone(),
        }));
        i = end;
    }
    canonical(output)
}

/// Typed endpoints keep the catch-all section's empty ID distinct from an end.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SectionGap {
    pub predecessor: Option<String>,
    pub successor: Option<String>,
}

impl SectionGap {
    pub fn index(&self, order: &[String], source: &str) -> Option<usize> {
        if !order.iter().any(|name| name == source) {
            return None;
        }
        let others: Vec<_> = order
            .iter()
            .filter(|name| name.as_str() != source)
            .collect();
        let index = match &self.predecessor {
            None => 0,
            Some(name) => others.iter().position(|other| *other == name)? + 1,
        };
        (others.get(index).map(|name| (*name).as_str()) == self.successor.as_deref())
            .then_some(index)
    }
}

#[cfg(test)]
mod tests;
