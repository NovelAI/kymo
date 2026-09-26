use super::*;
use serde_json::json;

fn key(name: &str) -> SectionKey {
    SectionKey::from(&SectionConfig::auto(name.to_owned(), Vec::new()))
}

fn placement(id: &str, before: bool, anchor: &str) -> Placement {
    Placement {
        id: id.to_owned(),
        at: if before {
            Anchor::Before(anchor.to_owned())
        } else {
            Anchor::After(anchor.to_owned())
        },
    }
}

fn context(world: &[SectionKey], visible: &[String], entries: &[Placement]) -> OrderContext {
    OrderContext::new(
        world
            .iter()
            .filter(|key| visible.contains(&key.name))
            .cloned()
            .collect(),
        entries.to_vec(),
        world.to_vec(),
    )
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WorldRow {
    name: String,
    priority: i32,
    display_name: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Vector {
    name: String,
    operation: String,
    world: Vec<WorldRow>,
    visible: Vec<String>,
    previous: Vec<Placement>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gap_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted: Option<String>,
    expected: Vec<Placement>,
    display: Vec<String>,
    full_display: Vec<String>,
}

#[derive(Deserialize)]
struct Vectors {
    vectors: Vec<Vector>,
}

impl Vector {
    fn evaluate(&self) -> (Vec<Placement>, Vec<String>, Vec<String>) {
        let mut world: Vec<_> = self
            .world
            .iter()
            .map(|row| {
                let mut section = SectionConfig::auto(row.name.clone(), Vec::new());
                section.priority = row.priority;
                section.display_name = row.display_name.clone();
                SectionKey::from(&section)
            })
            .collect();
        let mut visible = self.visible.clone();
        let ctx = context(&world, &visible, &self.previous);
        let entries = match self.operation.as_str() {
            "drop" => ctx
                .rebuild(self.source.as_deref().unwrap(), self.gap_index.unwrap())
                .unwrap(),
            "delete" => {
                let deleted = self.deleted.as_deref().unwrap();
                let result = ctx.delete_anchor(deleted);
                world.retain(|key| key.name != deleted);
                visible.retain(|name| name != deleted);
                result
            }
            operation => panic!("unknown vector operation: {operation}"),
        };
        let display = context(&world, &visible, &entries).project();
        let all: Vec<_> = world.iter().map(|key| key.name.clone()).collect();
        let full_display = context(&world, &all, &entries).project();
        (entries, display, full_display)
    }
}

#[test]
fn conformance_vectors() {
    let vectors: Vectors =
        serde_json::from_str(include_str!("section_order_vectors.json")).unwrap();
    assert_eq!(vectors.vectors.len(), 240);
    for vector in vectors.vectors {
        let (entries, display, full_display) = vector.evaluate();
        assert_eq!(entries, vector.expected, "{} entries", vector.name);
        assert_eq!(display, vector.display, "{} display", vector.name);
        assert_eq!(
            full_display, vector.full_display,
            "{} full display",
            vector.name
        );
    }
}

/// After reviewing a rule change, regenerate expectations; fixture inputs stay fixed.
#[test]
#[ignore = "rewrites the committed vectors; run explicitly after native properties pass"]
fn regenerate_section_order_vectors() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/state/section_order/section_order_vectors.json");
    let original = std::fs::read_to_string(&path).unwrap();
    // Keep the format and regeneration recipe in their existing header order.
    let (header, _) = original.split_once("  \"vectors\": ").unwrap();
    let mut vectors: Vectors = serde_json::from_str(&original).unwrap();
    assert_eq!(vectors.vectors.len(), 240);
    for vector in &mut vectors.vectors {
        (vector.expected, vector.display, vector.full_display) = vector.evaluate();
    }
    let body = vectors
        .vectors
        .iter()
        .map(|vector| serde_json::to_string(vector).unwrap())
        .collect::<Vec<_>>()
        .join(",\n    ");
    let output = format!("{header}  \"vectors\": [\n    {body}\n  ]\n}}\n");
    std::fs::write(path, output).unwrap();
}

#[test]
fn field_local_salvage_preserves_raw_until_an_actual_write() {
    let raw = json!([
        {"id": "A", "at": {"before": 4}},
        {"id": "A", "at": {"after": "B"}},
        {"id": "A", "at": {"before": "B"}},
        {"id": "gone", "at": {"before": "A"}},
        {"id": "C", "at": {"before": "gone"}},
        {"id": "bad", "at": {"after": "A"}, "unknown": true},
        {"id": "bad", "at": {"after": "A", "before": "B"}},
        {"id": "", "at": {"before": "A"}}
    ]);
    let mut saved: SectionOrder = serde_json::from_value(raw.clone()).unwrap();
    let parsed = saved.parsed(&["gone".to_owned()]);
    assert_eq!(
        parsed,
        vec![placement("", true, "A"), placement("A", false, "B")]
    );
    assert_eq!(serde_json::to_value(&saved).unwrap(), raw);
    assert!(!saved.is_empty());
    saved.replace(parsed.clone());
    assert_eq!(saved.parsed(&[]), parsed);
    assert_eq!(
        serde_json::to_value(&saved).unwrap(),
        serde_json::to_value(parsed).unwrap()
    );
    saved.replace(Vec::new());
    assert!(saved.is_empty());
    for value in [Value::Null, json!([])] {
        let saved: SectionOrder = serde_json::from_value(value).unwrap();
        assert!(saved.is_empty());
    }
    for value in [
        json!(false),
        json!(0),
        json!({}),
        json!("bad"),
        json!([false]),
    ] {
        let saved: SectionOrder = serde_json::from_value(value.clone()).unwrap();
        assert!(saved.parsed(&[]).is_empty());
        assert!(!saved.is_empty());
        assert_eq!(serde_json::to_value(saved).unwrap(), value);
    }
}

#[test]
fn canonical_groups_do_not_move_with_settings() {
    let entries = vec![
        placement("X", false, "section10"),
        placement("Y", false, "section2"),
        placement("Z", false, "section2"),
        placement("", true, "section2"),
    ];
    assert_eq!(
        canonical(entries),
        vec![
            placement("", true, "section2"),
            placement("Y", false, "section2"),
            placement("Z", false, "section2"),
            placement("X", false, "section10"),
        ]
    );
    let mut low = key("A");
    low.priority = i32::MIN;
    let mut high = key("B");
    high.priority = i32::MAX;
    assert!(high < low);
}

#[test]
fn readable_saves_are_canonical_but_unreadable_values_remain_exact() {
    let raw = json!([
        {"id":"X","at":{"after":"section10"}},
        {"id":"Y","at":{"after":"section2"}},
        {"id":"Z","at":{"after":"section2"}}
    ]);
    let saved: SectionOrder = serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(saved.for_save(&[])).unwrap(),
        json!([
            {"id":"Y","at":{"after":"section2"}},
            {"id":"Z","at":{"after":"section2"}},
            {"id":"X","at":{"after":"section10"}}
        ])
    );
    // Tombstones are unreadable in context, even if syntax alone is valid.
    assert_eq!(
        serde_json::to_value(saved.for_save(&["section2".into()])).unwrap(),
        raw
    );
    let mut malformed = raw.clone();
    malformed.as_array_mut().unwrap().push(json!({"bad": true}));
    let saved: SectionOrder = serde_json::from_value(malformed.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(saved.for_save(&[])).unwrap(),
        malformed
    );
}

#[test]
fn fresh_gap_validation_distinguishes_empty_id_from_endpoints() {
    let order = vec!["".to_owned(), "A".to_owned(), "B".to_owned()];
    let gap = SectionGap {
        predecessor: Some("".to_owned()),
        successor: Some("A".to_owned()),
    };
    assert_eq!(gap.index(&order, "B"), Some(1));
    assert_eq!(
        gap.index(&["A".to_owned(), "".to_owned(), "B".to_owned()], "B"),
        None
    );
    assert_eq!(gap.index(&order, "missing"), None);
    let start = SectionGap {
        predecessor: None,
        successor: Some("".to_owned()),
    };
    assert_eq!(start.index(&order, "B"), Some(0));
}

/// Independent coordinate sort: no core projection, known items or chain code.
fn positions(
    world: &[SectionKey],
    entries: &[Placement],
    names: &[String],
) -> BTreeMap<String, Coordinate> {
    let resolve = |name: &str| {
        world
            .iter()
            .find(|key| key.name == name)
            .cloned()
            .unwrap_or_else(|| key(name))
    };
    names
        .iter()
        .map(|name| {
            let point = match entries
                .iter()
                .enumerate()
                .find(|(_, entry)| entry.id == *name)
            {
                Some((slot, entry)) => Coordinate {
                    key: resolve(entry.at.name()),
                    phase: entry.at.phase(),
                    slot,
                },
                None => Coordinate {
                    key: resolve(name),
                    phase: 0,
                    slot: 0,
                },
            };
            (name.clone(), point)
        })
        .collect()
}

fn project_spec(world: &[SectionKey], entries: &[Placement], names: &[String]) -> Vec<String> {
    let points = positions(world, entries, names);
    let mut shown = names.to_vec();
    shown.sort_by(|a, b| points[a].cmp(&points[b]));
    shown
}

fn moved(order: &[String], source: &str, gap: usize) -> Vec<String> {
    let mut result: Vec<_> = order
        .iter()
        .filter(|name| name.as_str() != source)
        .cloned()
        .collect();
    result.insert(gap, source.to_owned());
    result
}

fn hidden_stability(
    world: &[SectionKey],
    visible: &[String],
    previous: &[Placement],
    result: &[Placement],
    source: &str,
) -> usize {
    let all: Vec<_> = world.iter().map(|key| key.name.clone()).collect();
    let before = project_spec(world, previous, &all);
    let after = project_spec(world, result, &all);
    let protected: BTreeSet<_> = previous
        .iter()
        .flat_map(|entry| [entry.id.as_str(), entry.at.name()])
        .filter(|name| !visible.iter().any(|id| id == *name))
        .collect();
    let mut checks = 0;
    for (i, a) in before.iter().enumerate() {
        for b in &before[i + 1..] {
            if a != source
                && b != source
                && (protected.contains(a.as_str()) || protected.contains(b.as_str()))
            {
                assert!(after.iter().position(|id| id == a) < after.iter().position(|id| id == b),
                    "hidden crossing: {before:?} -> {after:?}, X={source}, pair=({a},{b}), {previous:?} -> {result:?}");
                checks += 1;
            }
        }
    }
    checks
}

/// Enumerate automatic subsets independently of weighted LIS. A subset is
/// feasible iff default-coordinate, target, hidden and frozen edges are acyclic.
fn minimum(
    world: &[SectionKey],
    visible: &[String],
    previous: &[Placement],
    target: &[String],
    source: &str,
) -> usize {
    let names: Vec<_> = world
        .iter()
        .filter(|key| {
            visible.contains(&key.name)
                || previous
                    .iter()
                    .any(|entry| entry.id == key.name || entry.at.name() == key.name)
        })
        .map(|key| key.name.clone())
        .collect();
    let point = positions(world, previous, &names);
    let n = names.len();
    let index = |name: &str| names.iter().position(|id| id == name).unwrap();
    let frozen: BTreeSet<_> = previous
        .iter()
        .filter(|entry| {
            entry.id != source
                && (!visible.contains(&entry.id)
                    || !visible.iter().any(|name| name == entry.at.name()))
        })
        .map(|entry| entry.id.as_str())
        .collect();
    let mut base = vec![0_u64; n];
    for pair in target.windows(2) {
        base[index(&pair[0])] |= 1 << index(&pair[1]);
    }
    let before = project_spec(world, previous, &names);
    for (i, a) in before.iter().enumerate() {
        for b in &before[i + 1..] {
            if a != source && b != source && (!visible.contains(a) || !visible.contains(b)) {
                base[index(a)] |= 1 << index(b);
            }
        }
    }
    let mut best = 0;
    for mask in 0..(1_usize << visible.len()) {
        let automatic: Vec<_> = visible
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, name)| name)
            .collect();
        if automatic.iter().any(|name| frozen.contains(name.as_str())) {
            continue;
        }
        let fixed: Vec<_> = names
            .iter()
            .filter_map(|name| {
                if !visible.contains(name) || frozen.contains(name.as_str()) {
                    Some((name, point[name].clone()))
                } else if automatic.contains(&name) {
                    Some((
                        name,
                        Coordinate {
                            key: world.iter().find(|key| key.name == *name).unwrap().clone(),
                            phase: 0,
                            slot: 0,
                        },
                    ))
                } else {
                    None
                }
            })
            .collect();
        let mut reach = base.clone();
        for (a, ca) in &fixed {
            for (b, cb) in &fixed {
                if ca < cb {
                    reach[index(a)] |= 1 << index(b);
                }
            }
        }
        for mid in 0..n {
            for start in 0..n {
                if reach[start] & (1 << mid) != 0 {
                    reach[start] |= reach[mid];
                }
            }
        }
        if (0..n).all(|i| reach[i] & (1 << i) == 0) {
            best = best.max(automatic.len());
        }
    }
    visible.len() - best
}

fn all_encodings(names: &[String]) -> Vec<Vec<Placement>> {
    fn visit(
        names: &[String],
        current: &mut Vec<Placement>,
        found: &mut BTreeMap<String, Vec<Placement>>,
    ) {
        let entries = canonical(current.clone());
        found
            .entry(serde_json::to_string(&entries).unwrap())
            .or_insert(entries);
        for id in names {
            if current.iter().any(|entry| entry.id == *id) {
                continue;
            }
            for anchor in names {
                for before in [true, false] {
                    current.push(placement(id, before, anchor));
                    visit(names, current, found);
                    current.pop();
                }
            }
        }
    }
    let mut found = BTreeMap::new();
    visit(names, &mut Vec::new(), &mut found);
    found.into_values().collect()
}

#[test]
fn exhaustive_worlds_through_three_sections() {
    let mut drops = 0;
    let mut deletions = 0;
    let mut pairs = 0;
    for count in 1..=3 {
        let world: Vec<_> = ["", "section2", "section10"][..count]
            .iter()
            .map(|name| key(name))
            .collect();
        let names: Vec<_> = world.iter().map(|key| key.name.clone()).collect();
        for previous in all_encodings(&names) {
            for mask in 0..(1 << count) {
                let visible: Vec<_> = names
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| mask & (1 << i) != 0)
                    .map(|(_, name)| name.clone())
                    .collect();
                let ctx = context(&world, &visible, &previous);
                let shown = ctx.project();
                assert_eq!(shown, project_spec(&world, &previous, &visible));
                for source in &visible {
                    for gap in 0..visible.len() {
                        let target = moved(&shown, source, gap);
                        let result = ctx.rebuild(source, gap).unwrap();
                        assert_eq!(result, canonical(result.clone()));
                        assert_eq!(context(&world, &visible, &result).project(), target);
                        assert_eq!(result, ctx.rebuild(source, gap).unwrap(), "determinism");
                        let mut reverse_world = world.clone();
                        reverse_world.reverse();
                        let mut reverse_visible = visible.clone();
                        reverse_visible.reverse();
                        assert_eq!(
                            context(&reverse_world, &reverse_visible, &previous)
                                .rebuild(source, gap),
                            Some(result.clone()),
                            "input iteration order"
                        );
                        assert_eq!(
                            result
                                .iter()
                                .filter(|entry| !visible.contains(&entry.id))
                                .collect::<Vec<_>>(),
                            previous
                                .iter()
                                .filter(|entry| !visible.contains(&entry.id))
                                .collect::<Vec<_>>(),
                            "hidden entries keep values and slots"
                        );
                        pairs += hidden_stability(&world, &visible, &previous, &result, source);
                        if target == shown {
                            assert_eq!(result, previous, "nonminimal no-change must not write");
                        } else {
                            assert_eq!(
                                result
                                    .iter()
                                    .filter(|entry| visible.contains(&entry.id))
                                    .count(),
                                minimum(&world, &visible, &previous, &target, source),
                                "minimum {previous:?} -> {result:?}"
                            );
                            if visible.len() == names.len() {
                                let defaults = context(&world, &names, &[]).project();
                                let ranks: Vec<_> = target
                                    .iter()
                                    .map(|name| defaults.iter().position(|id| id == name).unwrap())
                                    .collect();
                                let longest = (0_usize..(1 << ranks.len()))
                                    .map(|mask| {
                                        let chosen: Vec<_> = ranks
                                            .iter()
                                            .enumerate()
                                            .filter(|(i, _)| mask & (1 << i) != 0)
                                            .map(|(_, rank)| *rank)
                                            .collect();
                                        if chosen.windows(2).all(|pair| pair[0] < pair[1]) {
                                            chosen.len()
                                        } else {
                                            0
                                        }
                                    })
                                    .max()
                                    .unwrap();
                                assert_eq!(
                                    result.len(),
                                    names.len() - longest,
                                    "unconstrained n minus LIS"
                                );
                                let adds_inversion = defaults.iter().enumerate().any(|(i, a)| {
                                    defaults[i + 1..].iter().any(|b| {
                                        target.iter().position(|id| id == a)
                                            > target.iter().position(|id| id == b)
                                            && shown.iter().position(|id| id == a)
                                                < shown.iter().position(|id| id == b)
                                    })
                                });
                                if !adds_inversion {
                                    assert!(
                                        result.len() <= previous.len(),
                                        "partial reversion grew"
                                    );
                                }
                            }
                            for old in &previous {
                                if old.id != *source
                                    && (!visible.contains(&old.id)
                                        || !visible.iter().any(|name| name == old.at.name()))
                                {
                                    assert!(result.contains(old), "frozen entry rewritten");
                                }
                            }
                        }
                        drops += 1;
                    }
                }
                for deleted in &names {
                    let result = ctx.delete_anchor(deleted);
                    assert!(result
                        .iter()
                        .all(|entry| &entry.id != deleted && entry.at.name() != deleted));
                    let remaining: Vec<_> = world
                        .iter()
                        .filter(|key| key.name != *deleted)
                        .cloned()
                        .collect();
                    let known_names: Vec<_> = names
                        .iter()
                        .filter(|name| {
                            visible.contains(name)
                                || previous.iter().any(|entry| {
                                    entry.id == **name || entry.at.name() == name.as_str()
                                })
                        })
                        .cloned()
                        .collect();
                    let remaining_names: Vec<_> = known_names
                        .iter()
                        .filter(|name| *name != deleted)
                        .cloned()
                        .collect();
                    let full_before = project_spec(&world, &previous, &known_names);
                    assert_eq!(
                        context(&remaining, &remaining_names, &result).project(),
                        full_before
                            .into_iter()
                            .filter(|name| name != deleted)
                            .collect::<Vec<_>>()
                    );
                    for old in &previous {
                        if &old.id != deleted && old.at.name() != deleted {
                            assert!(result.contains(old), "deletion changed unrelated entry");
                        }
                    }
                    deletions += 1;
                }
            }
            let automatic = context(&world, &names, &[]).project();
            if context(&world, &names, &previous).project() != automatic {
                let mut result = previous;
                for (gap, source) in automatic.iter().enumerate() {
                    result = context(&world, &names, &result)
                        .rebuild(source, gap)
                        .unwrap();
                }
                assert!(result.is_empty());
            }
        }
    }
    eprintln!(
        "section-order exhaustive: {drops} drops, {deletions} deletions, {pairs} protected pairs"
    );
    assert!(drops > 10_000 && deletions > 5_000 && pairs > 1_000);
}

#[test]
fn visible_and_hidden_anchor_renames_follow_current_coordinates() {
    let mut world: Vec<_> = ["A", "B", "C", "D"].iter().map(|name| key(name)).collect();
    let entries = vec![placement("D", false, "B")];
    for visible in [
        vec!["A", "B", "C", "D"],
        vec!["A", "C", "D"],
        vec!["A", "B", "C"],
    ] {
        let visible: Vec<_> = visible.into_iter().map(str::to_owned).collect();
        let before = context(&world, &visible, &entries).project();
        world[1].label = "Z".to_owned();
        let after = context(&world, &visible, &entries).project();
        assert_ne!(before, after);
        for source in &visible {
            let ctx = context(&world, &visible, &entries);
            for gap in 0..visible.len() {
                let result = ctx.rebuild(source, gap).unwrap();
                assert_eq!(
                    context(&world, &visible, &result).project(),
                    moved(&after, source, gap)
                );
                hidden_stability(&world, &visible, &entries, &result, source);
            }
        }
        world[1].label = "B".to_owned();
    }
}
