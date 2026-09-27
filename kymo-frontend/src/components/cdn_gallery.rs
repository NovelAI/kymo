use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use dioxus::prelude::*;

use crate::components::copy_text::CopyText;
use crate::components::icons::{CaretLeftIcon, CaretRightIcon};
use crate::components::metric_rect::CdnRunData;
use crate::state::layout_config::CdnDisplayMode;
use crate::state::panel_cache::Store;
use crate::state::visibility::retry_visible;
use crate::util::primary;

thread_local! {
    /// persist_key -> the step the user navigated to, so a gallery scrolled out of the band (its panel body unmounts, see metric_rect.rs) comes back on the same step. Only explicit navigation writes it — an untouched gallery keeps the default jump-to-latest on remount.
    static GALLERY_STEP: RefCell<Store<i64>> = RefCell::new(Store::new(2048));
    /// Select-index position uses the same panel-lifetime semantics: explicit navigation survives a far-zone body unmount, while untouched panels still start at the first image.
    static GALLERY_INDEX: RefCell<Store<usize>> = RefCell::new(Store::new(2048));
}

fn remembered_gallery_index(persist_key: Option<&str>) -> usize {
    persist_key
        .and_then(|key| GALLERY_INDEX.with(|cache| cache.borrow_mut().get(key)))
        .unwrap_or(0)
}

fn remember_gallery_index(persist_key: Option<&str>, index: usize) {
    if let Some(key) = persist_key {
        GALLERY_INDEX.with(|cache| cache.borrow_mut().put(key.to_string(), index));
    }
}

fn selected_step_index(steps: &[i64], selected_step: Option<i64>) -> Option<usize> {
    if steps.is_empty() {
        return None;
    }
    Some(match selected_step {
        Some(step) => match steps.binary_search(&step) {
            Ok(index) => index,
            Err(insertion) => insertion.min(steps.len() - 1),
        },
        // The signal can still be None when the gallery first mounted with no data. Preserve the normal untouched-gallery default when steps later arrive: start at latest (the caller then stores that concrete step).
        None => steps.len() - 1,
    })
}

fn opens_gallery_lightbox(modifiers: dioxus::html::Modifiers) -> bool {
    !modifiers.alt() && !modifiers.ctrl() && !modifiers.meta() && !modifiers.shift()
}

fn gallery_thumbnail_label(
    run_label: &str,
    source_index: usize,
    caption: Option<&str>,
    image_index: usize,
) -> String {
    // Run names are not unique, so retain the source position as well as its friendly label. Together with the item position, every link in a panel has a useful, distinct name even when captions are absent or repeated.
    let mut label = format!(
        "Open image {} from source {} ({run_label}) full-size",
        image_index + 1,
        source_index + 1
    );
    if let Some(caption) = caption.map(str::trim).filter(|caption| !caption.is_empty()) {
        label.push_str(": ");
        label.push_str(caption);
    }
    label
}

fn gallery_file_link_label(
    display_name: &str,
    source_label: &str,
    source_index: usize,
    item_index: usize,
) -> String {
    format!(
        "{display_name} — file {} from source {} ({source_label})",
        item_index + 1,
        source_index + 1
    )
}

fn gallery_image(
    item: &ManifestItem,
    run_label: &str,
    source_index: usize,
    image_index: usize,
    on_lightbox: EventHandler<String>,
) -> Element {
    let url = crate::runtime::cdn_url(&item.resource);
    let accessible_label = gallery_thumbnail_label(
        run_label,
        source_index,
        item.caption.as_deref(),
        image_index,
    );
    let click_url = url.clone();
    rsx! {
        a {
            href: "{url}",
            target: "_blank",
            aria_label: "{accessible_label}",
            onclick: move |event: Event<MouseData>| {
                if opens_gallery_lightbox(event.modifiers()) {
                    event.prevent_default();
                    on_lightbox.call(click_url.clone());
                }
            },
            img { src: "{url}", loading: "lazy", alt: "" }
        }
        if let Some(caption) = &item.caption {
            CopyText { text: caption.clone(), class: "cdn-caption" }
        }
    }
}

fn gallery_run_image(
    run: &RunManifest,
    source_index: usize,
    idx: usize,
    on_lightbox: EventHandler<String>,
) -> Element {
    let Some(item) = run.manifest.as_ref().and_then(|m| m.items.get(idx)) else {
        return rsx! {};
    };
    let color = &run.color;
    let run_label = &run.label;
    rsx! {
        div {
            class: "cdn-image-item",
            style: "background: {color}22;",
            {gallery_image(item, run_label, source_index, idx, on_lightbox)}
            div {
                class: "cdn-run-label fade-overflow",
                style: "color: {color};",
                title: "{run_label}",
                span { "{run_label}" }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GalleryPresentation {
    Loading,
    Empty,
    PendingOnly,
    Content,
}

/// The status line for sources whose key at the step is still a placeholder: a live run's images may still arrive; an ended run's placeholder is an upload that did not finish.
fn pending_status(live: &[&str], ended: &[&str]) -> Option<String> {
    let clauses: Vec<String> = [("Not uploaded yet", live), ("Upload did not finish", ended)]
        .into_iter()
        .filter(|(_, labels)| !labels.is_empty())
        .map(|(prefix, labels)| format!("{prefix}: {}", labels.join(", ")))
        .collect();
    (!clauses.is_empty()).then(|| clauses.join(" · "))
}

fn gallery_presentation(
    any_pending: bool,
    parsed_manifest_count: Option<usize>,
) -> GalleryPresentation {
    match (any_pending, parsed_manifest_count) {
        (_, Some(count)) if count > 0 => GalleryPresentation::Content,
        (true, _) => GalleryPresentation::PendingOnly,
        (false, None) => GalleryPresentation::Loading,
        (false, Some(_)) => GalleryPresentation::Empty,
    }
}

// Shared with the server's CDN collector by path; its half (`LINKS_VERSION`, `resources`) is dead code here.
#[path = "../../../shared/cdn_manifest.rs"]
#[allow(dead_code)]
mod cdn_manifest;
use cdn_manifest::ManifestItem;
type Manifest = cdn_manifest::Manifest<serde_json::Value>;

impl Manifest {
    fn is_image_gallery(&self) -> bool {
        self.class == "image_gallery" && self.items.iter().all(|item| item.filename.is_none())
    }
}

impl ManifestItem {
    fn display_name(&self) -> &str {
        self.filename
            .as_deref()
            .filter(|filename| !filename.is_empty())
            .unwrap_or(&self.resource)
    }
}

/// A fetched manifest paired with its run info.
#[derive(Clone, Debug, PartialEq)]
struct RunManifest {
    project_id: String,
    run_id: String,
    metric_name: String,
    label: String,
    color: String,
    manifest: Option<Manifest>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CdnManifestClass {
    ImageGallery,
    Metadata,
    FileList,
    Mixed,
}

impl CdnManifestClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::ImageGallery => "image_gallery",
            Self::Metadata => "metadata",
            Self::FileList => "file_list",
            Self::Mixed => "mixed",
        }
    }
}

fn classify_manifests(run_manifests: &[RunManifest]) -> Option<CdnManifestClass> {
    let mut parsed = run_manifests
        .iter()
        .filter_map(|run_manifest| run_manifest.manifest.as_ref());
    let first = parsed.next()?;
    let mut has_metadata = first.class == "metadata";
    let mut has_non_metadata = !has_metadata;
    let mut all_non_metadata_are_images = !has_metadata && first.is_image_gallery();

    for manifest in parsed {
        if manifest.class == "metadata" {
            has_metadata = true;
        } else {
            has_non_metadata = true;
            all_non_metadata_are_images &= manifest.is_image_gallery();
        }
    }

    Some(match (has_metadata, has_non_metadata) {
        (true, true) => CdnManifestClass::Mixed,
        (true, false) => CdnManifestClass::Metadata,
        (false, true) if all_non_metadata_are_images => CdnManifestClass::ImageGallery,
        (false, true) => CdnManifestClass::FileList,
        (false, false) => unreachable!("the first parsed manifest sets one family"),
    })
}

type ManifestFetchKey = (RunManifest, String);
type ManifestFetchResult = (Vec<ManifestFetchKey>, Vec<RunManifest>);

fn current_manifest_results<'a>(
    current_keys: &[ManifestFetchKey],
    result: Option<&'a ManifestFetchResult>,
) -> Option<&'a [RunManifest]> {
    let (source_keys, run_manifests) = result?;
    (source_keys == current_keys).then_some(run_manifests)
}

fn metadata_columns(
    run_manifests: &[RunManifest],
    display_runs: &[crate::grpc::proto::RunInfo],
) -> Vec<crate::components::metadata_viewer::MetadataColumn> {
    run_manifests
        .iter()
        .filter_map(|run_manifest| {
            let manifest = run_manifest.manifest.as_ref()?;
            if manifest.class != "metadata" {
                return None;
            }
            let mut data = manifest.data.clone()?;
            if run_manifest.metric_name == "info/run_info" {
                if let Some(run) = display_runs.iter().find(|run| {
                    run.project_id == run_manifest.project_id && run.run_id == run_manifest.run_id
                }) {
                    data = crate::components::metadata_viewer::add_server_timing(data, run);
                }
            }
            Some(crate::components::metadata_viewer::MetadataColumn {
                label: run_manifest.label.clone(),
                color: run_manifest.color.clone(),
                data,
                ignore_server_timing_in_diff: run_manifest.metric_name == "info/run_info",
            })
        })
        .collect()
}

fn file_groups(run_manifests: &[RunManifest]) -> Vec<(&str, &str, &[ManifestItem])> {
    run_manifests
        .iter()
        .filter_map(|run_manifest| {
            let items = run_manifest.manifest.as_ref()?.items.as_slice();
            (!items.is_empty()).then_some((
                run_manifest.label.as_str(),
                run_manifest.color.as_str(),
                items,
            ))
        })
        .collect()
}

/// Ok(None) is settled knowledge — a 404 (nothing at that key) or
/// unparseable content, neither of which heals on retry. Err is transient
/// (network failure, 5xx) and must be retried: a finished run's keys never
/// change, so a failure settled into the manifests resource would never be
/// refetched.
async fn fetch_manifest(cdn_key: &str) -> Result<Option<Manifest>, String> {
    let url = crate::runtime::cdn_url(cdn_key);
    let resp = gloo_net::http::Request::get(&url)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if resp.status() == 404 {
        return Ok(None);
    }
    if !resp.ok() {
        return Err(format!("{} for {url}", resp.status()));
    }
    let text = resp.text().await.map_err(|e| e.to_string())?;
    Ok(serde_json::from_str(&text).ok())
}

#[component]
pub fn CdnGallery(
    runs: Vec<CdnRunData>,
    #[props(default = 280)] height: u32,
    #[props(default)] display_mode: CdnDisplayMode,
    /// Signal the gallery writes to once the manifest class is known
    /// ("image_gallery" / "metadata" / "file_list" / "mixed"). Lets the editor hide
    /// panels that don't apply.
    mut cdn_class: Signal<Option<String>>,
    /// Metadata viewer: when true, hide rows whose values are identical
    /// across all runs (and any branches whose subtree contains no diffs).
    #[props(default = false)]
    metadata_diff_only: bool,
    /// Key for remembering the selected step across body unmounts (see
    /// GALLERY_STEP). None = don't persist.
    #[props(default)]
    persist_key: Option<String>,
) -> Element {
    // Collect all unique steps across runs
    let all_steps: Rc<[i64]> = {
        let mut steps = BTreeSet::new();
        for run in &runs {
            for (step, _) in &run.keys {
                steps.insert(*step);
            }
        }
        steps.into_iter().collect::<Vec<_>>().into()
    };

    let total_steps = all_steps.len();
    let mut selected_step = use_signal({
        let persist_key = persist_key.clone();
        let latest_step = all_steps.last().copied();
        move || {
            persist_key
                .as_ref()
                .and_then(|k| GALLERY_STEP.with(|c| c.borrow_mut().get(k)))
                .or(latest_step)
        }
    });
    // Explicit navigation below writes through; resolving a disappeared step
    // to its successor doesn't (that isn't the user picking another step).
    let remember = {
        let persist_key = persist_key.clone();
        move |step: i64| {
            if let Some(k) = &persist_key {
                GALLERY_STEP.with(|c| c.borrow_mut().put(k.clone(), step));
            }
        }
    };

    if all_steps.is_empty() {
        if cdn_class.peek().is_some() {
            cdn_class.set(None);
        }
        return rsx! {
            div { class: "cdn-gallery", style: "height: {height}px;",
                div { class: "cdn-gallery-empty", "No CDN data" }
            }
        };
    }

    let selected = *selected_step.read();
    let idx = selected_step_index(&all_steps, selected).unwrap();
    let step = all_steps[idx];
    let current_step = idx + 1;
    let step_value_text = format!("Step {step}, {current_step} of {total_steps}");
    if selected.is_none() {
        selected_step.set(Some(step));
    }

    // Pair the fetch key with the manifest metadata it will populate. `ended` stays out of the key, so a status change doesn't refetch manifests.
    let mut run_keys: Vec<ManifestFetchKey> = Vec::new();
    let (mut pending_live, mut pending_ended) = (Vec::new(), Vec::new());
    for run in &runs {
        let Some((_, key)) = run.keys.iter().find(|(s, _)| *s == step) else {
            continue;
        };
        if key.starts_with("pending:") {
            let group = if run.ended {
                &mut pending_ended
            } else {
                &mut pending_live
            };
            group.push(run.label.as_str());
        }
        run_keys.push((
            RunManifest {
                project_id: run.project_id.clone(),
                run_id: run.run_id.clone(),
                metric_name: run.metric_name.clone(),
                label: run.label.clone(),
                color: run.color.clone(),
                manifest: None,
            },
            key.clone(),
        ));
    }
    let status = pending_status(&pending_live, &pending_ended);
    let single_run = runs.len() <= 1;

    // Track keys in signal for use_resource
    let mut keys_signal = use_signal(|| run_keys.clone());
    if *keys_signal.read() != run_keys {
        keys_signal.set(run_keys.clone());
    }

    // Fetch ALL manifests for this step (one per run) — concurrently, with
    // per-manifest retry, so one transiently-failing manifest neither
    // blocks the others nor settles as missing.
    let manifests = use_resource(move || {
        let keys = keys_signal.read().clone();
        async move {
            let source_keys = keys.clone();
            let futs = keys.into_iter().map(|(mut run_manifest, key)| async move {
                let manifest = if key.starts_with("pending:") {
                    None
                } else {
                    retry_visible("manifest", async || fetch_manifest(&key).await).await
                };
                run_manifest.manifest = manifest;
                run_manifest
            });
            (source_keys, futures::future::join_all(futs).await)
        }
    });

    // Classify the CDN sub-type as soon as a manifest is available and
    // publish it upward so the BindingEditor only shows applicable panels.
    let mut cdn_class_sig = cdn_class;
    let class_keys = run_keys.clone();
    use_effect(move || {
        let read = manifests.read();
        let next = current_manifest_results(&class_keys, read.as_ref())
            .and_then(classify_manifests)
            .map(|class| class.as_str().to_string());
        if *cdn_class_sig.peek() != next {
            cdn_class_sig.set(next);
        }
    });

    let (presentation, run_manifests) = {
        let read = manifests.read();
        let current = current_manifest_results(&run_keys, read.as_ref());
        let parsed_manifest_count = current.map(|run_manifests| {
            run_manifests
                .iter()
                .filter(|run_manifest| run_manifest.manifest.is_some())
                .count()
        });
        (
            gallery_presentation(status.is_some(), parsed_manifest_count),
            current.map(<[RunManifest]>::to_vec).unwrap_or_default(),
        )
    };
    // If single run, always use GroupByRun behavior.
    let effective_mode = if single_run {
        CdnDisplayMode::GroupByRun
    } else {
        display_mode
    };

    rsx! {
        div { class: "cdn-gallery", style: "height: {height}px;",
            // Step slider — pointless with a single step (e.g. info/run_info metadata, always step 0)
            if total_steps > 1 {
                div { class: "cdn-step-nav",
                    button {
                        class: "cdn-nav-btn icon-button",
                        title: "Previous step",
                        aria_label: "Previous step",
                        disabled: idx == 0,
                        onmousedown: primary({
                            let remember = remember.clone();
                            let all_steps = all_steps.clone();
                            move |_| {
                                let Some(i) = selected_step_index(&all_steps, *selected_step.read()) else {
                                    return;
                                };
                                if let Some(&step) = i.checked_sub(1).and_then(|i| all_steps.get(i)) {
                                    selected_step.set(Some(step));
                                    remember(step);
                                }
                            }
                        }),
                        CaretLeftIcon {}
                    }
                    {
                        let max_val = total_steps.saturating_sub(1);
                        let remember_slider = remember.clone();
                        let all_steps = all_steps.clone();
                        rsx! {
                            input {
                                r#type: "range",
                                class: "cdn-step-slider",
                                aria_label: "Gallery step",
                                aria_valuetext: "{step_value_text}",
                                min: "0",
                                max: "{max_val}",
                                value: "{idx}",
                                oninput: move |e: Event<FormData>| {
                                    if let Ok(v) = e.value().parse::<usize>() {
                                        let v = v.min(total_steps.saturating_sub(1));
                                        let step = all_steps[v];
                                        selected_step.set(Some(step));
                                        remember_slider(step);
                                    }
                                },
                            }
                        }
                    }
                    button {
                        class: "cdn-nav-btn icon-button",
                        title: "Next step",
                        aria_label: "Next step",
                        disabled: idx >= total_steps - 1,
                        onmousedown: primary({
                            let remember = remember.clone();
                            let all_steps = all_steps.clone();
                            move |_| {
                                let Some(i) = selected_step_index(&all_steps, *selected_step.read()) else {
                                    return;
                                };
                                if i + 1 < total_steps {
                                    let step = all_steps[i + 1];
                                    selected_step.set(Some(step));
                                    remember(step);
                                }
                            }
                        }),
                        CaretRightIcon {}
                    }
                    span { class: "cdn-step-label", "step {step} ({current_step}/{total_steps})" }
                }
            }

            // Content
            div { class: "cdn-gallery-content",
                if let Some(text) = status {
                    div {
                        class: if pending_live.is_empty() { "cdn-gallery-pending" } else { "cdn-gallery-pending live" },
                        title: "{text}",
                        span { class: "fade-overflow",
                            span { "{text}" }
                        }
                    }
                }
                match presentation {
                    GalleryPresentation::PendingOnly => rsx! {},
                    GalleryPresentation::Content => rsx! {
                        GalleryRenderer {
                            run_manifests,
                            mode: effective_mode,
                            metadata_diff_only,
                            persist_key: persist_key.clone(),
                        }
                    },
                    GalleryPresentation::Empty => rsx! {
                        div { class: "cdn-gallery-empty", "No manifests loaded" }
                    },
                    GalleryPresentation::Loading => rsx! {
                        div { class: "cdn-gallery-loading", "Loading..." }
                    },
                }
            }
        }
    }
}

/// Renders manifests according to the display mode.
#[component]
fn GalleryRenderer(
    run_manifests: Vec<RunManifest>,
    mode: CdnDisplayMode,
    #[props(default = false)] metadata_diff_only: bool,
    #[props(default)] persist_key: Option<String>,
) -> Element {
    let mut lightbox_src = use_signal(|| Option::<String>::None);
    let state = use_context::<crate::state::DashboardState>();

    let content = match classify_manifests(&run_manifests) {
        Some(CdnManifestClass::Mixed) => rsx! {
            div { class: "rect-error",
                "Metadata cannot share a panel with image or file sources. Split them into separate panels."
            }
        },
        Some(CdnManifestClass::Metadata) => {
            // Only read dashboard state if an actual run-info document needs
            // enrichment.
            let display_runs = if run_manifests
                .iter()
                .any(|run_manifest| run_manifest.metric_name == "info/run_info")
            {
                state.display_runs()
            } else {
                Vec::new()
            };
            let metadata_columns = metadata_columns(&run_manifests, &display_runs);
            rsx! {
                crate::components::metadata_viewer::MetadataViewer {
                    columns: metadata_columns,
                    diff_only: metadata_diff_only,
                }
            }
        }
        Some(CdnManifestClass::FileList) => rsx! {
        // Fallback: file list. Keep the source heading: different runs or
        // metrics commonly publish resources with the same filename.
            div { class: "cdn-file-list",
                for (source_index, (source_label, color, items)) in file_groups(&run_manifests).into_iter().enumerate() {
                    div {
                        class: "cdn-file-group",
                        style: "background: {color}11;",
                        div { class: "cdn-file-group-label", style: "color: {color};", "{source_label}" }
                        for (item_index, item) in items.iter().enumerate() {
                            {
                                let url = crate::runtime::cdn_url(&item.resource);
                                let label = item.display_name();
                                let accessible_label = gallery_file_link_label(
                                    label,
                                    source_label,
                                    source_index,
                                    item_index,
                                );
                                rsx! {
                                    a {
                                        class: "cdn-file-link",
                                        href: "{url}",
                                        target: "_blank",
                                        aria_label: "{accessible_label}",
                                        "{label}"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        Some(CdnManifestClass::ImageGallery) => match mode {
            CdnDisplayMode::SelectIndex => rsx! {
                SelectIndexView { run_manifests: run_manifests.clone(), on_lightbox: move |src: String| lightbox_src.set(Some(src)), persist_key }
            },
            CdnDisplayMode::GroupByRun => rsx! {
                GroupByRunView { run_manifests: run_manifests.clone(), on_lightbox: move |src: String| lightbox_src.set(Some(src)) }
            },
            CdnDisplayMode::Interleaved => rsx! {
                InterleavedView { run_manifests: run_manifests.clone(), on_lightbox: move |src: String| lightbox_src.set(Some(src)) }
            },
        },
        None => rsx! { div { class: "cdn-gallery-empty", "No manifests loaded" } },
    };

    rsx! {
        {content}

        if let Some(src) = lightbox_src.read().as_ref() {
            {
                let src = src.clone();
                rsx! {
                    div {
                        class: "cdn-lightbox",
                        // mousedown not click so a drag-release in the backdrop doesn't dismiss;
                        // primary button only so right-click (e.g. "save image as") keeps it open.
                        onmousedown: primary(move |_| lightbox_src.set(None)),
                        // Esc dismisses too. Focus on mount so the key lands here (the opening click may leave focus on the thumbnail link or <body>, whose keydowns never reach this div); consume it so an enclosing Esc layer (e.g. the maximize overlay) doesn't also dismiss.
                        tabindex: "-1",
                        onmounted: move |e| {
                            spawn(async move {
                                let _ = e.data().set_focus(true).await;
                            });
                        },
                        onkeydown: move |e: Event<KeyboardData>| {
                            if e.key() == Key::Escape {
                                e.stop_propagation();
                                lightbox_src.set(None);
                            }
                        },
                        img { class: "cdn-lightbox-img", src: "{src}" }
                    }
                }
            }
        }
    }
}

/// Mode 1: Select Index — slider picks an index, shows that index from each run.
#[component]
fn SelectIndexView(
    run_manifests: Vec<RunManifest>,
    on_lightbox: EventHandler<String>,
    #[props(default)] persist_key: Option<String>,
) -> Element {
    // Find max item count across all manifests
    let max_items = run_manifests
        .iter()
        .filter_map(|rm| rm.manifest.as_ref().map(|m| m.items.len()))
        .max()
        .unwrap_or(0);

    let mut selected_index = use_signal({
        let persist_key = persist_key.clone();
        move || remembered_gallery_index(persist_key.as_deref())
    });

    if max_items == 0 {
        return rsx! { div { class: "cdn-gallery-empty", "No items" } };
    }

    let idx = (*selected_index.read()).min(max_items - 1);

    rsx! {
        // Index slider
        div { class: "cdn-index-nav",
            span { class: "cdn-index-label", "index" }
            {
                let max_val = max_items.saturating_sub(1);
                let current = idx + 1;
                let value_text = format!("Image {current} of {max_items}");
                rsx! {
                    input {
                        r#type: "range",
                        class: "cdn-step-slider",
                        aria_label: "Image index",
                        aria_valuetext: "{value_text}",
                        min: "0",
                        max: "{max_val}",
                        value: "{idx}",
                        oninput: move |e: Event<FormData>| {
                            if let Ok(v) = e.value().parse::<usize>() {
                                let index = v.min(max_items.saturating_sub(1));
                                selected_index.set(index);
                                remember_gallery_index(persist_key.as_deref(), index);
                            }
                        },
                    }
                    span { class: "cdn-index-label", "{current}/{max_items}" }
                }
            }
        }

        // Grid: one image per run at the selected index
        div { class: "cdn-image-grid",
            for (source_index, rm) in run_manifests.iter().enumerate() {
                {gallery_run_image(rm, source_index, idx, on_lightbox)}
            }
        }
    }
}

/// Mode 2: Group by Run — one grid per run showing all images.
#[component]
fn GroupByRunView(run_manifests: Vec<RunManifest>, on_lightbox: EventHandler<String>) -> Element {
    rsx! {
        for (source_index, rm) in run_manifests.iter().enumerate() {
            {
                let color = rm.color.clone();
                let run_label = rm.label.clone();
                if let Some(m) = &rm.manifest {
                    rsx! {
                        div { class: "cdn-run-group", style: "background: {color}11;",
                            div { class: "cdn-run-group-label", style: "color: {color};", "{run_label}" }
                            div { class: "cdn-image-grid",
                                for (idx, item) in m.items.iter().enumerate() {
                                    div { class: "cdn-image-item",
                                        {gallery_image(item, &run_label, source_index, idx, on_lightbox)}
                                    }
                                }
                            }
                        }
                    }
                } else {
                    rsx! {}
                }
            }
        }
    }
}

/// Mode 3: Interleaved — one grid per index, each showing all runs at that index.
#[component]
fn InterleavedView(run_manifests: Vec<RunManifest>, on_lightbox: EventHandler<String>) -> Element {
    let max_items = run_manifests
        .iter()
        .filter_map(|rm| rm.manifest.as_ref().map(|m| m.items.len()))
        .max()
        .unwrap_or(0);

    rsx! {
        for idx in 0..max_items {
            div { class: "cdn-index-group",
                div { class: "cdn-index-group-label", "#{idx}" }
                div { class: "cdn-image-grid",
                    for (source_index, rm) in run_manifests.iter().enumerate() {
                        {gallery_run_image(rm, source_index, idx, on_lightbox)}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        classify_manifests, current_manifest_results, file_groups, gallery_file_link_label,
        gallery_presentation, gallery_thumbnail_label, metadata_columns, pending_status,
        remember_gallery_index, remembered_gallery_index, selected_step_index, CdnManifestClass,
        GalleryPresentation, Manifest, ManifestFetchKey, ManifestItem, RunManifest,
    };
    use dioxus::html::Modifiers;

    #[test]
    fn selected_gallery_step_survives_step_insertions() {
        assert_eq!(selected_step_index(&[0, 10], Some(10)), Some(1));
        assert_eq!(selected_step_index(&[0, 5, 10], Some(10)), Some(2));
        assert_eq!(selected_step_index(&[0, 10], Some(5)), Some(1));
        assert_eq!(selected_step_index(&[0, 10], Some(20)), Some(1));
        // A gallery that mounted empty starts at the latest step when data first arrives, then keeps that concrete selection as newer steps appear instead of following latest forever.
        assert_eq!(selected_step_index(&[-10, -5], None), Some(1));
        assert_eq!(selected_step_index(&[-10, -5, 0], Some(-5)), Some(1));
        assert_eq!(selected_step_index(&[], None), None);
    }

    #[test]
    fn gallery_thumbnail_names_identify_the_source_image_and_caption() {
        assert_eq!(
            gallery_thumbnail_label("run a", 0, None, 0),
            "Open image 1 from source 1 (run a) full-size"
        );
        assert_eq!(
            gallery_thumbnail_label("run a", 0, Some("   "), 1),
            "Open image 2 from source 1 (run a) full-size"
        );
        assert_eq!(
            gallery_thumbnail_label("run a", 1, Some("validation sample"), 0),
            "Open image 1 from source 2 (run a) full-size: validation sample"
        );
    }

    #[test]
    fn gallery_file_names_identify_duplicate_sources_and_items() {
        assert_ne!(
            gallery_file_link_label("checkpoint.bin", "run", 0, 0),
            gallery_file_link_label("checkpoint.bin", "run", 0, 1),
        );
        assert_ne!(
            gallery_file_link_label("checkpoint.bin", "run", 0, 0),
            gallery_file_link_label("checkpoint.bin", "run", 1, 0),
        );
        let unnamed = ManifestItem {
            resource: "sha256-resource".to_string(),
            filename: Some(String::new()),
            caption: None,
        };
        assert_eq!(
            gallery_file_link_label(unnamed.display_name(), "first run", 0, 0),
            "sha256-resource — file 1 from source 1 (first run)"
        );
    }

    #[test]
    fn selected_gallery_index_survives_a_panel_body_remount() {
        let key = "test-select-index-remount";
        assert_eq!(remembered_gallery_index(Some(key)), 0);
        remember_gallery_index(Some(key), 3);
        assert_eq!(remembered_gallery_index(Some(key)), 3);
        remember_gallery_index(None, 7);
        assert_eq!(remembered_gallery_index(Some(key)), 3);
    }

    #[test]
    fn gallery_lightbox_preserves_native_modified_clicks() {
        assert!(super::opens_gallery_lightbox(Modifiers::empty()));
        assert!(super::opens_gallery_lightbox(Modifiers::CAPS_LOCK));
        for modifier in [
            Modifiers::ALT,
            Modifiers::CONTROL,
            Modifiers::META,
            Modifiers::SHIFT,
        ] {
            assert!(!super::opens_gallery_lightbox(modifier));
        }
    }

    #[test]
    fn pending_uploads_do_not_hide_settled_manifests() {
        assert_eq!(
            gallery_presentation(true, Some(1)),
            GalleryPresentation::Content
        );
        assert_eq!(
            gallery_presentation(true, Some(0)),
            GalleryPresentation::PendingOnly
        );
        assert_eq!(
            gallery_presentation(true, None),
            GalleryPresentation::PendingOnly
        );
        assert_eq!(
            gallery_presentation(false, None),
            GalleryPresentation::Loading
        );
        assert_eq!(
            gallery_presentation(false, Some(0)),
            GalleryPresentation::Empty
        );
        assert_eq!(
            gallery_presentation(false, Some(2)),
            GalleryPresentation::Content
        );
    }

    #[test]
    fn pending_status_names_sources_by_run_liveness() {
        assert_eq!(pending_status(&[], &[]), None);
        assert_eq!(
            pending_status(&["a", "b"], &[]).as_deref(),
            Some("Not uploaded yet: a, b")
        );
        assert_eq!(
            pending_status(&[], &["a"]).as_deref(),
            Some("Upload did not finish: a")
        );
        assert_eq!(
            pending_status(&["live"], &["old"]).as_deref(),
            Some("Not uploaded yet: live · Upload did not finish: old")
        );
    }

    #[test]
    fn mixed_metadata_sources_are_rejected_instead_of_partially_rendered() {
        fn source(manifest: Option<Manifest>) -> RunManifest {
            RunManifest {
                project_id: "project".into(),
                run_id: "run".into(),
                metric_name: "metric".into(),
                label: "run/metric".into(),
                color: "#123456".into(),
                manifest,
            }
        }
        fn manifest(class: &str, filename: Option<&str>) -> Manifest {
            Manifest {
                v: 1,
                class: class.into(),
                items: (class != "metadata")
                    .then(|| ManifestItem {
                        resource: "resource".into(),
                        filename: filename.map(str::to_owned),
                        caption: None,
                    })
                    .into_iter()
                    .collect(),
                data: (class == "metadata").then(|| serde_json::json!({"value": 1})),
            }
        }

        let metadata = source(Some(manifest("metadata", None)));
        let image = source(Some(manifest("image_gallery", None)));
        let resource = source(Some(manifest("image_gallery", Some("report.pdf"))));
        let missing = source(None);

        assert_eq!(
            classify_manifests(&[missing.clone(), metadata.clone(), image.clone()]),
            Some(CdnManifestClass::Mixed)
        );
        assert_eq!(
            classify_manifests(&[image.clone(), metadata.clone()]),
            Some(CdnManifestClass::Mixed)
        );
        assert_eq!(
            classify_manifests(&[metadata.clone(), resource.clone()]),
            Some(CdnManifestClass::Mixed)
        );
        assert_eq!(
            classify_manifests(&[image, resource]),
            Some(CdnManifestClass::FileList)
        );
        assert_eq!(
            classify_manifests(&[missing, metadata]),
            Some(CdnManifestClass::Metadata)
        );
        assert_eq!(classify_manifests(&[source(None), source(None)]), None);
        assert_eq!(
            classify_manifests(&[source(Some(manifest("image_gallery", None)))]),
            Some(CdnManifestClass::ImageGallery)
        );
    }

    #[test]
    fn retained_results_do_not_cross_manifest_key_generations() {
        fn fetch_key(key: &str) -> ManifestFetchKey {
            (
                RunManifest {
                    project_id: "project".to_string(),
                    run_id: "run".to_string(),
                    metric_name: "gallery".to_string(),
                    label: "run".to_string(),
                    color: "#123456".to_string(),
                    manifest: None,
                },
                key.to_string(),
            )
        }

        let old_keys = vec![fetch_key("old.json")];
        let current_keys = vec![fetch_key("pending:new")];
        let result = (old_keys.clone(), vec![old_keys[0].0.clone()]);

        assert!(current_manifest_results(&current_keys, Some(&result)).is_none());
        assert!(current_manifest_results(&old_keys, Some(&result)).is_some());
    }

    #[test]
    fn metadata_columns_preserve_source_labels_and_colors() {
        fn source(metric_name: &str, label: &str, color: &str) -> RunManifest {
            RunManifest {
                project_id: "project".to_string(),
                run_id: "same-run".to_string(),
                metric_name: metric_name.to_string(),
                label: label.to_string(),
                color: color.to_string(),
                manifest: Some(Manifest {
                    v: 1,
                    class: "metadata".to_string(),
                    items: Vec::new(),
                    data: Some(serde_json::json!({"source": metric_name})),
                }),
            }
        }

        let columns = metadata_columns(
            &[
                source("config/model", "run/config/model", "#111111"),
                source("config/data", "run/config/data", "#222222"),
            ],
            &[],
        );

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].label, "run/config/model");
        assert_eq!(columns[0].color, "#111111");
        assert_eq!(columns[1].label, "run/config/data");
        assert_eq!(columns[1].color, "#222222");
        assert_ne!(columns[0].data, columns[1].data);
    }

    #[test]
    fn file_groups_preserve_source_identity_for_duplicate_filenames() {
        fn source(label: &str, color: &str, resource: &str) -> RunManifest {
            RunManifest {
                project_id: "project".to_string(),
                run_id: label.to_string(),
                metric_name: "artifacts/checkpoint".to_string(),
                label: label.to_string(),
                color: color.to_string(),
                manifest: Some(Manifest {
                    v: 1,
                    class: "image_gallery".to_string(),
                    items: vec![super::ManifestItem {
                        resource: resource.to_string(),
                        filename: Some("checkpoint.bin".to_string()),
                        caption: None,
                    }],
                    data: None,
                }),
            }
        }

        let manifests = [
            source("first run", "#111111", "first.bin"),
            source("second run", "#222222", "second.bin"),
        ];
        let groups = file_groups(&manifests);

        assert_eq!(groups.len(), 2);
        assert_eq!((groups[0].0, groups[0].1), ("first run", "#111111"));
        assert_eq!((groups[1].0, groups[1].1), ("second run", "#222222"));
        assert_eq!(groups[0].2[0].display_name(), "checkpoint.bin");
        assert_eq!(groups[1].2[0].display_name(), "checkpoint.bin");
        assert_ne!(groups[0].2[0].resource, groups[1].2[0].resource);
    }

    #[test]
    fn manifest_equality_includes_rendered_metadata() {
        let first: Manifest = serde_json::from_value(serde_json::json!({
            "v": 1,
            "class": "metadata",
            "items": [],
            "data": {"config": {"learning_rate": 0.1}}
        }))
        .unwrap();
        let second: Manifest = serde_json::from_value(serde_json::json!({
            "v": 1,
            "class": "metadata",
            "items": [],
            "data": {"config": {"learning_rate": 0.2}}
        }))
        .unwrap();

        assert_ne!(first, second);
    }

    #[test]
    fn resource_manifests_use_file_labels_instead_of_image_rendering() {
        let images: Manifest = serde_json::from_value(serde_json::json!({
            "class": "image_gallery",
            "items": [
                {"resource": "image.png", "content_type": "image/png"},
                {"resource": "legacy.jpg"},
                {"resource": "bitmap.bmp", "content_type": "application/octet-stream"}
            ]
        }))
        .unwrap();
        let resources: Manifest = serde_json::from_value(serde_json::json!({
            "class": "image_gallery",
            "items": [{
                "resource": "report-key.pdf",
                "content_type": "application/pdf",
                "filename": "report.pdf"
            }]
        }))
        .unwrap();
        let mixed: Manifest = serde_json::from_value(serde_json::json!({
            "class": "image_gallery",
            "items": [
                {"resource": "image.png", "content_type": "image/png"},
                {
                    "resource": "report-key.pdf",
                    "content_type": "application/pdf",
                    "filename": ""
                }
            ]
        }))
        .unwrap();

        assert!(images.is_image_gallery());
        assert!(!resources.is_image_gallery());
        assert_eq!(resources.items[0].display_name(), "report.pdf");
        assert!(!mixed.is_image_gallery());
        assert_eq!(mixed.items[1].display_name(), "report-key.pdf");

        let empty: Manifest = serde_json::from_value(serde_json::json!({
            "class": "image_gallery",
            "items": []
        }))
        .unwrap();
        assert!(empty.is_image_gallery());

        let metadata: Manifest = serde_json::from_value(serde_json::json!({
            "class": "metadata",
            "items": [{"resource": "image.png", "content_type": "image/png"}]
        }))
        .unwrap();
        assert!(!metadata.is_image_gallery());
    }
}
