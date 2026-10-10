use std::cell::RefCell;
use std::collections::BTreeSet;

use dioxus::prelude::*;
use dioxus::web::WebEventExt;
use wasm_bindgen::JsCast;

use crate::components::copy_text::CopyText;
use crate::components::icons::{CaretDownIcon, CaretLeftIcon, CaretRightIcon, CaretUpIcon};
use crate::components::metric_rect::CdnRunData;
use crate::route::Route;
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

/// `steps` is non-empty.
fn selected_step_index(steps: &[i64], selected_step: Option<i64>) -> usize {
    match selected_step {
        Some(step) => steps
            .binary_search(&step)
            .unwrap_or_else(|insertion| insertion.min(steps.len() - 1)),
        // The signal can still be None when the gallery first mounted with no data. Preserve the normal untouched-gallery default when steps later arrive: start at latest (the caller then stores that concrete step).
        None => steps.len() - 1,
    }
}

/// No Alt, Ctrl, Meta or Shift: only a plain click opens the lightbox (modified clicks keep the link's native behaviour), and only plain arrow keys move it (Alt+← and Cmd+← are browser Back).
fn unmodified(modifiers: dioxus::html::Modifiers) -> bool {
    !modifiers.alt() && !modifiers.ctrl() && !modifiers.meta() && !modifiers.shift()
}

/// The image the lightbox shows: its (project, run, metric) source and its index in that source's manifest, which together identify an entry in a panel. The image itself is looked up on every render, which keeps the lightbox open across step changes and refetches. Its `Debug` spelling is the thumbnail's `data-image`, which Esc matches.
#[derive(Clone, Debug, PartialEq)]
struct LightboxImage {
    project_id: String,
    run_id: String,
    metric_name: String,
    item: usize,
}

/// Where ←/→ go from the open image, as (source, item) pairs.
#[derive(Debug, PartialEq)]
struct LightboxNeighbours {
    previous: Option<(usize, usize)>,
    next: Option<(usize, usize)>,
    /// The open image's 1-based place and the image count, when it exists at the shown step.
    position: Option<(usize, usize)>,
}

/// ←/→ neighbours of the open image `(source, item)`: source-major in GroupByRun, index-major in Interleaved and SelectIndex (whose slider follows). `counts[source]` is that source's item count at the shown step; an open image missing there (a placeholder, a shorter manifest) still has neighbours around its place.
fn lightbox_neighbours(
    mode: CdnDisplayMode,
    counts: &[usize],
    open: (usize, usize),
) -> LightboxNeighbours {
    let order = |&(source, item): &(usize, usize)| match mode {
        CdnDisplayMode::GroupByRun => (source, item),
        CdnDisplayMode::Interleaved | CdnDisplayMode::SelectIndex => (item, source),
    };
    let mut images: Vec<(usize, usize)> = counts
        .iter()
        .enumerate()
        .flat_map(|(source, &count)| (0..count).map(move |item| (source, item)))
        .collect();
    images.sort_unstable_by_key(order);
    // The images before `at` sort before the open image, which sits at `at` when it exists.
    let at = images.partition_point(|image| order(image) < order(&open));
    let present = images.get(at) == Some(&open);
    LightboxNeighbours {
        previous: at.checked_sub(1).map(|i| images[i]),
        next: images.get(at + usize::from(present)).copied(),
        position: present.then(|| (at + 1, images.len())),
    }
}

/// The step ↑ (`later`) or ↓ takes the lightbox to: the open source's own next or previous logged step, so it skips steps only other sources logged. Placeholder steps count; the lightbox shows their upload status.
fn adjacent_source_step(keys: &[(i64, String)], step: i64, later: bool) -> Option<i64> {
    let steps = keys.iter().map(|(s, _)| *s);
    if later {
        steps.filter(|&s| s > step).min()
    } else {
        steps.filter(|&s| s < step).max()
    }
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
    run: &RunManifest,
    source_index: usize,
    image_index: usize,
    on_lightbox: EventHandler<LightboxImage>,
) -> Element {
    let url = crate::runtime::cdn_url(&item.resource);
    let accessible_label = gallery_thumbnail_label(
        &run.label,
        source_index,
        item.caption.as_deref(),
        image_index,
    );
    let open = LightboxImage {
        project_id: run.project_id.clone(),
        run_id: run.run_id.clone(),
        metric_name: run.metric_name.clone(),
        item: image_index,
    };
    rsx! {
        a {
            href: "{url}",
            target: "_blank",
            aria_label: "{accessible_label}",
            // Esc in the preview hands focus back by it (`gallery_thumbnail`): a URL can name several entries, as identical images share a content-addressed key.
            "data-image": "{open:?}",
            onclick: move |event: Event<MouseData>| {
                if unmodified(event.modifiers()) {
                    event.prevent_default();
                    on_lightbox.call(open.clone());
                }
            },
            img { src: "{url}", loading: "lazy", alt: "" }
        }
        if let Some(caption) = &item.caption {
            CopyText { text: caption.clone(), class: "cdn-caption" }
        }
    }
}

/// The link `gallery_image` drew for `image` in the gallery holding `inside`.
fn gallery_thumbnail(
    inside: &web_sys::Element,
    image: &LightboxImage,
) -> Option<web_sys::HtmlElement> {
    let identity = format!("{image:?}");
    let links = inside
        .closest(".cdn-gallery")
        .ok()??
        .query_selector_all("[data-image]")
        .ok()?;
    (0..links.length())
        .filter_map(|i| links.item(i)?.dyn_into::<web_sys::HtmlElement>().ok())
        .find(|link| link.get_attribute("data-image").as_deref() == Some(identity.as_str()))
}

fn gallery_run_image(
    run: &RunManifest,
    source_index: usize,
    idx: usize,
    on_lightbox: EventHandler<LightboxImage>,
) -> Element {
    let Some(item) = run.manifest.as_ref().and_then(|m| m.items.get(idx)) else {
        return rsx! {};
    };
    rsx! {
        div {
            class: "cdn-image-item",
            style: "background: {run.color}22;",
            {gallery_image(item, run, source_index, idx, on_lightbox)}
            div {
                class: "cdn-run-label fade-overflow",
                style: "color: {run.color};",
                title: "{run.label}",
                span { "{run.label}" }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MediaKind {
    Video,
    Audio,
}

impl ManifestItem {
    fn display_name(&self) -> &str {
        self.filename
            .as_deref()
            .filter(|filename| !filename.is_empty())
            .unwrap_or(&self.resource)
    }

    /// Which inline player this resource gets, by the CDN key's extension: the CDN serves that extension's MIME type (kymo-server/src/cdn.rs) whatever the filename says, and the client uploads an unlisted extension as `.bin`. `.ogg` is audio because the CDN serves it as audio/ogg.
    fn media_kind(&self) -> Option<MediaKind> {
        let (_, extension) = self.resource.rsplit_once('.')?;
        match extension.to_ascii_lowercase().as_str() {
            "mp4" | "webm" => Some(MediaKind::Video),
            "mp3" | "wav" | "ogg" => Some(MediaKind::Audio),
            _ => None,
        }
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
    use CdnManifestClass::{FileList, ImageGallery, Metadata, Mixed};
    run_manifests
        .iter()
        .filter_map(|run_manifest| run_manifest.manifest.as_ref())
        .map(|manifest| {
            if manifest.class == "metadata" {
                Metadata
            } else if manifest.is_image_gallery() {
                ImageGallery
            } else {
                FileList
            }
        })
        .reduce(|left, right| match (left, right) {
            (Metadata, Metadata) => Metadata,
            (ImageGallery, ImageGallery) => ImageGallery,
            (ImageGallery | FileList, ImageGallery | FileList) => FileList,
            _ => Mixed,
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

/// Ok(None) is settled knowledge — a 404 (nothing at that key), a 400 (the
/// hosted route's answer to a malformed key, which ingest stores unvalidated)
/// or unparseable content, none of which heals on retry. Err (a network
/// failure or any other status) must be retried: a finished run's keys never
/// change, so a failure settled into the manifests resource would never be
/// refetched.
async fn fetch_manifest(cdn_key: &str) -> Result<Option<Manifest>, String> {
    let url = crate::runtime::cdn_url(cdn_key);
    let resp = gloo_net::http::Request::get(&url)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if matches!(resp.status(), 400 | 404) {
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
    display_mode: CdnDisplayMode,
    /// Signal the gallery writes to once the manifest class is known
    /// ("image_gallery" / "metadata" / "file_list" / "mixed"). Lets the editor hide
    /// panels that don't apply.
    mut cdn_class: Signal<Option<String>>,
    /// Metadata viewer: when true, hide rows whose values are identical
    /// across all runs (and any branches whose subtree contains no diffs).
    metadata_diff_only: bool,
    /// Key for remembering the selected step and index across body unmounts (GALLERY_STEP, GALLERY_INDEX).
    persist_key: String,
) -> Element {
    let all_steps: Vec<i64> = {
        let mut steps = BTreeSet::new();
        for run in &runs {
            for (step, _) in &run.keys {
                steps.insert(*step);
            }
        }
        steps.into_iter().collect()
    };

    let total_steps = all_steps.len();
    let mut selected_step = use_signal(|| {
        GALLERY_STEP
            .with(|c| c.borrow_mut().get(&persist_key))
            .or(all_steps.last().copied())
    });
    // Explicit navigation (the step controls, the lightbox's ↑/↓) goes through here and writes through; resolving a disappeared step to its successor doesn't (that isn't the user picking another step).
    let pick_step = use_callback({
        let persist_key = persist_key.clone();
        move |step: i64| {
            selected_step.set(Some(step));
            GALLERY_STEP.with(|c| c.borrow_mut().put(persist_key.clone(), step));
        }
    });
    // Select index's slider, held here beside the step so it can follow the lightbox.
    let mut selected_index = use_signal(|| {
        GALLERY_INDEX
            .with(|c| c.borrow_mut().get(&persist_key))
            .unwrap_or(0)
    });
    let pick_index = use_callback(move |index: usize| {
        selected_index.set(index);
        GALLERY_INDEX.with(|c| c.borrow_mut().put(persist_key.clone(), index));
    });
    // Held here rather than in GalleryRenderer, which unmounts whenever the step or its manifests are refetched.
    let mut lightbox = use_signal(|| None::<LightboxImage>);

    if all_steps.is_empty() {
        if cdn_class.peek().is_some() {
            cdn_class.set(None);
        }
        if lightbox.peek().is_some() {
            lightbox.set(None);
        }
        return rsx! {
            div { class: "cdn-gallery",
                div { class: "cdn-gallery-empty", "No CDN data" }
            }
        };
    }

    let selected = *selected_step.read();
    let idx = selected_step_index(&all_steps, selected);
    let step = all_steps[idx];
    let previous_step = idx.checked_sub(1).map(|i| all_steps[i]);
    let next_step = all_steps.get(idx + 1).copied();
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

    // Fetch this step's manifests concurrently, retrying each until it settles; the step publishes once every source has settled.
    let manifests = use_resource(use_reactive((&run_keys,), move |(keys,)| async move {
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
    }));

    // Publish the settled step's CDN sub-type upward so the BindingEditor only shows applicable panels.
    let class_keys = run_keys.clone();
    use_effect(move || {
        let read = manifests.read();
        let next = current_manifest_results(&class_keys, read.as_ref())
            .and_then(classify_manifests)
            .map(|class| class.as_str().to_string());
        if *cdn_class.peek() != next {
            cdn_class.set(next);
        }
    });

    let (presentation, run_manifests, lightbox_view) = {
        let read = manifests.read();
        let current = current_manifest_results(&run_keys, read.as_ref());
        let parsed_manifest_count = current.map(|run_manifests| {
            run_manifests
                .iter()
                .filter(|run_manifest| run_manifest.manifest.is_some())
                .count()
        });
        // An open lightbox shows the last settled manifests, current or not, so its image stays up while the next step loads.
        let lightbox_view = lightbox()
            .zip(read.as_ref())
            .map(|(open, (_, settled))| (open, settled.clone(), current.is_none()));
        (
            gallery_presentation(status.is_some(), parsed_manifest_count),
            current.map(<[RunManifest]>::to_vec).unwrap_or_default(),
            lightbox_view,
        )
    };
    let effective_mode = if runs.len() <= 1 {
        CdnDisplayMode::GroupByRun
    } else {
        display_mode
    };
    // While the lightbox is open in Select index, the slider shows its image, so the panel draws the thumbnail Esc focuses.
    // One rule covers ←/→, opening a thumbnail at an index the slider was clamped away from, and a run joining a one-run panel turning Select index on.
    if effective_mode == CdnDisplayMode::SelectIndex {
        if let Some(item) = lightbox.peek().as_ref().map(|open| open.item) {
            if *selected_index.peek() != item {
                pick_index.call(item);
            }
        }
    }

    rsx! {
        div { class: "cdn-gallery",
            // Step slider — pointless with a single step (e.g. info/run_info metadata, always step 0)
            if total_steps > 1 {
                // MAXIMIZE_KEYS_JS (dashboard_layout.rs) presses the step buttons by aria-label for ↑/↓ in a maximized panel.
                div { class: "cdn-step-nav",
                    button {
                        class: "cdn-nav-btn icon-button",
                        title: "Previous step",
                        aria_label: "Previous step",
                        disabled: previous_step.is_none(),
                        onmousedown: primary(move |_| {
                            if let Some(step) = previous_step {
                                pick_step.call(step);
                            }
                        }),
                        CaretLeftIcon {}
                    }
                    input {
                        r#type: "range",
                        class: "cdn-step-slider",
                        aria_label: "Gallery step",
                        aria_valuetext: "{step_value_text}",
                        min: "0",
                        max: "{total_steps - 1}",
                        value: "{idx}",
                        oninput: move |e: Event<FormData>| {
                            if let Ok(v) = e.value().parse::<usize>() {
                                pick_step.call(all_steps[v.min(total_steps - 1)]);
                            }
                        },
                    }
                    button {
                        class: "cdn-nav-btn icon-button",
                        title: "Next step",
                        aria_label: "Next step",
                        disabled: next_step.is_none(),
                        onmousedown: primary(move |_| {
                            if let Some(step) = next_step {
                                pick_step.call(step);
                            }
                        }),
                        CaretRightIcon {}
                    }
                    span { class: "cdn-step-label", "step {step} ({current_step}/{total_steps})" }
                }
            }

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
                            selected_index,
                            on_index: pick_index,
                            on_lightbox: move |open| lightbox.set(Some(open)),
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

            if let Some((open, shown, loading)) = lightbox_view {
                GalleryLightbox {
                    open,
                    runs,
                    shown,
                    loading,
                    mode: effective_mode,
                    step,
                    lightbox,
                    on_step: pick_step,
                }
            }
        }
    }
}

/// What a lightbox arrow key or button does.
#[derive(Clone)]
enum LightboxMove {
    Image(LightboxImage),
    Step(i64),
}

/// The full-size view of one gallery image. ←/→ walk the panel's images (`lightbox_neighbours`), ↑/↓ walk the open image's source through its steps (moving the panel's step), and the buttons do the same for pointers.
#[component]
fn GalleryLightbox(
    open: LightboxImage,
    runs: Vec<CdnRunData>,
    /// The last settled manifests; another step's while `loading`.
    shown: Vec<RunManifest>,
    loading: bool,
    mode: CdnDisplayMode,
    /// The panel's step, which ↑/↓ move.
    step: i64,
    mut lightbox: Signal<Option<LightboxImage>>,
    on_step: EventHandler<i64>,
) -> Element {
    // Navigating (Back, Forward) closes it: a preview opened on one page would otherwise stay modal over the next (Forward into a maximized chart), holding its keys.
    let opened_on = use_hook(|| router().current::<Route>());
    use_effect(move || {
        if router().current::<Route>() != opened_on {
            lightbox.set(None);
        }
    });
    // Each run's settled image manifest at the shown step, aligned with `runs`. A metric can log resources at other steps (the panel lists those as files).
    let manifests: Vec<Option<&Manifest>> = runs
        .iter()
        .map(|run| {
            shown
                .iter()
                .find(|settled| {
                    (&settled.project_id, &settled.run_id, &settled.metric_name)
                        == (&run.project_id, &run.run_id, &run.metric_name)
                })
                .and_then(|settled| settled.manifest.as_ref())
                .filter(|manifest| manifest.is_image_gallery())
        })
        .collect();
    let source = runs.iter().position(|run| {
        (&run.project_id, &run.run_id, &run.metric_name)
            == (&open.project_id, &open.run_id, &open.metric_name)
    });
    let run = source.map(|source| &runs[source]);
    let item = source
        .and_then(|source| manifests[source])
        .and_then(|manifest| manifest.items.get(open.item));

    let counts: Vec<usize> = manifests
        .iter()
        .map(|manifest| manifest.map_or(0, |manifest| manifest.items.len()))
        .collect();
    let neighbours = source.map(|source| lightbox_neighbours(mode, &counts, (source, open.item)));
    let image = |image: Option<(usize, usize)>| {
        image.map(|(source, item)| {
            let run = &runs[source];
            LightboxMove::Image(LightboxImage {
                project_id: run.project_id.clone(),
                run_id: run.run_id.clone(),
                metric_name: run.metric_name.clone(),
                item,
            })
        })
    };
    let left = image(neighbours.as_ref().and_then(|n| n.previous));
    let right = image(neighbours.as_ref().and_then(|n| n.next));
    // Prefetch the images ←/→ go to: lazy thumbnails may not have loaded them, and Safari showed about 0.3 s of empty stage per press on production images. Unkeyed, as both can be one URL (identical images share a content-addressed key).
    // A known inefficiency, kept: they carry no priority, so they queue with the shown image on the CDN's six HTTP/1.1 connections and can finish ahead of it (measured on production with a cold cache: by 0.17 s in Firefox and 0.02 s in WebKit, not in Chromium), though the other panels' lazy thumbnails in flight outnumber them. fetchpriority="low" changes nothing for a hidden image in Chromium, and fetching them only once the shown image has loaded would lose the head start that had already requested the image a quick → lands on.
    let ahead: Vec<String> = neighbours
        .iter()
        .flat_map(|n| [n.previous, n.next])
        .flatten()
        .filter_map(|(source, item)| manifests[source]?.items.get(item))
        .map(|item| crate::runtime::cdn_url(&item.resource))
        .collect();
    let source_step = |later: bool| {
        run.and_then(|run| adjacent_source_step(&run.keys, step, later))
            .map(LightboxMove::Step)
    };
    let (up, down) = (source_step(true), source_step(false));
    let position = neighbours.and_then(|n| n.position).filter(|_| !loading);

    let mut go = move |to: Option<LightboxMove>| match to {
        Some(LightboxMove::Image(open)) => lightbox.set(Some(open)),
        Some(LightboxMove::Step(step)) => on_step.call(step),
        None => {}
    };
    // A press on a control, with any button, neither closes the lightbox nor takes focus from it (a focused control that became disabled would drop focus to <body>, out of reach of the keys); a disabled control still swallows the press.
    let control = |to: &Option<LightboxMove>, label: &str, key: &str, icon: Element| {
        let mut act = primary({
            let to = to.clone();
            move |_| go(to.clone())
        });
        rsx! {
            button {
                class: "cdn-lightbox-nav icon-button",
                title: "{label} ({key})",
                aria_label: "{label}",
                disabled: to.is_none(),
                onmousedown: move |e: Event<MouseData>| {
                    e.stop_propagation();
                    e.prevent_default();
                    act(e);
                },
                {icon}
            }
        }
    };
    let previous_image = control(&left, "Previous image", "←", rsx! { CaretLeftIcon {} });
    let next_image = control(&right, "Next image", "→", rsx! { CaretRightIcon {} });
    let previous_step = control(&down, "Previous step", "↓", rsx! { CaretDownIcon {} });
    let next_step = control(&up, "Next step", "↑", rsx! { CaretUpIcon {} });

    let body = match (item, run) {
        (Some(item), _) => {
            let url = crate::runtime::cdn_url(&item.resource);
            // Keyed by URL: a reused <img> would keep painting the previous picture, under this one's info line, until the new one loads. No alt: the info line names the image.
            rsx! {
                img { key: "{url}", class: "cdn-lightbox-img", src: "{url}", alt: "" }
            }
        }
        // Nothing of this image to keep up until the step loads.
        (None, _) if loading => rsx! {},
        (None, None) => rsx! { div { class: "cdn-lightbox-note", "No longer in this panel" } },
        (None, Some(run)) => {
            let pending = run
                .keys
                .iter()
                .any(|(s, key)| *s == step && key.starts_with("pending:"));
            let note = match (pending, run.ended) {
                (true, false) => "Not uploaded yet".to_string(),
                (true, true) => "Upload did not finish".to_string(),
                (false, _) => format!("No image {} at step {step}", open.item + 1),
            };
            rsx! { div { class: "cdn-lightbox-note", "{note}" } }
        }
    };

    rsx! {
        // A modal dialog in the browser's top layer, which no ancestor can clip or cover: Safari clipped a fixed overlay to the content row, under the navbar.
        dialog {
            class: "cdn-lightbox",
            aria_label: "Full-size image",
            tabindex: "-1",
            onmounted: move |event| {
                let dialog = event.as_web_event().unchecked_into::<web_sys::HtmlDialogElement>();
                // Shown in the flush that inserts it, so no key meets it as a plain dialog. PREVIEW_KEYS_JS (main.rs) recovers a real Firefox window's later, eventless focus loss.
                let _ = dialog.show_modal();
                // The dialog itself, not a control (Chromium picks the first button, ignoring autofocus on the dialog): a focused control that turns disabled drops focus out from under the keys.
                let _ = dialog.focus();
            },
            // Esc: closed here first (the default action then finds it closed), as nothing behind a modal dialog can take focus; then the last shown image's thumbnail takes it, so Tab and Enter carry on from there. With none drawn (a step loading), close restores the opener if it survives. A pointer close leaves focus alone: Safari would ring the thumbnail.
            oncancel: move |e| {
                let dialog = e.as_web_event().target().unwrap().unchecked_into::<web_sys::HtmlDialogElement>();
                dialog.close();
                if let Some(thumbnail) = gallery_thumbnail(&dialog, &open) {
                    // One out of its scroller's view (SelectIndex scrolls its grid, the other modes the whole content) goes to the top edge first: focus() would centre it, and the unloaded squares around it would then grow as they load and push it out of view; above the top edge, scroll anchoring absorbs their growth.
                    if let Some(scroller) = thumbnail.closest(".cdn-gallery-content > .cdn-image-grid, .cdn-gallery-content").ok().flatten() {
                        let (at, view) = (thumbnail.get_bounding_client_rect(), scroller.get_bounding_client_rect());
                        if at.top() < view.top() || at.bottom() > view.bottom() {
                            scroller.scroll_by_with_x_and_y(0.0, at.top() - view.top());
                        }
                    }
                    let _ = thumbnail.focus();
                }
                lightbox.set(None);
            },
            // mousedown, not click, so a drag-release in the backdrop doesn't dismiss; primary button only, so right-click (e.g. "save image as") keeps it open.
            onmousedown: primary(move |_| lightbox.set(None)),
            // WebKit chained a wheel over the preview to the grid's scroller, and a panel scrolled out of the band unmounts, preview and all. Ctrl+wheel is a pinch-zoom, left alone.
            onwheel: move |e: Event<WheelData>| {
                if !e.modifiers().ctrl() {
                    e.prevent_default();
                }
            },
            onkeydown: move |e: Event<KeyboardData>| {
                let to = match e.key() {
                    // Nothing in here takes focus, so Tab (and Shift+Tab) would only move it out from under the keys.
                    Key::Tab => &None,
                    _ if !unmodified(e.modifiers()) => return,
                    Key::ArrowLeft => &left,
                    Key::ArrowRight => &right,
                    Key::ArrowUp => &up,
                    Key::ArrowDown => &down,
                    _ => return,
                };
                // Consumed at the ends too, so nothing beneath acts on it.
                e.prevent_default();
                go(to.clone());
            },
            {previous_image}
            div { class: "cdn-lightbox-stage", {body} }
            for url in ahead {
                img { src: "{url}", alt: "", hidden: true }
            }
            {next_image}
            div {
                class: "cdn-lightbox-info",
                // Selecting the caption must not dismiss.
                onmousedown: move |e| e.stop_propagation(),
                if let Some(run) = run {
                    span { class: "cdn-lightbox-run fade-overflow fade-lines", style: "color: {run.color};", title: "{run.label}",
                        span { "{run.label}" }
                    }
                }
                // Plain text, not CopyText: a focused caption that unmounts (the next image has none) would leave focus on <body>, out of reach of the keys.
                if let Some(caption) = item.and_then(|item| item.caption.as_ref()) {
                    span { class: "cdn-lightbox-caption fade-overflow fade-lines", title: "{caption}",
                        span { "{caption}" }
                    }
                }
                span { class: "cdn-lightbox-step",
                    {previous_step}
                    if loading {
                        "loading step {step}…"
                    } else {
                        "step {step}"
                    }
                    {next_step}
                }
                if let Some((place, total)) = position {
                    span { class: "cdn-lightbox-position", "{place} / {total}" }
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
    metadata_diff_only: bool,
    selected_index: ReadSignal<usize>,
    on_index: EventHandler<usize>,
    on_lightbox: EventHandler<LightboxImage>,
) -> Element {
    let state = use_context::<crate::state::DashboardState>();

    match classify_manifests(&run_manifests) {
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
                Default::default()
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
                                    // preload="none": nothing loads until the user presses play.
                                    match item.media_kind() {
                                        Some(MediaKind::Video) => rsx! {
                                            video {
                                                preload: "none",
                                                controls: true,
                                                playsinline: true,
                                                src: "{url}",
                                                aria_label: "{accessible_label}",
                                            }
                                        },
                                        Some(MediaKind::Audio) => rsx! {
                                            audio {
                                                preload: "none",
                                                controls: true,
                                                src: "{url}",
                                                aria_label: "{accessible_label}",
                                            }
                                        },
                                        None => rsx! {},
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
                SelectIndexView { run_manifests, selected_index, on_index, on_lightbox }
            },
            CdnDisplayMode::GroupByRun => rsx! {
                GroupByRunView { run_manifests, on_lightbox }
            },
            CdnDisplayMode::Interleaved => rsx! {
                InterleavedView { run_manifests, on_lightbox }
            },
        },
        None => rsx! { div { class: "cdn-gallery-empty", "No manifests loaded" } },
    }
}

/// Mode 1: Select Index — slider picks an index, shows that index from each run.
#[component]
fn SelectIndexView(
    run_manifests: Vec<RunManifest>,
    /// Read here, so a slider move re-renders this view alone.
    selected_index: ReadSignal<usize>,
    on_index: EventHandler<usize>,
    on_lightbox: EventHandler<LightboxImage>,
) -> Element {
    let max_items = run_manifests
        .iter()
        .filter_map(|rm| rm.manifest.as_ref().map(|m| m.items.len()))
        .max()
        .unwrap_or(0);

    if max_items == 0 {
        return rsx! { div { class: "cdn-gallery-empty", "No items" } };
    }

    let idx = selected_index().min(max_items - 1);
    let current = idx + 1;

    rsx! {
        div { class: "cdn-index-nav",
            span { class: "cdn-index-label", "index" }
            input {
                r#type: "range",
                class: "cdn-step-slider",
                aria_label: "Image index",
                aria_valuetext: "Image {current} of {max_items}",
                min: "0",
                max: "{max_items - 1}",
                value: "{idx}",
                oninput: move |e: Event<FormData>| {
                    if let Ok(index) = e.value().parse::<usize>() {
                        on_index.call(index);
                    }
                },
            }
            span { class: "cdn-index-label", "{current}/{max_items}" }
        }

        div { class: "cdn-image-grid",
            for (source_index, rm) in run_manifests.iter().enumerate() {
                {gallery_run_image(rm, source_index, idx, on_lightbox)}
            }
        }
    }
}

/// Mode 2: Group by Run — one grid per run showing all images.
#[component]
fn GroupByRunView(
    run_manifests: Vec<RunManifest>,
    on_lightbox: EventHandler<LightboxImage>,
) -> Element {
    rsx! {
        for (source_index, rm) in run_manifests.iter().enumerate() {
            if let Some(m) = &rm.manifest {
                div { class: "cdn-run-group", style: "background: {rm.color}11;",
                    div { class: "cdn-run-group-label", style: "color: {rm.color};", "{rm.label}" }
                    div { class: "cdn-image-grid",
                        for (idx, item) in m.items.iter().enumerate() {
                            div { class: "cdn-image-item",
                                {gallery_image(item, rm, source_index, idx, on_lightbox)}
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Mode 3: Interleaved — one grid per index, each showing all runs at that index.
#[component]
fn InterleavedView(
    run_manifests: Vec<RunManifest>,
    on_lightbox: EventHandler<LightboxImage>,
) -> Element {
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
        selected_step_index, CdnManifestClass, GalleryPresentation, Manifest, ManifestFetchKey,
        ManifestItem, RunManifest,
    };
    use dioxus::html::Modifiers;

    #[test]
    fn selected_gallery_step_survives_step_insertions() {
        assert_eq!(selected_step_index(&[0, 10], Some(10)), 1);
        assert_eq!(selected_step_index(&[0, 5, 10], Some(10)), 2);
        assert_eq!(selected_step_index(&[0, 10], Some(5)), 1);
        assert_eq!(selected_step_index(&[0, 10], Some(20)), 1);
        // A gallery that mounted empty starts at the latest step when data first arrives, then keeps that concrete selection as newer steps appear instead of following latest forever.
        assert_eq!(selected_step_index(&[-10, -5], None), 1);
        assert_eq!(selected_step_index(&[-10, -5, 0], Some(-5)), 1);
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
    fn media_players_follow_the_cdn_key_extension() {
        use super::MediaKind::{Audio, Video};
        for (resource, kind) in [
            ("ab.mp4", Some(Video)),
            ("ab.webm", Some(Video)),
            ("ab.MP4", Some(Video)),
            ("ab.mp3", Some(Audio)),
            ("ab.wav", Some(Audio)),
            ("ab.ogg", Some(Audio)),
            ("ab.png", None),
            ("ab", None),
            ("ab.", None),
            ("ab.mp4.bin", None),
            ("ab.bin", None),
        ] {
            // The filename never decides: the CDN serves the key's type.
            let item = ManifestItem {
                resource: resource.to_string(),
                filename: Some("clip.mp4".to_string()),
                caption: None,
            };
            assert_eq!(item.media_kind(), kind, "{resource}");
        }
    }

    #[test]
    fn gallery_lightbox_preserves_native_modified_clicks() {
        assert!(super::unmodified(Modifiers::empty()));
        assert!(super::unmodified(Modifiers::CAPS_LOCK));
        for modifier in [
            Modifiers::ALT,
            Modifiers::CONTROL,
            Modifiers::META,
            Modifiers::SHIFT,
        ] {
            assert!(!super::unmodified(modifier));
        }
    }

    #[test]
    fn lightbox_arrows_follow_each_modes_drawing_order() {
        use super::{lightbox_neighbours, CdnDisplayMode, LightboxNeighbours};
        let counts = [2, 3, 1];
        let at = |mode, open| lightbox_neighbours(mode, &counts, open);
        let neighbours = |previous, next, position| LightboxNeighbours {
            previous,
            next,
            position,
        };
        // GroupByRun: each source's batch in turn.
        assert_eq!(
            at(CdnDisplayMode::GroupByRun, (0, 1)),
            neighbours(Some((0, 0)), Some((1, 0)), Some((2, 6)))
        );
        assert_eq!(
            at(CdnDisplayMode::GroupByRun, (0, 0)),
            neighbours(None, Some((0, 1)), Some((1, 6)))
        );
        assert_eq!(
            at(CdnDisplayMode::GroupByRun, (2, 0)),
            neighbours(Some((1, 2)), None, Some((6, 6)))
        );
        // Interleaved: every source's image #0, then #1.
        assert_eq!(
            at(CdnDisplayMode::Interleaved, (2, 0)),
            neighbours(Some((1, 0)), Some((0, 1)), Some((3, 6)))
        );
        assert_eq!(
            at(CdnDisplayMode::Interleaved, (1, 1)),
            neighbours(Some((0, 1)), Some((1, 2)), Some((5, 6)))
        );
        // SelectIndex walks every image as Interleaved does, present or missing, its slider following, so one source's images are all reachable.
        for open in [(0, 0), (2, 0), (0, 1), (1, 2), (2, 1)] {
            assert_eq!(
                at(CdnDisplayMode::SelectIndex, open),
                at(CdnDisplayMode::Interleaved, open)
            );
        }
        assert_eq!(
            lightbox_neighbours(CdnDisplayMode::SelectIndex, &[14, 0], (0, 0)),
            neighbours(None, Some((0, 1)), Some((1, 14)))
        );
    }

    #[test]
    fn lightbox_arrows_leave_a_missing_image_for_its_neighbours() {
        use super::{lightbox_neighbours, CdnDisplayMode, LightboxNeighbours};
        let missing = |previous, next| LightboxNeighbours {
            previous,
            next,
            position: None,
        };
        // Past the source's item count.
        assert_eq!(
            lightbox_neighbours(CdnDisplayMode::GroupByRun, &[2, 3, 1], (0, 5)),
            missing(Some((0, 1)), Some((1, 0)))
        );
        // A source with nothing at the step (a placeholder).
        assert_eq!(
            lightbox_neighbours(CdnDisplayMode::GroupByRun, &[2, 0, 1], (1, 0)),
            missing(Some((0, 1)), Some((2, 0)))
        );
    }

    #[test]
    fn lightbox_steps_walk_the_sources_own_steps() {
        use super::adjacent_source_step;
        let keys: Vec<(i64, String)> = [30, 0, 10]
            .into_iter()
            .map(|step| (step, format!("key-{step}")))
            .collect();
        assert_eq!(adjacent_source_step(&keys, 10, true), Some(30));
        assert_eq!(adjacent_source_step(&keys, 10, false), Some(0));
        assert_eq!(adjacent_source_step(&keys, 30, true), None);
        assert_eq!(adjacent_source_step(&keys, 0, false), None);
        // From a step this source never logged (another source's), to the ones around it.
        assert_eq!(adjacent_source_step(&keys, 20, true), Some(30));
        assert_eq!(adjacent_source_step(&keys, 20, false), Some(10));
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
