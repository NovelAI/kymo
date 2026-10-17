use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;

use dioxus::prelude::*;
use dioxus::web::WebEventExt;
use wasm_bindgen::JsCast;

use crate::components::copy_text::CopyText;
use crate::components::icons::{CaretDownIcon, CaretLeftIcon, CaretRightIcon, CaretUpIcon};
use crate::components::metric_rect::CdnRunData;
use crate::route::Route;
use crate::state::layout_config::CdnDisplayMode;
use crate::state::panel_cache::Store;
use crate::state::visibility::RETRY_WAIT;
use crate::util::{primary, set_if_changed};

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

/// The status line for sources whose key at the step is still a placeholder: a live run's images may still arrive; an ended run's placeholder is an upload that did not finish. It also names the sources whose manifest failed to load, each asked again RETRY_WAIT after its failure, and those still loading past the grace.
fn pending_status(
    live: &[&str],
    ended: &[&str],
    failed: &[&str],
    loading: &[&str],
) -> Option<String> {
    let clauses: Vec<String> = [
        ("Not uploaded yet", live),
        ("Upload did not finish", ended),
        ("Failed to load, retrying", failed),
        ("Still loading", loading),
    ]
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

/// What one source's manifest is fetched as: its run and metric, and its CDN key at the shown step. Label and color stay out, so a rename or recolor neither refetches nor blanks the panel; render attaches them.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ManifestSource {
    project_id: String,
    run_id: String,
    metric_name: String,
    key: String,
}

/// A source's last answer: settled (None for nothing usable at its key, or a placeholder key), failed, which is asked again, or still out when its fetches' MANIFEST_GRACE passed.
#[derive(Clone, Debug, PartialEq)]
enum Fetched {
    Settled(Option<Manifest>),
    Failed,
    Waiting,
}

/// How long a step's manifest fetches hold their answers so they land in one render: past it, what has loaded is shown and each later answer as it lands, so one slow or stalled manifest holds no other source back. It divides none of RETRY_WAIT (5 s), the Near prefetch cap (2 s) or the offline notice's grace (3 s).
const MANIFEST_GRACE: std::time::Duration = std::time::Duration::from_millis(1100);

/// Fetches every source in `asking` at once, each failure asked again on its own after `retry` (RETRY_WAIT, on a visible page) until it settles. Answers within `grace` are published together, with every source still out marked Waiting, and each later answer as it lands. A deadline abort doubles that source's next deadline, up to MANIFEST_DEADLINE_CAP_MS; deadlines start over when this call does (a change to the step's sources, or a remount).
async fn fetch_sources<R>(
    asking: Vec<ManifestSource>,
    fetch: impl AsyncFn(&ManifestSource, u32) -> (Fetched, bool),
    retry: impl Fn() -> R,
    grace: impl std::future::Future<Output = ()>,
    mut publish: impl FnMut(Vec<(ManifestSource, Fetched)>),
) where
    R: std::future::Future<Output = ()>,
{
    use futures::{FutureExt, StreamExt};
    let (fetch, retry) = (&fetch, &retry);
    // A source's answers: each failure is followed by a retry, and the first settled answer ends them.
    let answers_of = |source: ManifestSource| {
        Box::pin(futures::stream::unfold(
            Some((source, false, MANIFEST_DEADLINE_MS)),
            move |state| async move {
                let (source, failed_before, deadline) = state?;
                if failed_before {
                    retry().await;
                }
                let (answer, at_deadline) = fetch(&source, deadline).await;
                let deadline = if at_deadline {
                    (deadline * 2).min(MANIFEST_DEADLINE_CAP_MS)
                } else {
                    deadline
                };
                let again = (answer == Fetched::Failed).then(|| (source.clone(), true, deadline));
                Some(((source, answer), again))
            },
        ))
    };
    let mut out = futures::stream::select_all(asking.iter().cloned().map(answers_of));
    let mut grace = std::pin::pin!(grace.fuse());
    let mut held = HashMap::new();
    while held.len() < asking.len() {
        futures::select! {
            answer = out.next() => match answer {
                Some((source, answer)) => { held.insert(source, answer); }
                None => break,
            },
            () = grace => break,
        }
    }
    let waiting: Vec<_> = asking
        .into_iter()
        .filter(|source| !held.contains_key(source))
        .map(|source| (source, Fetched::Waiting))
        .collect();
    publish(held.into_iter().chain(waiting).collect());
    while let Some(answer) = out.next().await {
        publish(vec![answer]);
    }
}

type FetchedManifests = HashMap<ManifestSource, Fetched>;

/// Whether publishing `answers` would change `fetched`, which only `current` (this step's sources) may keep: an answer it doesn't hold, or another step's answer to drop. A source failing again answers Failed again, and fetches with nothing to ask answer nothing; neither needs a render.
fn publish_changes(
    fetched: &FetchedManifests,
    current: &HashSet<ManifestSource>,
    answers: &[(ManifestSource, Fetched)],
) -> bool {
    fetched.keys().any(|source| !current.contains(source))
        || answers
            .iter()
            .any(|(source, answer)| fetched.get(source) != Some(answer))
}

/// The sources a step's fetches ask: every one not yet settled.
fn unsettled_sources(
    sources: &[ManifestSource],
    fetched: &FetchedManifests,
) -> Vec<ManifestSource> {
    sources
        .iter()
        .filter(|source| !matches!(fetched.get(*source), Some(Fetched::Settled(_))))
        .cloned()
        .collect()
}

/// A source's run and metric, which a preview's image belongs to at every step.
type SourceIdentity = (String, String, String);

/// What the open preview shows, per source identity: its last settled image manifest, for the shown step's `sources` only. A run with no key at this step has none; an answer settling with an image gallery replaces its run's, one settling without removes it (the step has none), and one still out or failing leaves the previous step's up. `fetched` can still hold the previous step's answers, which count for nothing. Returns what it holds, in `sources`' order.
fn latch_settled(
    latched: &mut HashMap<SourceIdentity, (ManifestSource, Manifest)>,
    sources: &[ManifestSource],
    fetched: &FetchedManifests,
) -> Vec<(ManifestSource, Manifest)> {
    let identity = |source: &ManifestSource| {
        (
            source.project_id.clone(),
            source.run_id.clone(),
            source.metric_name.clone(),
        )
    };
    let present: HashSet<SourceIdentity> = sources.iter().map(identity).collect();
    latched.retain(|held, _| present.contains(held));
    for source in sources {
        match fetched.get(source) {
            Some(Fetched::Settled(Some(manifest))) if manifest.is_image_gallery() => {
                if latched.get(&identity(source)).map(|(held, _)| held) != Some(source) {
                    latched.insert(identity(source), (source.clone(), manifest.clone()));
                }
            }
            Some(Fetched::Settled(_)) => {
                latched.remove(&identity(source));
            }
            Some(Fetched::Failed | Fetched::Waiting) | None => {}
        }
    }
    sources
        .iter()
        .filter_map(|source| latched.get(&identity(source)).cloned())
        .collect()
}

/// Whether the open image's own source at the shown step has yet to settle (not asked yet, waiting, or failing), and whether its last answer failed, so it is asked again.
fn open_source_state(
    open: &LightboxImage,
    sources: &[ManifestSource],
    fetched: &FetchedManifests,
) -> (bool, bool) {
    let answer = sources
        .iter()
        .find(|source| {
            (&source.project_id, &source.run_id, &source.metric_name)
                == (&open.project_id, &open.run_id, &open.metric_name)
        })
        .map(|source| fetched.get(source));
    (
        !matches!(answer, None | Some(Some(Fetched::Settled(_)))),
        answer == Some(Some(&Fetched::Failed)),
    )
}

/// `shown` (the step's sources as drawn, aligned with `sources`) with their manifests, once every source has answered or their grace passed; a failed or waiting source draws as having none.
fn current_manifests(
    shown: &[RunManifest],
    sources: &[ManifestSource],
    fetched: &FetchedManifests,
) -> Option<Vec<RunManifest>> {
    shown
        .iter()
        .zip(sources)
        .map(|(run_manifest, source)| {
            let manifest = match fetched.get(source)? {
                Fetched::Settled(manifest) => manifest.clone(),
                Fetched::Failed | Fetched::Waiting => None,
            };
            Some(RunManifest {
                manifest,
                ..run_manifest.clone()
            })
        })
        .collect()
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

/// How long a manifest fetch may first take, headers through body, before it is aborted and retried, so a stalled response can't hold its source, and one of the browser's six HTTP/1.1 connections to the CDN, forever. The wait for a connection counts too. Chromium gives a fetch to the CDN, another origin, without credentials connections apart from the gallery's images, but WebKit queues it behind them (4.7 s behind six images stalled for 5 s), so on a slow link a manifest can run out its deadline before it is sent; each deadline abort doubles that source's next one, up to MANIFEST_DEADLINE_CAP_MS. 47 s is no multiple of RETRY_WAIT (5 s), the socket's 20 s keepalive ping, the series cache's 40 s hot window or 60 s (the socket's request timeout and several other clocks).
const MANIFEST_DEADLINE_MS: u32 = 47_000;

/// The longest a source's fetch gets after deadline aborts, 47 s doubled twice: a manifest still queued behind images that long is on a link too slow to show them, and each fetch of a hung request is capped at 188 s, a multiple of none of those clocks.
const MANIFEST_DEADLINE_CAP_MS: u32 = 4 * MANIFEST_DEADLINE_MS;

/// Aborts its fetch when dropped, so a step change or an unmount cancels the manifest requests still in flight rather than leaving them on the CDN origin's connections.
struct AbortOnDrop(web_sys::AbortController);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Ok(None) is settled knowledge — a 404 (nothing at that key), a 400 (the
/// hosted route's answer to a malformed key, which ingest stores unvalidated)
/// or unparseable content, none of which heals on retry. Err (a network
/// failure or any other status) must be retried: a finished run's keys never
/// change, so a failure taken as settled would never be
/// refetched.
/// The Err also says whether `deadline_ms` ran out.
async fn fetch_manifest(
    cdn_key: &str,
    deadline_ms: u32,
) -> Result<Option<Manifest>, (String, bool)> {
    let url = crate::runtime::cdn_url(cdn_key);
    let abort =
        AbortOnDrop(web_sys::AbortController::new().map_err(|e| (format!("{e:?}"), false))?);
    let _deadline = gloo_timers::callback::Timeout::new(deadline_ms, {
        let abort = abort.0.clone();
        move || abort.abort()
    });
    // Only the deadline aborts the request while this runs.
    let failed = |error: String| (error, abort.0.signal().aborted());
    let resp = gloo_net::http::Request::get(&url)
        .abort_signal(Some(&abort.0.signal()))
        .send()
        .await
        .map_err(|e| failed(e.to_string()))?;
    if matches!(resp.status(), 400 | 404) {
        return Ok(None);
    }
    if !resp.ok() {
        return Err(failed(format!("{} for {url}", resp.status())));
    }
    let text = resp.text().await.map_err(|e| failed(e.to_string()))?;
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
    let latched = use_hook(|| Rc::new(RefCell::new(HashMap::new())));

    // The shown step, or none while the panel has no CDN data.
    let selected = *selected_step.read();
    let idx = (!all_steps.is_empty()).then(|| selected_step_index(&all_steps, selected));
    let step = idx.map(|idx| all_steps[idx]);

    // Each source as drawn, with the fetch identity it is answered under. `ended` stays out of the identity, so a status change doesn't refetch manifests.
    let mut shown: Vec<RunManifest> = Vec::new();
    let mut sources: Vec<ManifestSource> = Vec::new();
    let (mut pending_live, mut pending_ended) = (Vec::new(), Vec::new());
    for run in &runs {
        let Some((_, key)) = run.keys.iter().find(|(s, _)| Some(*s) == step) else {
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
        shown.push(RunManifest {
            project_id: run.project_id.clone(),
            run_id: run.run_id.clone(),
            metric_name: run.metric_name.clone(),
            label: run.label.clone(),
            color: run.color.clone(),
            manifest: None,
        });
        sources.push(ManifestSource {
            project_id: run.project_id.clone(),
            run_id: run.run_id.clone(),
            metric_name: run.metric_name.clone(),
            key: key.clone(),
        });
    }

    // Fetch this step's unsettled manifests (fetch_sources), so a source that fails shows as failed beside the ones that loaded. The resource is keyed on the sources and only peeks the answers it writes, so publishing restarts nothing; a step change drops the fetches, aborting their requests, and so does data that leaves no step, whose empty sources run before the panel says so.
    let mut fetched = use_signal(FetchedManifests::new);
    let _fetch = use_resource(use_reactive((&sources,), move |(sources,)| async move {
        let current: HashSet<ManifestSource> = sources.iter().cloned().collect();
        let asking = unsettled_sources(&sources, &fetched.peek());
        if !asking.is_empty() {
            crate::grpc::wait_until_page_visible().await;
        }
        let fetch = async |source: &ManifestSource, deadline_ms: u32| {
            if source.key.starts_with("pending:") {
                return (Fetched::Settled(None), false);
            }
            match fetch_manifest(&source.key, deadline_ms).await {
                Ok(manifest) => (Fetched::Settled(manifest), false),
                Err((error, at_deadline)) => {
                    crate::util::warn(&format!("[manifest] failed: {error}; retrying"));
                    (Fetched::Failed, at_deadline)
                }
            }
        };
        let retry = || async {
            gloo_timers::future::sleep(RETRY_WAIT).await;
            crate::grpc::wait_until_page_visible().await;
        };
        // Only this step's answers stay; the open preview latches what it shows (latch_settled).
        let publish = |answers: Vec<(ManifestSource, Fetched)>| {
            if !publish_changes(&fetched.peek(), &current, &answers) {
                return;
            }
            let mut fetched = fetched.write();
            fetched.retain(|source, _| current.contains(source));
            fetched.extend(answers);
        };
        let grace = gloo_timers::future::sleep(MANIFEST_GRACE);
        fetch_sources(asking, fetch, retry, grace, publish).await;
    }));

    let Some(idx) = idx else {
        set_if_changed(cdn_class, None);
        set_if_changed(lightbox, None);
        return rsx! {
            div { class: "cdn-gallery",
                div { class: "cdn-gallery-empty", "No CDN data" }
            }
        };
    };
    let step = all_steps[idx];
    let previous_step = idx.checked_sub(1).map(|i| all_steps[i]);
    let next_step = all_steps.get(idx + 1).copied();
    let current_step = idx + 1;
    let step_value_text = format!("Step {step}, {current_step} of {total_steps}");
    if selected.is_none() {
        selected_step.set(Some(step));
    }

    let (status, presentation, run_manifests, lightbox_view) = {
        let fetched = fetched.read();
        let current = current_manifests(&shown, &sources, &fetched);
        let labels = |answer: Fetched| -> Vec<&str> {
            shown
                .iter()
                .zip(&sources)
                .filter(|(_, source)| fetched.get(*source) == Some(&answer))
                .map(|(run_manifest, _)| run_manifest.label.as_str())
                .collect()
        };
        let status = pending_status(
            &pending_live,
            &pending_ended,
            &labels(Fetched::Failed),
            &labels(Fetched::Waiting),
        );
        let parsed_manifest_count = current.as_ref().map(|run_manifests| {
            run_manifests
                .iter()
                .filter(|run_manifest| run_manifest.manifest.is_some())
                .count()
        });
        // An open lightbox shows each run's last settled image manifest, so its image stays up while the next step's is still out, and loads until the open image's own source settles. Closing it forgets them.
        let lightbox_view = match lightbox() {
            Some(open) => {
                let shown = latch_settled(&mut latched.borrow_mut(), &sources, &fetched);
                let (loading, failing) = open_source_state(&open, &sources, &fetched);
                Some((open, shown, loading, failing))
            }
            None => {
                latched.borrow_mut().clear();
                None
            }
        };
        let presentation = gallery_presentation(status.is_some(), parsed_manifest_count);
        (
            status,
            presentation,
            current.unwrap_or_default(),
            lightbox_view,
        )
    };
    // Publish the shown step's CDN sub-type upward so the BindingEditor only shows applicable panels.
    let class = classify_manifests(&run_manifests).map(|class| class.as_str().to_string());
    set_if_changed(cdn_class, class);
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

            if let Some((open, shown, loading, failing)) = lightbox_view {
                GalleryLightbox {
                    open,
                    runs,
                    shown,
                    loading,
                    failing,
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
    /// Each run's last settled image manifest; another step's while its own is still out.
    shown: Vec<(ManifestSource, Manifest)>,
    /// The open image's own source at `step` has yet to settle.
    loading: bool,
    /// Its last answer failed: it is asked again, and the step says so.
    failing: bool,
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
    // `shown` aligned with `runs`.
    let manifests: Vec<Option<&Manifest>> = runs
        .iter()
        .map(|run| {
            shown
                .iter()
                .find(|(settled, _)| {
                    (&settled.project_id, &settled.run_id, &settled.metric_name)
                        == (&run.project_id, &run.run_id, &run.metric_name)
                })
                .map(|(_, manifest)| manifest)
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
                    if failing {
                        "step {step} failed to load, retrying…"
                    } else if loading {
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
        classify_manifests, current_manifests, fetch_sources, file_groups, gallery_file_link_label,
        gallery_presentation, gallery_thumbnail_label, latch_settled, metadata_columns,
        open_source_state, pending_status, publish_changes, selected_step_index, unsettled_sources,
        CdnManifestClass, Fetched, FetchedManifests, GalleryPresentation, LightboxImage, Manifest,
        ManifestItem, ManifestSource, RunManifest,
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
        assert_eq!(pending_status(&[], &[], &[], &[]), None);
        assert_eq!(
            pending_status(&["a", "b"], &[], &[], &[]).as_deref(),
            Some("Not uploaded yet: a, b")
        );
        assert_eq!(
            pending_status(&[], &["a"], &[], &[]).as_deref(),
            Some("Upload did not finish: a")
        );
        assert_eq!(
            pending_status(&["live"], &["old"], &[], &[]).as_deref(),
            Some("Not uploaded yet: live · Upload did not finish: old")
        );
        assert_eq!(
            pending_status(&[], &[], &["broken"], &[]).as_deref(),
            Some("Failed to load, retrying: broken")
        );
        assert_eq!(
            pending_status(&[], &[], &[], &["slow"]).as_deref(),
            Some("Still loading: slow")
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

    fn drawn(run_id: &str, label: &str) -> RunManifest {
        RunManifest {
            project_id: "project".to_string(),
            run_id: run_id.to_string(),
            metric_name: "gallery".to_string(),
            label: label.to_string(),
            color: "#123456".to_string(),
            manifest: None,
        }
    }

    fn source(run_id: &str, key: &str) -> ManifestSource {
        ManifestSource {
            project_id: "project".to_string(),
            run_id: run_id.to_string(),
            metric_name: "gallery".to_string(),
            key: key.to_string(),
        }
    }

    fn image_manifest() -> Manifest {
        Manifest {
            v: 1,
            class: "image_gallery".into(),
            items: vec![ManifestItem {
                resource: "resource".into(),
                filename: None,
                caption: None,
            }],
            data: None,
        }
    }

    #[test]
    fn retained_answers_do_not_cross_manifest_key_generations() {
        let shown = [drawn("run", "run")];
        let fetched = FetchedManifests::from([(source("run", "old.json"), Fetched::Settled(None))]);

        assert!(current_manifests(&shown, &[source("run", "pending:new")], &fetched).is_none());
        assert!(current_manifests(&shown, &[source("run", "old.json")], &fetched).is_some());
    }

    /// A source that keeps failing must not withhold the ones that loaded: it answers as failed, the panel draws the others and names it, and only it is asked again.
    #[test]
    fn a_failed_manifest_shows_beside_the_settled_ones_and_is_asked_again() {
        let shown = [drawn("healthy", "Healthy"), drawn("broken", "Broken")];
        let sources = [source("healthy", "a.json"), source("broken", "b.json")];
        let mut fetched = FetchedManifests::new();
        assert_eq!(unsettled_sources(&sources, &fetched), sources);

        fetched.insert(sources[0].clone(), Fetched::Settled(Some(image_manifest())));
        assert!(
            current_manifests(&shown, &sources, &fetched).is_none(),
            "one source has not answered"
        );
        fetched.insert(sources[1].clone(), Fetched::Failed);

        let current = current_manifests(&shown, &sources, &fetched).expect("every source answered");
        assert_eq!(current[0].manifest, Some(image_manifest()));
        assert_eq!(current[1].manifest, None);
        assert_eq!(current[1].label, "Broken");
        let parsed = current.iter().filter(|run| run.manifest.is_some()).count();
        assert_eq!(
            gallery_presentation(true, Some(parsed)),
            GalleryPresentation::Content
        );
        assert_eq!(
            unsettled_sources(&sources, &fetched),
            [sources[1].clone()],
            "only the failed source is asked again"
        );
    }

    type Shared<T> = std::rc::Rc<std::cell::RefCell<T>>;

    /// fetch_sources driven by hand: `answer` resolves a source's fetch still out, `pass_retries` every retry wait under way, `pass_grace` the grace.
    struct Fetches {
        pool: futures::executor::LocalPool,
        /// Each source's fetch still out, answered with (its answer, whether its deadline ran out).
        out: Shared<
            std::collections::HashMap<String, futures::channel::oneshot::Sender<(Fetched, bool)>>,
        >,
        /// Each fetch asked, as (run id, deadline).
        asked: Shared<Vec<(String, u32)>>,
        retries: Shared<Vec<futures::channel::oneshot::Sender<()>>>,
        grace: Option<futures::channel::oneshot::Sender<()>>,
        /// Each publication, as (run id, answer) pairs.
        published: Shared<Vec<Vec<(String, Fetched)>>>,
        done: std::rc::Rc<std::cell::Cell<bool>>,
    }

    impl Fetches {
        fn start(sources: &[ManifestSource]) -> Self {
            use futures::task::LocalSpawnExt;
            let pool = futures::executor::LocalPool::new();
            let out = std::rc::Rc::new(std::cell::RefCell::new(std::collections::HashMap::new()));
            let asked = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let retries = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let (grace_tx, grace_rx) = futures::channel::oneshot::channel::<()>();
            let published = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let done = std::rc::Rc::new(std::cell::Cell::new(false));
            let (fetches, asking_log, waits, sink, finished) = (
                out.clone(),
                asked.clone(),
                retries.clone(),
                published.clone(),
                done.clone(),
            );
            let asking = sources.to_vec();
            pool.spawner()
                .spawn_local(async move {
                    let fetch = async |source: &ManifestSource, deadline: u32| {
                        let (tx, rx) = futures::channel::oneshot::channel();
                        fetches.borrow_mut().insert(source.run_id.clone(), tx);
                        asking_log
                            .borrow_mut()
                            .push((source.run_id.clone(), deadline));
                        rx.await.unwrap()
                    };
                    let retry = || {
                        let (tx, rx) = futures::channel::oneshot::channel::<()>();
                        waits.borrow_mut().push(tx);
                        async move {
                            let _ = rx.await;
                        }
                    };
                    let grace = async move {
                        let _ = grace_rx.await;
                    };
                    let publish = |answers: Vec<(ManifestSource, Fetched)>| {
                        sink.borrow_mut()
                            .push(answers.into_iter().map(|(s, a)| (s.run_id, a)).collect())
                    };
                    fetch_sources(asking, fetch, retry, grace, publish).await;
                    finished.set(true);
                })
                .unwrap();
            let mut fetches = Fetches {
                pool,
                out,
                asked,
                retries,
                grace: Some(grace_tx),
                published,
                done,
            };
            fetches.pool.run_until_stalled();
            fetches
        }

        fn answer(&mut self, run_id: &str, answer: Fetched) {
            self.reply(run_id, (answer, false));
        }

        /// The source's fetch still out runs out its deadline.
        fn time_out(&mut self, run_id: &str) {
            self.reply(run_id, (Fetched::Failed, true));
        }

        fn reply(&mut self, run_id: &str, reply: (Fetched, bool)) {
            self.out
                .borrow_mut()
                .remove(run_id)
                .unwrap()
                .send(reply)
                .unwrap();
            self.pool.run_until_stalled();
        }

        fn pass_retries(&mut self) {
            for wait in self.retries.borrow_mut().drain(..) {
                let _ = wait.send(());
            }
            self.pool.run_until_stalled();
        }

        fn pass_grace(&mut self) {
            self.grace.take().unwrap().send(()).unwrap();
            self.pool.run_until_stalled();
        }

        fn published(&self) -> Vec<Vec<(String, Fetched)>> {
            self.published.borrow().clone()
        }

        fn asks(&self, run_id: &str) -> usize {
            self.deadlines(run_id).len()
        }

        fn deadlines(&self, run_id: &str) -> Vec<u32> {
            self.asked
                .borrow()
                .iter()
                .filter(|(asked, _)| asked == run_id)
                .map(|(_, deadline)| *deadline)
                .collect()
        }
    }

    /// Answers that land within the grace render once, together.
    #[test]
    fn answers_within_the_grace_publish_once() {
        let mut fetches = Fetches::start(&[source("a", "a.json"), source("b", "b.json")]);
        fetches.answer("b", Fetched::Settled(None));
        assert!(
            fetches.published().is_empty(),
            "nothing shows before every source answered"
        );
        fetches.answer("a", Fetched::Settled(Some(image_manifest())));
        let published = fetches.published();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].len(), 2);
        assert!(fetches.done.get());
    }

    /// A stalled source holds the others back only for the grace: then they show, it is marked Waiting, and its answer lands on its own.
    #[test]
    fn past_the_grace_what_loaded_shows_and_then_each_answer() {
        let mut fetches = Fetches::start(&[source("a", "a.json"), source("stalled", "s.json")]);
        fetches.answer("a", Fetched::Settled(Some(image_manifest())));
        assert!(fetches.published().is_empty());
        fetches.pass_grace();
        assert_eq!(
            fetches.published(),
            [vec![
                ("a".to_string(), Fetched::Settled(Some(image_manifest()))),
                ("stalled".to_string(), Fetched::Waiting),
            ]]
        );
        fetches.answer("stalled", Fetched::Failed);
        assert_eq!(
            fetches.published()[1],
            [("stalled".to_string(), Fetched::Failed)]
        );
        // A failed source is asked again once its retry wait passes.
        assert_eq!(fetches.asks("stalled"), 1);
        fetches.pass_retries();
        assert_eq!(fetches.asks("stalled"), 2);
        assert!(!fetches.done.get());
        // Drawn: the loaded source beside the waiting one, which counts as answered.
        let shown = [drawn("a", "A"), drawn("stalled", "Stalled")];
        let sources = [source("a", "a.json"), source("stalled", "s.json")];
        let fetched = FetchedManifests::from([
            (sources[0].clone(), Fetched::Settled(Some(image_manifest()))),
            (sources[1].clone(), Fetched::Waiting),
        ]);
        let current = current_manifests(&shown, &sources, &fetched).expect("drawn past the grace");
        assert_eq!(current[1].manifest, None);
        assert_eq!(
            unsettled_sources(&sources, &fetched),
            [sources[1].clone()],
            "a waiting source is asked again if the fetches restart"
        );
    }

    /// A source aborted at its deadline gets twice the time on its next ask, up to the cap; a plain failure keeps its deadline.
    #[test]
    fn a_deadline_abort_doubles_that_sources_next_deadline() {
        let mut fetches = Fetches::start(&[source("queued", "q.json"), source("fails", "f.json")]);
        for _ in 0..3 {
            fetches.time_out("queued");
            fetches.answer("fails", Fetched::Failed);
            fetches.pass_retries();
        }
        assert_eq!(
            fetches.deadlines("queued"),
            [47_000, 94_000, 188_000, 188_000]
        );
        assert_eq!(fetches.deadlines("fails"), [47_000; 4]);
    }

    /// A failing source is asked again on its own clock: a sibling still out holds back neither its retry nor its answer.
    #[test]
    fn a_failing_source_retries_while_a_sibling_stalls() {
        let mut fetches = Fetches::start(&[source("fails", "f.json"), source("stalled", "s.json")]);
        fetches.answer("fails", Fetched::Failed);
        fetches.pass_grace();
        assert_eq!(
            fetches.published(),
            [vec![
                ("fails".to_string(), Fetched::Failed),
                ("stalled".to_string(), Fetched::Waiting),
            ]]
        );
        fetches.pass_retries();
        assert_eq!(
            fetches.asks("fails"),
            2,
            "asked again with its sibling still out"
        );
        fetches.answer("fails", Fetched::Settled(Some(image_manifest())));
        assert_eq!(
            fetches.published()[1],
            [(
                "fails".to_string(),
                Fetched::Settled(Some(image_manifest()))
            )]
        );
        assert!(!fetches.done.get());
        fetches.answer("stalled", Fetched::Settled(None));
        assert!(fetches.done.get());
    }

    /// An open preview keeps the previous step's image while the next step's manifest for its run is out, waiting or failing, and says there is none only once that manifest settles without one, or at once for a run with no key at the step.
    #[test]
    fn the_preview_keeps_its_image_until_the_next_step_settles() {
        let image = |resource: &str| Manifest {
            items: vec![ManifestItem {
                resource: resource.into(),
                filename: None,
                caption: None,
            }],
            ..image_manifest()
        };
        let open = LightboxImage {
            project_id: "project".into(),
            run_id: "run".into(),
            metric_name: "gallery".into(),
            item: 0,
        };
        let shows = |latched: &std::collections::HashMap<_, (ManifestSource, Manifest)>| {
            latched
                .values()
                .map(|(_, manifest)| manifest.items[0].resource.clone())
                .collect::<Vec<_>>()
        };
        let (step5, step6) = ([source("run", "k5.json")], [source("run", "k6.json")]);
        let mut latched = std::collections::HashMap::new();
        let mut fetched =
            FetchedManifests::from([(step5[0].clone(), Fetched::Settled(Some(image("five"))))]);
        latch_settled(&mut latched, &step5, &fetched);
        assert_eq!(shows(&latched), ["five"]);
        assert_eq!(open_source_state(&open, &step5, &fetched), (false, false));
        // Stepped to 6: not asked yet, then waiting past the grace, then failing.
        latch_settled(&mut latched, &step6, &fetched);
        assert_eq!(open_source_state(&open, &step6, &fetched), (true, false));
        for answer in [Fetched::Waiting, Fetched::Failed] {
            fetched = FetchedManifests::from([(step6[0].clone(), answer.clone())]);
            latch_settled(&mut latched, &step6, &fetched);
            assert_eq!(shows(&latched), ["five"], "the previous image stays up");
            assert_eq!(
                open_source_state(&open, &step6, &fetched),
                (true, answer == Fetched::Failed),
                "a failing source says so"
            );
        }
        // Settled without a manifest: a true absence.
        fetched = FetchedManifests::from([(step6[0].clone(), Fetched::Settled(None))]);
        latch_settled(&mut latched, &step6, &fetched);
        assert!(shows(&latched).is_empty());
        assert_eq!(open_source_state(&open, &step6, &fetched), (false, false));
        fetched =
            FetchedManifests::from([(step6[0].clone(), Fetched::Settled(Some(image("six"))))]);
        latch_settled(&mut latched, &step6, &fetched);
        assert_eq!(shows(&latched), ["six"]);

        // Runs log at different steps: A at 1, B at 1 and 2. At step 2 A has no image, whatever step 1's answers `fetched` still holds; B's stays up until its own answer settles.
        let (step1, step2) = (
            [source("a", "a1.json"), source("b", "b1.json")],
            [source("b", "b2.json")],
        );
        latched.clear();
        fetched = FetchedManifests::from([
            (step1[0].clone(), Fetched::Settled(Some(image("a1")))),
            (step1[1].clone(), Fetched::Settled(Some(image("b1")))),
        ]);
        latch_settled(&mut latched, &step1, &fetched);
        let mut held = shows(&latched);
        held.sort();
        assert_eq!(held, ["a1", "b1"]);
        latch_settled(&mut latched, &step2, &fetched);
        assert_eq!(shows(&latched), ["b1"]);
        fetched = FetchedManifests::from([(step2[0].clone(), Fetched::Settled(Some(image("b2"))))]);
        latch_settled(&mut latched, &step2, &fetched);
        assert_eq!(shows(&latched), ["b2"]);
    }

    /// Publishing writes the panel's answers, and so renders it, only when they change: not for a source that fails again, nor for fetches that asked nothing.
    #[test]
    fn publishing_writes_only_answers_that_change_something() {
        let (a, b) = (source("a", "a.json"), source("b", "b.json"));
        let current: std::collections::HashSet<_> = [a.clone(), b.clone()].into();
        let fetched = FetchedManifests::from([
            (a.clone(), Fetched::Settled(Some(image_manifest()))),
            (b.clone(), Fetched::Failed),
        ]);
        let changes =
            |answers: &[(ManifestSource, Fetched)]| publish_changes(&fetched, &current, answers);
        assert!(!changes(&[(b.clone(), Fetched::Failed)]));
        assert!(!changes(&[]));
        assert!(changes(&[(b.clone(), Fetched::Waiting)]));
        assert!(changes(&[(b.clone(), Fetched::Settled(None))]));
        assert!(changes(&[(source("c", "c.json"), Fetched::Failed)]));
        // Another step's answers still held are dropped, which is a change.
        let next_step: std::collections::HashSet<_> = [b.clone()].into();
        assert!(publish_changes(&fetched, &next_step, &[]));
    }

    /// A rename or recolor changes only what is drawn: the fetch identity stays, so nothing refetches, and the answers held under it take the new label.
    #[test]
    fn a_relabeled_source_keeps_its_answer_and_takes_the_new_label() {
        let sources = [source("run", "a.json")];
        let fetched = FetchedManifests::from([(
            sources[0].clone(),
            Fetched::Settled(Some(image_manifest())),
        )]);
        let current = current_manifests(&[drawn("run", "renamed")], &sources, &fetched).unwrap();
        assert_eq!(current[0].label, "renamed");
        assert_eq!(current[0].manifest, Some(image_manifest()));
        assert!(unsettled_sources(&sources, &fetched).is_empty());
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
