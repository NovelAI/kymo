// Mirrors UserConfigState's stored-else-OS theme rule (state/user_config.rs) before the wasm mounts.
{
    let stored = null;
    try {
        stored = localStorage.getItem("kymo_theme") ?? localStorage.getItem("mkdb2_theme");
    } catch (_) {}
    document.documentElement.dataset.theme =
        stored === "light" || stored === "dark"
            ? stored
            : matchMedia("(prefers-color-scheme: light)").matches
              ? "light"
              : "dark";
}
