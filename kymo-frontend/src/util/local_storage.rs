use web_sys::window;

fn storage() -> Option<web_sys::Storage> {
    window()?.local_storage().ok()?
}

pub fn get(key: &str) -> Option<String> {
    storage()?.get_item(key).ok()?
}

/// Read the canonical key, falling back to the legacy key. Reads are pure:
/// nothing is written or deleted here, so a read-only visit leaves rollback
/// state intact.
pub fn get_migrating(key: &str, legacy_key: &str) -> Option<String> {
    get(key).or_else(|| get(legacy_key))
}

fn retire_after_write<E>(write: Result<(), E>, retire: impl FnOnce()) -> bool {
    if write.is_err() {
        return false;
    }
    retire();
    true
}

/// Store a value; `false` when storage is unavailable or the browser refused
/// the write (quota, security). Callers that retire legacy state must check it.
pub fn set(key: &str, value: &str) -> bool {
    storage().is_some_and(|storage| storage.set_item(key, value).is_ok())
}

/// Store the canonical value and retire its legacy key only after the browser
/// confirms the write. A quota/security failure must leave rollback state
/// intact rather than deleting the sole durable copy.
pub fn set_migrating(key: &str, legacy_key: &str, value: &str) -> bool {
    let Some(storage) = storage() else {
        return false;
    };
    retire_after_write(storage.set_item(key, value), || {
        let _ = storage.remove_item(legacy_key);
    })
}

pub fn remove(key: &str) -> bool {
    storage().is_some_and(|storage| storage.remove_item(key).is_ok())
}

/// Remove both the canonical and the legacy key: a reset must not let the next
/// fallback read resurrect the legacy value. `false` when either removal failed
/// (storage unavailable or refused), so callers do not report a reset that did
/// not stick.
pub fn remove_migrating(key: &str, legacy_key: &str) -> bool {
    let Some(storage) = storage() else {
        return false;
    };
    let canonical = storage.remove_item(key).is_ok();
    let legacy = storage.remove_item(legacy_key).is_ok();
    canonical && legacy
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::retire_after_write;

    #[test]
    fn legacy_state_is_retired_only_after_a_confirmed_write() {
        let retired = Cell::new(false);
        assert!(!retire_after_write(Err::<(), _>("quota"), || retired.set(true)));
        assert!(!retired.get());

        assert!(retire_after_write(Ok::<_, &str>(()), || retired.set(true)));
        assert!(retired.get());
    }
}
