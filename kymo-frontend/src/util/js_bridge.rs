use dioxus::prelude::*;

use super::unique_id;

const MOUNT_CALL: &str = "window.__kymo_bridges.mount(__BRIDGE_NAME__,__BRIDGE_OWNER__,";

/// One registry for page-wide JavaScript bridges. Mounting a successor tears down its predecessor synchronously; an old Rust scope can unmount only the owner it created, so a late drop cannot kill that successor. Use this registry for page-role singleton listeners, with or without a Rust channel; keep element-lifetime listeners on element flags, per-gesture window listeners self-removing, and permanent teardown-free installs on window flags. Bridge-local `catch (_) { td(); }` paths are defensive only: Dioxus web sends currently do not throw, so ordinary teardown must always come through this registry.
pub const LIFECYCLE_JS: &str = r#"window.__kymo_bridges??=(()=>{const slots=new Map();return{
mount(name,owner,cleanup){
  try{slots.get(name)?.teardown();}catch(error){console.error('[kymo bridge] predecessor cleanup failed',error);}
  let live=true;
  const slot={owner,teardown:null};
  slot.teardown=()=>{
    if(!live)return;
    live=false;
    try{cleanup();}finally{if(slots.get(name)===slot)slots.delete(name);}
  };
  slots.set(name,slot);
  return slot.teardown;
},
unmount(name,owner){const slot=slots.get(name);if(slot?.owner===owner)slot.teardown();}
};})()"#;

/// Encode a complete quoted JavaScript string expression; unlike uPlot's `create_js::esc_js`, this includes the surrounding quotes.
pub(crate) fn js_string(value: &str) -> String {
    serde_json::to_string(value).expect("serializing a string cannot fail")
}

pub(crate) fn validate_template(template: &str) {
    assert!(
        template.contains(MOUNT_CALL),
        "JavaScript bridge template is missing the lifecycle mount call"
    );
}

#[derive(Clone)]
pub struct JsBridge {
    name: &'static str,
    owner: String,
}

impl JsBridge {
    /// Fill the shared lifecycle placeholders in a bridge-specific JavaScript body. JSON encoding keeps the name and generated owner safe as JS literals.
    pub fn script(&self, template: &str) -> String {
        validate_template(template);
        template
            .replace("__BRIDGE_NAME__", &js_string(self.name))
            .replace("__BRIDGE_OWNER__", &js_string(&self.owner))
    }
}

/// Allocate this scope's bridge owner and release it after unmount. A name is a singleton role: mounting two live scopes under the same name intentionally evicts the older one. The unmount eval must live on the root scope because tasks spawned on a dying Dioxus scope are never polled.
pub fn use_bridge(name: &'static str) -> JsBridge {
    let owner = use_hook(|| unique_id(name));
    use_drop({
        let owner = owner.clone();
        move || {
            let name = js_string(name);
            let owner = js_string(&owner);
            let js = format!("window.__kymo_bridges?.unmount({name},{owner});");
            dioxus::core::spawn_forever(async move {
                let _ = document::eval(&js).await;
            });
        }
    });
    JsBridge { name, owner }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_script_encodes_names_and_owners_as_literals() {
        assert_eq!(
            JsBridge {
                name: "zo'om",
                owner: "owner\n2".to_string(),
            }
            .script("window.__kymo_bridges.mount(__BRIDGE_NAME__,__BRIDGE_OWNER__,cleanup)"),
            "window.__kymo_bridges.mount(\"zo'om\",\"owner\\n2\",cleanup)"
        );
    }

    #[test]
    #[should_panic(expected = "missing the lifecycle mount call")]
    fn bridge_script_requires_the_exact_mount_call() {
        JsBridge {
            name: "zones",
            owner: "owner".to_string(),
        }
        .script("// __BRIDGE_NAME__ __BRIDGE_OWNER__");
    }
}
