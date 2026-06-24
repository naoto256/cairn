//! Static method dispatch for Tier-2.5 PHP.
//!
//! We resolve a call when the receiver is statically pinnable:
//!   * `Foo::bar()` / `\Foo\Bar::baz()` — receiver is a class name.
//!   * `self::bar()` — current lexical class.
//!   * `parent::bar()` — parent class of the current lexical class.
//!   * `static::bar()` — late-static binding; conservatively resolved
//!     to the lexical class (we don't model runtime overrides).
//!
//! `$obj->method()` and dynamic receivers (`Foo::$method()`,
//! `call_user_func`) are deliberately *not* recorded — those belong
//! to Tier-3.

use std::collections::HashMap;

use crate::const_resolver::{CallReceiver, ConstIndex, FileConstFacts, MethodCall};
use crate::mro::Mro;

#[derive(Debug, Clone)]
pub struct DispatchResolution {
    pub path: String,
    pub qualified: String,
}

/// Workspace-wide method index keyed by `(owner_qualified, method_name)`.
#[derive(Debug, Default)]
pub struct MethodIndex {
    by_owner: HashMap<(String, String), MethodEntry>,
}

#[derive(Debug, Clone)]
struct MethodEntry {
    qualified: String,
    path: String,
}

impl MethodIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_owner = HashMap::new();
        for (path, _, facts) in per_file {
            for m in &facts.method_defs {
                by_owner
                    .entry((m.owner.clone(), m.name.clone()))
                    .or_insert(MethodEntry {
                        qualified: m.qualified.clone(),
                        path: path.clone(),
                    });
            }
        }
        Self { by_owner }
    }

    fn get(&self, owner: &str, method: &str) -> Option<&MethodEntry> {
        self.by_owner.get(&(owner.to_string(), method.to_string()))
    }
}

pub fn resolve_call(
    call: &MethodCall,
    const_index: &ConstIndex,
    mro: &Mro,
    methods: &MethodIndex,
    aliases: &HashMap<String, String>,
) -> Option<DispatchResolution> {
    let owner = match &call.receiver {
        CallReceiver::Const { parts, absolute } => {
            let target =
                const_index.resolve(parts, *absolute, call.namespace.as_deref(), aliases)?;
            target.qualified
        }
        CallReceiver::SelfClass => call.lexical_class.clone()?,
        CallReceiver::StaticClass => call.lexical_class.clone()?,
        CallReceiver::Parent => {
            let lex = call.lexical_class.as_deref()?;
            mro.parent_of(lex)?.to_string()
        }
        CallReceiver::Unknown => return None,
    };

    let chain = mro.ancestors(&owner);
    let start = if matches!(call.receiver, CallReceiver::Parent) {
        // Skip the lexical class itself; parent:: starts at the parent.
        chain.iter().position(|c| c == owner.as_str()).unwrap_or(0)
    } else {
        0
    };
    for ancestor in chain.iter().skip(start) {
        if let Some(hit) = methods.get(ancestor, &call.method) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
    }
    None
}
