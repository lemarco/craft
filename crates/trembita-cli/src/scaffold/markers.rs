//! Marker region names in scaffolded `manifest.rs` / `app.rs` (convention for manual edits).

/// Marker region names in generated sources.
pub mod names {
    /// Import block inside `app.rs`.
    pub const IMPORTS: &str = "trembita:imports";
    /// Job registrations inside `.jobs([...])`.
    pub const JOBS: &str = "trembita:jobs";
    /// Topic registrations inside `.topics([...])`.
    pub const TOPICS: &str = "trembita:topics";
    /// Worker registrations inside `.workers(workers!(...))`.
    pub const WORKERS: &str = "trembita:workers";
    /// Workflow registrations inside `.workflows([...])`.
    pub const WORKFLOWS: &str = "trembita:workflows";
    /// Custom gateway surfaces inside `.surfaces(|…| { … })`.
    pub const SURFACES: &str = "trembita:surfaces";
}

/// `handle_foo` → `HandleFooConsumer` (matches `#[consumer]` macro naming).
#[must_use]
pub fn consumer_type_name(module: &str) -> String {
    let pascal = module
        .split('_')
        .filter(|s| !s.is_empty())
        .map(|s| {
            let mut c = s.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
            }
        })
        .collect::<String>();
    format!("Handle{pascal}Consumer")
}

#[cfg(test)]
mod tests {

    #[test]
    fn consumer_type_name_from_module() {
        assert_eq!(super::consumer_type_name("emails"), "HandleEmailsConsumer");
    }
}
