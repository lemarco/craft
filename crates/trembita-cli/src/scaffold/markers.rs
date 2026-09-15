//! Marker-based patching of `src/app.rs`.

/// Marker region names used by scaffold and `trembita add`.
pub mod names {
    /// Import block inside `app.rs`.
    pub const IMPORTS: &str = "trembita:imports";
    /// Job registrations inside `.jobs([...])`.
    pub const JOBS: &str = "trembita:jobs";
    /// Topic registrations inside `.topics([...])`.
    pub const TOPICS: &str = "trembita:topics";
    /// Worker registrations inside `.workers(workers!(...))`.
    pub const WORKERS: &str = "trembita:workers";
    /// Custom gateway surfaces inside `.surfaces(|…| { … })`.
    pub const SURFACES: &str = "trembita:surfaces";
}

/// Patch failure.
#[derive(Debug, thiserror::Error)]
pub enum PatchError {
    /// File missing.
    #[error("missing file: {0}")]
    MissingFile(String),
    /// Marker not found (legacy app.rs).
    #[error(
        "marker `{marker}` not found in app.rs — re-scaffold or add `{marker}` / `{marker}-end` comments"
    )]
    MissingMarker {
        /// Marker name.
        marker: String,
    },
    /// Duplicate registration.
    #[error("{0}")]
    Duplicate(String),
    /// I/O error.
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// In-memory editor for `app.rs`.
#[derive(Debug, Clone)]
pub struct AppRsPatch {
    content: String,
}

impl AppRsPatch {
    /// Load `app.rs`.
    pub fn load(path: &std::path::Path) -> Result<Self, PatchError> {
        if !path.is_file() {
            return Err(PatchError::MissingFile(path.display().to_string()));
        }
        Ok(Self {
            content: std::fs::read_to_string(path)?,
        })
    }

    /// Raw contents (for doctor).
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Whether a marker region exists.
    #[must_use]
    pub fn has_marker(&self, marker: &str) -> bool {
        self.content.contains(&format!("// {marker}"))
    }

    /// Whether `needle` appears anywhere in the file.
    #[must_use]
    pub fn contains(&self, needle: &str) -> bool {
        self.content.contains(needle)
    }

    /// Insert `line` before the `{marker}-end` comment (deduped by substring match on `line`).
    pub fn insert_before_end(&mut self, marker: &str, line: &str) -> Result<(), PatchError> {
        if self.contains(line.trim()) {
            return Err(PatchError::Duplicate(format!(
                "app.rs already contains: {}",
                line.trim()
            )));
        }
        let end = format!("// {marker}-end");
        let start = format!("// {marker}");
        if !self.content.contains(&start) {
            return Err(PatchError::MissingMarker {
                marker: marker.to_string(),
            });
        }
        let Some(idx) = self.content.find(&end) else {
            return Err(PatchError::MissingMarker {
                marker: marker.to_string(),
            });
        };
        let insertion = format!("{line}\n    ");
        self.content.insert_str(idx, &insertion);
        Ok(())
    }

    /// Insert an import after `// trembita:imports` (or at top if marker missing).
    pub fn insert_import(&mut self, line: &str) -> Result<(), PatchError> {
        if self.contains(line.trim()) {
            return Ok(());
        }
        let marker = format!("// {}", names::IMPORTS);
        if self.content.contains(&marker) {
            self.insert_before_end(names::IMPORTS, line)
        } else {
            // Legacy: prepend after first doc comment block.
            self.content = format!("{line}\n{}", self.content);
            Ok(())
        }
    }

    /// Insert a full block before `.configure(TrembitaConfigure` when marker region absent.
    pub fn insert_block_before_configure(&mut self, block: &str) -> Result<(), PatchError> {
        if self.contains(block.trim()) {
            return Err(PatchError::Duplicate("block already present".into()));
        }
        let anchor = ".configure(TrembitaConfigure";
        let Some(idx) = self.content.find(anchor) else {
            return Err(PatchError::MissingMarker {
                marker: "configure anchor".into(),
            });
        };
        self.content.insert_str(idx, block);
        Ok(())
    }

    /// Ensure `.surfaces(|…| { // trembita:surfaces … })` exists on `GatewayOpts`.
    pub fn ensure_surfaces_block(&mut self) -> Result<(), PatchError> {
        if self.has_marker(names::SURFACES) {
            return Ok(());
        }
        const NEEDLE: &str = ".identity(GatewayBearerIdentity::from_env())";
        const REPLACEMENT: &str = ".identity(GatewayBearerIdentity::from_env())
                    .surfaces(|state| {
                        // trembita:surfaces
                        Gateway::new(false)
                        // trembita:surfaces-end
                    })";
        if self.content.contains(NEEDLE) {
            self.content = self.content.replacen(NEEDLE, REPLACEMENT, 1);
            return Ok(());
        }
        Err(PatchError::MissingMarker {
            marker: "gateway .identity(...)".into(),
        })
    }

    /// Insert a `.surface(...)` chain before `// trembita:surfaces-end`.
    pub fn insert_surface(&mut self, surface_lines: &str) -> Result<(), PatchError> {
        self.ensure_surfaces_block()?;
        if self.contains(surface_lines.trim()) {
            return Err(PatchError::Duplicate("surface already registered".into()));
        }
        self.insert_before_end(names::SURFACES, surface_lines)
    }

    /// Replace the first occurrence of `from` with `to`.
    pub fn replace_once(&mut self, from: &str, to: &str) -> Result<(), PatchError> {
        if !self.content.contains(from) {
            return Err(PatchError::MissingMarker {
                marker: from.to_string(),
            });
        }
        if self.content.contains(to) {
            return Err(PatchError::Duplicate(format!("already contains: {to}")));
        }
        self.content = self.content.replacen(from, to, 1);
        Ok(())
    }

    /// Write back to disk.
    pub fn save(&self, path: &std::path::Path) -> Result<(), PatchError> {
        std::fs::write(path, &self.content)?;
        Ok(())
    }
}

/// Ensure `mod.rs` contains `pub mod {name};`.
///
/// Returns `true` when a new declaration was written.
pub fn ensure_mod_declaration(mod_rs: &std::path::Path, module: &str) -> Result<bool, PatchError> {
    let decl = format!("pub mod {module};");
    if !mod_rs.is_file() {
        std::fs::create_dir_all(mod_rs.parent().expect("mod.rs parent"))?;
        std::fs::write(mod_rs, format!("//! Generated module root.\n\n{decl}\n"))?;
        return Ok(true);
    }
    let content = std::fs::read_to_string(mod_rs)?;
    if content.lines().any(|l| l.trim() == decl) {
        return Ok(false);
    }
    let mut out = content;
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(&format!("{decl}\n"));
    std::fs::write(mod_rs, out)?;
    Ok(true)
}

/// Ensure `src/main.rs` declares `mod {module};` after `mod app;`.
///
/// Returns `true` when a new declaration was written.
pub fn ensure_main_module(
    project: &super::project::TrembitaProject,
    module: &str,
) -> Result<bool, PatchError> {
    let main_path = project.main_rs();
    let content = std::fs::read_to_string(&main_path)?;
    let decl = format!("mod {module};");
    if content.lines().any(|l| l.trim() == decl) {
        return Ok(false);
    }
    let needle = "mod app;";
    let Some(idx) = content.find(needle) else {
        return Ok(false);
    };
    let insert_at = idx + needle.len();
    let mut out = content;
    out.insert_str(insert_at, &format!("\n{decl}"));
    std::fs::write(main_path, out)?;
    Ok(true)
}

/// Convert stream/group name to a Rust module identifier (`platform.events` → `platform_events`).
#[must_use]
pub fn module_name(raw: &str) -> String {
    raw.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
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
    use super::*;

    #[test]
    fn module_name_sanitizes_dots() {
        assert_eq!(module_name("platform.events"), "platform_events");
    }

    #[test]
    fn consumer_type_name_from_module() {
        assert_eq!(super::consumer_type_name("emails"), "HandleEmailsConsumer");
    }
}
