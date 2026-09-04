//! Compile-time embedded static assets.

use std::collections::HashMap;

use super::{Precompressed, StaticResponse, mime};

/// One embedded file.
#[derive(Clone, Debug)]
pub struct EmbeddedFile {
    /// Raw file bytes.
    pub bytes: &'static [u8],
    /// MIME type without encoding suffix.
    pub content_type: String,
    /// `gzip` or `br` when this variant is precompressed.
    pub encoding: Option<String>,
}

/// In-memory map of site files keyed by normalized relative path (`index.html`, `assets/app.js`).
#[derive(Clone, Debug, Default)]
pub struct EmbeddedAssets {
    /// Path → file (paths use `/` separators, no leading slash).
    pub files: HashMap<String, EmbeddedFile>,
}

impl EmbeddedAssets {
    /// Register one file.
    pub fn insert(&mut self, path: impl AsRef<str>, file: EmbeddedFile) {
        self.files
            .insert(normalize_embed_path(path.as_ref()), file);
    }
}

/// Build [`EmbeddedAssets`] from an [`include_dir::Dir`] snapshot.
#[must_use]
pub fn embedded_from_dir(dir: &'static include_dir::Dir) -> EmbeddedAssets {
    let mut assets = EmbeddedAssets::default();
    for file in dir.files() {
        let path = file.path().to_string_lossy().replace('\\', "/");
        let content_type = mime::from_path(&path);
        assets.insert(
            path,
            EmbeddedFile {
                bytes: file.contents(),
                content_type,
                encoding: None,
            },
        );
    }
    for entry in dir.entries() {
        if let include_dir::DirEntry::Dir(sub) = entry {
            let sub_assets = embedded_from_dir(sub);
            for (path, file) in sub_assets.files {
                assets.insert(path, file);
            }
        }
    }
    assets
}

/// Resolve a request path against embedded assets.
pub fn resolve(
    assets: &EmbeddedAssets,
    path: &str,
    precompressed: Precompressed,
) -> Option<StaticResponse> {
    let key = normalize_embed_path(path.trim_start_matches('/'));
    if let Some(file) = assets.files.get(&key) {
        return Some(StaticResponse {
            body: file.bytes.to_vec(),
            content_type: file.content_type.clone(),
            content_encoding: file.encoding.clone(),
            redirect_to: None,
        });
    }

    if precompressed == Precompressed::Auto {
        if let Some(file) = assets.files.get(&format!("{key}.gz")) {
            return Some(StaticResponse {
                body: file.bytes.to_vec(),
                content_type: mime::from_path(&key),
                content_encoding: Some("gzip".to_string()),
                redirect_to: None,
            });
        }
        if let Some(file) = assets.files.get(&format!("{key}.br")) {
            return Some(StaticResponse {
                body: file.bytes.to_vec(),
                content_type: mime::from_path(&key),
                content_encoding: Some("br".to_string()),
                redirect_to: None,
            });
        }
    }

    None
}

fn normalize_embed_path(path: &str) -> String {
    path.trim_start_matches('/').replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_strips_leading_slash() {
        assert_eq!(normalize_embed_path("/assets/x.js"), "assets/x.js");
    }

    #[test]
    fn resolve_returns_not_found_as_none() {
        let assets = EmbeddedAssets::default();
        assert!(resolve(&assets, "/missing", Precompressed::Off).is_none());
    }
}
