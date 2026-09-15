//! Built-in product showcase metadata (examples/*).

/// Local cluster definition for `trembita dev`.
#[derive(Debug, Clone, Copy)]
pub struct Showcase {
    /// CLI id (`stateful-workers`, …).
    pub id: &'static str,
    /// `examples/<dir>` folder name.
    pub dir: &'static str,
    /// Release binary name (`target/release/…`).
    pub binary: &'static str,
    /// Node 1 QUIC+HTTP port (nodes *n* use `base_port + n - 1`).
    pub base_port: u16,
    /// `target/<name>` cluster state directory.
    pub cluster_dir: &'static str,
    /// Node cert ids for `dev/certs/generate.sh` (includes `0` join bootstrap).
    pub cert_ids: &'static [u32],
    /// Single-process HTTP-only demo (no multi-node QUIC cluster).
    pub solo_http: bool,
}

const SHOWCASES: &[Showcase] = &[
    Showcase {
        id: "background-jobs",
        dir: "background-jobs",
        binary: "trembita-showcase-background-jobs",
        base_port: 8090,
        cluster_dir: "trembita-bg-jobs-cluster",
        cert_ids: &[0, 1, 2, 3, 4],
        solo_http: false,
    },
    Showcase {
        id: "stateful-workers",
        dir: "stateful-workers",
        binary: "trembita-showcase-stateful-workers",
        base_port: 8190,
        cluster_dir: "trembita-stateful-workers-cluster",
        cert_ids: &[0, 1, 2, 3, 4],
        solo_http: false,
    },
    Showcase {
        id: "realtime",
        dir: "realtime",
        binary: "trembita-showcase-realtime",
        base_port: 8290,
        cluster_dir: "trembita-realtime-cluster",
        cert_ids: &[0, 1, 2, 3, 4],
        solo_http: false,
    },
    Showcase {
        id: "workflows",
        dir: "workflows",
        binary: "trembita-showcase-workflows",
        base_port: 8490,
        cluster_dir: "trembita-workflows-cluster",
        cert_ids: &[0, 1, 2, 3, 4],
        solo_http: false,
    },
    Showcase {
        id: "self-update",
        dir: "self-update",
        binary: "trembita-showcase-self-update",
        base_port: 8190,
        cluster_dir: "trembita-self-update-cluster",
        cert_ids: &[0, 1, 2, 3],
        solo_http: false,
    },
    Showcase {
        id: "ws-minimal",
        dir: "ws-minimal",
        binary: "trembita-showcase-ws-minimal",
        base_port: 8390,
        cluster_dir: "trembita-ws-minimal",
        cert_ids: &[],
        solo_http: true,
    },
    Showcase {
        id: "market-ws",
        dir: "market-ws",
        binary: "trembita-showcase-market-ws",
        base_port: 8391,
        cluster_dir: "trembita-market-ws",
        cert_ids: &[],
        solo_http: true,
    },
    Showcase {
        id: "ws-notify",
        dir: "ws-notify",
        binary: "trembita-showcase-ws-notify",
        base_port: 8392,
        cluster_dir: "trembita-ws-notify",
        cert_ids: &[],
        solo_http: true,
    },
];

/// All registered showcases.
#[must_use]
pub fn all() -> &'static [Showcase] {
    SHOWCASES
}

/// Resolve showcase id (accepts `stateful-workers` or `stateful_workers`).
pub fn find(id: &str) -> Option<&'static Showcase> {
    let norm = id.replace('_', "-");
    SHOWCASES.iter().find(|s| s.id == norm)
}

/// Comma-separated ids for error messages.
#[must_use]
pub fn id_list() -> String {
    SHOWCASES
        .iter()
        .map(|s| s.id)
        .collect::<Vec<_>>()
        .join(", ")
}

impl Showcase {
    /// `examples/<dir>` path under workspace root.
    #[must_use]
    pub fn example_dir(&self, workspace: &std::path::Path) -> std::path::PathBuf {
        workspace.join("examples").join(self.dir)
    }

    /// Release binary path after `cargo build --release`.
    #[must_use]
    pub fn release_bin(&self, workspace: &std::path::Path) -> std::path::PathBuf {
        self.example_dir(workspace)
            .join("target/release")
            .join(self.binary)
    }

    /// Cluster state under `target/<cluster_dir>`.
    #[must_use]
    pub fn cluster_root(&self, workspace: &std::path::Path) -> std::path::PathBuf {
        workspace.join("target").join(self.cluster_dir)
    }

    /// `127.0.0.1:<port>` for node index `1..=nodes`.
    #[must_use]
    pub fn listen_addr(&self, node: u32) -> String {
        let port = u32::from(self.base_port) + u32::from(node.saturating_sub(1));
        format!("127.0.0.1:{port}")
    }

    /// Dynamic join seed (`1@127.0.0.1:base_port`).
    #[must_use]
    pub fn join_seed(&self) -> String {
        format!("1@127.0.0.1:{}", self.base_port)
    }
}
