---
layout: home

hero:
  name: trembita
  text: Distributed Raft + actors for Rust
  tagline: One codebase, N nodes — elastic, self-healing product apps with embedded redb. Library-first on VPS; no mandatory Redis.
  image:
    src: /logo.svg
    alt: trembita
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: Browse Examples
      link: /examples
    - theme: alt
      text: API on docs.rs
      link: https://docs.rs/trembita

features:
  - icon: 🏔️
    title: Embedded consensus
    details: Pure Raft FSM, HTTP/3/mTLS, and redb persistence live in your binary — no sidecar control plane.
  - icon: 🎭
    title: Cross-node actors
    details: Supervised actors with mailbox routing, sessions, migration, and optional Redis-backed state.
  - icon: 📬
    title: Jobs & event topics
    details: Durable queues, DLQ, cron, pub/sub topics, and external Postgres backlog — all on embedded redb by default.
  - icon: 🌐
    title: Product gateway
    details: TrembitaApp + HTTP/WebSocket gateway with sticky sessions, TLS, drain, and opt-in operator APIs.
  - icon: 🔀
    title: Multi-Raft scaling
    details: Meta-Raft catalog, shard migration, cross-shard sagas, and optional 2PC for write scaling.
  - icon: 🔄
    title: Self-update coordinator
    details: Leader-driven rolling upgrades with wire N/N−1 compatibility — same artifact on every node.
---

## Install

<div class="install-strip">

```sh
cargo add trembita --features dev-certs
```

</div>

## What to read next

- [Getting started](/guide/getting-started) — minimal `TrembitaApp`, multi-node env vars, first deploy
- [Scenarios](/scenarios/) — pick jobs, topics, workers, sessions, or workflows
- [Examples](/examples) — five runnable showcases in the repo
- [Status](/reference/status) — shipped capabilities and explicit non-goals
- [Changelog](/changelog) — semver history for the workspace

API reference lives on [docs.rs/trembita](https://docs.rs/trembita). Source and issues on [GitLab](https://gitlab.com/lemarco/trembita).
