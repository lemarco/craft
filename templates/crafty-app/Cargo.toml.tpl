[package]
name = "{{PROJECT_NAME}}"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
crafty = { version = "0.2", features = ["dev-certs"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }

[features]
default = ["http-jobs"]
http-jobs = ["crafty/http-jobs"]
