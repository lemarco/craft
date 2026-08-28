[package]
name = "{{PROJECT_NAME}}"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
crafty = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }

[features]
default = []
http-jobs = ["crafty/http-jobs"]
