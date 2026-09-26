Development happens internally. I won't read your PRs and issues, but an LLM might.

The standard of code quality is to survive multiple review rounds from a diversity of frontier models. I, the human, rarely read the code. The standard for external contributors will be higher.

To build and test locally, run `scripts/ci.sh`, or name sections to run only those (`root`, `wasm`, `local-runtime`, `qualification`, `client`). It needs Rust (with clippy, rustfmt, and the `wasm32-unknown-unknown` target), `protoc`, and Python 3.10 or newer.
