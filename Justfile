test:
    cargo clippy -- -Dwarnings
    uv run ruff check
    uv run ty check python/

    cargo test
    uv run pytest -v

stub:
    cargo build --release --features python
    cargo run -p gen-stubs
