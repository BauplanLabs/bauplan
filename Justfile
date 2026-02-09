test:
    cargo clippy -- -Dwarnings
    uv run ruff check
    uv run ty check python/

    cargo test --features _integration-tests -- --test-threads=4
    uv run pytest -v
