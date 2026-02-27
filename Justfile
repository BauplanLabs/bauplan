lint:
    uv run ruff check
    uv run ty check python/
    buf lint
    cargo clippy -- -Dwarnings

test: lint
    cargo test --features _integration-tests -- --test-threads=4
    uv run pytest -v
