lint:
    uv run ruff check
    uv run ruff format --diff
    uv run ty check python/
    buf lint
    cargo clippy -- -Dwarnings

    # Prose linting.
    vale sync
    vale docs/pages examples --minAlertLevel warning

    # Don't even think about it.
    ! grep -rn '[—–]' \
        --include="*.md" --include="*.mdx" \
        docs/pages examples

    # These are technically tests, but they just check the source.
    cargo test --test snippets

    # Lint CI/CD
    zizmor . --persona pedantic

test: lint
    cargo test --features _integration-tests -- --test-threads=4
    uv run pytest -v
