lint:
    uv run ruff check
    uv run ruff format --diff
    uv run ty check python/
    buf lint
    cargo clippy -- -Dwarnings

    # Prose linting.
    # https://github.com/vale-cli/vale/issues/575
    vale sync
    vale docs/pages examples --minAlertLevel warning |\
        awk 'BEGIN {status = 1} 1; END {if(/^✔/) status = 0; exit(status)}'

    # Don't even think about it.
    ! grep -rn '[—–]' \
        --include="*.md" --include="*.mdx" --include "*.rs" --include "*.pyi" \
        docs/pages examples python src

    # These are technically tests, but they just check the source.
    cargo test --test snippets

    # Lint CI/CD.
    zizmor . --persona pedantic

test: lint
    cargo test --features _integration-tests -- --test-threads=4
    uv run pytest -v
