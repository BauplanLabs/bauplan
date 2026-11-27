lint:
    uv run ruff check
    uv run ruff format --diff
    uv run ty check python/
    buf lint
    cargo fmt --all -- --check
    cargo clippy -- -Dwarnings
    cargo clippy -p bauplan-api -p bauplan-api-macros --all-targets -- -Dwarnings
    cargo clippy -p bauplan-api --all-targets --features client-reqwest -- -Dwarnings
    cargo clippy -p bauplan-api --all-targets --features client-ureq -- -Dwarnings

    # Prose linting.
    # https://github.com/vale-cli/vale/issues/575
    vale sync
    vale docs/pages examples --minAlertLevel warning |\
        awk 'BEGIN {status = 1} 1; END {if(/^✔/) status = 0; exit(status)}'

    # Don't even think about it.
    ! grep -rn '[—–]' \
        --include="*.md" --include="*.mdx" --include "*.rs" --include "*.pyi" \
        docs/pages examples python src bauplan-api bauplan-api-macros

    # These are technically tests, but they just check the source.
    just docs/generate-reference
    cargo test --test snippets

    # Lint CI/CD.
    zizmor . --persona pedantic

test:
    cargo test --features _integration-tests -- --test-threads=4
    cargo test -p bauplan-api -p bauplan-api-macros
    uv run pytest -v
