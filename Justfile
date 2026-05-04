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

# Build an ARM Linux wheel and unpack it into dist/site-packages/ for mounting
# into a Docker container. Usage:
#   just wheel
#   docker run -v $(pwd)/dist/site-packages/bauplan:/usr/local/lib/python3.13/site-packages/bauplan ...
wheel:
    rm -rf dist
    BPLN_ENABLE_TYPE_CONTRACT=1 mise x -- maturin build --release --target aarch64-unknown-linux-gnu --zig -i python3.13 --out dist
    mkdir -p dist/site-packages
    unzip -o dist/bauplan-*.whl -d dist/site-packages
    rm -rf dist/site-packages/bauplan-*.data
