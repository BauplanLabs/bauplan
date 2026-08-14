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

# Typecheck the doc snippets against the oldest SDK we support instead of the
# working tree. Readers install from PyPI, so a snippet can typecheck here and
# still be broken for them. Defaults to tool.bauplan.docs.min-sdk; pass a
# version to check a different one.
check-docs-release version="":
    #!/usr/bin/env bash
    set -euo pipefail

    version="{{ version }}"
    if [ -z "$version" ]; then
        version=$(uv run python -c \
            'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["tool"]["bauplan"]["docs"]["min-sdk"])')
    fi

    dir=$(mktemp -d)
    trap 'rm -rf "$dir"' EXIT

    # Everything the snippets import except the SDK itself, so `bauplan` is the
    # only thing that differs from the working tree's environment.
    uv export --only-group dev --no-emit-project --no-hashes -q -o "$dir/requirements.txt"
    uv venv -q "$dir/venv"
    uv pip install -q --python "$dir/venv" -r "$dir/requirements.txt" "bauplan==$version"

    # --nocapture so the log records which version we actually checked against.
    BAUPLAN_SNIPPET_ENV="$dir/venv" cargo test --test snippets -- --nocapture
