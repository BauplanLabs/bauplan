"""Tests for Client construction."""

from concurrent.futures import ThreadPoolExecutor, as_completed

import bauplan


def test_version():
    assert bauplan.__version__


def test_api_key_param(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n"
        "  nokey:\n"
        "    api_endpoint: https://example.com\n"
    )

    bauplan.Client(
        profile="nokey",
        api_key="bpln_dummy_key",
        config_file_path=str(config),
    )


def test_concurrent_queries():
    """A single Client should be usable from multiple threads (#106)."""
    client = bauplan.Client()

    def run_query(n):
        return client.query("SELECT COUNT(*) as n from titanic", ref="main")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run_query, i) for i in range(3)]
        results = [f.result().column("n")[0].as_py() for f in as_completed(futures)]

    assert results == [891, 891, 891]
