"""Tests for Client construction."""

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
