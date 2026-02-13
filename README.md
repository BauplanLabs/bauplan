# Bauplan

Bauplan is a code-first data platform with git-like semantics that lets you bring modern engineering practices to your pipelines.

This repo contains the CLI and SDK code for interacting with the platform. 

For more information about Bauplan or the SDK, you can check out:

 - Our website: [bauplanlabs.com](https://bauplanlabs.com)
 - The documentation, including SDK docs: [docs.bauplanlabs.com](https://bauplanlabs.com)

## Contributing

We're not really looking for external contributions at the moment. However, feel free to [open an issue](https://github.com/bauplanlabs/bauplan/issues/new) if you encounter any problems with any part of the platform! 

### Running the test suite

Running `BAUPLAN_PROFILE=... just test` will run the tests.

> !IMPORTANT
> Running the tests requires a valid API key, and will create (and hopefully also clean up) a bunch of garbage, so you shouldn't run it against production!

The tests cover both the code in this repo and the behavior of the platform altogether. There are three types of tests:

 - Rust integration tests. These are gated behind a `_integration-tests` cargo feature.
 - CLI end-to-end tests. These live in `tests` and run the CLI and check the output.
 - Pytests for testing the python-side SDK surface. These can be found in `python/tests`.
 
The command `cargo test --features _integration-tests` will run the first two. `uv run pytest` will run the python tests, and `uv run ty check` will validate the python stubs.

### Maintaining the python stubs

You can generate stub definitions for the python SDK with `cargo run -p gen-stubs`. However, the stub generation is currently [incomplete](https://github.com/PyO3/pyo3/issues/5137), so the output needs to be merged by hand with the existing stubs. The instructions for that can be found in [`gen-stubs/README.md`](/gen-stubs/README.md). This is best done with an LLM of some kind.
