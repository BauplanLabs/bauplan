# docs.bauplanlabs.com

This directory contains the source for the documentation website, which is built using [docusaurus](https://docusaurus.io).

To preview changes to the markdown (in `pages`), run the following command:

    make up

That will also generate the SDK reference into `pages/reference`. Since the SDK changes infrequently, it won't be rebuilt automatically. To force a rebuild of the SDK documentation, run:

    make clean up

## Markdown tests

All python snippets in markdown are tested by default. To test the examples in the docs (and the sdk docstrings), use a dev or qa API key:

    export BAUPLAN_PROFILE=dev
    make test

If you want to skip a test, add a `notest` attribute:

    ```python notest
    raise Exception('this ship is going down!!!')
    ```

If you need a fixture, take a look at what's already available in `conftest.py`. Then you can create or use an existing one with the `fixture` attribute:

    ```python fixture:client fixture:my_branch
    assert client.has_branch("my_branch_name")
    ```
