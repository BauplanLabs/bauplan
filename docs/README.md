# docs.bauplanlabs.com

This directory contains the source for the documentation website, which is built using [docusaurus](https://docusaurus.io).

To preview changes to the markdown (in `pages`), run the following command:

    just serve

# Typechecking snippets

All python snippets are typechecked by default.

If you really need to skip one, you can use the `type:ignore` code fence attribute:

  ```python type:ignore
  foo: str = 123
  ```
