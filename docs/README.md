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

# Diagrams

Diagrams are Mermaid code blocks. The look is set once, in
`src/mermaid/theme.js` and `src/css/diagrams.css`, and follows the light and
dark themes. Don't style a diagram on its own page: no `%%{init}%%`, `style`,
`classDef`, or inline HTML beyond `<b>`, `<br/>`, `<code>`, and a node's
divider and list (`<hr/><ul><li>…</li></ul>`).

Tag a node with one of these classes when its role matters:

| Class         | Use for                               |
| ------------- | ------------------------------------- |
| (none)        | your code, your environment, data     |
| `:::accent`   | anything Bauplan runs or manages      |
| `:::external` | your bucket or cloud account          |
| `:::code`     | a node whose label is a code snippet  |
| `:::title`    | a group title that arrows must avoid  |
| `:::spacer`   | invisible; balances a `:::title` node |

  ```mermaid
  flowchart TB
      cli["<b>Bauplan CLI</b>"]
      subgraph cloud["Bauplan Managed Cloud"]
          runner["<b>EC2 Runner</b><br/>(compute runtime)"]:::accent
      end
      bucket["<b>Your S3 Bucket</b>"]:::external
      cli --> cloud --> bucket
  ```

Label a box with its name in bold, then an optional detail line in brackets:
`"<b>EC2 Runner</b><br/>(compute runtime)"`. A `:::code` box gets a bold name
too, above the code. Table and model names stay plain. Wrap a code name on an
arrow in `<code>`, as in `A -- "<code>BaseResult</code>" --> B`.

Use subgraph titles. If arrows entering a group cross its title, give the
group a blank title (`[" "]`) and put a `:::title` node inside it instead.
If that pushes the group off-centre, add a `:::spacer` node with the same
text in `<b>`, so it matches the title's width.

Branches in a `gitGraph` take the logo colours in order: blue, yellow, red.
