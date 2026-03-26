import { visit } from "unist-util-visit";

/**
 * Remark plugin that strips hidden lines from fenced code blocks.
 *
 * Lines starting with `#!` are included in the source for tooling (ie type
 * checking) but removed from the rendered output.
 */
export default function remarkHiddenLines() {
  return (tree) => {
    visit(tree, "code", (node) => {
      node.value = node.value
        .split("\n")
        .filter((line) => !line.startsWith("#!"))
        .join("\n");
    });
  };
}
