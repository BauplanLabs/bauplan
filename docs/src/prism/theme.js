// Code block colours in the brand palette: keywords blue, strings red,
// literals yellow, comments muted. Everything else is plain text.
const makeTheme = ({ plain, background, keyword, string, literal, boolean, comment }) => ({
  plain: { color: plain, backgroundColor: background },
  styles: [
    {
      types: ["keyword", "builtin", "decorator", "key", "atrule", "tag", "selector"],
      style: { color: keyword },
    },
    {
      types: ["string", "char", "attr-value", "regex", "url"],
      style: { color: string },
    },
    {
      types: ["number", "constant", "symbol"],
      style: { color: literal },
    },
    {
      types: ["boolean", "null"],
      style: { color: boolean },
    },
    {
      types: ["comment", "prolog", "doctype", "cdata"],
      style: { color: comment, fontStyle: "italic" },
    },
  ],
});

export const lightTheme = makeTheme({
  plain: "#1f2328",
  background: "#ffffff",
  keyword: "#006a9a",
  string: "#c8001a",
  literal: "#735f00",
  // None, True and False: dark yellow reads as olive on white
  boolean: "#006a9a",
  comment: "#6a737d",
});

export const darkTheme = makeTheme({
  plain: "#e4e4e7",
  background: "#1e1e1e",
  keyword: "#00b0ff",
  string: "#ff5468",
  literal: "#ffd600",
  boolean: "#ffd600",
  comment: "#8d949e",
});
