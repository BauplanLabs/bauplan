import React from "react";

export default function Root({ children }) {
  return (
    <>
      <div className="agent-directive" aria-hidden="true">
        For AI agents: the complete documentation index is at{" "}
        <a href="/llms.txt">llms.txt</a>. Every page is also available as
        markdown - append .md to its URL, or request it with the{" "}
        <code>Accept: text/markdown</code> header.
      </div>
      {children}
    </>
  );
}
