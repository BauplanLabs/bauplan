import React, { useState } from "react";
import Admonition from "@theme-original/Admonition";

export function Tip(props) {
  return <Admonition type="tip" {...props} />;
}

export function CollapsibleTip({ summary, children }) {
  const [open, setOpen] = useState(false);
  return (
    <Admonition type="tip" title="Tip">
      <p style={{ cursor: "pointer", fontWeight: "bold", margin: 0, display: "flex", alignItems: "center", justifyContent: "space-between" }} onClick={() => setOpen(!open)}>
        {summary}
        <span style={{ transition: "transform 0.2s", transform: open ? "rotate(180deg)" : "rotate(0deg)", fontSize: "1em", marginLeft: "0.5em" }}>▾</span>
      </p>
      {open && children}
    </Admonition>
  );
}

export function CollapsibleNote({ summary, children }) {
  const [open, setOpen] = useState(false);
  return (
    <Admonition type="note" title="Note">
      <p style={{ cursor: "pointer", fontWeight: "normal", margin: 0, display: "flex", alignItems: "center", justifyContent: "space-between" }} onClick={() => setOpen(!open)}>
        {summary}
        <span style={{ transition: "transform 0.2s", transform: open ? "rotate(180deg)" : "rotate(0deg)", fontSize: "1em", marginLeft: "0.5em" }}>▾</span>
      </p>
      {open && children}
    </Admonition>
  );
}

export function Note(props) {
  return <Admonition type="note" {...props} />;
}

export function Warning(props) {
  return <Admonition type="caution" {...props} />;
}

export function Danger(props) {
  return <Admonition type="danger" {...props} />;
}
