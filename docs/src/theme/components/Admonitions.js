import React from "react";
import Admonition from "@theme-original/Admonition";

export function Tip(props) {
  return <Admonition type="tip" {...props} />;
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
