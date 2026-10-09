import { useContext } from "react";
import clsx from "clsx";
import Line from "@theme-original/CodeBlock/Line";
import {
  CodeBlockLanguageContext,
  TERMINAL_LANGUAGES,
} from "@site/src/theme/CodeBlock/languageContext";

function lineText(line) {
  return line.map((token) => token.content).join("");
}

// Indented lines continue the command above them (after a trailing "\")
function isCommand(line) {
  const text = lineText(line);
  return text.trim() !== "" && !/^\s/.test(text) && !text.startsWith("#");
}

export default function LineWrapper(props) {
  const language = useContext(CodeBlockLanguageContext);
  const classNames =
    TERMINAL_LANGUAGES.has(language) && isCommand(props.line)
      ? clsx(props.classNames, "terminal-window__command")
      : props.classNames;
  return <Line {...props} classNames={classNames} />;
}
