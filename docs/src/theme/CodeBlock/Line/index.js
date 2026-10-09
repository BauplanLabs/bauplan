import { useContext } from "react";
import clsx from "clsx";
import Line from "@theme-original/CodeBlock/Line";
import {
  CodeBlockLanguageContext,
  TERMINAL_LANGUAGES,
} from "@site/src/theme/CodeBlock/languageContext";

const LITERAL_TYPES = ["number", "boolean", "null"];

function lineText(line) {
  return line.map((token) => token.content).join("");
}

// Indented lines continue the command above them (after a trailing "\")
function isCommand(line) {
  const text = lineText(line);
  return text.trim() !== "" && !/^\s/.test(text) && !text.startsWith("#");
}

// Prism reads any "-" as a possible list marker, so in a value like
// 550e8400-a716-446655440000 it colors the digits after the last dash as a
// number. A dash attached to text is part of the value, not a marker
function withoutSplitYamlLiterals(line) {
  return line.map((token, i) => {
    const dash = line[i - 1];
    const before = line[i - 2];
    const isSplit =
      token.types.some((type) => LITERAL_TYPES.includes(type)) &&
      dash?.content === "-" &&
      before !== undefined &&
      /\S$/.test(before.content);
    return isSplit ? { ...token, types: ["plain"] } : token;
  });
}

export default function LineWrapper(props) {
  const language = useContext(CodeBlockLanguageContext);
  const line =
    language === "yaml" ? withoutSplitYamlLiterals(props.line) : props.line;
  const classNames =
    TERMINAL_LANGUAGES.has(language) && isCommand(line)
      ? clsx(props.classNames, "terminal-window__command")
      : props.classNames;
  return <Line {...props} line={line} classNames={classNames} />;
}
