import { createContext } from "react";

// Docusaurus doesn't pass the code block's language to each line, so we share
// it through context
export const CodeBlockLanguageContext = createContext(undefined);

// Only commands the reader types get the terminal window, so project code
// (Python, SQL, YAML) stays visually distinct from what you run in a shell
export const TERMINAL_LANGUAGES = new Set([
  "bash",
  "sh",
  "shell",
  "zsh",
  "console",
  "shell-session",
]);
