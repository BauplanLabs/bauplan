import { createContext } from "react";

// Docusaurus doesn't pass the code block's language to each line, so we share
// it through context
export const CodeBlockLanguageContext = createContext(undefined);
