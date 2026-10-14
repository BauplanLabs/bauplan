import CodeBlock from "@theme-original/CodeBlock";
import {
  CodeBlockLanguageContext,
  TERMINAL_LANGUAGES,
} from "@site/src/theme/CodeBlock/languageContext";
import FileTree from "@site/src/theme/components/FileTree";

function codeBlockLanguage({ language, className }) {
  if (language) {
    return language;
  }
  // MDX fences arrive as className="language-sh" rather than a language prop
  return className?.match(/language-([\w-]+)/)?.[1];
}

/**
 * Wraps shell code blocks in a terminal window frame with the Bauplan
 * traffic-light dots and renders `tree` blocks as a file explorer. Other
 * languages render as standard code blocks.
 */
export default function CodeBlockWrapper(props) {
  const language = codeBlockLanguage(props);
  if (language === "tree") {
    return <FileTree source={String(props.children)} />;
  }
  if (!TERMINAL_LANGUAGES.has(language)) {
    return (
      <CodeBlockLanguageContext.Provider value={language}>
        <CodeBlock {...props} />
      </CodeBlockLanguageContext.Provider>
    );
  }
  // A fence title (```sh title="...") arrives inside the metastring, so it is
  // moved from there into the window bar instead of the default title strip
  const { title: titleProp, metastring, ...codeBlockProps } = props;
  const titleMatch = metastring?.match(/title=(["'])(.*?)\1/);
  const title = titleProp ?? titleMatch?.[2];
  const remainingMetastring = titleMatch
    ? metastring.replace(titleMatch[0], "").trim()
    : metastring;
  return (
    <div className="terminal-window">
      <div className="terminal-window__bar" aria-hidden="true">
        <span className="terminal-window__dot terminal-window__dot--red" />
        <span className="terminal-window__dot terminal-window__dot--blue" />
        <span className="terminal-window__dot terminal-window__dot--yellow" />
        {title && <span className="terminal-window__title">{title}</span>}
      </div>
      <CodeBlockLanguageContext.Provider value={language}>
        <CodeBlock {...codeBlockProps} metastring={remainingMetastring} />
      </CodeBlockLanguageContext.Provider>
    </div>
  );
}
