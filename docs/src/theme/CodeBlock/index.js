import CodeBlock from "@theme-original/CodeBlock";
import { useRef, useState } from "react";
import { flushSync } from "react-dom";
import { CodeBlockLanguageContext } from "@site/src/theme/CodeBlock/languageContext";
import FileTree from "@site/src/theme/components/FileTree";

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
    <TerminalWindow title={title}>
      <CodeBlockLanguageContext.Provider value={language}>
        <CodeBlock {...codeBlockProps} metastring={remainingMetastring} />
      </CodeBlockLanguageContext.Provider>
    </TerminalWindow>
  );
}

function TerminalWindow({ title, children }) {
  const [minimized, setMinimized] = useState(false);
  const [closed, setClosed] = useState(false);
  const [maximized, setMaximized] = useState(false);
  // Holds the window's spot in the page while it is lifted into the dialog
  const [placeholderHeight, setPlaceholderHeight] = useState(0);
  const windowRef = useRef(null);
  const dialogRef = useRef(null);
  const redRef = useRef(null);
  const blueRef = useRef(null);
  const yellowRef = useRef(null);
  const restoreRef = useRef(null);
  // A drag that starts inside the window and ends on the backdrop also fires
  // a click on the dialog, so only a press that began on the backdrop exits
  const pressedBackdropRef = useRef(false);

  // Each change below swaps which element holds the button the reader just
  // pressed, so focus is handed to its replacement to keep keyboard users in place
  const enterFullScreen = () => {
    setPlaceholderHeight(windowRef.current.getBoundingClientRect().height);
    flushSync(() => {
      setMinimized(false);
      setMaximized(true);
    });
    dialogRef.current.showModal();
    yellowRef.current.focus();
  };

  const exitFullScreen = () => {
    flushSync(() => setMaximized(false));
    yellowRef.current.focus();
  };

  const toggleMinimized = () => {
    flushSync(() => {
      setMinimized(!minimized);
      setMaximized(false);
    });
    blueRef.current.focus();
  };

  const close = () => {
    flushSync(() => {
      setClosed(true);
      setMaximized(false);
    });
    restoreRef.current.focus();
  };

  const restore = () => {
    flushSync(() => {
      setClosed(false);
      setMinimized(false);
    });
    redRef.current.focus();
  };

  if (closed) {
    return (
      <div className="terminal-window-closed">
        <span>Oops, where&rsquo;d it go?</span>
        <button
          ref={restoreRef}
          type="button"
          className="terminal-window-closed__restore"
          onClick={restore}
        >
          Bring it back
        </button>
      </div>
    );
  }

  const terminal = (
    <div
      ref={windowRef}
      className="terminal-window"
      data-minimized={minimized ? "true" : undefined}
      data-maximized={maximized ? "true" : undefined}
    >
      <div className="terminal-window__bar">
        <button
          ref={redRef}
          type="button"
          className="terminal-window__dot terminal-window__dot--red"
          aria-label="Close terminal"
          onClick={close}
        >
          <svg viewBox="0 0 12 12" aria-hidden="true">
            <path d="M4 4l4 4M8 4l-4 4" />
          </svg>
        </button>
        <button
          ref={blueRef}
          type="button"
          className="terminal-window__dot terminal-window__dot--blue"
          aria-label={minimized ? "Restore terminal" : "Minimize terminal"}
          aria-expanded={!minimized}
          onClick={toggleMinimized}
        >
          <svg viewBox="0 0 12 12" aria-hidden="true">
            <path d="M3 6h6" />
            {minimized && <path d="M6 3v6" />}
          </svg>
        </button>
        <button
          ref={yellowRef}
          type="button"
          className="terminal-window__dot terminal-window__dot--yellow"
          aria-label={maximized ? "Exit full screen" : "Full screen"}
          aria-pressed={maximized}
          onClick={maximized ? exitFullScreen : enterFullScreen}
        >
          <svg viewBox="0 0 12 12" aria-hidden="true">
            {maximized ? (
              <path
                className="terminal-window__glyph-fill"
                d="M6.5 1.5V5.5H10.5zM5.5 10.5V6.5H1.5z"
              />
            ) : (
              <path
                className="terminal-window__glyph-fill"
                d="M3.5 2.5H9.5V8.5zM2.5 3.5V9.5H8.5z"
              />
            )}
          </svg>
        </button>
        {title && <span className="terminal-window__title">{title}</span>}
        {maximized && (
          <button
            type="button"
            className="terminal-window__exit"
            onClick={exitFullScreen}
          >
            Exit full screen
          </button>
        )}
      </div>
      <div className="terminal-window__body" inert={minimized}>
        <div className="terminal-window__body-inner">{children}</div>
      </div>
    </div>
  );

  if (!maximized) {
    return terminal;
  }
  return (
    <>
      <div
        className="terminal-window-placeholder"
        style={{ height: placeholderHeight }}
      />
      {/* A modal dialog makes the rest of the page unreachable and closes on Esc */}
      <dialog
        ref={dialogRef}
        className="terminal-window-dialog"
        aria-label={title ?? "Terminal"}
        onCancel={(event) => {
          event.preventDefault();
          exitFullScreen();
        }}
        // The browser can close the dialog without a cancel event, e.g. on a
        // repeated Esc, so state follows the close too
        onClose={() => setMaximized(false)}
        onKeyDown={(event) => {
          // Search opens outside the dialog, where it can't be seen or typed into
          const opensSearch =
            ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") ||
            event.key === "/";
          if (opensSearch) {
            exitFullScreen();
          }
        }}
        onPointerDown={(event) => {
          pressedBackdropRef.current = event.target === event.currentTarget;
        }}
        onClick={(event) => {
          // Clicks on the dimmed backdrop land on the dialog element itself
          if (event.target === event.currentTarget && pressedBackdropRef.current) {
            exitFullScreen();
          }
        }}
      >
        {terminal}
      </dialog>
    </>
  );
}
