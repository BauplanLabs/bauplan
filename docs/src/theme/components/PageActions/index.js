import React, { useEffect, useRef, useState } from "react";
import { useLocation } from "@docusaurus/router";
import { Check, ChevronDown, Copy, ExternalLink, FileText } from "lucide-react";

/** The markdown twin of a page, published by scripts/generate-llm-docs.js. */
function markdownPath(pathname) {
  const route = pathname.replace(/\/+$/, "");
  return route === "" ? "/index.md" : `${route}.md`;
}

const menuItemClass =
  "flex items-center gap-2.5 w-full px-3 py-2 text-sm text-left no-underline " +
  "text-[var(--ifm-font-color-base)] hover:bg-[var(--ifm-hover-overlay)] " +
  "hover:text-[var(--ifm-font-color-base)] hover:no-underline";

export default function PageActions() {
  const { pathname } = useLocation();
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const container = useRef(null);
  const mdPath = markdownPath(pathname);

  useEffect(() => {
    if (!open) return undefined;
    const onPointerDown = (e) => {
      if (!container.current?.contains(e.target)) setOpen(false);
    };
    const onKeyDown = (e) => e.key === "Escape" && setOpen(false);
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  useEffect(() => {
    if (!copied) return undefined;
    const timer = setTimeout(() => setCopied(false), 2000);
    return () => clearTimeout(timer);
  }, [copied]);

  async function copyPage() {
    const response = await fetch(mdPath);
    if (!response.ok) throw new Error(`${mdPath} returned ${response.status}`);
    await navigator.clipboard.writeText(await response.text());
    setCopied(true);
    setOpen(false);
  }

  function assistantLink(base) {
    const url = `${window.location.origin}${mdPath}`;
    return `${base}${encodeURIComponent(`Read ${url} so I can ask you questions about it.`)}`;
  }

  return (
    <div ref={container} className="relative flex-shrink-0">
      <div className="flex items-center rounded-md border border-solid border-[var(--ifm-color-emphasis-300)]">
        <button
          type="button"
          onClick={() => copyPage().catch(() => setOpen(true))}
          className="flex items-center gap-1.5 px-2.5 py-1.5 text-sm bg-transparent border-0 cursor-pointer text-[var(--ifm-font-color-base)] hover:bg-[var(--ifm-hover-overlay)] rounded-l-md"
        >
          {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
          {copied ? "Copied" : "Copy page"}
        </button>
        <button
          type="button"
          aria-label="More ways to use this page with an assistant"
          aria-haspopup="menu"
          aria-expanded={open}
          onClick={() => setOpen((wasOpen) => !wasOpen)}
          className="flex items-center px-1.5 py-1.5 bg-transparent border-0 border-l border-solid border-[var(--ifm-color-emphasis-300)] cursor-pointer text-[var(--ifm-font-color-base)] hover:bg-[var(--ifm-hover-overlay)] rounded-r-md"
        >
          <ChevronDown className="h-3.5 w-3.5" />
        </button>
      </div>

      {open && (
        <div
          role="menu"
          className="absolute right-0 z-10 mt-1 w-60 py-1 rounded-md shadow-md border border-solid border-[var(--ifm-color-emphasis-300)] bg-[var(--ifm-background-surface-color)]"
        >
          <a role="menuitem" href={mdPath} className={menuItemClass} onClick={() => setOpen(false)}>
            <FileText className="h-3.5 w-3.5" />
            View as markdown
          </a>
          <a
            role="menuitem"
            href={assistantLink("https://claude.ai/new?q=")}
            target="_blank"
            rel="noopener noreferrer"
            className={menuItemClass}
            onClick={() => setOpen(false)}
          >
            <ExternalLink className="h-3.5 w-3.5" />
            Open in Claude
          </a>
          <a
            role="menuitem"
            href={assistantLink("https://chatgpt.com/?hints=search&q=")}
            target="_blank"
            rel="noopener noreferrer"
            className={menuItemClass}
            onClick={() => setOpen(false)}
          >
            <ExternalLink className="h-3.5 w-3.5" />
            Open in ChatGPT
          </a>
        </div>
      )}
    </div>
  );
}
