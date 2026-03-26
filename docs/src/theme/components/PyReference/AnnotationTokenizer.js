import React, { useMemo } from "react";
import { usePrismTheme } from "@docusaurus/theme-common";

export const BUILTIN_TYPES = new Set([
  'str', 'int', 'float', 'bool', 'bytes', 'dict', 'list', 'tuple', 'set',
  'None', 'True', 'False', 'Optional', 'Union', 'Any', 'Literal',
  'Annotated', 'Sequence', 'Callable', 'Iterator', 'Type',
]);

export function getTokenStyle(prismTheme, tokenType) {
  for (const rule of prismTheme.styles) {
    if (rule.types.includes(tokenType)) {
      return rule.style;
    }
  }
  return {};
}

export function useSignatureStyles() {
  const prismTheme = usePrismTheme();
  return useMemo(() => ({
    plain: prismTheme.plain,
    punctuation: getTokenStyle(prismTheme, 'punctuation'),
    operator: getTokenStyle(prismTheme, 'operator'),
    builtin: getTokenStyle(prismTheme, 'builtin'),
    className: getTokenStyle(prismTheme, 'class-name'),
    string: getTokenStyle(prismTheme, 'string'),
  }), [prismTheme]);
}

export function classifyToken(token, styles) {
  if (/^['"].*['"]$/.test(token)) return styles.string;
  if (BUILTIN_TYPES.has(token)) return styles.builtin;
  return { color: styles.plain.color };
}

export function AnnotationTokens({ text, styles }) {
  // Split on markdown links, punctuation, and known builtins
  // Regex captures: markdown links, punctuation chars, or word tokens
  // [^\[\]]+ in the link pattern prevents matching across nested brackets
  const parts = text.split(/(\[[^\[\]]+\]\([^)]+\)|'[^']*'|"[^"]*"|[,\[\]()]|\s+)/);
  return parts.map((part, i) => {
    if (!part) return null;

    // Markdown link: [Name](/reference/page#anchor)
    const linkMatch = part.match(/^\[([^\[\]]+)\]\(([^)]+)\)$/);
    if (linkMatch) {
      return <a key={i} href={linkMatch[2]} style={styles.className}>{linkMatch[1]}</a>;
    }

    // String literals: 'BRANCH', "value"
    if (/^['"].*['"]$/.test(part)) {
      return <span key={i} style={styles.string}>{part}</span>;
    }

    // Punctuation: [ ] , ( )
    if (/^[,\[\]()]$/.test(part)) {
      return <span key={i} style={styles.punctuation}>{part}</span>;
    }

    // Whitespace
    if (/^\s+$/.test(part)) {
      return <span key={i}>{part}</span>;
    }

    // Builtin type names
    if (BUILTIN_TYPES.has(part)) {
      return <span key={i} style={styles.builtin}>{part}</span>;
    }

    // Everything else: plain text color
    return <span key={i} style={{ color: styles.plain.color }}>{part}</span>;
  });
}
