import { useState } from "react";

// Icon color per file extension; anything not listed gets the neutral color
const EXTENSION_KINDS = {
  py: "python",
  ipynb: "python",
  sql: "sql",
  yml: "yaml",
  yaml: "yaml",
  md: "docs",
  txt: "docs",
  toml: "config",
  lock: "config",
  json: "config",
};

const INDENT_WIDTH = 2;

/**
 * Parses a `tree` fence into nodes: two spaces per level, a trailing slash
 * marks a folder, and everything after `#` is a comment.
 */
function parseTree(source) {
  const roots = [];
  const stack = [];
  for (const line of source.split("\n")) {
    if (!line.trim()) {
      continue;
    }
    const match = line.match(/^( *)([^#]*?)\s*(?:#\s?(.*))?$/);
    const depth = Math.floor(match[1].length / INDENT_WIDTH);
    const name = match[2];
    const node = {
      name,
      depth,
      comment: match[3],
      isFolder: name.endsWith("/"),
      children: [],
    };
    // A jump of several levels attaches to the last open folder
    stack.length = Math.min(stack.length, depth);
    const parent = stack[stack.length - 1];
    (parent ? parent.children : roots).push(node);
    stack.push(node);
  }
  return roots;
}

function fileKind(name) {
  const extension = name.includes(".")
    ? name.split(".").pop().toLowerCase()
    : "";
  return EXTENSION_KINDS[extension] ?? "other";
}

function displayName(node) {
  return node.isFolder ? node.name.slice(0, -1) : node.name;
}

// Where a row's name ends: CSS units, since indents are rem and names are ch
function nameEnd(node) {
  return `${node.depth} * var(--file-tree-level) + ${displayName(node).length}ch`;
}

function commentedNodes(nodes) {
  return nodes.flatMap((node) => [
    ...(node.comment ? [node] : []),
    ...commentedNodes(node.children),
  ]);
}

function FolderIcon({ open }) {
  return (
    <svg
      className="file-tree__icon file-tree__icon--folder"
      viewBox="0 0 16 16"
      aria-hidden="true"
    >
      {open ? (
        <>
          <path
            d="M1.75 11.5V4.25c0-.69.56-1.25 1.25-1.25h3.1l1.4 1.5h5.5c.69 0 1.25.56 1.25 1.25v1"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.25"
            strokeLinejoin="round"
          />
          <path
            d="M1.75 12.75 3.4 7.6c.15-.45.57-.75 1.05-.75h9.6c.5 0 .86.49.71.97l-1.4 4.4c-.17.5-.63.78-1.15.78H3c-.69 0-1.25-.56-1.25-1.25z"
            fill="currentColor"
            fillOpacity="0.22"
            stroke="currentColor"
            strokeWidth="1.25"
            strokeLinejoin="round"
          />
        </>
      ) : (
        <path
          d="M1.75 4.25c0-.69.56-1.25 1.25-1.25h3.1l1.4 1.5h5.5c.69 0 1.25.56 1.25 1.25v6c0 .69-.56 1.25-1.25 1.25H3c-.69 0-1.25-.56-1.25-1.25z"
          fill="currentColor"
          fillOpacity="0.22"
          stroke="currentColor"
          strokeWidth="1.25"
          strokeLinejoin="round"
        />
      )}
    </svg>
  );
}

function FileIcon({ kind }) {
  return (
    <svg
      className={`file-tree__icon file-tree__icon--${kind}`}
      viewBox="0 0 16 16"
      aria-hidden="true"
    >
      <path
        d="M3.75 1.75h5.5l3 3v9.5h-8.5z"
        fill="currentColor"
        fillOpacity="0.16"
        stroke="currentColor"
        strokeWidth="1.25"
        strokeLinejoin="round"
      />
      <path
        d="M9.25 1.75v3h3"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.25"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function RowContent({ node, open }) {
  return (
    <>
      {node.isFolder ? (
        <FolderIcon open={open} />
      ) : (
        <FileIcon kind={fileKind(node.name)} />
      )}
      <span
        className={
          node.isFolder ? "file-tree__name file-tree__name--folder" : "file-tree__name"
        }
      >
        {displayName(node)}
      </span>
      {node.comment && (
        <span
          className="file-tree__comment"
          style={{
            marginLeft: `calc(var(--file-tree-comment-column) - (${nameEnd(node)}))`,
          }}
        >
          {node.comment}
        </span>
      )}
    </>
  );
}

function TreeItem({ node }) {
  const [open, setOpen] = useState(true);
  // "..." stands for omitted entries, so it gets no icon
  if (node.name === "...") {
    return (
      <li className="file-tree__item">
        <div className="file-tree__row file-tree__row--ellipsis">…</div>
      </li>
    );
  }
  if (node.children.length === 0) {
    return (
      <li className="file-tree__item">
        <div className="file-tree__row">
          <RowContent node={node} open={false} />
        </div>
      </li>
    );
  }
  return (
    <li className="file-tree__item">
      <button
        type="button"
        className="file-tree__row file-tree__row--toggle"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
      >
        <RowContent node={node} open={open} />
      </button>
      {/* Hidden rather than unmounted, so nested folders keep their own state */}
      <div hidden={!open}>
        <TreeNodes nodes={node.children} />
      </div>
    </li>
  );
}

function TreeNodes({ nodes }) {
  return (
    <ul className="file-tree__list">
      {nodes.map((node, index) => (
        <TreeItem key={`${node.name}-${index}`} node={node} />
      ))}
    </ul>
  );
}

/**
 * Renders a `tree` code fence as a file explorer card.
 */
export default function FileTree({ source }) {
  const nodes = parseTree(source);
  // Comments share one column, just past the longest commented name
  const commented = commentedNodes(nodes);
  const style = commented.length
    ? {
        "--file-tree-comment-column": `max(${commented
          .map((node) => `calc(${nameEnd(node)})`)
          .join(", ")})`,
      }
    : undefined;
  return (
    <div className="file-tree" style={style}>
      <TreeNodes nodes={nodes} />
    </div>
  );
}
