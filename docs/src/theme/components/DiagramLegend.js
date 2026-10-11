import { useEffect, useRef, useState } from "react";

// Keys match the node classes in src/mermaid/theme.js.
const ITEMS = {
  default: "Your environment",
  accent: "Managed by Bauplan",
  external: "Your storage",
};

// Nodes that label or pad a group rather than stand for something.
const LAYOUT_CLASSES = ["title", "spacer"];

// The legend follows its diagram, sometimes with a wrapping <div> between.
function findDiagram(legend) {
  const previous = legend?.previousElementSibling;
  if (previous?.matches(".docusaurus-mermaid-container")) {
    return previous;
  }
  return previous?.querySelector(".docusaurus-mermaid-container");
}

function kindOf(node) {
  if (node.classList.contains("accent")) {
    return "accent";
  }
  if (node.classList.contains("external")) {
    return "external";
  }
  return "default";
}

function contains(outer, inner) {
  const x = inner.left + inner.width / 2;
  const y = inner.top + inner.height / 2;
  return x >= outer.left && x <= outer.right && y >= outer.top && y <= outer.bottom;
}

// Mermaid keeps groups and nodes in separate layers, so which node sits in
// which group is only known from where they're drawn.
function markGroups(diagram, kind) {
  const nodes = [...diagram.querySelectorAll("g.node")].map((node) => ({
    node,
    box: node.getBoundingClientRect(),
    isLayout: LAYOUT_CLASSES.some((name) => node.classList.contains(name)),
  }));
  for (const group of diagram.querySelectorAll("g.cluster")) {
    const outline = group.querySelector("rect").getBoundingClientRect();
    const inside = nodes.filter(({ box }) => contains(outline, box));
    const members = inside.filter(({ isLayout }) => !isLayout);
    const matches = members.length > 0 && members.every(({ node }) => kindOf(node) === kind);
    group.classList.toggle("is-highlighted", matches);
    for (const { node, isLayout } of inside) {
      if (isLayout) {
        node.classList.toggle("is-highlighted", matches);
      }
    }
  }
}

function clearGroups(diagram) {
  for (const element of diagram.querySelectorAll(".is-highlighted")) {
    element.classList.remove("is-highlighted");
  }
}

export function DiagramLegend({ items = Object.keys(ITEMS) }) {
  const ref = useRef(null);
  const [hovered, setHovered] = useState(null);
  const [pinned, setPinned] = useState(null);
  const highlighted = hovered ?? pinned;

  // The diagram is Mermaid's SVG, outside React, so we mark it directly.
  useEffect(() => {
    const diagram = findDiagram(ref.current);
    if (!diagram) {
      return;
    }
    clearGroups(diagram);
    if (highlighted) {
      diagram.dataset.highlight = highlighted;
      markGroups(diagram, highlighted);
    } else {
      delete diagram.dataset.highlight;
    }
  }, [highlighted]);

  // Esc clears a selection wherever focus is, so it listens on the document.
  useEffect(() => {
    if (!pinned) {
      return;
    }
    function onKeyDown(event) {
      if (event.key === "Escape") {
        setPinned(null);
        setHovered(null);
      }
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [pinned]);

  return (
    <div className="diagram-legend" ref={ref}>
      {items.map((item) => (
        <button
          key={item}
          type="button"
          className="diagram-legend__item"
          aria-pressed={pinned === item}
          onClick={() => setPinned(pinned === item ? null : item)}
          onMouseEnter={() => setHovered(item)}
          onMouseLeave={() => setHovered(null)}
          onFocus={() => setHovered(item)}
          onBlur={() => setHovered(null)}
        >
          <span className={`diagram-legend__swatch diagram-legend__swatch--${item}`} />
          {ITEMS[item]}
        </button>
      ))}
    </div>
  );
}
