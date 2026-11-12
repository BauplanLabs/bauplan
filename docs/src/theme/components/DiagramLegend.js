import { useEffect, useRef, useState } from "react";

// Keys match the node and group classes in src/mermaid/theme.js.
const ITEMS = {
  default: "Your environment",
  accent: "Managed by Bauplan",
  external: "Your storage",
  iceberg: "Iceberg types",
  arrow: "Arrow types",
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

function containsWhole(outer, inner) {
  return (
    inner.left >= outer.left &&
    inner.right <= outer.right &&
    inner.top >= outer.top &&
    inner.bottom <= outer.bottom
  );
}

// Arrows that start and end inside the group stay bright with it.
function markEdges(diagram, outline) {
  const edges = diagram.querySelectorAll(".edgePaths path, .edgeLabels g.edgeLabel");
  for (const edge of edges) {
    const box = edge.getBoundingClientRect();
    if (box.width > 0 && containsWhole(outline, box)) {
      edge.classList.add("is-highlighted");
    }
  }
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
    const isGroupKind = group.classList.contains(kind);
    const matches =
      isGroupKind || (members.length > 0 && members.every(({ node }) => kindOf(node) === kind));
    group.classList.toggle("is-highlighted", matches);
    if (isGroupKind) {
      markEdges(diagram, outline);
    }
    for (const { node, isLayout } of inside) {
      if (isLayout || isGroupKind) {
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

  // Esc or a click outside the legend clears a selection, so both listen on
  // the document.
  useEffect(() => {
    if (!pinned) {
      return;
    }
    function clear() {
      setPinned(null);
      setHovered(null);
    }
    function onKeyDown(event) {
      if (event.key === "Escape") {
        clear();
      }
    }
    function onPointerDown(event) {
      if (!ref.current?.contains(event.target)) {
        clear();
      }
    }
    document.addEventListener("keydown", onKeyDown);
    document.addEventListener("pointerdown", onPointerDown);
    return () => {
      document.removeEventListener("keydown", onKeyDown);
      document.removeEventListener("pointerdown", onPointerDown);
    };
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
