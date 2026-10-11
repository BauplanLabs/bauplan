// Mermaid's own rules are `#svg-id .node rect`; adding the element name
// (`g.node`) out-ranks them without `!important`, so a diagram's `classDef`
// can still override anything here.
const themeCSS = `
  /* The site's paragraph margin out-ranks Mermaid's reset and adds a blank
     line under every label. */
  foreignObject p {
    margin: 0 !important;
  }

  .label, .nodeLabel, .edgeLabel, .cluster-label, text {
    font-family: 'Satoshi', sans-serif;
    color: var(--diagram-text);
    fill: var(--diagram-text);
  }

  g.node rect, g.node polygon, g.node circle, g.node path {
    fill: var(--diagram-node-bg);
    stroke: var(--diagram-node-border);
    stroke-width: 1px;
    rx: 6px;
    ry: 6px;
  }

  g.cluster rect {
    fill: var(--diagram-group-bg);
    stroke: var(--diagram-group-border);
    stroke-width: 1px;
    rx: 10px;
    ry: 10px;
  }
  g.cluster .cluster-label span, g.cluster .cluster-label text {
    color: var(--diagram-text);
    fill: var(--diagram-text);
    font-weight: 600;
  }

  path.flowchart-link {
    stroke: var(--diagram-line);
    stroke-width: 1.25px;
  }
  marker path, .marker {
    fill: var(--diagram-line);
    stroke: var(--diagram-line);
  }
  div.labelBkg, .edgeLabels span.edgeLabel, .edgeLabels .edgeLabel p, .edgeLabels .edgeLabel rect {
    background-color: var(--diagram-label-bg);
    fill: var(--diagram-label-bg);
    color: var(--diagram-text-muted);
    font-size: 13px;
    opacity: 1;
  }

  /* A node can carry a title, a divider and a list of what it does. The
     site's article spacing for these out-ranks this, as with paragraphs. */
  .nodeLabel hr {
    margin: 8px 0 !important;
    border: 0;
    border-top: 1px solid currentColor;
    opacity: 0.35;
  }
  .nodeLabel ul {
    margin: 0 !important;
    padding: 0 !important;
    list-style: none;
    text-align: left;
  }
  .nodeLabel li {
    margin: 0 !important;
    white-space: nowrap;
  }
  .nodeLabel li::before {
    content: '- ';
  }

  /* Schema and other identifiers on arrows look like the page's inline code,
     so readers can match them to the text. Inline-block keeps the chip's
     padding inside the label's measured height. */
  .edgeLabels .edgeLabel code {
    display: inline-block;
    color: var(--diagram-code-text);
    font-size: 13px;
    line-height: 1.4;
  }

  g.node.accent rect, g.node.accent polygon {
    fill: var(--diagram-accent-bg);
    stroke: var(--diagram-accent);
    stroke-width: 1.5px;
  }
  g.node.accent .nodeLabel {
    color: var(--diagram-accent-text);
  }

  g.node.external rect, g.node.external polygon {
    fill: transparent;
    stroke-dasharray: 4 3;
  }

  /* white-space: pre lets snippets indent with plain spaces. */
  g.node.code .nodeLabel {
    font-family: 'IBMPlexMono', monospace;
    font-size: 13px;
    text-align: left;
    white-space: pre;
  }
  g.node.code .nodeLabel b {
    font-family: 'Satoshi', sans-serif;
    font-size: 14px;
  }

  .commit-label {
    fill: var(--diagram-text-muted);
    font-size: 12px;
  }
  /* Commit labels can cross branch lines, so they get a node's chip to stay
     readable over them */
  .commit-label-bkg {
    fill: var(--diagram-node-bg);
    stroke: var(--diagram-node-border);
    stroke-width: 1px;
    rx: 4px;
    ry: 4px;
    opacity: 1;
  }
  g.branchLabel .label, g.branchLabel .label text {
    fill: var(--diagram-git-label);
    color: var(--diagram-git-label);
    font-weight: 600;
  }
  circle.commit-merge {
    fill: var(--diagram-label-bg);
  }
  line.branch {
    stroke: var(--diagram-line);
  }
${[0, 1, 2]
  .map(
    (i) => `
  path.arrow${i} { stroke: var(--diagram-git${i}); }
  circle.commit${i}:not(.commit-merge) { fill: var(--diagram-git${i}); stroke: var(--diagram-git${i}); }
  circle.commit-merge.commit${i} { stroke: var(--diagram-git${i}); }
  rect.label${i} { fill: var(--diagram-git${i}); }`,
  )
  .join('')}

  /* The yellow edge. Lines get it from a wider copy drawn underneath them
     (see gitGraphLayout.js) */
  circle.commit1:not(.commit-merge) {
    stroke: var(--diagram-git1-edge);
    stroke-width: 1.5px;
  }
  path.arrow-edge {
    stroke: var(--diagram-git1-edge);
    stroke-width: 10;
    stroke-linecap: round;
    fill: none;
  }

  g.node.title rect {
    fill: none;
    stroke: none;
  }
  g.node.title .nodeLabel {
    font-weight: 600;
  }

  g.node.spacer {
    visibility: hidden;
  }

`;

export { themeCSS };
