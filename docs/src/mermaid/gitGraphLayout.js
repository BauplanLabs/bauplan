// Mermaid hard-codes gitGraph label geometry: commit labels hang down-left at
// 45°, their chips leave uneven padding, and branch names sit 30px further
// out to make room for those labels. This redraws both after Mermaid renders.

// Distance from a commit dot's centre to its label's chip.
const LABEL_GAP = 14;
// Minimum space between two labels on the same side of a branch.
const LABEL_SPACING = 4;
const PAD_X = 8;
const PAD_Y = 5;
// Mermaid's offset between a commit's label position and its bullet.
const LAYOUT_OFFSET = 10;
const BRANCH_LABEL_SHIFT = 26;
const DIAGRAM_PADDING = 8;
// A failed merge stops short of the branch it was going into
const FAILED_MERGE_CROSS = 15;
const FAILED_MERGE_LINE = 24;
// Space between a failed merge's cross and its label, which sits to the right
const FAILED_MERGE_LABEL_GAP = 12;

const ROTATE = /rotate\(\s*-45\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\)/;
const TRANSLATE = /translate\(\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\)/;

let measureContext;

function capHeight(text) {
  const style = getComputedStyle(text);
  measureContext ??= document.createElement("canvas").getContext("2d");
  measureContext.font = `${style.fontSize} ${style.fontFamily}`;
  return measureContext.measureText("H").actualBoundingBoxAscent;
}

function readCommitLabel(text) {
  const wrapper = text.parentNode;
  const chip = text.previousElementSibling;
  const match = wrapper.getAttribute("transform")?.match(ROTATE);
  if (!match || !chip?.classList.contains("commit-label-bkg")) {
    return null;
  }
  const cx = Number(match[1]) + LAYOUT_OFFSET;
  const width = text.getBBox().width + 2 * PAD_X;
  return { text, chip, wrapper, cx, cy: Number(match[2]), left: cx - width / 2, width };
}

function placeCommitLabel({ text, chip, wrapper, width }, left, y, cap) {
  const height = cap + 2 * PAD_Y;
  chip.setAttribute("x", left);
  chip.setAttribute("y", y);
  chip.setAttribute("width", width);
  chip.setAttribute("height", height);
  text.setAttribute("x", left + PAD_X);
  text.setAttribute("y", y + PAD_Y + cap);
  wrapper.removeAttribute("transform");
}

function drawCommitLabel(label, side, cap) {
  const height = cap + 2 * PAD_Y;
  const y = side === "above" ? label.cy - LABEL_GAP - height : label.cy + LABEL_GAP;
  placeCommitLabel(label, label.left, y, cap);
}

// Each label sits centred above its dot. When it would hit the previous label
// above on the same branch it drops below, so crowded runs alternate. A failed
// merge's label sits to the right of its cross instead.
function layOutCommitLabels(texts, failedMerges) {
  const labels = [...texts]
    .map(readCommitLabel)
    .filter(Boolean)
    .sort((a, b) => a.cx - b.cx);
  if (labels.length === 0) {
    return;
  }
  const cap = capHeight(labels[0].text);
  // Per branch, where the last label above and below the dots ends
  const branches = new Map();
  for (const label of labels) {
    const failed = failedMerges.find(({ cx, cy }) => near(cx, label.cx) && near(cy, label.cy));
    if (failed) {
      const height = cap + 2 * PAD_Y;
      const left = failed.cx + FAILED_MERGE_LABEL_GAP;
      placeCommitLabel(label, left, failed.crossY - height / 2, cap);
      continue;
    }
    const branch = branches.get(label.cy) ?? { above: -Infinity, below: -Infinity, last: "below" };
    const fits = (side) => label.left >= branch[side] + LABEL_SPACING;
    let side = branch.last === "above" ? "below" : "above";
    if (fits("above")) {
      side = "above";
    } else if (fits("below")) {
      side = "below";
    }
    branch[side] = label.left + label.width;
    branch.last = side;
    branches.set(label.cy, branch);
    drawCommitLabel(label, side, cap);
  }
}

function moveBranchLabel(label) {
  const chip = label.previousElementSibling;
  const match = label.firstElementChild?.getAttribute("transform")?.match(TRANSLATE);
  if (!match || !chip?.classList.contains("branchLabelBkg")) {
    return;
  }
  label.firstElementChild.setAttribute(
    "transform",
    `translate(${Number(match[1]) + BRANCH_LABEL_SHIFT}, ${match[2]})`,
  );
  chip.setAttribute("x", Number(chip.getAttribute("x")) + BRANCH_LABEL_SHIFT);
}

function numbers(path) {
  return (path.getAttribute("d") ?? "").match(/-?[\d.]+/g)?.map(Number) ?? [];
}

const near = (a, b) => Math.abs(a - b) < 1;

// `merge <branch> type: REVERSE` is a merge that didn't happen. Mermaid draws
// it as a solid line into a crossed commit on the target branch, which reads
// as if that branch changed. Instead the line turns dashed and stops short,
// with the cross at its end, and the target branch gets no commit dot.
function findFailedMerges(svg) {
  const arrows = [...svg.querySelectorAll("path.arrow")];
  return [...svg.querySelectorAll("path.commit-reverse")].flatMap((cross) => {
    const [x1, y1, x2, y2] = numbers(cross);
    const cx = (x1 + x2) / 2;
    const cy = (y1 + y2) / 2;
    // Only merges have a line coming in from another branch
    const arrow = arrows.find((path) => {
      const n = numbers(path);
      return near(n.at(-2), cx) && near(n.at(-1), cy) && !near(n[1], cy);
    });
    if (!arrow) {
      return [];
    }
    const side = Math.sign(numbers(arrow)[1] - cy);
    return [{ cross, arrow, cx, cy, side, crossY: cy + side * FAILED_MERGE_CROSS }];
  });
}

function drawFailedMerges(svg, failedMerges) {
  for (const { cross, arrow, cx, cy, side } of failedMerges) {
    arrow.setAttribute(
      "d",
      arrow.getAttribute("d").replace(/-?[\d.]+\s*$/, cy + side * FAILED_MERGE_LINE),
    );
    arrow.setAttribute("class", "arrow arrow-failed");
    cross.setAttribute("transform", `translate(0, ${side * FAILED_MERGE_CROSS})`);
    cross.setAttribute("class", "commit-failed-merge");
    svg.querySelectorAll("circle.commit").forEach((dot) => {
      if (near(dot.getAttribute("cx"), cx) && near(dot.getAttribute("cy"), cy)) {
        dot.remove();
      }
    });
  }
}

export function layOutGitGraph(svg) {
  if (svg.dataset.labelsLaidOut) {
    return;
  }
  svg.dataset.labelsLaidOut = "true";
  const labels = svg.querySelectorAll("text.commit-label");
  const failedMerges = findFailedMerges(svg);
  layOutCommitLabels(labels, failedMerges);
  svg.querySelectorAll("g.branchLabel").forEach(moveBranchLabel);
  // Before the yellow edges below, so a failed merge line doesn't get one
  drawFailedMerges(svg, failedMerges);

  // A wider copy under each yellow line draws its edge on the light page
  svg.querySelectorAll("path.arrow1").forEach((line) => {
    const edge = line.cloneNode();
    edge.setAttribute("class", "arrow-edge");
    line.before(edge);
  });

  // Mermaid draws branch lines over the labels; labels go on top so the
  // lines pass behind their chips instead
  const labelLayer = labels[0]?.parentNode.parentNode;
  labelLayer?.parentNode.appendChild(labelLayer);

  const box = svg.getBBox();
  const width = box.width + 2 * DIAGRAM_PADDING;
  svg.setAttribute(
    "viewBox",
    `${box.x - DIAGRAM_PADDING} ${box.y - DIAGRAM_PADDING} ${width} ${box.height + 2 * DIAGRAM_PADDING}`,
  );
  svg.style.maxWidth = `${width}px`;
}
