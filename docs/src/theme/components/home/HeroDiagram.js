import Link from "@docusaurus/Link";
import React, { useState } from "react";

const BLUE = "#00a3ff";
const TEAL = "#02b1a7";
const INK = "var(--ifm-font-color-base)";
const MUTED = "var(--ifm-color-emphasis-600)";
const SURFACE = "var(--ifm-background-color)";

const MAIN_Y = 100;
const TOP_Y = 44;
const FORK_X = 72;
const MERGE_X = 284;
const LOOP_Y = 134;
const COMMIT_XS = [152, 204];

const SUMMARY =
  "An agent creates a zero-copy branch and runs its pipeline there. You review the changes, and production changes only when you merge.";

// Hover zones in viewBox units, sized well beyond the shapes so they're easy to hit.
// Anywhere outside them the tooltip shows SUMMARY.
const ZONES = [
  {
    id: "branch",
    hotspot: { x: 178, y: TOP_Y, color: BLUE },
    box: { x: 40, y: 14, width: 230, height: 92 },
    text: "An agent creates a zero-copy branch of production and runs its pipeline there. Production stays as it is.",
  },
  {
    id: "merge",
    hotspot: { x: MERGE_X, y: MAIN_Y, color: TEAL },
    box: { x: 270, y: 60, width: 86, height: 86 },
    text: "You review the changes and merge them once they meet your standards. Production changes only when you merge.",
  },
  {
    id: "rollback",
    hotspot: { x: (FORK_X + MERGE_X) / 2, y: LOOP_Y, color: TEAL },
    box: { x: 56, y: 106, width: 214, height: 40 },
    text: "Every commit records the code that made it. If something slips through, roll the tables back to an earlier commit.",
  },
];

const branchPath = `M${FORK_X} ${MAIN_Y}
  C${FORK_X + 32} ${MAIN_Y} ${FORK_X + 26} ${TOP_Y} ${FORK_X + 56} ${TOP_Y}
  L${MERGE_X - 56} ${TOP_Y}
  C${MERGE_X - 26} ${TOP_Y} ${MERGE_X - 32} ${MAIN_Y} ${MERGE_X} ${MAIN_Y}`;

// One 6s loop: a dot rides the branch to the merge, then the check pulses once.
// Reduced motion hides both, leaving the static drawing.
const STYLES = `
  .hd-traveler {
    stroke-dasharray: 0.1 100;
    stroke-dashoffset: 0;
    opacity: 0;
    animation: hd-travel 6s ease-in-out infinite;
  }
  .hd-pulse {
    transform-box: fill-box;
    transform-origin: center;
    opacity: 0;
    animation: hd-pulse 6s ease-out infinite;
  }
  @keyframes hd-travel {
    0%, 8% { stroke-dashoffset: 0; opacity: 0; }
    12% { opacity: 1; }
    56% { opacity: 1; }
    60% { stroke-dashoffset: -100; opacity: 0; }
    100% { stroke-dashoffset: -100; opacity: 0; }
  }
  @keyframes hd-pulse {
    0%, 58% { transform: scale(1); opacity: 0; }
    60% { opacity: 0.55; }
    80%, 100% { transform: scale(2.2); opacity: 0; }
  }
  .hd-hotspot {
    transform-box: fill-box;
    transform-origin: center;
    animation: hd-hotspot 2.4s ease-out infinite;
  }
  @keyframes hd-hotspot {
    0% { transform: scale(0.6); opacity: 0.9; }
    70%, 100% { transform: scale(1.6); opacity: 0; }
  }
  @media (prefers-reduced-motion: reduce) {
    .hd-traveler, .hd-pulse { animation: none; opacity: 0; }
    .hd-hotspot { animation: none; opacity: 0.6; }
  }
`;

const Robot = ({ x, y }) => (
  <g stroke={BLUE} strokeWidth={1.5} fill="none">
    <path d={`M${x} ${y - 11}v-4`} />
    <circle cx={x} cy={y - 16.5} r={1.6} fill={BLUE} stroke="none" />
    <rect x={x - 9} y={y - 11} width={18} height={14} rx={4} />
    <circle cx={x - 3.5} cy={y - 4} r={1.5} fill={BLUE} stroke="none" />
    <circle cx={x + 3.5} cy={y - 4} r={1.5} fill={BLUE} stroke="none" />
  </g>
);

const Person = ({ x, y }) => (
  <g stroke={INK} strokeWidth={1.5} fill="none">
    <circle cx={x} cy={y - 13} r={4} />
    <path d={`M${x - 7.5} ${y + 1}a7.5 7.5 0 0 1 15 0`} />
  </g>
);

export const HeroDiagram = () => {
  const [zoneId, setZoneId] = useState(null);
  const [explored, setExplored] = useState(false);
  const tooltip = ZONES.find((z) => z.id === zoneId)?.text ?? SUMMARY;
  // Fading the other parts is what shows the drawing has more than one tooltip.
  const part = (id) => ({
    opacity: zoneId && zoneId !== id ? 0.25 : 1,
    className: "transition-opacity duration-200",
  });

  return (
    <Link
      to="/concepts/catalog-and-branches"
      aria-label={`${SUMMARY} Learn how Git for Data works.`}
      className="group relative block hover:no-underline"
    >
      <svg
        viewBox="0 0 360 150"
        className="block w-full h-auto transition-transform duration-300 group-hover:-translate-y-0.5"
        style={{ fontFamily: "var(--ifm-font-family-monospace)" }}
        aria-hidden="true"
      >
        <style>{STYLES}</style>

        <g {...part("main")}>
          <path d={`M8 ${MAIN_Y}H352`} stroke={INK} strokeWidth={2.5} strokeLinecap="round" />
          <text x={8} y={MAIN_Y - 9} fill={MUTED} fontSize={11}>
            main
          </text>
        </g>

        <g {...part("rollback")}>
          <path
            d={`M${MERGE_X} ${MAIN_Y + 14}V${LOOP_Y}H${FORK_X}V${MAIN_Y + 14}`}
            stroke={TEAL}
            strokeWidth={1.25}
            strokeDasharray="3 3"
            fill="none"
            opacity={0.8}
          />
          <path d={`M${FORK_X} ${MAIN_Y + 8}l-3.5 6h7z`} fill={TEAL} opacity={0.8} />
        </g>

        <g {...part("branch")}>
          <path d={branchPath} stroke={BLUE} strokeWidth={2} fill="none" />
          <path
            d={branchPath}
            pathLength={100}
            className="hd-traveler"
            stroke={BLUE}
            strokeWidth={7}
            strokeLinecap="round"
            fill="none"
          />
          <text
            x={(FORK_X + MERGE_X) / 2}
            y={TOP_Y - 14}
            textAnchor="middle"
            fill={BLUE}
            fontSize={11}
          >
            agent branch
          </text>
          {COMMIT_XS.map((x) => (
            <circle key={x} cx={x} cy={TOP_Y} r={4} fill={SURFACE} stroke={BLUE} strokeWidth={2} />
          ))}

          <circle cx={FORK_X} cy={MAIN_Y} r={3.5} fill={BLUE} />
          <Robot x={FORK_X - 11} y={MAIN_Y - 14} />
        </g>

        <g {...part("merge")}>
          <Person x={MERGE_X + 24} y={MAIN_Y - 14} />
          <circle cx={MERGE_X} cy={MAIN_Y} r={9} fill={TEAL} className="hd-pulse" />
          <circle cx={MERGE_X} cy={MAIN_Y} r={9} fill={TEAL} />
          <path
            d={`M${MERGE_X - 4} ${MAIN_Y}l3 3l5.5 -6`}
            stroke="#fff"
            strokeWidth={1.75}
            fill="none"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          <text x={MERGE_X + 16} y={MAIN_Y + 22} fill={TEAL} fontSize={11}>
            merged
          </text>
        </g>

        {/* Pulsing rings mark the parts with their own tooltip until the first hover. */}
        {!explored &&
          ZONES.map(({ id, hotspot: { x, y, color } }, i) => (
            <circle
              key={id}
              cx={x}
              cy={y}
              r={12}
              fill="none"
              stroke={color}
              strokeWidth={1.5}
              className="hd-hotspot"
              style={{ animationDelay: `${i * 0.8}s` }}
            />
          ))}

        {ZONES.map((z) => (
          <rect
            key={z.id}
            {...z.box}
            fill="transparent"
            onMouseEnter={() => {
              setZoneId(z.id);
              setExplored(true);
            }}
            onMouseLeave={() => setZoneId(null)}
          />
        ))}
      </svg>
      {/* The SVG has no <title> because the browser's native tooltip waits about a second. */}
      <span
        className="pointer-events-none absolute left-1/2 bottom-full z-10 mb-1 w-72 -translate-x-1/2
          rounded-lg px-3 py-2 text-xs leading-snug text-left shadow-md opacity-0 transition-opacity duration-150
          group-hover:opacity-100 group-focus-visible:opacity-100
          bg-[var(--ifm-font-color-base)] text-[var(--ifm-background-color)]"
      >
        {tooltip}
      </span>
    </Link>
  );
};
