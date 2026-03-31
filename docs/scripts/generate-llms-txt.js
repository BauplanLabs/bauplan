/* *
 * Generate llms.txt from sidebar configuration and MDX source files.
 *
 * Reads the Docusaurus sidebar config and each MDX file's frontmatter /
 * first paragraph to produce a structured llms.txt index at static/llms.txt.
 */

const fs = require('fs');
const path = require('path');

const PAGES_DIR = path.join(__dirname, '..', 'pages');
const OUTPUT_PATH = path.join(__dirname, '..', 'static', 'llms.txt');

/** Read the site URL, matching what docusaurus.config.js uses. */
function getBaseUrl() {
  if (process.env.DOCS_URL) return process.env.DOCS_URL.replace(/\/+$/, '');
  const config = fs.readFileSync(path.join(__dirname, '..', 'docusaurus.config.js'), 'utf8');
  const m = config.match(/["']([^"']*bauplanlabs\.com[^"']*)["']/);
  if (!m) throw new Error('Could not find fallback url in docusaurus.config.js');
  return m[1].replace(/\/+$/, ''); // strip trailing slash
}

const BASE_URL = getBaseUrl();

const HEADER = [
  '# Bauplan Documentation',
  '',
  'Bauplan is a serverless data lakehouse platform where data changes follow a Git-like workflow.',
  'You develop and test on isolated data branches, then publish by merging into main.',
  'The platform handles compute, storage, and orchestration automatically.',
].join('\n');

// Descriptions that are not useful for a docs index.
const JUNK_DESCRIPTIONS = [
  /^open in github/i,
  /^export\s+const\b/i,
  /^try this conversational prompt/i,
  /^drop us a line/i,
  /^citi bikers? system data/i,
];

// ── File discovery ──────────────────────────────────────────

function getAllMdxFiles(dir, baseDir = dir) {
  const files = [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...getAllMdxFiles(fullPath, baseDir));
    } else if (entry.name.endsWith('.mdx') || entry.name.endsWith('.md')) {
      files.push({ fullPath, relativePath: path.relative(baseDir, fullPath) });
    }
  }

  return files;
}

// ── Frontmatter & description helpers ───────────────────────

/** Docusaurus strips leading number prefixes (e.g. "03_") from doc IDs. */
function stripNumberPrefix(segment) {
  return segment.replace(/^\d+[-_]/, '');
}

/** Convert a file's relative path to its Docusaurus doc ID. */
function filePathToDocId(relativePath) {
  return relativePath
    .replace(/\.(mdx|md)$/, '')
    .split(path.sep)
    .map(stripNumberPrefix)
    .join('/');
}

/** Minimal YAML frontmatter parser (single-line values only). */
function parseFrontmatter(content) {
  const match = content.match(/^---\n([\s\S]*?)\n---/);
  if (!match) return {};

  const fm = {};
  for (const line of match[1].split('\n')) {
    const m = line.match(/^([\w_]+)\s*:\s*(?:"([^"]*)"|'([^']*)'|(.+))\s*$/);
    if (m) fm[m[1]] = (m[2] ?? m[3] ?? m[4]).trim();
  }
  return fm;
}

/** Extract the first `# Heading` from content (fallback when frontmatter has no title). */
function extractHeadingTitle(content) {
  const m = content.match(/^#\s+(.+)$/m);
  return m ? m[1].trim() : null;
}

function isJunkDescription(desc) {
  return JUNK_DESCRIPTIONS.some(re => re.test(desc));
}

/** Extract the first meaningful sentence(s) from MDX content. */
function extractDescription(content) {
  let text = content;

  // Strip frontmatter, imports, exports
  text = text.replace(/^---\n[\s\S]*?\n---\n?/, '');
  text = text.replace(/^import\s+.*\bfrom\s+['"].*\n/gm, '');
  // Single-line exports only; multi-line leaks are caught by JUNK_DESCRIPTIONS.
  text = text.replace(/^export\s+.+$/gm, '');

  // Strip JSX
  text = text.replace(/<[A-Z][a-zA-Z]*\s*[^>]*\/>\s*/g, '');              // self-closing
  text = text.replace(/<[A-Z][a-zA-Z]*[^>]*>[\s\S]*?<\/[A-Z][a-zA-Z]*>\s*/g, ''); // block
  text = text.replace(/<\/?[A-Z][a-zA-Z]*[^>]*>/g, '');                   // remaining tags
  text = text.replace(/<\/?[a-z][a-zA-Z]*[^>]*>/g, '');                   // HTML tags

  // Strip markdown / MDX syntax
  text = text.replace(/^#+\s+.*$/gm, '');       // headings
  text = text.replace(/<!--[\s\S]*?-->/g, '');   // HTML comments
  text = text.replace(/\{\/\*[\s\S]*?\*\/\}/g, ''); // JSX comments
  text = text.replace(/^:::.*$/gm, '');          // admonitions

  // Find the first non-trivial paragraph (skip lists, code, tables)
  const paragraphs = text
    .split(/\n\n+/)
    .map(p => p.trim())
    .filter(p => p.length > 15 && !/^[-*`<{|]/.test(p));

  if (paragraphs.length === 0) return '';

  // Collapse to single line, strip inline markdown
  let desc = paragraphs[0].replace(/\s+/g, ' ').trim();
  desc = desc.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1'); // links
  desc = desc.replace(/\*{1,2}([^*]+)\*{1,2}/g, '$1'); // bold/italic
  desc = desc.replace(/`([^`]+)`/g, '$1');               // inline code

  if (isJunkDescription(desc)) return '';

  // Take 1–2 sentences. Split on ". " / "! " / "? " before a capital letter
  // so we don't break on "3.10" or "e.g.".
  const parts = desc.split(/(?<=[.!?])\s+(?=[A-Z])/);
  let result = parts[0].trim();
  if (result.length < 80 && parts.length > 1) {
    result += ' ' + parts[1].trim();
  }
  if (result.length > 250) {
    result = result.substring(0, 247) + '...';
  }

  // Clean up trailing colons (from sentences that precede lists/code blocks)
  result = result.replace(/:$/, '');

  // Trim dangling fragments after the last complete sentence.
  // e.g. "Every run is a transaction. That means" → "Every run is a transaction."
  // Only ". " / "! " / "? " count as sentence boundaries (not "3.10").
  const ends = [...result.matchAll(/[.!?](?=\s)/g)];
  if (ends.length > 0) {
    const last = ends[ends.length - 1];
    const tail = result.substring(last.index + 1).trim();
    if (tail.length > 0 && tail.length < 40 && !/[.!?]/.test(tail)) {
      result = result.substring(0, last.index + 1);
    }
  }

  return result;
}

// ── Sidebar loading ─────────────────────────────────────────

function loadSidebarConfig() {
  let content = fs.readFileSync(path.join(__dirname, '..', 'sidebar.js'), 'utf8');

  const sdkPages = JSON.parse(
    fs.readFileSync(path.join(PAGES_DIR, 'reference', '_sidebar.json'), 'utf8'),
  );

  // Transform ESM → evaluable code so we can run it with `new Function`.
  // Safe because sidebar.js is a local project file, not user input.
  content = content.replace(/^import\s+.*$\n?/gm, '');
  content = content.replace('export default', 'return');
  content = content.replace(/\bsdkPages\b/g, JSON.stringify(sdkPages));
  content = content.replace(/\/\/.*$/gm, '');

  return new Function(content)();
}

// ── Build doc-ID → metadata map ─────────────────────────────

function buildDocMap() {
  const files = getAllMdxFiles(PAGES_DIR);
  const map = {};

  for (const { fullPath, relativePath } of files) {
    const docId = filePathToDocId(relativePath);
    const raw = fs.readFileSync(fullPath, 'utf8');
    const fm = parseFrontmatter(raw);

    const title = fm.title || fm.sidebar_label || extractHeadingTitle(raw)
      || docId.split('/').pop();
    const description = fm.description || extractDescription(raw);
    // URL mirrors what generate-llm-docs.js writes into static/
    // Strip numeric prefixes from path segments to match the output filenames.
    const cleanPath = relativePath
      .split(path.sep)
      .map(s => s.replace(/^\d+[-_]/, ''))
      .join('/');
    const url = `${BASE_URL}/${cleanPath.replace(/\.mdx$/, '.md')}`;

    map[docId] = { title, description, url, docId, relativePath };
  }

  return map;
}

// ── Output formatting ───────────────────────────────────────

function formatEntry(doc) {
  const desc = doc.description ? `: ${doc.description}` : '';
  return `- [${doc.title}](${doc.url})${desc}`;
}

/** Look up a doc ID, warning on stderr if it's missing. */
function lookupDoc(docId, docMap) {
  const doc = docMap[docId];
  if (!doc) {
    console.warn(`  warning: sidebar doc "${docId}" has no matching file`);
  }
  return doc;
}

/** Recursively collect every doc ID referenced by a sidebar tree. */
function collectDocIds(items, ids) {
  for (const item of items) {
    if (typeof item === 'string') {
      ids.add(item);
    } else if (item && typeof item === 'object') {
      if (item.link?.id) ids.add(item.link.id);
      if (item.items) collectDocIds(item.items, ids);
    }
  }
}

/**
 * Flatten a sidebar tree into a list of doc entries (ignoring categories).
 * Category link-docs are included; category labels/headings are not.
 */
function flattenItems(items, docMap) {
  const docs = [];
  for (const item of items) {
    if (typeof item === 'string') {
      const doc = lookupDoc(item, docMap);
      if (doc) docs.push(doc);
    } else if (item?.type === 'category') {
      if (item.link?.id) {
        const doc = lookupDoc(item.link.id, docMap);
        if (doc) docs.push(doc);
      }
      docs.push(...flattenItems(item.items || [], docMap));
    }
  }
  return docs;
}

/**
 * Render a sidebar tree with markdown headings for each category.
 * If a category has a link-doc with a description, that description
 * becomes a section blurb and the link-doc entry omits its description
 * to avoid duplication.
 */
function renderItems(items, docMap, headingLevel) {
  const lines = [];

  for (const item of items) {
    if (typeof item === 'string') {
      const doc = lookupDoc(item, docMap);
      if (doc) lines.push(formatEntry(doc));
      continue;
    }
    if (item?.type !== 'category') continue;

    lines.push('');
    lines.push(`${'#'.repeat(headingLevel)} ${item.label}`);

    if (item.link?.id) {
      const linkDoc = lookupDoc(item.link.id, docMap);
      if (linkDoc?.description) {
        lines.push(linkDoc.description);
        lines.push('');
        // Entry without description (already shown as blurb)
        lines.push(`- [${linkDoc.title}](${linkDoc.url})`);
      } else {
        lines.push('');
        if (linkDoc) lines.push(formatEntry(linkDoc));
      }
    } else {
      lines.push('');
    }

    lines.push(...renderItems(item.items || [], docMap, headingLevel + 1));
  }

  return lines;
}

// ── Section helpers ─────────────────────────────────────────

/** Emit a flat section: heading, optional blurb, then list entries. */
function renderFlatSection(title, blurb, items, docMap) {
  const lines = ['', `## ${title}`];
  if (blurb) lines.push(blurb);
  lines.push('');
  for (const doc of flattenItems(items, docMap)) {
    lines.push(formatEntry(doc));
  }
  return lines;
}

// ── Main ────────────────────────────────────────────────────

function main() {
  const sidebar = loadSidebarConfig();
  const docMap = buildDocMap();
  const usedIds = new Set();
  const output = [HEADER];

  function track(items) {
    collectDocIds(items, usedIds);
  }

  // Documentation sidebar renders with full category headings.
  output.push(...renderItems(sidebar.documentationSidebar, docMap, 2));
  track(sidebar.documentationSidebar);

  // Reference sidebar is rendered as a flat list under a single heading.
  output.push(...renderFlatSection(
    'API Reference', null, sidebar.referenceSidebar, docMap,
  ));
  track(sidebar.referenceSidebar);

  // Pages not referenced by any sidebar.
  const extras = Object.values(docMap)
    .filter(doc => !usedIds.has(doc.docId))
    .filter(doc => {
      if (doc.docId === 'integrations/index') return false;
      return true;
    })
    .sort((a, b) => a.docId.localeCompare(b.docId));

  if (extras.length > 0) {
    output.push('');
    output.push('## Other');
    for (const doc of extras) {
      output.push(formatEntry(doc));
    }
  }

  const text = output.join('\n') + '\n';
  fs.writeFileSync(OUTPUT_PATH, text);

  console.log(
    `Generated llms.txt (${usedIds.size} sidebar pages` +
      (extras.length > 0 ? ` + ${extras.length} extra` : '') +
      ')',
  );
}

main();
