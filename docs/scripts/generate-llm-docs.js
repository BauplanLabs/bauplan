#!/usr/bin/env node

/**
 * Generate LLM-friendly markdown files from .mdx source files.
 * Copies all .mdx files from pages/ to static/ with .md extension,
 * stripping numeric prefixes so URLs match Docusaurus routes
 * (e.g. 03-import.mdx → import.md, accessible at /tutorial/import.md).
 *
 * Every page is written at both `<route>.md` and its source path, so that
 * appending .md to any documentation URL resolves - including index pages
 * (/overview → overview.md) and pages that override their route with `slug:`.
 */

const fs = require('fs');
const path = require('path');
const { createSlugger, parseMarkdownHeadingId } = require('@docusaurus/utils');

const PAGES_DIR = path.join(__dirname, '..', 'pages');
const STATIC_DIR = path.join(__dirname, '..', 'static');
const REDIRECTS_FILE = path.join(__dirname, '..', 'redirects.js');
const HOME_CARDS_FILE = path.join(__dirname, '..', 'src', 'theme', 'components', 'home', 'cards.json');

const AGENT_DIRECTIVE = '> The complete documentation index is at [llms.txt](/llms.txt).\n\n';

function getAllMdxFiles(dir, baseDir = dir) {
  const files = [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...getAllMdxFiles(fullPath, baseDir));
    } else if (entry.name.endsWith('.mdx') || entry.name.endsWith('.md')) {
      const relativePath = path.relative(baseDir, fullPath);
      files.push({ fullPath, relativePath });
    }
  }

  return files;
}

function stripFrontmatter(content) {
  // Remove YAML frontmatter (content between --- markers at the start)
  const frontmatterRegex = /^---\n[\s\S]*?\n---\n/;
  return content.replace(frontmatterRegex, '');
}

function stripImports(content) {
  // Only strip ESM imports (must contain `from '...'` or `from "..."`).
  // Plain "import ..." at the start of a prose line must survive.
  // Braced imports may span several lines, so match those first.
  return content
    .replace(/^import\s*\{[^}]*\}\s*from\s+['"][^'"]*['"];?[ \t]*\n?/gm, '')
    .replace(/^import\s+.*\bfrom\s+['"].*$\n?/gm, '');
}

/** Parse JSX-style attributes from a tag body, handling both "quoted" and {expr} values. */
function parseAttrs(attrString) {
  const attrs = {};
  const re = /(\w+)=(?:"([^"]*)"|[{]([^}]*)[}])/g;
  let m;
  while ((m = re.exec(attrString)) !== null) {
    attrs[m[1]] = m[2] !== undefined ? m[2] : m[3];
  }
  return attrs;
}

/** Extract all PyParameters blocks (there can be several, e.g. "Raises") as markdown. */
function extractAllParameters(content, indent = '') {
  let result = '';
  const blocks = content.matchAll(/<PyParameters([^>]*)>([\s\S]*?)<\/PyParameters>/g);
  for (const block of blocks) {
    const blockAttrs = parseAttrs(block[1]);
    const title = blockAttrs.title || 'Parameters';

    let params = '';
    const paramTags = block[2].matchAll(/<PyParameter\s+([^>]*?)\/>/g);
    for (const tag of paramTags) {
      const attrs = parseAttrs(tag[1]);
      const name = attrs.name || '?';
      const annotation = attrs.annotation || '';
      const description = attrs.description || 'No description';
      params += `${indent}- \`${name}\`${annotation ? ` (${annotation})` : ''}: ${description}\n`;
    }
    if (params) {
      result += `\n${indent}**${title}:**\n${params}`;
    }
  }
  return result;
}

/** Extract a PySignature block as a markdown code signature. */
function extractSignature(content) {
  const sigMatch = content.match(/<PySignature\s+([^>]*)>([\s\S]*?)<\/PySignature>/);
  if (!sigMatch) return '';
  const sigAttrs = parseAttrs(sigMatch[1]);
  const name = sigAttrs.name || '';

  const params = [];
  const paramTags = sigMatch[2].matchAll(/<PySignatureParam\s+([^>]*?)\/>/g);
  for (const tag of paramTags) {
    const attrs = parseAttrs(tag[1]);
    if (attrs.separator !== undefined) {
      params.push(attrs.name || '*');
      continue;
    }
    let p = attrs.name || '';
    if (attrs.annotation) p += `: ${attrs.annotation}`;
    if (attrs.defaultValue && attrs.defaultValue !== 'None') p += ` = ${attrs.defaultValue}`;
    params.push(p);
  }

  return `\`\`\`python\n${name}(${params.join(', ')})\n\`\`\`\n`;
}

/** Extract a PyClassBase block as a markdown line. */
function extractClassBase(content) {
  // bases={[...]} - match a JSON array; links are /reference/... paths with no ]/> sequences.
  const baseMatch = content.match(/<PyClassBase\s+bases=\{(\[[\s\S]*?\])\}\s*\/>/);
  if (!baseMatch) return '';
  try {
    const parsed = JSON.parse(baseMatch[1]);
    const names = parsed.map(b => `\`${b.name}\``).join(', ');
    return `Bases: ${names}\n`;
  } catch {
    return '';
  }
}

/** Extract PyAttributesList blocks as markdown. */
function extractAttributes(content) {
  let result = '';
  const blocks = content.matchAll(/<PyAttributesList([^>]*)>([\s\S]*?)<\/PyAttributesList>/g);
  for (const block of blocks) {
    const blockAttrs = parseAttrs(block[1]);
    const title = blockAttrs.title || 'Attributes';

    let attrs = '';
    const attrTags = block[2].matchAll(/<PyAttribute\s+([^>]*?)\/>/g);
    for (const tag of attrTags) {
      const a = parseAttrs(tag[1]);
      const name = a.name || '?';
      const type = a.type || '';
      const description = a.description || '';
      attrs += `- \`${name}\`${type ? ` (${type})` : ''}${description ? `: ${description}` : ''}\n`;
    }
    if (attrs) {
      result += `\n**${title}:**\n${attrs}`;
    }
  }
  return result;
}

function convertPyDocsToMarkdown(content) {
  // Convert PyClass to markdown header with description, signature, bases, attributes, and methods
  content = content.replace(/<PyClass\s+id="([^"]+)"\s+name="([^"]+)">\s*([\s\S]*?)<\/PyClass>/g, (_match, _id, name, inner) => {
    // Split class-level content from method content
    const firstMemberIdx = inner.indexOf('<PyClassMember>');
    const classLevel = firstMemberIdx >= 0 ? inner.slice(0, firstMemberIdx) : inner;

    const descMatch = classLevel.match(/^([\s\S]*?)(?:<PyParameters>|<PySignature|<PyClassBase|<PyAttributesList|$)/);
    const description = descMatch ? descMatch[1].trim() : '';

    const signature = extractSignature(classLevel);
    const bases = extractClassBase(classLevel);
    const attributes = extractAttributes(classLevel);
    const classParams = extractAllParameters(classLevel);

    let methods = '';
    const methodMatches = inner.matchAll(/<PyFunction\s+id="([^"]+)"\s+name="([^"]+)">([\s\S]*?)<\/PyFunction>/g);
    for (const m of methodMatches) {
      const methodDescMatch = m[3].match(/^([\s\S]*?)(?:<PyParameters>|<PySignature|$)/);
      const methodDesc = methodDescMatch ? methodDescMatch[1].trim() : '';
      const methodSig = extractSignature(m[3]);
      const methodParams = extractAllParameters(m[3], '  ');

      methods += `\n### ${m[2]}\n\n`;
      if (methodSig) methods += `${methodSig}\n`;
      if (methodDesc) methods += `${methodDesc}\n`;
      if (methodParams) methods += `${methodParams}`;
    }

    let result = `## ${name}\n\n`;
    if (signature) result += `${signature}\n`;
    if (bases) result += `${bases}\n`;
    if (description) result += `${description}\n`;
    if (attributes) result += `${attributes}`;
    if (classParams) result += `${classParams}`;
    result += methods;
    return result;
  });

  // Convert standalone PyFunction (not inside PyClass)
  content = content.replace(/<PyFunction\s+id="([^"]+)"\s+name="([^"]+)">([\s\S]*?)<\/PyFunction>/g, (_match, _id, name, inner) => {
    const descMatch = inner.match(/^([\s\S]*?)(?:<PyParameters>|<PySignature|$)/);
    const description = descMatch ? descMatch[1].trim() : '';
    const signature = extractSignature(inner);
    const params = extractAllParameters(inner);

    let result = `## ${name}\n\n`;
    if (signature) result += `${signature}\n`;
    if (description) result += `${description}\n`;
    if (params) result += `${params}`;
    return result;
  });

  return content;
}

/** Render the links inside react card components on landing page. */
function convertHomePage(content) {
  if (!content.includes('<HomePage')) return content;

  const { sections, agentsCard } = JSON.parse(fs.readFileSync(HOME_CARDS_FILE, 'utf8'));
  const markdown = sections
    .map(({ title, cards }) => {
      const links = cards.map((c) => `- [${c.title}](${c.href}): ${c.description}`).join('\n');
      return `## ${title}\n\n${links}`;
    })
    .concat(`[${agentsCard.title}](${agentsCard.href}): ${agentsCard.description}`)
    .join('\n\n');

  return content.replace(/<HomePage\s*\/>/g, markdown);
}

/** Turn <VideoCard /> grids into links to the videos, with their blurbs. */
function convertVideoCards(content) {
  return content.replace(/^[ \t]*<VideoCard\s+([\s\S]*?)\/>/gm, (_match, attrString) => {
    const attrs = parseAttrs(attrString);
    if (!attrs.id) return '';
    const duration = attrs.duration ? ` (${attrs.duration})` : '';
    const blurb = attrs.blurb ? `: ${attrs.blurb}` : '';
    return `- [${attrs.title || 'Video'}](https://www.youtube.com/watch?v=${attrs.id})${duration}${blurb}`;
  });
}

/** Keep hand-written HTML headings as headings. */
function convertHtmlHeadings(content) {
  return content.replace(/<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi, (_match, level, inner) => {
    return `\n${'#'.repeat(Number(level))} ${inner.trim()}\n`;
  });
}

/** Turn a <DocCardList items={[...]} /> card grid into a plain markdown link list. */
function convertDocCardLists(content) {
  return content.replace(/<DocCardList\s+items=\{\[([\s\S]*?)\]\}\s*\/>/g, (_match, items) => {
    let list = '';
    for (const item of items.matchAll(/\{([\s\S]*?)\n\s*\},?/g)) {
      const href = item[1].match(/href:\s*"([^"]+)"/);
      const label = item[1].match(/label:\s*"([^"]+)"/);
      const description = item[1].match(/description:\s*"?([\s\S]*?)"\s*,?\s*$/m);
      if (!href || !label) continue;
      list += `- [${label[1]}](${href[1]})`;
      if (description) list += `: ${description[1].replace(/^\s*"/, '').replace(/\s+/g, ' ').trim()}`;
      list += '\n';
    }
    return list;
  });
}

function stripJsxComponents(content) {
  // First, try to convert PyDocs components to markdown
  content = convertPyDocsToMarkdown(content);

  // Turn card grids into plain links
  content = convertDocCardLists(content);
  content = convertHomePage(content);
  content = convertVideoCards(content);
  content = convertHtmlHeadings(content);

  // Remove PyModuleMember and PyClassMember wrappers (keep content)
  content = content.replace(/<\/?Py(?:ModuleMember|ClassMember|Parameters|Parameter)[^>]*>/g, '');

  // Remove JSX comments like {/* text */}
  content = content.replace(/\{\/\*[\s\S]*?\*\/\}/g, '');

  // Remove export statements
  content = content.replace(/^export\s+const\s+.*$/gm, '');

  // Turn video embeds into links, before the self-closing strip below discards them
  content = content.replace(/<YouTube\s+([^>]*?)\/>/g, (_match, attrs) => {
    const id = attrs.match(/\bid=["']([^"']+)["']/);
    if (!id) return '';
    const title = attrs.match(/\btitle=["']([^"']+)["']/);
    const start = attrs.match(/\bstart=\{(\d+)\}/);
    const url = `https://www.youtube.com/watch?v=${id[1]}${start ? `&t=${start[1]}` : ''}`;
    return `[${title ? title[1] : 'Video'}](${url})\n`;
  });

  // Remove self-closing tags like <Component /> and <br />
  content = content.replace(/<[a-zA-Z][a-zA-Z]*\s*[^>]*\/>/g, '');

  // Convert admonition components to blockquotes, prefixing every line
  content = content.replace(/<(Note|Tip|Warning|Info|Callout)[^>]*>([\s\S]*?)<\/\1>/gi, (_match, _tag, inner) => {
    return inner.trim().split('\n').map(l => `> ${l}`).join('\n') + '\n';
  });

  // Remove remaining JSX tags but keep content
  content = content.replace(/<\/?[A-Z][a-zA-Z]*[^>]*>/g, '');

  // Remove remaining HTML tags but keep content
  content = content.replace(/<\/?[a-z][a-zA-Z]*[^>]*>/g, '');

  return content;
}

function cleanMdxContent(content) {
  let cleaned = content;

  // Strip frontmatter (safe: only matches at the very start of the file)
  cleaned = stripFrontmatter(cleaned);

  // Protect fenced code blocks from import / tag stripping.
  const codeBlocks = [];
  cleaned = cleaned.replace(/```[\s\S]*?```/g, (block) => {
    codeBlocks.push(block);
    return `\x00CODEBLOCK${codeBlocks.length - 1}\x00`;
  });

  cleaned = stripImports(cleaned);
  cleaned = stripJsxComponents(cleaned);

  // Strip decorative emoji-only navigation lines (e.g. "📚 [Learn more →](/overview/)")
  cleaned = cleaned.replace(/^[^\x00-\x7F]+\s*\[[^\]]+\]\([^)]+\)\s*$/gm, '');

  // Restore code blocks
  cleaned = cleaned.replace(/\x00CODEBLOCK(\d+)\x00/g, (_, i) => codeBlocks[i]);

  // Remove lines that are only whitespace
  cleaned = cleaned.replace(/^\s+$/gm, '');

  // Remove excessive blank lines (more than 2 consecutive)
  cleaned = cleaned.replace(/\n{3,}/g, '\n\n');

  // Trim leading/trailing whitespace
  cleaned = cleaned.trim();

  return cleaned;
}

/** Read the `slug:` frontmatter value, which overrides a page's route. */
function parseSlug(content) {
  const frontmatter = content.match(/^---\n([\s\S]*?)\n---/);
  if (!frontmatter) return null;
  const slug = frontmatter[1].match(/^slug:\s*["']?(.+?)["']?\s*$/m);
  return slug ? slug[1] : null;
}

/** Strip leading numeric prefixes (e.g. "03-import" → "import") from each path segment. */
function stripNumberPrefixes(relativePath) {
  return relativePath
    .split(path.sep)
    .map(segment => segment.replace(/^\d+[-_]/, ''))
    .join(path.sep);
}

/**
 * Where a page's markdown is written: its source path (e.g. tutorial/index.md,
 * which llms.txt links to) plus its route with .md appended (e.g. tutorial.md),
 * which is what agents ask for. `slug:` frontmatter wins over the file path.
 */
function outputPathsFor(relativePath, content) {
  const sourcePath = stripNumberPrefixes(relativePath).split(path.sep).join('/').replace(/\.mdx?$/, '');
  const slug = parseSlug(content);
  const route =
    slug !== null
      ? slug.replace(/^\/+|\/+$/g, '')
      : sourcePath.replace(/(^|\/)index$/, '');

  return [...new Set([`${sourcePath}.md`, route === '' ? 'index.md' : `${route}.md`])];
}

/** The anchor ids Docusaurus gives a page's headings, including explicit `{#id}` ones. */
function headingIds(content) {
  const slugger = createSlugger();
  const body = stripFrontmatter(content).replace(/```[\s\S]*?```/g, '');
  const ids = new Set();
  for (const [, heading] of body.matchAll(/^#{1,6}\s+(.+)$/gm)) {
    const text = heading
      .replace(/`([^`]*)`/g, '$1')
      .replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')
      .replace(/<[^>]+>/g, '')
      .trim();
    const { id, text: title } = parseMarkdownHeadingId(text);
    ids.add(id ?? slugger.slug(title));
  }
  return ids;
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/**
 * Old URLs that only exist as HTML redirect pages get a markdown stub, so an
 * agent following a stale link with `Accept: text/markdown` is pointed at the
 * new page instead of hitting a 404.
 *
 * Fails when a redirect points at an anchor its target page doesn't have,
 * which Docusaurus's own broken-anchor check doesn't cover.
 */
function writeRedirectStubs(pages) {
  const source = fs.readFileSync(REDIRECTS_FILE, 'utf8');
  const entries = [...source.matchAll(/from:\s*"([^"]+)",\s*to:\s*"([^"]+)"/g)];
  const declared = (source.match(/\bfrom:/g) || []).length;
  if (entries.length !== declared) {
    throw new Error(`Parsed ${entries.length} of ${declared} redirects - redirects.js changed shape`);
  }

  const brokenAnchors = [];
  let count = 0;
  for (const [, from, to] of entries) {
    const [route, anchor] = to.split('#');
    if (anchor) {
      const page = pages.get(`${route.replace(/^\/+|\/+$/g, '') || 'index'}.md`);
      if (!page || !headingIds(fs.readFileSync(path.join(PAGES_DIR, page), 'utf8')).has(anchor)) {
        brokenAnchors.push(`${from} → ${to}`);
      }
    }

    const stubPath = `${from.replace(/^\/+|\/+$/g, '')}.md`;
    if (pages.has(stubPath)) continue;
    const outputPath = path.join(STATIC_DIR, stubPath);
    ensureDir(path.dirname(outputPath));
    fs.writeFileSync(outputPath, `${AGENT_DIRECTIVE}This page moved to [${to}](${to}).\n`);
    count++;
  }

  if (brokenAnchors.length > 0) {
    throw new Error(`Redirects point at anchors that don't exist:\n  ${brokenAnchors.join('\n  ')}`);
  }
  return count;
}

function main() {
  console.log('Generating LLM-friendly markdown files...');

  const mdxFiles = getAllMdxFiles(PAGES_DIR);
  // Output path → the page that wrote it. Each page writes up to two files, so two
  // pages can map to the same one (e.g. foo.mdx and foo/index.mdx both write foo.md);
  // we fail instead of letting the second silently overwrite the first.
  // writeRedirectStubs also uses it to find the page behind a redirect target.
  const written = new Map();
  let count = 0;

  for (const { fullPath, relativePath } of mdxFiles) {
    const content = fs.readFileSync(fullPath, 'utf8');
    const cleanedContent = cleanMdxContent(content);

    for (const outputRelativePath of outputPathsFor(relativePath, content)) {
      const claimedBy = written.get(outputRelativePath);
      if (claimedBy) {
        throw new Error(`${relativePath} and ${claimedBy} both write ${outputRelativePath}`);
      }
      written.set(outputRelativePath, relativePath);

      const outputPath = path.join(STATIC_DIR, outputRelativePath);
      ensureDir(path.dirname(outputPath));
      fs.writeFileSync(outputPath, AGENT_DIRECTIVE + cleanedContent);
      count++;
    }
  }

  const stubs = writeRedirectStubs(written);

  console.log(`Generated ${count} markdown files (+ ${stubs} redirect stubs) in static/`);
}

main();
