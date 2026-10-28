#!/usr/bin/env node

/**
 * Writes a markdown copy of every page in pages/ to static/, so appending .md
 * to any documentation URL returns the page as markdown.
 */

const fs = require('fs');
const path = require('path');
const { createSlugger, parseMarkdownHeadingId } = require('@docusaurus/utils');

const PAGES_DIR = path.join(__dirname, '..', 'pages');
const STATIC_DIR = path.join(__dirname, '..', 'static');
const REDIRECTS_FILE = path.join(__dirname, '..', 'redirects.js');
const HOME_CARDS_FILE = path.join(__dirname, '..', 'src', 'theme', 'components', 'home', 'cards.json');

const AGENT_DIRECTIVE = '> The complete documentation index is at [llms.txt](/llms.txt).\n\n';

/** Every .md/.mdx file under a directory, with its path relative to it. */
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

/** Remove the YAML frontmatter. */
function stripFrontmatter(content) {
  return content.replace(/^---\n[\s\S]*?\n---\n/, '');
}

/** Remove ESM import statements. */
function stripImports(content) {
  // Requiring `from '...'` keeps prose lines that start with "import".
  return content
    .replace(/^import\s*\{[^}]*\}\s*from\s+['"][^'"]*['"];?[ \t]*\n?/gm, '')
    .replace(/^import\s+.*\bfrom\s+['"].*$\n?/gm, '');
}

const HTML_ENTITIES = { quot: '"', amp: '&', lt: '<', gt: '>', apos: "'" };

/** Decode the HTML entities in SDK reference attribute values. */
function decodeEntities(text) {
  return text.replace(/&(#x[0-9a-f]+|#\d+|\w+);/gi, (match, entity) => {
    if (entity[0] === '#') {
      const code = entity[1].toLowerCase() === 'x' ? parseInt(entity.slice(2), 16) : parseInt(entity.slice(1), 10);
      return String.fromCodePoint(code);
    }
    return HTML_ENTITIES[entity.toLowerCase()] ?? match;
  });
}

/** Parse JSX attributes, both "quoted" and {expr} values. */
function parseAttrs(attrString) {
  const attrs = {};
  const re = /(\w+)=(?:"([^"]*)"|[{]([^}]*)[}])/g;
  let m;
  while ((m = re.exec(attrString)) !== null) {
    attrs[m[1]] = decodeEntities(m[2] !== undefined ? m[2] : m[3]);
  }
  return attrs;
}

/** Wrap a name in backticks, unless it is a markdown link. */
function codeName(name) {
  return /^\[.*\]\(.*\)$/.test(name) ? name : `\`${name}\``;
}

/** Replace markdown links with their text. */
function stripLinks(text) {
  return text.replace(/\[([^\[\]]*)\]\([^)]*\)/g, '$1');
}

/** Render a PySignature as a python code block. */
function renderSignature(attrString, inner) {
  const sigAttrs = parseAttrs(attrString);
  const params = [];
  for (const tag of inner.matchAll(/<PySignatureParam\s+([^>]*?)\/>/g)) {
    const attrs = parseAttrs(tag[1]);
    if (attrs.separator !== undefined) {
      params.push(attrs.name || '*');
      continue;
    }
    let p = attrs.name || '';
    if (attrs.annotation) p += `: ${stripLinks(attrs.annotation)}`;
    if (attrs.defaultValue) p += ` = ${attrs.defaultValue}`;
    params.push(p);
  }
  const returns = sigAttrs.returns ? ` -> ${sigAttrs.returns}` : '';
  return `\`\`\`python\n${sigAttrs.name || ''}(${params.join(', ')})${returns}\n\`\`\`\n`;
}

/** Render a PyParameters block (Parameters, Raises, ...) as a list. */
function renderParameters(attrString, inner) {
  const title = parseAttrs(attrString).title || 'Parameters';
  let params = '';
  for (const tag of inner.matchAll(/<PyParameter\s+([^>]*?)\/>/g)) {
    const attrs = parseAttrs(tag[1]);
    const annotation = attrs.annotation ? ` (${attrs.annotation})` : '';
    params += `- ${codeName(attrs.name || '?')}${annotation}: ${attrs.description || 'No description'}\n`;
  }
  return params ? `\n**${title}:**\n${params}\n` : '';
}

/** Render a PyAttributesList block as a list. */
function renderAttributes(attrString, inner) {
  const title = parseAttrs(attrString).title || 'Attributes';
  let attrs = '';
  for (const tag of inner.matchAll(/<PyAttribute\s+([^>]*?)\/>/g)) {
    const a = parseAttrs(tag[1]);
    const type = a.type ? ` (${a.type})` : '';
    const description = a.description ? `: ${a.description}` : '';
    attrs += `- ${codeName(a.name || '?')}${type}${description}\n`;
  }
  return attrs ? `\n**${title}:**\n${attrs}\n` : '';
}

/** Render a PyClassBase tag as a "Bases:" line. */
function renderClassBase(bases) {
  try {
    return `\nBases: ${JSON.parse(bases).map(b => codeName(b.name)).join(', ')}\n`;
  } catch {
    return '';
  }
}

/**
 * Render a PyClass or PyFunction as a heading, signature and body.
 * Components are converted where they stand, so prose after the parameters is kept.
 */
function renderMember(name, inner, level) {
  let signature = '';
  const body = inner
    .replace(/<PySignature\s+([^>]*)>([\s\S]*?)<\/PySignature>/, (_m, attrs, params) => {
      signature = renderSignature(attrs, params);
      return '';
    })
    .replace(/<PyParameters([^>]*)>([\s\S]*?)<\/PyParameters>/g, (_m, attrs, params) => renderParameters(attrs, params))
    .replace(/<PyAttributesList([^>]*)>([\s\S]*?)<\/PyAttributesList>/g, (_m, attrs, list) => renderAttributes(attrs, list))
    .replace(/<PyClassBase\s+bases=\{(\[[\s\S]*?\])\}\s*\/>/g, (_m, bases) => renderClassBase(bases))
    .replace(/<PyReturns\s+([^>]*?)\/>/g, (_m, attrs) => {
      const { description } = parseAttrs(attrs);
      return description ? `\n**Returns:** ${description}\n` : '';
    });

  return `${'#'.repeat(level)} ${name}\n\n${signature}\n${body.trim()}\n`;
}

/** Convert the SDK reference components to markdown. */
function convertPyDocsToMarkdown(content) {
  content = content.replace(/<PyClass\s+([^>]*)>([\s\S]*?)<\/PyClass>/g, (_match, attrs, inner) => {
    const withMethods = inner.replace(
      /<PyFunction\s+([^>]*)>([\s\S]*?)<\/PyFunction>/g,
      (_m, methodAttrs, methodInner) => renderMember(parseAttrs(methodAttrs).name, methodInner, 3),
    );
    return renderMember(parseAttrs(attrs).name, withMethods, 2);
  });

  content = content.replace(/<PyFunction\s+([^>]*)>([\s\S]*?)<\/PyFunction>/g, (_match, attrs, inner) =>
    renderMember(parseAttrs(attrs).name, inner, 2),
  );

  content = content.replace(/<PyTypeAlias\s+([^>]*?)\/>/g, (_match, attrString) => {
    const { name, annotation } = parseAttrs(attrString);
    return `## ${name}\n\n\`\`\`python\n${name} = ${annotation}\n\`\`\`\n`;
  });

  return content;
}

/** Render the home page cards as links. */
function convertHomePage(content) {
  if (!content.includes('<HomePage')) return content;

  const { sections, agentsCard } = JSON.parse(fs.readFileSync(HOME_CARDS_FILE, 'utf8'));
  const markdown = [
    `[${agentsCard.title}](${agentsCard.href}): ${agentsCard.description}`,
    ...sections.map(({ title, cards }) => {
      const links = cards.map((c) => `- [${c.title}](${c.href}): ${c.description}`).join('\n');
      return `## ${title}\n\n${links}`;
    }),
  ].join('\n\n');

  return content.replace(/<HomePage\s*\/>/g, markdown);
}

/** Turn VideoCard grids into video links. */
function convertVideoCards(content) {
  return content.replace(/^[ \t]*<VideoCard\s+([\s\S]*?)\/>/gm, (_match, attrString) => {
    const attrs = parseAttrs(attrString);
    if (!attrs.id) return '';
    const duration = attrs.duration ? ` (${attrs.duration})` : '';
    const blurb = attrs.blurb ? `: ${attrs.blurb}` : '';
    return `- [${attrs.title || 'Video'}](https://www.youtube.com/watch?v=${attrs.id})${duration}${blurb}`;
  });
}

/** Turn HTML headings into markdown headings. */
function convertHtmlHeadings(content) {
  return content.replace(/<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi, (_match, level, inner) => {
    return `\n${'#'.repeat(Number(level))} ${inner.trim()}\n`;
  });
}

/** Turn a DocCardList grid into a list of links. */
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

/** Convert or remove the JSX and HTML in a page. */
function stripJsxComponents(content) {
  content = convertPyDocsToMarkdown(content);
  content = convertDocCardLists(content);
  content = convertHomePage(content);
  content = convertVideoCards(content);
  content = convertHtmlHeadings(content);

  content = content.replace(/<\/?Py(?:ModuleMember|ClassMember|Parameters|Parameter)[^>]*>/g, '');

  content = content.replace(/\{\/\*[\s\S]*?\*\/\}/g, '');

  content = content.replace(/^export\s+const\s+.*$/gm, '');

  // Must run before self-closing tags are stripped
  content = content.replace(/<YouTube\s+([^>]*?)\/>/g, (_match, attrs) => {
    const id = attrs.match(/\bid=["']([^"']+)["']/);
    if (!id) return '';
    const title = attrs.match(/\btitle=["']([^"']+)["']/);
    const start = attrs.match(/\bstart=\{(\d+)\}/);
    const url = `https://www.youtube.com/watch?v=${id[1]}${start ? `&t=${start[1]}` : ''}`;
    return `[${title ? title[1] : 'Video'}](${url})\n`;
  });

  content = content.replace(/<[a-zA-Z][a-zA-Z]*\s*[^>]*\/>/g, '');

  content = content.replace(/<(Note|Tip|Warning|Info|Callout)[^>]*>([\s\S]*?)<\/\1>/gi, (_match, _tag, inner) => {
    return inner.trim().split('\n').map(l => `> ${l}`).join('\n') + '\n';
  });

  content = content.replace(/<\/?[A-Z][a-zA-Z]*[^>]*>/g, '');

  content = content.replace(/<\/?[a-z][a-zA-Z]*[^>]*>/g, '');

  return content;
}

/** Turn an .mdx page into plain markdown. */
function cleanMdxContent(content) {
  let cleaned = stripFrontmatter(content);

  // Keep code out of the import and tag stripping, so `<placeholders>` survive
  const code = [];
  cleaned = cleaned.replace(/```[\s\S]*?```|``[^\n]+?``|`[^`\n]+`/g, (span) => {
    code.push(span);
    return `\x00CODE${code.length - 1}\x00`;
  });

  cleaned = stripImports(cleaned);
  cleaned = stripJsxComponents(cleaned);

  // Decorative nav lines like "📚 [Learn more →](/overview/)"
  cleaned = cleaned.replace(/^[^\x00-\x7F]+\s*\[[^\]]+\]\([^)]+\)\s*$/gm, '');

  cleaned = cleaned.replace(/^\s+$/gm, '').replace(/\n{3,}/g, '\n\n').trim();

  return cleaned.replace(/\x00CODE(\d+)\x00/g, (_, i) => code[i]);
}

/** Fail if an SDK reference page has a tag left unconverted or a class/function with no heading. */
function checkSdkReferenceConverted(relativePath, source, markdown) {
  const errors = [];
  if (/<\/?Py[A-Z]/.test(markdown.replace(/```[\s\S]*?```/g, ''))) {
    errors.push('unconverted <Py...> tag');
  }
  for (const [, attrs] of source.matchAll(/<Py(?:Class|Function|TypeAlias)\s([^>]*)>/g)) {
    const { name } = parseAttrs(attrs);
    const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    if (!new RegExp(`^#{2,3} ${escaped}$`, 'm').test(markdown)) errors.push(`no heading for ${name}`);
  }
  if (errors.length > 0) {
    throw new Error(`${relativePath}:\n  ${errors.join('\n  ')}`);
  }
}

/** The `slug:` frontmatter value, which overrides a page's route. */
function parseSlug(content) {
  const frontmatter = content.match(/^---\n([\s\S]*?)\n---/);
  if (!frontmatter) return null;
  const slug = frontmatter[1].match(/^slug:\s*["']?(.+?)["']?\s*$/m);
  return slug ? slug[1] : null;
}

/** Strip numeric prefixes ("03-import" → "import") from each path segment. */
function stripNumberPrefixes(relativePath) {
  return relativePath
    .split(path.sep)
    .map(segment => segment.replace(/^\d+[-_]/, ''))
    .join(path.sep);
}

/**
 * The paths a page's markdown is written to: its source path (tutorial/index.md),
 * which llms.txt links to, and its route (tutorial.md), which agents ask for.
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

/** The anchor ids Docusaurus gives a page's headings. */
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

/** Create a directory if it doesn't exist. */
function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/**
 * Write a markdown stub at every old URL that redirects, so agents following a
 * stale link find the new page. Fails on redirects to missing anchors, which
 * Docusaurus doesn't check.
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

/** Write the markdown copy of every page and the redirect stubs. */
function main() {
  console.log('Generating LLM-friendly markdown files...');

  const mdxFiles = getAllMdxFiles(PAGES_DIR);
  // Output path → source page. foo.mdx and foo/index.mdx both write foo.md, so
  // a second claim fails instead of overwriting.
  const written = new Map();
  let count = 0;

  for (const { fullPath, relativePath } of mdxFiles) {
    const content = fs.readFileSync(fullPath, 'utf8');
    const cleanedContent = cleanMdxContent(content);
    checkSdkReferenceConverted(relativePath, content, cleanedContent);

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
