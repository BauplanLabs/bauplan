#!/usr/bin/env node

/**
 * Generate LLM-friendly markdown files from .mdx source files.
 * Copies all .mdx files from pages/ to static/ with .md extension,
 * stripping numeric prefixes so URLs match Docusaurus routes
 * (e.g. 03-import.mdx → import.md, accessible at /tutorial/import.md).
 */

const fs = require('fs');
const path = require('path');

const PAGES_DIR = path.join(__dirname, '..', 'pages');
const STATIC_DIR = path.join(__dirname, '..', 'static');

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
  return content.replace(/^import\s+.*\bfrom\s+['"].*$\n?/gm, '');
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
  // bases={[...]} — match a JSON array; links are /reference/... paths with no ]/> sequences.
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

function stripJsxComponents(content) {
  // First, try to convert PyDocs components to markdown
  content = convertPyDocsToMarkdown(content);

  // Remove PyModuleMember and PyClassMember wrappers (keep content)
  content = content.replace(/<\/?Py(?:ModuleMember|ClassMember|Parameters|Parameter)[^>]*>/g, '');

  // Remove JSX comments like {/* text */}
  content = content.replace(/\{\/\*[\s\S]*?\*\/\}/g, '');

  // Remove export statements
  content = content.replace(/^export\s+const\s+.*$/gm, '');

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

/** Strip leading numeric prefixes (e.g. "03-import" → "import") from each path segment. */
function stripNumberPrefixes(relativePath) {
  return relativePath
    .split(path.sep)
    .map(segment => segment.replace(/^\d+[-_]/, ''))
    .join(path.sep);
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function main() {
  console.log('Generating LLM-friendly markdown files...');

  const mdxFiles = getAllMdxFiles(PAGES_DIR);
  let count = 0;

  for (const { fullPath, relativePath } of mdxFiles) {
    // Convert .mdx to .md and strip numeric prefixes from path segments
    const outputRelativePath = stripNumberPrefixes(relativePath).replace(/\.mdx$/, '.md');
    const outputPath = path.join(STATIC_DIR, outputRelativePath);

    // Ensure output directory exists
    ensureDir(path.dirname(outputPath));

    // Read, clean, and write the file
    const content = fs.readFileSync(fullPath, 'utf8');
    const cleanedContent = cleanMdxContent(content);

    fs.writeFileSync(outputPath, cleanedContent);
    count++;
  }

  console.log(`Generated ${count} markdown files in static/`);
}

main();
