#!/usr/bin/env uv run

import html
import json
import logging
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TextIO, cast

import griffe

# This script generates the reference pages for the sdk under ./pages/reference.
# The strategy is to spit out mdx with pre-existing, custom react components:
#
# ```mdx
# <PyFunction name="apply_table_creation_plan">
# Apply a plan for creating a table...
#
#   <PyParameters>
#     <PyParameter name="plan" annotation="Union[Dict, TableCreatePlanState]" default="" description="The plan to apply."/>
#     <PyParameter name="debug" annotation="Optional[bool]" default="None" description="Whether to enable or disable debug mode for the query."/>
#     <PyParameter name="priority" annotation="Optional[int]" default="None" description="Optional job priority (1-10, where 10 is highest priority)."/>
#     <PyParameter name="verbose" annotation="Optional[bool]" default="None" description="Whether to enable or disable verbose mode."/>
#     <PyParameter name="client_timeout" annotation="Optional[Union[int, float]]" default="None" description="seconds to timeout; this also cancels the remote job execution."/>
#   </PyParameters>
# </PyFunction>
# ```
#
# This means that the display layer is decoupled from the generation.


SKIP_MODULES = ['bauplan.extras']

# Single pass over three mutually exclusive patterns:
#   bauplan.module.Name  — full_ref=bauplan.module.Name, name=Name (dotted)
#   Client.method        — full_ref=Client.method,       name=method
#   bauplan.ClassName    — full_ref=bauplan.InfoState,   name=InfoState (short)
# Methods register as full_ref ("Client.run"); classes register as name ("InfoState"),
# so bauplan.InfoState in prose needs the name fallback to match.
_REF_PATTERN = re.compile(
    r'(?P<open>`?)'
    r'(?P<full>'
    r'bauplan\.(?:\w+\.)+(?P<dotted>\w+)'
    r'|Client\.(?P<method>\w+)'
    r'|bauplan\.(?P<short>[A-Z]\w+)'
    r')'
    r'(?P<close>`?)'
)


def _walk_module_tree(module: griffe.Module, seen: set[str] | None = None) -> Iterator[griffe.Module]:
    """Yield root and all reachable sub-modules, skipping _bpln_proto internals and SKIP_MODULES."""
    if seen is None:
        seen = set()
    if '._bpln_proto' in module.path:
        return
    page = path_slug(module)
    if page in seen:
        return
    seen.add(page)
    yield module
    for member in module.members.values():
        if (
            member.is_public
            and member.kind == griffe.Kind.MODULE
            and member.path not in SKIP_MODULES
            and member.members
        ):
            assert isinstance(member, griffe.Module)
            yield from _walk_module_tree(member, seen)


class TypeLinker:
    """Owns the type-name → (page_slug, anchor) lookup and all linking/formatting methods."""

    def __init__(self) -> None:
        self._lookup: dict[str, tuple[str, str]] = {}
        self._pattern: re.Pattern | None = None

    def register(self, name: str, page_slug: str, anchor: str) -> None:
        self._lookup[name] = (page_slug, anchor)
        self._pattern = None  # invalidate cached pattern

    def _get_pattern(self) -> re.Pattern | None:
        if self._pattern is None:
            names = sorted(self._lookup.keys(), key=len, reverse=True)
            if not names:
                return None
            self._pattern = re.compile(r'(?<![.\w])(' + '|'.join(re.escape(n) for n in names) + r')\b')
        return self._pattern

    def linkify(self, text: str) -> str:
        """Replace bare type names with markdown links."""
        if not text:
            return text
        pattern = self._get_pattern()
        if not pattern:
            return text

        def replace_type(match: re.Match) -> str:
            name = match.group(0)
            page_slug, anchor = self._lookup[name]
            return f'[{name}](/reference/{page_slug}#{anchor})'

        return pattern.sub(replace_type, text)

    def format_annotation(self, text: str) -> str:
        """Shorten FQ names, strip Pydantic FieldInfo wrappers, remove typing. prefix, linkify."""
        if not text:
            return ''
        text = re.sub(r'bauplan\.\w+\.(\w+)', r'\1', text)
        text = re.sub(r'Annotated\[(.+?),\s*FieldInfo\([^)]*\)\]', r'\1', text)
        text = re.sub(r'typing\.', '', text)
        return self.linkify(text)

    def resolve_bauplan_refs(self, text: str) -> str:
        """Replace known reference patterns with anchor links in prose.

        Handles three forms:
        - ``bauplan.module.Name``  (e.g. bauplan.schema.TableWithMetadata)
        - ``Client.method``        (e.g. Client.get_table)
        - ``bauplan.Name``         (e.g. bauplan.InfoState)

        Code blocks (``` fenced) are left untouched.
        """
        if not text:
            return text

        def replace(match: re.Match) -> str:
            open_tick = match.group('open')
            full_ref = match.group('full')
            close_tick = match.group('close')
            name = match.group('dotted') or match.group('method') or match.group('short')
            lookup = self._lookup.get(full_ref) or self._lookup.get(name)
            if not lookup:
                return match.group(0)
            page_slug, anchor = lookup
            return f'[{open_tick}{full_ref}{close_tick}](/reference/{page_slug}#{anchor})'

        def linkify(chunk: str) -> str:
            return _REF_PATTERN.sub(replace, chunk)

        # re.split with (...) includes the matched delimiters in the result, so code blocks survive intact
        parts = re.split(r'(```[\s\S]*?```)', text)
        return ''.join(p if p.startswith('```') else linkify(p) for p in parts)

    def build_lookup(self, module: griffe.Module) -> None:
        """Walk module tree to build name -> (page, anchor) lookup."""
        for mod in _walk_module_tree(module):
            page = path_slug(mod)
            for member in mod.members.values():
                if not member.is_public:
                    continue
                try:
                    resolved = resolve(member)
                except Exception:
                    logging.debug('Could not resolve member %s', member.name)
                    continue

                match resolved.kind:
                    case griffe.Kind.CLASS:
                        anchor = f'{page}-{member.name.lower()}'
                        self.register(resolved.name, page, anchor)
                        for cls_member in resolved.members.values():
                            if not cls_member.is_public or cls_member.name.startswith('_'):
                                continue
                            try:
                                cls_resolved = resolve(cls_member)
                            except Exception:
                                logging.debug('Could not resolve class member %s', cls_member.name)
                                continue
                            if cls_resolved.kind == griffe.Kind.FUNCTION:
                                method_anchor = f'{anchor}-{cls_member.name.lower()}'
                                self.register(f'{resolved.name}.{cls_member.name}', page, method_anchor)
                    case griffe.Kind.FUNCTION:
                        anchor = function_slug(page, member.name)
                        self.register(resolved.name, page, anchor)
                    # MODULE case: handled by _walk_module_tree


class ParsedDocstring:
    # Two-pass rendering strategy used by callers that interleave a code snippet:
    #   1. write_preamble  — first paragraph, shown before the snippet
    #   2. write_body      — everything after the first paragraph, shown after the snippet
    # Callers that don't need the split use write() directly (e.g. module docstrings).

    def __init__(self, docstring: griffe.Docstring, linker: TypeLinker) -> None:
        self.sections = list(docstring.parse('google'))
        self._linker = linker
        self._process_bauplan_references()

    def _process_bauplan_references(self) -> None:
        """Process all sections to replace bauplan references with markdown links."""
        resolve_refs = self._linker.resolve_bauplan_refs
        for section in self.sections:
            match section.kind:
                case griffe.DocstringSectionKind.text:
                    section.value = resolve_refs(section.value)

                case griffe.DocstringSectionKind.parameters:
                    for param in section.value:
                        if hasattr(param, 'description') and param.description:
                            param.description = resolve_refs(param.description)

                case griffe.DocstringSectionKind.returns | griffe.DocstringSectionKind.yields:
                    for return_item in section.value:
                        if hasattr(return_item, 'description') and return_item.description:
                            return_item.description = resolve_refs(return_item.description)

                case griffe.DocstringSectionKind.raises:
                    for exception in section.value:
                        if hasattr(exception, 'description') and exception.description:
                            exception.description = resolve_refs(exception.description)

                case griffe.DocstringSectionKind.examples:
                    for example in section.value:
                        if hasattr(example, 'description') and example.description:
                            example.description = resolve_refs(example.description)

                case _:
                    if hasattr(section, 'value'):
                        if isinstance(section.value, str):
                            section.value = resolve_refs(section.value)
                        elif hasattr(section.value, 'description'):
                            section.value.description = resolve_refs(section.value.description)

    def _apply_heading_demotion(self, text: str) -> str:
        # Demote h1-h3 headings in docstring prose to h4 so they don't
        # appear in the Docusaurus TOC (which only indexes h2/h3).
        # Skip code fences — Python comments start with '#' too.
        parts = re.split(r'(```[\s\S]*?```)', text)
        return ''.join(
            re.sub(r'^#{1,3} ', '#### ', part, flags=re.MULTILINE) if i % 2 == 0 else part
            for i, part in enumerate(parts)
        )

    def _write_returns(self, output: TextIO, section: griffe.DocstringSection) -> None:
        if section.value:
            label = 'Yields' if section.kind == griffe.DocstringSectionKind.yields else 'Returns'
            output.write(f'{label}:\n')
            for item in section.value:
                description = item.description or str(item.annotation or '')
                output.write(f'    {description}\n')
            output.write('\n')

    def write_returns_component(self, output: TextIO) -> None:
        """Write returns/yields as a <PyReturns> component instead of prose."""
        for section in self.sections:
            if section.kind in (griffe.DocstringSectionKind.returns, griffe.DocstringSectionKind.yields):
                if section.value:
                    for item in section.value:
                        description = item.description or str(item.annotation or '')
                        if description:
                            output.write(
                                f'<PyReturns description="{escape_xml_attribute(description)}" />\n\n'
                            )
            elif section.kind == griffe.DocstringSectionKind.text and self._is_returns_text(section.value):
                # griffe emits some Returns sections as plain text — extract the description
                text = section.value.strip()
                # Strip "Returns:\n    " prefix
                m = re.match(r'(?:Returns|Yields):\s*\n\s+(.+)', text, re.DOTALL)
                if m:
                    description = m.group(1).strip()
                    output.write(f'<PyReturns description="{escape_xml_attribute(description)}" />\n\n')

    def write(self, output: TextIO) -> None:
        """Write text and returns sections. Raises are deferred to write_raises()."""
        for section in self.sections:
            match section.kind:
                case griffe.DocstringSectionKind.text:
                    output.write(self._apply_heading_demotion(section.value))
                    output.write('\n\n')

                case griffe.DocstringSectionKind.returns | griffe.DocstringSectionKind.yields:
                    # Render structured returns/yields as plain text for consistency,
                    # since griffe only parses some Returns sections as structured.
                    self._write_returns(output, section)

    def write_preamble(self, output: TextIO) -> None:
        """Write the first paragraph of the first text section."""
        for section in self.sections:
            if section.kind == griffe.DocstringSectionKind.text:
                first_para = section.value.split('\n\n')[0].strip()
                if first_para:
                    output.write(first_para + '\n\n')
                return

    # griffe sometimes emits "Returns:\n    ..." as a plain text section rather than a
    # structured returns section (common with .pyi stubs). Detect and route correctly.
    _RETURNS_HEADER = re.compile(r'^(Returns|Yields):', re.IGNORECASE)

    def _is_returns_text(self, text: str) -> bool:
        return bool(self._RETURNS_HEADER.match(text.strip()))

    def _first_text_section(self) -> griffe.DocstringSection | None:
        return next((s for s in self.sections if s.kind == griffe.DocstringSectionKind.text), None)

    def _after_first_paragraph(self, section_value: str) -> str:
        """Return everything after the first paragraph break, or '' if there is none.

        When there is no \\n\\n the entire section IS the first paragraph (already written by
        write_preamble), so returning '' is correct — no content is lost.
        """
        _, sep, rest = section_value.partition('\n\n')
        return rest.lstrip('\n') if sep else ''

    _UPON_FAILURE_RE = re.compile(r'Upon failure,?\s+raises\b.*', re.IGNORECASE)

    def _filter_upon_failure(self, text: str) -> str:
        """Remove 'Upon failure, raises...' sentences from prose."""
        paragraphs = re.split(r'\n\n+', text)
        kept = [p for p in paragraphs if not self._UPON_FAILURE_RE.search(p)]
        return '\n\n'.join(kept)

    def write_description(self, output: TextIO) -> None:
        """Write prose (non-code-block) parts of text sections, skipping the first paragraph."""
        first = self._first_text_section()
        for section in self.sections:
            if section.kind != griffe.DocstringSectionKind.text:
                continue
            text = self._after_first_paragraph(section.value) if section is first else section.value
            if not text.strip() or self._is_returns_text(text):
                continue
            parts = re.split(r'(```[\s\S]*?```)', text)
            prose = ''.join(part for i, part in enumerate(parts) if i % 2 == 0).strip()
            prose = self._filter_upon_failure(prose).strip()
            if prose:
                prose = self._apply_heading_demotion(prose)
                # strip standalone Example/Examples headings — write_examples adds its own
                prose = re.sub(r'(?m)^####\s+Examples?\s*$\n?', '', prose).strip()
            if prose:
                output.write(prose)
                output.write('\n\n')

    def write_body(self, output: TextIO) -> None:
        """Write the full body (everything after the first paragraph), preserving prose+code order."""
        first = self._first_text_section()
        for section in self.sections:
            if section.kind == griffe.DocstringSectionKind.text:
                text = self._after_first_paragraph(section.value) if section is first else section.value
                if not text.strip() or self._is_returns_text(text):
                    continue
                output.write(self._apply_heading_demotion(text.strip()))
                output.write('\n\n')
            elif section.kind in (griffe.DocstringSectionKind.returns, griffe.DocstringSectionKind.yields):
                self._write_returns(output, section)

    def write_returns(self, output: TextIO) -> None:
        """Write returns/yields — both structured sections and returns-like text sections."""
        for section in self.sections:
            if section.kind in (griffe.DocstringSectionKind.returns, griffe.DocstringSectionKind.yields):
                self._write_returns(output, section)
            elif section.kind == griffe.DocstringSectionKind.text and self._is_returns_text(section.value):
                output.write(section.value.strip())
                output.write('\n\n')

    def write_examples(self, output: TextIO) -> None:
        """Write code block examples from text sections, skipping the first paragraph."""
        first = self._first_text_section()
        blocks: list[str] = []
        for section in self.sections:
            if section.kind != griffe.DocstringSectionKind.text:
                continue
            text = self._after_first_paragraph(section.value) if section is first else section.value
            if not text.strip() or self._is_returns_text(text):
                continue
            parts = re.split(r'(```[\s\S]*?```)', text)
            for i, part in enumerate(parts):
                if i % 2 == 1:
                    blocks.append(part)

        if blocks:
            label = 'Examples' if len(blocks) > 1 else 'Example'
            output.write(f'#### {label}\n\n')
            for block in blocks:
                output.write(block)
                output.write('\n\n')

    def write_raises(self, output: TextIO) -> None:
        """Write Raises sections. Call after parameters."""
        for section in self.sections:
            match section.kind:
                case griffe.DocstringSectionKind.raises:
                    if section.value:
                        output.write('<PyParameters title="Raises" defaultOpen={false}>\n')
                        for i, item in enumerate(section.value):
                            raw_name = str(item.annotation or '').strip('`')
                            name = html.escape(self._linker.resolve_bauplan_refs(raw_name))
                            description = html.escape(item.description or '')
                            is_odd = i % 2 == 1
                            output.write(
                                f'<PyParameter name="{name}" annotation="" default="" description="{description}" showBadge={{false}} isOdd={{{str(is_odd).lower()}}}/>\n'
                            )
                        output.write('</PyParameters>\n\n')

    def get_parameter_description(self, name: str) -> str | None:
        for section in self.sections:
            if section.kind == griffe.DocstringSectionKind.parameters:
                for param in section.value:
                    if param.name == name:
                        return param.description
        return None


def main() -> None:
    output_dir = Path(__file__).parent / 'pages' / 'reference'
    output_dir.mkdir(parents=True, exist_ok=True)

    bauplan = griffe.load('bauplan')

    assert isinstance(bauplan, griffe.Module)
    linker = TypeLinker()
    linker.build_lookup(bauplan)
    module_names = process_module(output_dir, bauplan, linker)

    print(f'Processed {len(module_names)} modules')
    with open(output_dir / '_sidebar.json', 'w') as f:
        pages = sorted([f'reference/{name}' for name in module_names])
        json.dump(pages, f)


def _member_sort_key(m: griffe.Object) -> tuple[int, str]:
    return (0 if m.name == 'Client' else 1, m.name)


def process_module(output_dir: Path, module: griffe.Module, linker: TypeLinker) -> list[str]:
    names = []
    for mod in _walk_module_tree(module):
        name = path_slug(mod)
        names.append(name)

        with open(output_dir / f'{name}.mdx', 'w') as f:
            f.write('---\n')
            f.write(f'title: "{mod.path}"\n')
            f.write('---\n\n')

            # Build a table of contents.
            toc = []

            if mod.docstring:
                ParsedDocstring(mod.docstring, linker).write(f)

            for member in sorted(mod.members.values(), key=_member_sort_key): # ty:ignore
                if not member.is_public:
                    continue

                match member.kind:
                    case griffe.Kind.CLASS:
                        with wrap(f, 'PyModuleMember', member.name):
                            process_class(f, toc, member, linker, page_slug=name)
                    case griffe.Kind.FUNCTION:
                        with wrap(f, 'PyModuleMember', member.name):
                            process_function(f, toc, 2, member, linker, slug=function_slug(name, member.name))
                    case griffe.Kind.MODULE:
                        pass  # handled by _walk_module_tree
                    case _:
                        print(f'WARNING: skipping {member.path}: {member.kind}')

            toc_json = json.dumps(json.dumps(toc))
            f.write(f'export const toc = JSON.parse({toc_json});\n')

    return names


def process_parameters(
    output: TextIO, parameters: griffe.Parameters, docstring: ParsedDocstring | None, linker: TypeLinker
) -> None:
    if not parameters or (len(parameters) == 1 and parameters[0].name == 'self'):
        return
    with wrap(output, 'PyParameters'):
        for parameter in parameters:
            if parameter.name in ['self', 'cls', 'args', 'kwargs']:
                continue

            annotation = linker.format_annotation(str(parameter.annotation or ''))
            annotation = escape_xml_attribute(annotation)
            default = escape_xml_attribute(str(parameter.default or ''))
            description = docstring.get_parameter_description(parameter.name) if docstring else None
            description = escape_xml_attribute(description or '')
            output.write(
                f'<PyParameter name="{parameter.name}" annotation="{annotation}" default="{default}" description="{description}"/>\n'
            )


def get_base_class_link(base_class: griffe.Class) -> str:
    """Generate a link to a base class documentation."""
    # Get the module path from the parent module
    module_path = base_class.parent.path if base_class.parent else ''
    base_module_path = module_path.replace('.', '-')

    # Create the anchor (module_path + class_name in lowercase)
    anchor = f'{base_module_path}-{base_class.name.lower()}'

    return f'/reference/{base_module_path}#{anchor}'


def _get_signature_params(cls: griffe.Class, linker: TypeLinker) -> list[tuple[str, str, str]] | None:
    """
    Extract structured (name, annotation, default) params for a class.

    Returns None if the signature should be skipped (empty or unavailable).
    Checks __init__ first, then __new__ (used by PyO3/Rust extension classes).
    """
    constructor = cls.members.get('__init__') or cls.members.get('__new__')
    if constructor is None:
        return None

    try:
        resolved_constructor = resolve(constructor)
    except Exception:
        return None

    params = []
    for p in resolved_constructor.parameters:
        if p.name in ('self', 'cls', 'args', 'kwargs'):
            continue
        if p.name == '*':
            params.append(('*', '', ''))
            continue
        ann = linker.format_annotation(str(p.annotation) if p.annotation else '')
        default = str(p.default) if p.default else ''
        params.append((p.name, ann, default))

    return params or None


def format_and_write_signature(cls: griffe.Class, output: TextIO, linker: TypeLinker) -> None:
    """
    Format and write a class signature as a <PySignature> JSX component.

    Args:
        cls: Griffe class object with members
        output: File-like object to write to
        linker: TypeLinker instance for annotation formatting
    """
    params = _get_signature_params(cls, linker)
    if not params:
        return

    output.write(f'<PySignature name="{cls.path}">\n')
    for name, ann, default in params:
        if name == '*':
            output.write('<PySignatureParam name="*" separator=""/>\n')
            continue
        ann_attr = f' annotation="{escape_xml_attribute(ann)}"' if ann else ''
        def_attr = f' defaultValue="{escape_xml_attribute(default)}"' if default else ''
        output.write(f'<PySignatureParam name="{name}"{ann_attr}{def_attr}/>\n')
    output.write('</PySignature>\n\n')


def _get_class_bases(cls: griffe.Class, linker: TypeLinker) -> list[dict[str, str]]:
    """Build a list of {name, link} dicts for the class's bases."""
    bases_list = []
    for base_name in cls.bases:
        formatted = linker.format_annotation(str(base_name))

        link = ''
        try:
            base_obj = cls.modules_collection[str(base_name)]
            if isinstance(base_obj, griffe.Class) and base_obj.is_public:
                link = get_base_class_link(base_obj)
        except (KeyError, TypeError):
            pass

        bases_list.append({'name': formatted, 'link': link})

    return bases_list


def process_class(
    output: TextIO, toc: list[dict], cls: griffe.Class, linker: TypeLinker, page_slug: str | None = None
) -> None:
    slug = f'{page_slug}-{cls.name.lower()}' if page_slug else path_slug(cls)
    toc.append({'value': cls.name, 'id': slug, 'level': 2})

    is_enum = any('Enum' in str(b) for b in cls.bases)
    is_sealed = _get_signature_params(cls, linker) is None and not is_enum
    sealed_attr = ' sealed={true}' if is_sealed else ''
    output.write(f'<PyClass id="{slug}" name="{cls.name}"{sealed_attr}>\n\n')

    parsed = ParsedDocstring(cls.docstring, linker) if cls.docstring is not None else None
    if parsed:
        parsed.write_preamble(output)

    # code snippet
    if is_enum:
        enum_members = [
            (name, member)
            for name, member in cls.members.items()
            if not name.startswith('_') and member.kind == griffe.Kind.ATTRIBUTE
        ]
        if enum_members:
            bases_str = ', '.join(str(b).rsplit('.', 1)[-1] for b in cls.bases)
            output.write(f'```python notest\nclass {cls.name}({bases_str}):\n')
            for name, member in enum_members:
                # Extract value from repr like "<EntryType.TABLE: 'TABLE'>" -> 'TABLE'
                raw = str(member.value)
                m = re.search(r':\s*(.+?)>$', raw)
                val = m.group(1).strip() if m else repr(raw)
                output.write(f'    {name} = {val}\n')
            output.write('```\n\n')
    else:
        format_and_write_signature(cls, output, linker)

    # collect attributes and functions first so we know if attributes exist
    attributes = []
    functions = []
    for member in cls.members.values():
        if not member.is_public or member.name.startswith('_'):
            continue
        match member.kind:
            case griffe.Kind.ATTRIBUTE:
                attributes.append(member)
            case griffe.Kind.FUNCTION:
                functions.append(member)

    # show constructor parameters only when there are no attribute members
    # (attributes and constructor params are usually the same fields — avoid duplication)
    if not attributes:
        constructor = cls.members.get('__init__') or cls.members.get('__new__')
        if constructor:
            try:
                resolved_constructor = resolve(constructor)
                process_parameters(output, resolved_constructor.parameters, parsed, linker)
            except Exception:
                logging.debug('Could not resolve constructor for %s', cls.name)

    # raises, then full body (prose + code blocks interleaved — classes often have rich sectioned docs)
    if parsed:
        parsed.write_raises(output)
        parsed.write_body(output)

    # bases
    bases_list = _get_class_bases(cls, linker)
    if bases_list:
        output.write(f'<PyClassBase bases={{{json.dumps(bases_list)}}}/>\n')

    if attributes:
        output.write('<PyAttributesList>\n\n')
        for i, attr in enumerate(attributes):
            process_class_attribute(output, attr, linker, i % 2 == 1)
        output.write('\n</PyAttributesList>\n\n')

    for member in functions:
        with wrap(output, 'PyClassMember', member.name):
            process_function(output, toc, 3, member, linker, slug=f'{slug}-{member.name.lower()}')

    output.write('</PyClass>\n\n')


def process_class_attribute(output: TextIO, attr: griffe.Attribute, linker: TypeLinker, is_odd: bool) -> None:
    name = attr.name
    annotation = linker.format_annotation(str(attr.annotation)) if attr.annotation else ''
    description = attr.docstring.value if attr.docstring else ''
    if description:
        description = linker.resolve_bauplan_refs(description)
    output.write(
        f'<PyAttribute name="{escape_xml_attribute(name)}" type="{escape_xml_attribute(annotation)}" description="{escape_xml_attribute(description)}" isOdd={{{str(is_odd).lower()}}}/>\n'
    )


def _strip_md_links(text: str) -> str:
    return re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)


def format_function_signature(fn: griffe.Function, linker: TypeLinker) -> str:
    """Generate a Black-formatted Python def signature for display in a code block."""
    parts: list[str] = []
    prev_kind: griffe.ParameterKind | None = None

    for p in fn.parameters:
        if p.name in ('self', 'cls'):
            continue

        kind = p.kind

        # Omit '/' (positional-only marker) — implementation detail, not user-facing.
        # Keep '*' (keyword-only separator) — callers must use keyword syntax.
        if kind == griffe.ParameterKind.keyword_only and prev_kind not in (
            griffe.ParameterKind.keyword_only,
            griffe.ParameterKind.var_positional,
        ):
            parts.append('*')

        if kind == griffe.ParameterKind.var_positional:
            prefix = '*'
        elif kind == griffe.ParameterKind.var_keyword:
            prefix = '**'
        else:
            prefix = ''

        param_str = prefix + p.name
        if p.annotation:
            ann = _strip_md_links(linker.format_annotation(str(p.annotation)))
            param_str += f': {ann}'
        if p.default:
            param_str += f' = {p.default}'

        parts.append(param_str)
        prev_kind = kind

    ret = ''
    if fn.returns:
        ret_str = _strip_md_links(linker.format_annotation(str(fn.returns)))
        ret = f' -> {ret_str}'

    single_line = f'def {fn.name}({", ".join(parts)}){ret}: ...'
    if len(single_line) <= 88:
        return single_line

    # Black-style multi-line: one param per line, trailing comma, closing paren alone
    indented = ',\n    '.join(parts)
    return f'def {fn.name}(\n    {indented},\n){ret}: ...'


def write_function_signature(output: TextIO, fn: griffe.Function, linker: TypeLinker) -> None:
    """Write a function signature as a <PySignature> component (no copy button, syntax-highlighted)."""
    returns_ann = (
        escape_xml_attribute(_strip_md_links(linker.format_annotation(str(fn.returns)))) if fn.returns else ''
    )
    returns_attr = f' returns="{returns_ann}"' if returns_ann else ''

    output.write(f'<PySignature name="def {fn.name}"{returns_attr}>\n')

    prev_kind: griffe.ParameterKind | None = None
    for p in fn.parameters:
        if p.name in ('self', 'cls'):
            continue

        kind = p.kind

        if kind == griffe.ParameterKind.keyword_only and prev_kind not in (
            griffe.ParameterKind.keyword_only,
            griffe.ParameterKind.var_positional,
        ):
            output.write('<PySignatureParam name="*" separator=""/>\n')

        if kind == griffe.ParameterKind.var_positional:
            prefix = '*'
        elif kind == griffe.ParameterKind.var_keyword:
            prefix = '**'
        else:
            prefix = ''

        name = prefix + p.name
        ann = escape_xml_attribute(_strip_md_links(linker.format_annotation(str(p.annotation or ''))))
        default = escape_xml_attribute(str(p.default or ''))
        ann_attr = f' annotation="{ann}"' if ann else ''
        def_attr = f' defaultValue="{default}"' if default else ''
        output.write(f'<PySignatureParam name="{name}"{ann_attr}{def_attr}/>\n')
        prev_kind = kind

    output.write('</PySignature>\n\n')


def process_function(
    output: TextIO,
    toc: list[dict],
    toc_level: int,
    fn: griffe.Function,
    linker: TypeLinker,
    slug: str | None = None,
) -> None:
    slug = slug or path_slug(fn)
    toc.append({'value': f'{fn.name}()', 'id': slug, 'level': toc_level})

    output.write(f'<PyFunction id="{slug}" name="{fn.name}">\n')

    docstring = None
    if fn.docstring is not None:
        docstring = ParsedDocstring(fn.docstring, linker)
        docstring.write_preamble(output)
        docstring.write_returns_component(output)

    write_function_signature(output, fn, linker)

    if fn.parameters:
        process_parameters(output, fn.parameters, docstring, linker)

    if docstring:
        docstring.write_raises(output)
        docstring.write_description(output)
        docstring.write_examples(output)

    output.write('</PyFunction>\n\n')


def process_type_alias(output: TextIO, toc: list[dict], alias: griffe.TypeAlias) -> None:
    slug = path_slug(alias)
    toc.append({'value': alias.name, 'id': slug, 'level': 2})

    annotation = html.escape(str(alias.value or ''))
    output.write(f'<PyTypeAlias id="{slug}" name="{alias.name}" annotation="{annotation}"/>\n')


@contextmanager
def wrap(output: TextIO, component_name: str, comment: str | None = None) -> Iterator[None]:
    comment = f' {jsx_comment(comment)}' if comment else ''

    output.write(f'<{component_name}>{comment}\n\n')
    yield
    output.write(f'</{component_name}>{comment}\n\n')


def jsx_comment(text: str) -> str:
    return '{/* ' + html.escape(text) + ' */}'


def function_slug(page_slug: str, function_name: str) -> str:
    return f'{page_slug}-{function_name.lower()}-function'


def path_slug(obj: griffe.Object) -> str:
    return re.sub(r'[^a-z0-9-]', '-', obj.path.lower())


def resolve[T](obj: griffe.Alias | griffe.Object) -> T:
    if isinstance(obj, griffe.Alias):
        return resolve(obj.final_target)
    return cast(T, obj)


def escape_xml_attribute(value: str) -> str:
    """Escape special characters for XML/JSX attributes."""
    if not value:
        return ''
    return (
        value.replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&#39;')
    )


if __name__ == '__main__':
    main()
