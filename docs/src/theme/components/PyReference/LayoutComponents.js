import React, { useState } from "react";
import { ChevronDown, Link, Check } from "lucide-react";
import Markdown from "react-markdown";
import useBrokenLinks from "@docusaurus/useBrokenLinks";

function CopyAnchorLink({ id }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = (e) => {
    e.preventDefault();
    const url = `${window.location.origin}${window.location.pathname}#${id}`;
    navigator.clipboard.writeText(url).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <a
      href={`#${id}`}
      onClick={handleCopy}
      className="ml-2 opacity-0 group-hover:opacity-100 transition-opacity duration-150 text-gray-400 hover:text-gray-600 no-underline"
      aria-label="Copy link to section"
      title={copied ? "Copied!" : "Copy link"}
    >
      {copied ? <Check size={16} /> : <Link size={16} />}
    </a>
  );
}

export function PyModuleMember(props) {
  return (
    <>
      <hr />
      <div>{props.children}</div>
    </>
  );
}

export function PyClass(props) {
  useBrokenLinks().collectAnchor(props.id);
  return (
    <>
      <h2 id={props.id} className="anchor group">
        <div className="flex flex-row items-center gap-2">
          <code className="mb-0">class</code>
          <span className="mb-0">{props.name}</span>
          {props.sealed && (
            <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600 border border-gray-200">
              Non-instantiable
            </span>
          )}
          <CopyAnchorLink id={props.id} />
        </div>
      </h2>

      {props.children}
    </>
  );
}

export function PyClassMember(props) {
  return (
    <div className="border-t-2 border-[var(--ifm-color-emphasis-200)] mt-10 pt-10">
      {props.children}
    </div>
  );
}

export function PyFunction(props) {
  useBrokenLinks().collectAnchor(props.id);
  return (
    <>
      <h2 id={props.id} className="anchor group">
        <div className="flex flex-row items-center gap-2">
          <code style={{ fontSize: '0.775em' }}>def</code>
          <span className="mb-0">{props.name}<span className="font-normal opacity-50">{" (...)"}</span></span>
          <CopyAnchorLink id={props.id} />
        </div>
      </h2>

      {props.children}
    </>
  );
}

export function PyTypeAlias(props) {
  useBrokenLinks().collectAnchor(props.id);
  return (
    <>
      <h2 id={props.id} className="anchor group">
        <div className="flex flex-row items-center gap-2">
          <code>type</code>
          <span>{props.name}</span>
          <CopyAnchorLink id={props.id} />
        </div>
      </h2>

      <code>{props.annotation}</code>
    </>
  );
}

function CollapsibleSection({ title, defaultOpen = true, children }) {
  const [isOpen, setIsOpen] = useState(defaultOpen);
  return (
    <div className="bg-[var(--ifm-card-background-color)] border border-gray-300 rounded-lg overflow-hidden mb-6">
      <div className="bg-[var(--ifm-card-background-color)] px-6 py-3 border-b border-gray-300 flex flex-row items-center justify-between cursor-pointer hover:bg-[var(--ifm-menu-color-background-hover)] transition-colors duration-150"
        onClick={() => setIsOpen(!isOpen)}>
        <h3 className="text-lg font-semibold text-[var(--docsearch-text-color)] mb-0">{title}</h3>
        <ChevronDown className={`transition-transform duration-200 ${isOpen ? 'rotate-180' : ''}`} />
      </div>
      <div className={`divide-y divide-gray-100 ${isOpen ? '' : 'hidden'}`}>
        {children}
      </div>
    </div>
  );
}

export function PyParameters({ children, title = "Parameters", defaultOpen = false }) {
  return (
    <CollapsibleSection title={title} defaultOpen={defaultOpen}>
      {children}
    </CollapsibleSection>
  );
}

export function PyParameter({ name, annotation, default: defaultValue, description, isOdd = false, showBadge = true }) {
  const isRequired = !defaultValue || defaultValue === '';

  return (
    <div className={`flex flex-col gap-4 px-6 py-4 ${isOdd ? 'bg-[var(--ifm-background-color)]' : 'bg-[var(--ifm-card-background-color)]'} hover:bg-[var(--ifm-menu-color-background-hover)] transition-colors duration-150`}>
      <div className="font-mono text-sm flex flex-row items-center gap-2">
        <span className="text-[var(--docsearch-text-color)] font-semibold m-0 [&_p]:m-0 [&_p]:inline"><Markdown inline>{name}</Markdown></span>

        {showBadge && (isRequired ? (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-50 text-red-700 border border-red-200 dark:bg-red-950 dark:text-red-300 dark:border-red-800">
            REQUIRED
          </span>
        ) : (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600 border border-gray-200">
            Optional
          </span>
        ))}

        {annotation && (
          <span className="text-[var(--docsearch-hit-color)] m-0 [&_p]:m-0 [&_p]:inline">
            (<Markdown inline>{annotation}</Markdown>)
          </span>
        )}
      </div>

      <div className="text-sm text-[var(--docsearch-hit-color)] leading-relaxed">
        {description && (
          <div className="mb-2 [&_p]:m-0 [&_p]:inline">
            <Markdown inline>{description}</Markdown>
          </div>
        )}

        {!isRequired && defaultValue && defaultValue !== 'None' && (
          <div className="text-xs text-gray-500">
            Default: <code className="bg-gray-100 px-1.5 py-0.5 rounded">{defaultValue}</code>
          </div>
        )}
      </div>
    </div>
  );
}

export function PyReturns({ description }) {
  return (
    <p className="[&_p]:m-0 [&_p]:inline">
      <span className="font-semibold">Returns: </span>
      <Markdown inline>{description}</Markdown>
    </p>
  );
}

export function PyClassBase(props) {
  const bases = Array.isArray(props.bases) ? props.bases : [];
  return (
    <div>
      <p>Bases: {bases.map((b, i) => (
        <React.Fragment key={i}>
          {i > 0 && ', '}
          <code className="[&_p]:m-0 [&_p]:inline"><Markdown inline>{b.name}</Markdown></code>
        </React.Fragment>
      ))}</p>
    </div>
  );
}

export function PyAttributesList({ children, title = "Attributes" }) {
  return (
    <CollapsibleSection title={title} defaultOpen={true}>
      {children}
    </CollapsibleSection>
  );
}

export function PyAttribute({ name, type, description, isOdd = false }) {
  return (
    <div className={`flex flex-col lg:grid lg:grid-cols-2 gap-4 px-6 py-4 ${isOdd ? 'bg-[var(--ifm-background-color)]' : 'bg-[var(--ifm-card-background-color)]'} hover:bg-[var(--ifm-menu-color-background-hover)] transition-colors duration-150`}>
      <div className="font-mono text-sm flex flex-row items-baseline gap-1">
        <span className="text-[var(--docsearch-text-color)] font-semibold">{name}:</span>
        {type && (
          <>
            <span className="text-[var(--docsearch-hit-color)] m-0 [&_p]:m-0 [&_p]:inline">
              <Markdown inline>{type}</Markdown>
            </span>
          </>
        )}
      </div>
      {description && (
        <div className="text-sm text-[var(--docsearch-hit-color)] leading-relaxed">
          {description}
        </div>
      )}
    </div>
  );
}

export function PyAttributesTable({ children, title = "Attributes" }) {
  return (
    <div className="border border-gray-200 rounded-lg overflow-hidden bg-[var(--ifm-card-background-color)]">
      <div className="bg-[var(--ifm-card-background-color)] px-6 py-3 border-b border-gray-200">
        <h3 className="text-lg font-semibold text-gray-900">{title}</h3>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full">
          <tbody className="divide-y divide-gray-100">
            {children}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function PyAttributeRow({ name, type, description, index = 0 }) {
  const isOdd = index % 2 === 1;

  return (
    <tr className={`${isOdd ? 'bg-[var(--ifm-background-color)]' : 'bg-[var(--ifm-card-background-color)]'} transition-colors duration-150`}>
      <td className="px-6 py-4 font-mono text-sm w-1/2">
        <span className="font-semibold">
          {name}
        </span>
        {type && (
          <span className="text-gray-700"> ({type})</span>
        )}
      </td>
      <td className="px-6 py-4 text-sm text-gray-700 leading-relaxed">
        {description}
      </td>
    </tr>
  );
}
