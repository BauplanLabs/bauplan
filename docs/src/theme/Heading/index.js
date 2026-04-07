import React, { useState } from "react";
import clsx from "clsx";
import { translate } from "@docusaurus/Translate";
import Link from "@docusaurus/Link";
import { useAnchorTargetClassName } from "@docusaurus/theme-common";
import useBrokenLinks from "@docusaurus/useBrokenLinks";
import { Check, Link as LinkIcon } from "lucide-react";

export default function Heading({ as: As, id, ...props }) {
  const brokenLinks = useBrokenLinks();
  const anchorTargetClassName = useAnchorTargetClassName(id);
  const [copied, setCopied] = useState(false);

  if (As === "h1" || !id) {
    return <As {...props} id={undefined} />;
  }

  brokenLinks.collectAnchor(id);

  const anchorTitle = translate(
    {
      id: "theme.common.headingLinkTitle",
      message: "Direct link to {heading}",
      description: "Title for link to heading",
    },
    {
      heading: typeof props.children === "string" ? props.children : id,
    }
  );

  const handleCopy = (e) => {
    e.preventDefault();
    const url = `${window.location.origin}${window.location.pathname}#${id}`;
    navigator.clipboard.writeText(url).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <As
      {...props}
      className={clsx("anchor", anchorTargetClassName, "group", props.className)}
      id={id}
    >
      {props.children}
      <Link
        className="hash-link ml-2 inline-flex align-middle opacity-0 transition-opacity duration-150 text-gray-400 hover:text-gray-600 no-underline group-hover:opacity-100 focus:opacity-100"
        to={`#${id}`}
        onClick={handleCopy}
        aria-label={anchorTitle}
        title={copied ? "Copied!" : anchorTitle}
        translate="no"
      >
        {copied ? <Check size={16} /> : <LinkIcon size={16} />}
      </Link>
    </As>
  );
}
