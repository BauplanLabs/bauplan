import React from "react";
import Head from "@docusaurus/Head";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { useDoc } from "@docusaurus/plugin-content-docs/client";
import Layout from "@theme-original/DocItem/Layout";

// Emit TechArticle JSON-LD per doc page. BreadcrumbList is already emitted by
// the theme's DocBreadcrumbs; Organization + WebSite are emitted site-wide via
// headTags in docusaurus.config.js. We wrap the original Layout so future
// Docusaurus changes to it are inherited automatically.
export default function DocItemLayout(props) {
  const { siteConfig } = useDocusaurusContext();
  const { metadata } = useDoc();
  const { title, description, permalink, lastUpdatedAt } = metadata;

  const jsonLd = {
    "@context": "https://schema.org",
    "@type": "TechArticle",
    headline: title,
    ...(description && { description }),
    url: siteConfig.url + permalink,
    inLanguage: "en",
    isPartOf: {
      "@type": "WebSite",
      name: siteConfig.title,
      url: siteConfig.url,
    },
    author: {
      "@type": "Organization",
      name: "Bauplan",
      url: "https://www.bauplanlabs.com",
    },
    publisher: {
      "@type": "Organization",
      name: "Bauplan",
      logo: {
        "@type": "ImageObject",
        url: `${siteConfig.url}/img/bauplan_nav_logo.png`,
      },
    },
    // lastUpdatedAt is a Unix timestamp (seconds); only present when the docs
    // plugin computes it. Emit dateModified as YYYY-MM-DD when available.
    ...(lastUpdatedAt && {
      dateModified: new Date(lastUpdatedAt * 1000).toISOString().split("T")[0],
    }),
  };

  return (
    <>
      <Head>
        <script type="application/ld+json">{JSON.stringify(jsonLd)}</script>
      </Head>
      <Layout {...props} />
    </>
  );
}
