import { themes as prismThemes } from "prism-react-renderer";
import remarkHiddenLines from "./src/plugins/remark-hidden-lines.js";
import redirects from "./redirects.js";

const siteUrl = process.env.DOCS_URL || "https://docs.bauplanlabs.com";

export default {
  clientModules: [
    require.resolve("./src/clientModules/scrollToAnchor.js"),
    require.resolve("./src/clientModules/tabTocSync.js"),
  ],
  title: "Bauplan Documentation",
  tagline:
    "Version, build, and ship data pipelines like code, on a serverless Apache Iceberg lakehouse.",
  url: siteUrl,
  baseUrl: "/",
  // Site-wide structured data. Per-page TechArticle is added in
  // src/theme/DocItem/Layout; BreadcrumbList is emitted by the theme already.
  headTags: [
    {
      tagName: "script",
      attributes: { type: "application/ld+json" },
      innerHTML: JSON.stringify({
        "@context": "https://schema.org",
        "@type": "Organization",
        name: "Bauplan",
        url: "https://www.bauplanlabs.com",
        logo: `${siteUrl}/img/bauplan_nav_logo.png`,
        sameAs: [
          "https://github.com/BauplanLabs",
          "https://www.linkedin.com/company/bauplanlabs/",
          "https://www.youtube.com/@bauplan_labs",
        ],
      }),
    },
    {
      tagName: "script",
      attributes: { type: "application/ld+json" },
      innerHTML: JSON.stringify({
        "@context": "https://schema.org",
        "@type": "WebSite",
        name: "Bauplan Documentation",
        url: siteUrl,
        potentialAction: {
          "@type": "SearchAction",
          target: {
            "@type": "EntryPoint",
            urlTemplate: `${siteUrl}/search?q={search_term_string}`,
          },
          "query-input": "required name=search_term_string",
        },
      }),
    },
  ],
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        gtag: process.env.GTAG_ID
          ? {
              trackingID: process.env.GTAG_ID,
              anonymizeIP: true,
            }
          : false,
        docs: {
          routeBasePath: "/", // The docs are served at the root.
          path: "pages", // The local path to the markdown files.
          exclude: ["node_modules/**"],
          remarkPlugins: [remarkHiddenLines],
          sidebarPath: require.resolve("./sidebar.js"),
        },
        theme: {
          customCss: ["./src/css/global.css"],
        },
        blog: false,
        sitemap: {
          lastmod: "date", // emit <lastmod> so crawlers re-fetch changed pages
          changefreq: "weekly",
          priority: 0.5,
          // Exclude the /search results page.
          ignorePatterns: ["/search"],
          filename: "sitemap.xml",
        },
      },
    ],
  ],
  markdown: {
    mermaid: true,
  },

  themes: ["@docusaurus/theme-mermaid"],
  customFields: {
    socials: [
      {
        href: "https://www.linkedin.com/company/bauplanlabs/",
        src: "/img/icons/linkedin.avif",
        alt: "LinkedIn",
        title: "LinkedIn",
        invertInLight: true,
      },
      {
        href: "https://github.com/BauplanLabs",
        src: "/img/icons/github.avif",
        alt: "GitHub",
        title: "GitHub",
        invertInLight: true,
      },
      {
        href: "https://www.youtube.com/@bauplan_labs",
        src: "/img/icons/youtube.avif",
        alt: "YouTube",
        title: "YouTube",
        invertInLight: true,
      },
    ],
  },
  themeConfig: {
    // SEO: add `image: "img/social-card.png"` here once a 1200x630 social card
    // exists - it drives og:image + twitter:image. twitter:card (summary_large_image)
    // is emitted by the theme automatically.
    metadata: [
      {
        name: "keywords",
        content:
          "data lakehouse, git for data, apache iceberg, data pipelines, python, sql, serverless data platform, data version control",
      },
      { property: "og:type", content: "website" },
      { property: "og:site_name", content: "Bauplan Documentation" },
    ],
    navbar: {
      logo: {
        src: "img/bauplan_nav_logo.png",
        srcDark: "img/bauplan_logo.png",
        href: "/",
      },
      items: [
        {
          type: "docSidebar",
          sidebarId: "documentationSidebar",
          position: "left",
          label: "Documentation",
        },
        {
          type: "docSidebar",
          sidebarId: "referenceSidebar",
          position: "left",
          label: "API Reference",
        },
        {
          href: "https://github.com/BauplanLabs/bauplan/tree/main/examples",
          target: "_blank",
          position: "left",
          label: "Examples",
        },
      ],
    },
    footer: {
      logo: {
        alt: "Bauplan Labs",
        src: "img/bauplan_nav_logo.png",
        srcDark: "img/bauplan_logo.png",
        href: "https://bauplanlabs.com",
      },
      copyright: `Copyright © ${new Date().getFullYear()} Bauplan Inc.<br/> All rights reserved.`,
    },
    algolia: {
      appId: "X4Z23JL83Y",
      apiKey: "b460e7a7104d43707f168e267aa60788",
      indexName: "Bauplan Docs Website",
      insights: true,
    },
    prism: {
      additionalLanguages: ["bash", "shell-session"],
      theme: prismThemes.vsDark,
    },
    mermaid: {
      theme: {
        light: "base",
        dark: "dark",
      },
      options: {
        themeVariables: {
          fontFamily: "IBMPlexMono, monospace",
        },
      },
    },
  },
  plugins: [
    require.resolve("./src/plugins/tailwind-config.js"),
    ["@docusaurus/plugin-client-redirects", { redirects }],
  ],

  future: {
    v4: true, // Improve compatibility with the upcoming Docusaurus v4
    experimental_faster: true, // Use rust
  },
};
