import { themes as prismThemes } from 'prism-react-renderer';

export default {
  clientModules: [require.resolve('./src/clientModules/scrollToAnchor.js')],
  title: "Bauplan Documentation",
  url: process.env.DOCS_URL || "https://docs.bauplanlabs.com",
  baseUrl: "/",
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        gtag: process.env.GTAG_ID ? {
          trackingID: process.env.GTAG_ID,
          anonymizeIP: true,
        } : false,
        docs: {
          routeBasePath: "/", // The docs are served at the root.
          path: "pages", // The local path to the markdown files.
          exclude: ["node_modules/**"],
          sidebarPath: require.resolve("./sidebar.js"),
        },
        theme: {
          customCss: ["./src/css/global.css"],
        },
        blog: false,
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
        href: 'https://www.linkedin.com/company/bauplanlabs/',
        src: '/img/icons/linkedin.avif',
        alt: 'LinkedIn',
        title: 'LinkedIn',
        invertInLight: true,
      },
      {
        href: 'https://github.com/BauplanLabs',
        src: '/img/icons/github.avif',
        alt: 'GitHub',
        title: 'GitHub',
        invertInLight: true,
      },
      {
        href: 'https://www.youtube.com/@bauplan_labs',
        src: '/img/icons/youtube.avif',
        alt: 'YouTube',
        title: 'YouTube',
        invertInLight: true,
      },
    ],
  },
  themeConfig: {
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
          type: "docSidebar",
          sidebarId: "buildWithLLMsSidebar",
          position: "left",
          label: "Build with LLMs",
        },
        {
          type: "docSidebar",
          sidebarId: "examplesSidebar",
          position: "left",
          label: "Examples",
        },
        {
          type: "docSidebar",
          sidebarId: "integrationsSidebar",
          position: "left",
          label: "Integrations",
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
      appId: 'X4Z23JL83Y',
      apiKey: 'b460e7a7104d43707f168e267aa60788',
      indexName: 'Bauplan Docs Website',
      insights: true,
    },
    prism: {
      additionalLanguages: ['bash'],
      theme: prismThemes.vsDark,
    },
    mermaid: {
      theme: {
        light: 'base',
        dark: 'dark',
      },
      options: {
        themeVariables: {
          fontFamily: 'IBMPlexMono, monospace',
        },
      },
    },
  },
  plugins: [
    require.resolve("./src/plugins/tailwind-config.js"),
  ],

  future: {
    v4: true, // Improve compatibility with the upcoming Docusaurus v4
    experimental_faster: true, // Use rust
  },
};
