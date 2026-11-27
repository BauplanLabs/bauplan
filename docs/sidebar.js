import sdkPages from "./pages/reference/_sidebar.json";

export default {
  documentationSidebar: [
    "what-bauplan-is-not",
    {
      type: "category",
      label: "Getting Started",
      collapsed: true,
      items: [
        "tutorial/installation",
        "tutorial/connect-your-agent",
        {
          type: "category",
          label: "Quick Start",
          link: {
            type: "doc",
            id: "tutorial/quick-start",
          },
          items: ["tutorial/data-branches", "tutorial/import"],
        },
      ],
    },
    {
      type: "category",
      label: "Platform Overview",
      collapsed: true,
      link: {
        type: "doc",
        id: "overview/index",
      },
      items: [
        "overview/execution-model",
        "overview/architecture",
        "overview/deployment",
      ],
    },
    {
      type: "category",
      label: "Core Concepts",
      collapsed: true,
      items: [
        "concepts/catalog-and-branches",
        "concepts/tables",
        "concepts/semantics",
        "concepts/pipelines",
      ],
    },
    {
      type: "category",
      label: "Building with Bauplan",
      collapsed: true,
      // Kept until the new pages cover what these explain
      items: ["concepts/projects", "concepts/models", "concepts/expectations"],
    },
    {
      type: "category",
      label: "Common Scenarios",
      collapsed: true,
      // link: {
      //   type: "doc",
      //   id: "common-scenarios/index",
      // },
      items: [
        "common-scenarios/multi-stage-pipelines",
        "common-scenarios/schema-conflicts",
        "common-scenarios/branching-workflows",
        "common-scenarios/detached-runs",
        "common-scenarios/parameterized-runs",
        "common-scenarios/conflict-management",
        "common-scenarios/sql-files",
        "common-scenarios/sdk-or-cli",
        "common-scenarios/partitioning",
      ],
    },
    {
      type: "category",
      label: "Integrations",
      link: {
        type: "doc",
        id: "integrations/index",
      },
      collapsed: true,
      items: [
        {
          type: "category",
          label: "Orchestrators",
          collapsed: true,
          link: {
            type: "doc",
            id: "integrations/orchestrators/index",
          },
          items: [
            "integrations/orchestrators/airflow",
            "integrations/orchestrators/step-functions",
            "integrations/orchestrators/dagster",
            "integrations/orchestrators/dbos",
            "integrations/orchestrators/kestra",
            "integrations/orchestrators/orchestra",
            "integrations/orchestrators/prefect",
            "integrations/orchestrators/temporal",
          ],
        },
        {
          type: "category",
          label: "Storage and query engines",
          collapsed: true,
          items: [
            "integrations/storage-and-query-engines/query-bauplan-tables-from-snowflake",
            "integrations/storage-and-query-engines/query-bauplan-tables-from-bigquery",
            "integrations/storage-and-query-engines/import-data-from-cloudflare-r2",
          ],
        },
        {
          type: "category",
          label: "BI tools",
          collapsed: true,
          link: {
            type: "doc",
            id: "integrations/bi-tools/index",
          },
          items: [
            "integrations/bi-tools/metabase",
            "integrations/bi-tools/textql",
          ],
        },
        {
          type: "category",
          label: "Data Integration and ELT Tools",
          collapsed: true,
          link: {
            type: "doc",
            id: "integrations/data-int-and-etl/index",
          },
          items: [
            "integrations/data-int-and-etl/fivetran",
            "integrations/data-int-and-etl/estuary",
          ],
        },
        {
          type: "doc",
          id: "integrations/dbt/index",
          label: "DBT",
          className: "sidebar-standalone-header",
        },
      ],
    },
    "faq",
  ],
  referenceSidebar: [
    "reference/cli",
    {
      type: "category",
      label: "SDK",
      collapsed: false,
      items: sdkPages,
    },
  ],
};
