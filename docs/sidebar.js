import sdkPages from "./pages/reference/_sidebar.json";

export default {
  documentationSidebar: [
    {
      type: "category",
      label: "Getting Started",
      collapsed: true,
      items: [
        "tutorial/index",
        "tutorial/installation",
        {
          type: "category",
          label: "Quick Start",
          link: {
            type: "doc",
            id: "tutorial/quick-start",
          },
          items: [
            "tutorial/data-branches",
            "tutorial/import",
          ],
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
        "concepts/projects",
        "concepts/models",
        "concepts/pipelines",
        "concepts/tables",
        "concepts/namespaces",
        "concepts/expectations",
        {
          type: "category",
          label: "Git for Data",
          link: {
            type: "doc",
            id: "concepts/git-for-data/index",
          },
          items: [
            "concepts/git-for-data/transactional-pipelines",
            "concepts/git-for-data/commits-refs",
            "concepts/git-for-data/tags",
            "concepts/git-for-data/data-branches",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Agents",
      collapsed: true,
      items: [
        "agents/overview",
        "agents/setup",
      ],
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
        "common-scenarios/sdk-or-cli",
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
            "integrations/orchestrators/temporal",
            "integrations/orchestrators/dagster",
            "integrations/orchestrators/dbos",
            "integrations/orchestrators/prefect",
            "integrations/orchestrators/orchestra",
          ],
        },
        {
          type: "category",
          label: "Interactive notebooks and data apps",
          collapsed: true,
          link: {
            type: "doc",
            id: "integrations/notebooks-data-apps/index",
          },
          items: [
            "integrations/notebooks-data-apps/jupyter-notebooks",
            "integrations/notebooks-data-apps/marimo",
            "integrations/notebooks-data-apps/streamlit",
          ],
        },
        {
          type: "category",
          label: "Warehouses and Lakehouses",
          collapsed: true,
          link: {
            type: "doc",
            id: "integrations/warehouses-lakehouses/index",
          },
          items: [
            "integrations/warehouses-lakehouses/snowflake-inbound",
            "integrations/warehouses-lakehouses/snowflake-outbound",
            "integrations/warehouses-lakehouses/big-query-inbound",
            "integrations/warehouses-lakehouses/big-query-outbound",
            "integrations/warehouses-lakehouses/gcs",
          ],
        },
        {
          type: "category",
          label: "BI tools and Postgres client",
          collapsed: true,
          link: {
            type: "doc",
            id: "integrations/bi-tools-postgres/index",
          },
          items: ["integrations/bi-tools-postgres/metabase"],
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
