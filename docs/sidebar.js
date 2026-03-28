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
            id: "tutorial/quick_start",
          },
          items: [
            "tutorial/data_branches",
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
        "overview/execution_model",
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
        "concepts/schema_conflicts",
        {
          type: "category",
          label: "Git for Data",
          link: {
            type: "doc",
            id: "concepts/git_for_data/index",
          },
          items: [
            "concepts/git_for_data/transactional_pipelines",
            "concepts/git_for_data/commits_refs",
            "concepts/git_for_data/tags",
            "concepts/git_for_data/data_branches",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Agents",
      collapsed: true,
      link: {
        type: "doc",
        id: "agents/index",
      },
      items: [
        "agents/setup",
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
            id: "integrations/notebooks_data_apps/index",
          },
          items: [
            "integrations/notebooks_data_apps/jupyter_notebooks",
            "integrations/notebooks_data_apps/marimo",
            "integrations/notebooks_data_apps/streamlit",
          ],
        },
        {
          type: "category",
          label: "Warehouses and Lakehouses",
          collapsed: true,
          link: {
            type: "doc",
            id: "integrations/warehouses_lakehouses/index",
          },
          items: [
            "integrations/warehouses_lakehouses/snowflake_inbound",
            "integrations/warehouses_lakehouses/snowflake_outbound",
            "integrations/warehouses_lakehouses/big_query_inbound",
            "integrations/warehouses_lakehouses/big_query_outbound",
            "integrations/warehouses_lakehouses/gcs",
          ],
        },
        {
          type: "category",
          label: "BI tools and Postgres client",
          collapsed: true,
          link: {
            type: "doc",
            id: "integrations/bi_tools_postgres/index",
          },
          items: ["integrations/bi_tools_postgres/metabase"],
        },
        {
          type: "category",
          label: "Data Integration and ELT Tools",
          collapsed: true,
          link: {
            type: "doc",
            id: "integrations/data_int_and_etl/index",
          },
          items: [
            "integrations/data_int_and_etl/fivetran",
            "integrations/data_int_and_etl/estuary",
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
