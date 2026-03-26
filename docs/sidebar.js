import sdkPages from "./pages/reference/_sidebar.json";

export default {
  documentationSidebar: [
    {
      type: "category",
      label: "Getting Started",
      collapsed: false,
      items: [
        "tutorial/index",
        "tutorial/installation",
        "tutorial/quick_start",
        "tutorial/data_branches",
        "tutorial/pipelines",
        "tutorial/import",
      ],
    },
    {
      type: "category",
      label: "Overview",
      collapsed: false,
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
      collapsed: false,
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
            id: "concepts/git_for_data/index",
          },
          items: [
            "concepts/git_for_data/transactional_pipelines",
            "concepts/git_for_data/commits_refs",
            "concepts/git_for_data/tags",
            "concepts/git_for_data/data_branches",
            "concepts/git_for_data/walkthrough",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Guides",
      collapsed: false,
      items: [
        "guides/import_data",
        "guides/schema_conflicts",
        "guides/casting",
        "guides/secrets",
        "guides/parameters",
        "guides/partitioning",
        "guides/detached_runs",
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
  buildWithLLMsSidebar: [
    "llms/index",
    "llms/quick_start",
    "llms/explore_data",
    {
      type: "category",
      label: "Build and run a pipeline",
      link: {
        type: "doc",
        id: "llms/tutorial/index",
      },
      collapsed: false,
      items: [
        "llms/tutorial/create",
        "llms/tutorial/test",
        "llms/tutorial/publish",
      ],
    },
    "llms/mcp_server",
  ],
  examplesSidebar: [
    {
      type: "category",
      label: "Examples",
      link: {
        type: "doc",
        id: "examples/index",
      },
      collapsed: false,
      items: [
        "examples/data_product",
        "examples/rag",
        "examples/medallion",
        "examples/llm_tabular",
        "examples/mongo",
        "examples/import_data",
        "examples/pdf_analysis_openai",
        "examples/llm",
        "examples/expectations",
        "examples/nrt",
        "examples/data_app",
      ],
    },
  ],
  integrationsSidebar: [
    {
      type: "category",
      label: "Integrations",
      link: {
        type: "doc",
        id: "integrations/index",
      },
      collapsed: false,
      items: [
        {
          type: "category",
          label: "Orchestrators",
          collapsed: false,
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
          collapsed: false,
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
          collapsed: false,
          link: {
            type: "doc",
            id: "integrations/warehouses_lakehouses/index",
          },
          items: [
            {
              type: "category",
              label: "Snowflake",
              link: {
                type: "doc",
                id: "integrations/warehouses_lakehouses/snowflake/index",
              },
              items: [
                "integrations/warehouses_lakehouses/snowflake/snowflake_inbound",
                "integrations/warehouses_lakehouses/snowflake/snowflake_outbound",
              ],
            },
            {
              type: "category",
              label: "Big Query",
              link: {
                type: "doc",
                id: "integrations/warehouses_lakehouses/big_query/index",
              },
              items: [
                "integrations/warehouses_lakehouses/big_query/big_query_inbound",
                "integrations/warehouses_lakehouses/big_query/big_query_outbound",
              ],
            },
            "integrations/warehouses_lakehouses/gcs",
          ],
        },
        {
          type: "category",
          label: "BI tools and Postgres client",
          collapsed: false,
          link: {
            type: "doc",
            id: "integrations/bi_tools_postgres/index",
          },
          items: ["integrations/bi_tools_postgres/metabase"],
        },
        {
          type: "category",
          label: "Data Integration and ELT Tools",
          collapsed: false,
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
  ],
};
