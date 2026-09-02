const redirects = [
  // Legacy guides redirects
  { from: "/guides/casting", to: "/common-scenarios/schema-conflicts" },
  {
    from: "/guides/schema_conflicts",
    to: "/common-scenarios/schema-conflicts",
  },
  {
    from: "/concepts/schema-conflicts",
    to: "/common-scenarios/schema-conflicts",
  },
  {
    from: "/concepts/semantic_annotations",
    to: "/concepts/semantic-annotations",
  },
  { from: "/guides/detached_runs", to: "/common-scenarios/detached-runs" },
  { from: "/guides/import_data", to: "/tutorial/import" },
  { from: "/guides/parameters", to: "/common-scenarios/parameterized-runs" },
  { from: "/guides/partitioning", to: "/concepts/tables#partitioning" },
  {
    from: "/guides/secrets",
    to: "/common-scenarios/parameterized-runs#secrets",
  },

  // The parallel /agents tree and its older /llms URLs both collapse into the
  // single setup page. Each entry also produces the .md stub agents follow.
  { from: "/llms", to: "/tutorial/connect-your-agent" },
  { from: "/llms/quick_start", to: "/tutorial/connect-your-agent" },
  {
    from: "/llms/explore_data",
    to: "/tutorial/connect-your-agent#available-skills",
  },
  {
    from: "/llms/mcp_server",
    to: "/tutorial/connect-your-agent#mcp-server",
  },
  { from: "/agents", to: "/tutorial/connect-your-agent" },
  { from: "/llms/tutorial", to: "/tutorial/connect-your-agent#skills" },
  {
    from: "/llms/tutorial/create",
    to: "/tutorial/connect-your-agent#skills",
  },
  {
    from: "/llms/tutorial/publish",
    to: "/tutorial/connect-your-agent#skills",
  },
  { from: "/llms/tutorial/test", to: "/tutorial/connect-your-agent#skills" },
  { from: "/agents/overview", to: "/tutorial/connect-your-agent" },
  { from: "/agents/skills", to: "/tutorial/connect-your-agent#skills" },
  { from: "/agents/mcp", to: "/tutorial/connect-your-agent#mcp-server" },
  {
    from: "/agents/context",
    to: "/tutorial/connect-your-agent#docs-and-repository-context",
  },

  // Legacy nested warehouse paths
  {
    from: "/integrations/warehouses_lakehouses/snowflake/snowflake_inbound",
    to: "/integrations/warehouses-lakehouses/snowflake-inbound",
  },
  {
    from: "/integrations/warehouses_lakehouses/snowflake/snowflake_outbound",
    to: "/integrations/warehouses-lakehouses/snowflake-outbound",
  },
  {
    from: "/integrations/warehouses_lakehouses/big_query/big_query_inbound",
    to: "/integrations/warehouses-lakehouses/big-query-inbound",
  },
  {
    from: "/integrations/warehouses_lakehouses/big_query/big_query_outbound",
    to: "/integrations/warehouses-lakehouses/big-query-outbound",
  },
  {
    from: "/integrations/warehouses_lakehouses/snowflake",
    to: "/integrations/warehouses-lakehouses/snowflake-inbound",
  },
  {
    from: "/integrations/warehouses_lakehouses/big_query",
    to: "/integrations/warehouses-lakehouses/big-query-inbound",
  },

  // Legacy tutorial paths
  { from: "/tutorial/03_pipelines", to: "/concepts/pipelines" },
  { from: "/tutorial/04_import", to: "/tutorial/import" },
  { from: "/tutorial/pipelines", to: "/concepts/pipelines" },

  // Legacy git_for_data paths
  { from: "/concepts/git_for_data/walkthrough", to: "/concepts/git-for-data" },

  // Underscore-to-dash redirects (old URLs -> new URLs)
  { from: "/tutorial/quick_start", to: "/tutorial/quick-start" },
  { from: "/tutorial/data_branches", to: "/tutorial/data-branches" },
  { from: "/tutorial/s3_permissions", to: "/tutorial/s3-permissions" },
  { from: "/overview/execution_model", to: "/overview/execution-model" },
  {
    from: "/concepts/schema_conflicts",
    to: "/common-scenarios/schema-conflicts",
  },
  { from: "/concepts/git_for_data", to: "/concepts/git-for-data" },
  {
    from: "/concepts/git_for_data/transactional_pipelines",
    to: "/concepts/git-for-data/transactional-pipelines",
  },
  {
    from: "/concepts/git_for_data/commits_refs",
    to: "/concepts/git-for-data/commits-refs",
  },
  {
    from: "/concepts/git_for_data/data_branches",
    to: "/concepts/git-for-data/data-branches",
  },
  { from: "/concepts/git_for_data/tags", to: "/concepts/git-for-data/tags" },
  {
    from: "/integrations/warehouses_lakehouses",
    to: "/integrations/warehouses-lakehouses",
  },
  {
    from: "/integrations/warehouses_lakehouses/snowflake_inbound",
    to: "/integrations/warehouses-lakehouses/snowflake-inbound",
  },
  {
    from: "/integrations/warehouses_lakehouses/snowflake_outbound",
    to: "/integrations/warehouses-lakehouses/snowflake-outbound",
  },
  {
    from: "/integrations/warehouses_lakehouses/big_query_inbound",
    to: "/integrations/warehouses-lakehouses/big-query-inbound",
  },
  {
    from: "/integrations/warehouses_lakehouses/big_query_outbound",
    to: "/integrations/warehouses-lakehouses/big-query-outbound",
  },
  {
    from: "/integrations/warehouses_lakehouses/gcs",
    to: "/integrations/warehouses-lakehouses/gcs",
  },
  {
    from: "/integrations/notebooks_data_apps",
    to: "/integrations/notebooks-data-apps",
  },
  {
    from: "/integrations/notebooks_data_apps/jupyter_notebooks",
    to: "/integrations/notebooks-data-apps/jupyter-notebooks",
  },
  {
    from: "/integrations/notebooks_data_apps/marimo",
    to: "/integrations/notebooks-data-apps/marimo",
  },
  {
    from: "/integrations/notebooks_data_apps/streamlit",
    to: "/integrations/notebooks-data-apps/streamlit",
  },
  {
    from: "/integrations/bi_tools_postgres",
    to: "/integrations/bi-tools-postgres",
  },
  {
    from: "/integrations/bi_tools_postgres/metabase",
    to: "/integrations/bi-tools-postgres/metabase",
  },
  {
    from: "/integrations/data_int_and_etl",
    to: "/integrations/data-int-and-etl",
  },
  {
    from: "/integrations/data_int_and_etl/fivetran",
    to: "/integrations/data-int-and-etl/fivetran",
  },
  {
    from: "/integrations/data_int_and_etl/estuary",
    to: "/integrations/data-int-and-etl/estuary",
  },
  {
    from: "/reference/bauplan_exceptions",
    to: "/reference/bauplan-exceptions",
  },
  { from: "/reference/bauplan_schema", to: "/reference/bauplan-schema" },
  {
    from: "/reference/bauplan_standard_expectations",
    to: "/reference/bauplan-standard-expectations",
  },
  { from: "/reference/bauplan_state", to: "/reference/bauplan-state" },
];

export default redirects;
