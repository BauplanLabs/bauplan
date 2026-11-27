const redirects = [
  // Legacy guides redirects
  { from: "/guides/casting", to: "/how-to/import-and-lay-out-tables/how-to-control-types-at-import" },
  {
    from: "/guides/schema_conflicts",
    to: "/how-to/import-and-lay-out-tables/how-to-control-types-at-import",
  },
  {
    from: "/concepts/schema-conflicts",
    to: "/how-to/import-and-lay-out-tables/how-to-control-types-at-import",
  },
  {
    from: "/concepts/semantic_annotations",
    to: "/concepts/semantics",
  },
  { from: "/guides/detached_runs", to: "/how-to/deploy/how-to-run-jobs-detached-and-poll-them" },
  { from: "/guides/import_data", to: "/tutorial/import" },
  { from: "/guides/parameters", to: "/how-to/build-pipelines/how-to-parametrize-a-pipeline" },
  { from: "/guides/partitioning", to: "/concepts/tables#file-layout-and-partitioning" },
  {
    from: "/guides/secrets",
    to: "/how-to/build-pipelines/how-to-parametrize-a-pipeline",
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

  // Sections replaced by the How-to guides
  { from: "/common-scenarios", to: "/learn/install-bauplan" },
  { from: "/common-scenarios/sdk-or-cli", to: "/learn/install-bauplan" },
  { from: "/common-scenarios/detached-runs", to: "/how-to/deploy/how-to-run-jobs-detached-and-poll-them" },
  { from: "/common-scenarios/parameterized-runs", to: "/how-to/build-pipelines/how-to-parametrize-a-pipeline" },
  { from: "/common-scenarios/conflict-management", to: "/how-to/publish/how-to-handle-merge-conflicts" },
  { from: "/common-scenarios/branching-workflows", to: "/how-to/publish/how-to-write-audit-publish" },
  { from: "/common-scenarios/multi-stage-pipelines", to: "/how-to/build-pipelines/how-to-structure-a-pipeline" },
  { from: "/common-scenarios/schema-conflicts", to: "/how-to/import-and-lay-out-tables/how-to-control-types-at-import" },
  { from: "/common-scenarios/partitioning", to: "/how-to/import-and-lay-out-tables/how-to-partition-tables-at-import-time" },
  { from: "/common-scenarios/sql-files", to: "/how-to/organize-code/how-to-organize-sql-files" },
  { from: "/concepts/projects", to: "/how-to/build-pipelines/how-to-set-up-a-project" },
  { from: "/concepts/models", to: "/how-to/build-pipelines/how-to-write-a-pipeline" },
  { from: "/concepts/expectations", to: "/how-to/build-pipelines/how-to-add-expectations-to-a-pipeline" },

  // Legacy tutorial paths
  { from: "/tutorial/03_pipelines", to: "/concepts/pipelines" },
  { from: "/tutorial/04_import", to: "/tutorial/import" },
  { from: "/tutorial/pipelines", to: "/concepts/pipelines" },

  // Legacy git_for_data paths
  { from: "/concepts/git_for_data/walkthrough", to: "/concepts/catalog-and-branches" },

  { from: "/concepts/namespaces", to: "/concepts/tables#namespaces" },

  // Git for Data pages replaced by the Catalog and branches page
  { from: "/concepts/git-for-data", to: "/concepts/catalog-and-branches" },
  {
    from: "/concepts/git-for-data/tags",
    to: "/concepts/catalog-and-branches#commits-tags-and-time-travel",
  },
  {
    from: "/concepts/git-for-data/transactional-pipelines",
    to: "/concepts/catalog-and-branches#transactional-branches",
  },
  {
    from: "/concepts/git-for-data/commits-refs",
    to: "/concepts/catalog-and-branches#commits-tags-and-time-travel",
  },
  {
    from: "/concepts/git-for-data/data-branches",
    to: "/concepts/catalog-and-branches#data-branches",
  },

  // Semantic annotations page replaced by the Semantic layer page
  { from: "/concepts/semantic-annotations", to: "/concepts/semantics" },

  // Underscore-to-dash redirects (old URLs -> new URLs)
  { from: "/tutorial/quick_start", to: "/tutorial/quick-start" },
  { from: "/tutorial/data_branches", to: "/tutorial/data-branches" },
  { from: "/tutorial/s3_permissions", to: "/tutorial/s3-permissions" },
  {
    from: "/concepts/schema_conflicts",
    to: "/how-to/import-and-lay-out-tables/how-to-control-types-at-import",
  },
  { from: "/concepts/git_for_data", to: "/concepts/catalog-and-branches" },
  {
    from: "/concepts/git_for_data/transactional_pipelines",
    to: "/concepts/catalog-and-branches#transactional-branches",
  },
  {
    from: "/concepts/git_for_data/commits_refs",
    to: "/concepts/catalog-and-branches#commits-tags-and-time-travel",
  },
  {
    from: "/concepts/git_for_data/data_branches",
    to: "/concepts/catalog-and-branches#data-branches",
  },
  {
    from: "/concepts/git_for_data/tags",
    to: "/concepts/catalog-and-branches#commits-tags-and-time-travel",
  },
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
    to: "/integrations/warehouses-lakehouses",
  },
  {
    from: "/integrations/warehouses-lakehouses/gcs",
    to: "/integrations/warehouses-lakehouses",
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
    to: "/integrations/bi-tools",
  },
  {
    from: "/integrations/bi-tools-postgres",
    to: "/integrations/bi-tools",
  },
  {
    from: "/integrations/bi_tools_postgres/metabase",
    to: "/integrations/bi-tools/metabase",
  },
  {
    from: "/integrations/bi-tools-postgres/metabase",
    to: "/integrations/bi-tools/metabase",
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
