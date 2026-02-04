use crate::cli::Cli;

#[derive(Debug, clap::Args)]
#[command(after_long_help = r#"Examples:
  bauplan checkout main                   Checkout existing branch
  bauplan checkout username.dev_branch    Checkout user branch
  bauplan checkout -b username.new_feature --from-ref main  Create and checkout new branch from main
  bauplan checkout -b username.new_feature  Create and checkout from active branch
"#)]
pub(crate) struct CheckoutArgs {
    /// Create a new branch (alias for "branch create")
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Ref from which to create when using --branch. If not specified, default is active branch
    #[arg(long)]
    pub from_ref: Option<String>,
    /// Branch name
    pub branch_name: String,
}

pub(crate) fn handle(_cli: &Cli, _args: CheckoutArgs) -> anyhow::Result<()> {
    todo!()
}
