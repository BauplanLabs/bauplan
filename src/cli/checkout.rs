use crate::cli::Cli;

#[derive(Debug, clap::Args)]
pub(crate) struct CheckoutArgs {
    /// Branch name
    pub branch_name: String,
    /// Create a new branch (alias for "branch create")
    #[arg(short, long)]
    pub branch: Option<String>,
    /// Ref from which to create when using --branch. If not specified, default is active branch
    #[arg(long)]
    pub from_ref: Option<String>,
}

pub(crate) fn handle(_cli: &Cli, _args: CheckoutArgs) -> anyhow::Result<()> {
    todo!()
}
