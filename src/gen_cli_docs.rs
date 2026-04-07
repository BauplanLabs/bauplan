#![allow(dead_code)]

mod cli;

use anyhow::{Result, anyhow};
use clap::{ArgAction, Args, CommandFactory};
use serde::Serialize;
use std::collections::HashSet;
use tera::{Context, Tera};
use textwrap::dedent;

const TEMPLATE: &str = include_str!("gen_cli_docs.tera");

#[derive(Serialize)]
struct OptionDoc {
    long: Option<String>,
    short: Option<String>,
    value_name: Option<String>,
    default: Option<String>,
    repeatable: bool,
    description: String,
}

impl TryFrom<&clap::Arg> for OptionDoc {
    type Error = anyhow::Error;

    fn try_from(arg: &clap::Arg) -> Result<Self> {
        if arg.is_positional() {
            return Err(anyhow!(
                "expected option arg, got positional {}",
                arg.get_id()
            ));
        }

        Ok(Self {
            long: arg.get_long().map(|l| format!("--{l}")),
            short: arg.get_short().map(|s| format!("-{s}")),
            value_name: arg
                .get_action()
                .takes_values()
                .then(|| default_value_name(arg)),
            default: arg
                .get_default_values()
                .first()
                .map(|v| v.to_string_lossy().to_string()),
            repeatable: matches!(arg.get_action(), ArgAction::Append | ArgAction::Count),
            description: description(arg),
        })
    }
}

#[derive(Serialize)]
struct ArgumentDoc {
    name: String,
    required: bool,
    description: String,
}

impl TryFrom<&clap::Arg> for ArgumentDoc {
    type Error = anyhow::Error;

    fn try_from(arg: &clap::Arg) -> Result<Self> {
        if !arg.is_positional() {
            return Err(anyhow!(
                "expected positional arg, got option {}",
                arg.get_id()
            ));
        }

        Ok(Self {
            name: default_value_name(arg),
            required: arg.is_required_set(),
            description: description(arg),
        })
    }
}

#[derive(Serialize)]
struct SubcommandLink {
    name: String,
    id: String,
    summary: String,
}

impl SubcommandLink {
    fn from_subcommand(parent_path: &[String], sub: &clap::Command) -> Self {
        let mut sub_path = parent_path.to_vec();
        sub_path.push(sub.get_name().to_string());
        Self {
            name: sub.get_name().to_string(),
            id: sub_path.join("-"),
            summary: sub
                .get_about()
                .map(|a| a.to_string().lines().next().unwrap_or("").to_string())
                .unwrap_or_default(),
        }
    }
}

#[derive(Serialize)]
struct CommandDoc {
    id: String,
    path_parts: Vec<String>,
    depth: usize,
    about: String,
    aliases: Vec<String>,
    subcommands: Vec<SubcommandLink>,
    positional: Vec<ArgumentDoc>,
    options: Vec<OptionDoc>,
    examples: Option<String>,
}

/// Skip clap built-in args (`--help`, `--version`).
fn is_bauplan_arg(arg: &clap::Arg) -> bool {
    arg.get_id() != "help" && arg.get_id() != "version"
}

/// Skip clap built-in subcommands (`help`).
fn is_bauplan_cmd(cmd: &&clap::Command) -> bool {
    cmd.get_name() != "help"
}

/// The placeholder name for an arg's value (e.g. `BRANCH_NAME` from `--branch <BRANCH_NAME>`).
/// Falls back to the uppercased arg id if clap doesn't have one set.
fn default_value_name(arg: &clap::Arg) -> String {
    arg.get_value_names()
        .and_then(|n| n.first().map(|s| s.to_string()))
        .unwrap_or_else(|| arg.get_id().to_string().to_uppercase())
}

fn description(arg: &clap::Arg) -> String {
    arg.get_help()
        .map(|h| h.to_string().replace('\n', " "))
        .unwrap_or_default()
}

fn collect_command(
    cmd: &clap::Command,
    path: &[String],
    depth: usize,
    global_ids: &HashSet<String>,
    out: &mut Vec<CommandDoc>,
) -> Result<()> {
    let cmd_flags: Vec<&clap::Arg> = cmd
        .get_arguments()
        .filter(|arg| is_bauplan_arg(arg) && !global_ids.contains(arg.get_id().as_str()))
        .collect();
    let (cmd_args, option_args): (Vec<&clap::Arg>, Vec<&clap::Arg>) =
        cmd_flags.into_iter().partition(|a| a.is_positional());

    let subcommands: Vec<SubcommandLink> = cmd
        .get_subcommands()
        .filter(is_bauplan_cmd)
        .map(|sub| SubcommandLink::from_subcommand(path, sub))
        .collect();

    let positional = cmd_args
        .into_iter()
        .map(ArgumentDoc::try_from)
        .collect::<Result<Vec<_>>>()?;
    let options = option_args
        .into_iter()
        .map(OptionDoc::try_from)
        .collect::<Result<Vec<_>>>()?;

    let command_aliases = cmd.get_all_aliases().map(str::to_string).collect::<Vec<_>>();

    out.push(CommandDoc {
        id: path.join("-"),
        path_parts: path.to_vec(),
        depth,
        about: cmd.get_about().map(|a| a.to_string()).unwrap_or_default(),
        aliases: command_aliases,
        subcommands,
        positional,
        options,
        examples: cmd
            .get_after_long_help()
            .map(|help| {
                let raw = help.to_string();
                let trimmed = raw.trim_end();
                let stripped = trimmed.strip_prefix("Examples\n").unwrap_or(trimmed);
                dedent(stripped)
            })
            .filter(|examples| !examples.is_empty()),
    });

    for sub in cmd.get_subcommands().filter(is_bauplan_cmd) {
        let mut sub_path = path.to_vec();
        sub_path.push(sub.get_name().to_string());
        collect_command(sub, &sub_path, depth + 1, global_ids, out)?;
    }

    Ok(())
}

fn main() -> Result<()> {
    let cmd = cli::Args::command();
    let global_ids: HashSet<_> = cli::GlobalArgs::augment_args(clap::Command::new(""))
        .get_arguments()
        .map(|a| a.get_id().to_string())
        .collect();

    let global_args = cmd
        .get_arguments()
        .filter(|arg| is_bauplan_arg(arg) && global_ids.contains(arg.get_id().as_str()))
        .map(OptionDoc::try_from)
        .collect::<Result<Vec<_>>>()?;

    let root = cmd.get_name().to_string();
    let mut commands = Vec::new();
    for sub in cmd.get_subcommands().filter(is_bauplan_cmd) {
        let path = vec![root.clone(), sub.get_name().to_string()];
        collect_command(sub, &path, 0, &global_ids, &mut commands)?;
    }

    let mut tera = Tera::default();
    tera.add_raw_template("cli.mdx", TEMPLATE)?;

    let mut ctx = Context::new();
    ctx.insert("global_args", &global_args);
    ctx.insert("commands", &commands);

    let rendered = tera.render("cli.mdx", &ctx)?;
    print!("{rendered}");
    Ok(())
}
