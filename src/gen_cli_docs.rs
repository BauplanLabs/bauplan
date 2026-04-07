#![allow(dead_code)]

mod cli;

use clap::{ArgAction, Args, CommandFactory};
use serde::Serialize;
use std::collections::HashSet;
use tera::{Context, Tera};

const TEMPLATE: &str = include_str!("gen_cli_docs.tera");

#[derive(Serialize)]
struct OptionDoc {
    long: String,
    short: Option<String>,
    value_name: Option<String>,
    default: Option<String>,
    repeatable: bool,
    description: String,
    type_name: &'static str,
}

#[derive(Serialize)]
struct ArgumentDoc {
    name: String,
    description: String,
    type_name: &'static str,
}

#[derive(Serialize)]
struct SubcommandLink {
    name: String,
    id: String,
    about: String,
}

#[derive(Serialize)]
struct CommandDoc {
    id: String,
    full_name: String,
    heading_prefix: String,
    // `depth == 2` means a direct child of the root (e.g. `bauplan branch`);
    // used to insert a horizontal rule before top-level commands.
    is_top_level: bool,
    about: String,
    aliases: Vec<String>,
    synopsis: String,
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
fn is_bauplan_cmd(cmd: &clap::Command) -> bool {
    cmd.get_name() != "help"
}

fn global_arg_ids() -> HashSet<String> {
    cli::GlobalArgs::augment_args(clap::Command::new(""))
        .get_arguments()
        .map(|a| a.get_id().to_string())
        .collect()
}

/// The placeholder name for an arg's value (e.g. `BRANCH_NAME` from `--branch <BRANCH_NAME>`).
/// Falls back to the uppercased arg id if clap doesn't have one set.
fn default_value_name(arg: &clap::Arg) -> String {
    arg.get_value_names()
        .and_then(|n| n.first().map(|s| s.to_string()))
        .unwrap_or_else(|| arg.get_id().to_string().to_uppercase())
}

fn type_name(arg: &clap::Arg) -> &'static str {
    match arg.get_action() {
        ArgAction::SetTrue | ArgAction::SetFalse => "boolean",
        ArgAction::Count => "int",
        _ => "string",
    }
}

fn description(arg: &clap::Arg) -> String {
    arg.get_help()
        .map(|h| h.to_string().replace('\n', " "))
        .unwrap_or_default()
}

fn option_doc(arg: &clap::Arg) -> OptionDoc {
    debug_assert!(!arg.is_positional());
    let value_name = arg.get_action().takes_values().then(|| default_value_name(arg));
    OptionDoc {
        long: arg.get_long().map(|l| format!("--{l}")).unwrap_or_default(),
        short: arg.get_short().map(|s| format!("-{s}")),
        value_name,
        default: arg
            .get_default_values()
            .first()
            .map(|v| v.to_string_lossy().to_string()),
        repeatable: matches!(arg.get_action(), ArgAction::Append | ArgAction::Count),
        description: description(arg),
        type_name: type_name(arg),
    }
}

fn argument_doc(arg: &clap::Arg) -> ArgumentDoc {
    debug_assert!(arg.is_positional());
    ArgumentDoc {
        name: format!("<{}>", default_value_name(arg)),
        description: description(arg),
        type_name: type_name(arg),
    }
}

fn synopsis(path: &[String], positional: &[&clap::Arg], has_options: bool) -> String {
    let mut s = path.join(" ");
    if has_options {
        s.push_str(" [OPTIONS]");
    }
    for p in positional {
        let name = default_value_name(p);
        if p.is_required_set() {
            s.push_str(&format!(" <{name}>"));
        } else {
            s.push_str(&format!(" [{name}]"));
        }
    }
    s
}

/// Extract and dedent the clap "after help" block, stripping a leading
/// "Examples" header line if present.
fn examples_block(cmd: &clap::Command) -> Option<String> {
    let raw = cmd
        .get_after_long_help()
        .or_else(|| cmd.get_after_help())
        .map(|h| h.to_string())?;
    Some(dedent(strip_examples_header(raw.trim_end())))
}

fn strip_examples_header(s: &str) -> &str {
    match s.split_once('\n') {
        // Header line is present when the first line is non-empty, non-indented text.
        Some((first, rest)) if !first.trim().is_empty() && !first.starts_with(' ') => rest,
        _ => s,
    }
}

fn dedent(s: &str) -> String {
    let min_indent = s
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);
    s.lines()
        .map(|l| if l.len() >= min_indent { &l[min_indent..] } else { l })
        .collect::<Vec<_>>()
        .join("\n")
        .trim_matches('\n')
        .to_string()
}

fn collect_command(
    cmd: &clap::Command,
    path: &[String],
    depth: usize,
    global_ids: &HashSet<String>,
    out: &mut Vec<CommandDoc>,
) {
    let cmd_flags: Vec<&clap::Arg> = cmd
        .get_arguments()
        .filter(|a| is_bauplan_arg(a) && !global_ids.contains(a.get_id().as_str()))
        .collect();
    let (cmd_args, option_args): (Vec<&clap::Arg>, Vec<&clap::Arg>) =
        cmd_flags.into_iter().partition(|a| a.is_positional());

    let subcommand_links: Vec<SubcommandLink> = cmd
        .get_subcommands()
        .filter(|s| is_bauplan_cmd(s))
        .map(|sub| {
            let mut sub_path = path.to_vec();
            sub_path.push(sub.get_name().to_string());
            SubcommandLink {
                name: sub.get_name().to_string(),
                id: sub_path.join("-"),
                about: sub
                    .get_about()
                    .map(|a| a.to_string().lines().next().unwrap_or("").to_string())
                    .unwrap_or_default(),
            }
        })
        .collect();

    out.push(CommandDoc {
        id: path.join("-"),
        full_name: path.join(" "),
        heading_prefix: "#".repeat(depth.min(6)),
        is_top_level: depth == 2,
        about: cmd.get_about().map(|a| a.to_string()).unwrap_or_default(),
        aliases: cmd.get_visible_aliases().map(|s| s.to_string()).collect(),
        synopsis: synopsis(path, &cmd_args, !option_args.is_empty()),
        subcommands: subcommand_links,
        positional: cmd_args.iter().map(|a| argument_doc(a)).collect(),
        options: option_args.iter().map(|a| option_doc(a)).collect(),
        examples: examples_block(cmd),
    });

    for sub in cmd.get_subcommands().filter(|s| is_bauplan_cmd(s)) {
        let mut sub_path = path.to_vec();
        sub_path.push(sub.get_name().to_string());
        collect_command(sub, &sub_path, depth + 1, global_ids, out);
    }
}

fn main() {
    let cmd = cli::Args::command();
    let global_ids = global_arg_ids();

    let global_args: Vec<OptionDoc> = cmd
        .get_arguments()
        .filter(|a| global_ids.contains(a.get_id().as_str()) && is_bauplan_arg(a))
        .map(option_doc)
        .collect();

    let root = cmd.get_name().to_string();
    let mut commands: Vec<CommandDoc> = Vec::new();
    for sub in cmd.get_subcommands().filter(|s| is_bauplan_cmd(s)) {
        let path = vec![root.clone(), sub.get_name().to_string()];
        collect_command(sub, &path, 2, &global_ids, &mut commands);
    }

    let mut tera = Tera::default();
    tera.add_raw_template("cli.mdx", TEMPLATE)
        .expect("template parse");

    let mut ctx = Context::new();
    ctx.insert("global_args", &global_args);
    ctx.insert("commands", &commands);

    let rendered = tera.render("cli.mdx", &ctx).expect("template render");
    print!("{rendered}");
}
