mod cli;

use clap::{ArgAction, Args, CommandFactory};
use serde_json::{Value, json};
use std::collections::HashSet;

fn global_arg_ids() -> HashSet<String> {
    cli::GlobalArgs::augment_args(clap::Command::new(""))
        .get_arguments()
        .map(|a| a.get_id().to_string())
        .collect()
}

fn arg_to_json(arg: &clap::Arg, global_ids: &HashSet<String>) -> Value {
    let long = arg.get_long().map(|s| Value::String(s.to_string()));
    let short = arg.get_short().map(|c| Value::String(c.to_string()));
    let help = arg
        .get_help()
        .map(|h| Value::String(h.to_string()))
        .unwrap_or(Value::Null);
    let default = arg
        .get_default_values()
        .first()
        .map(|v| Value::String(v.to_string_lossy().to_string()));

    let aliases: Vec<Value> = arg
        .get_visible_aliases()
        .map(|a| a.into_iter().map(|s| Value::String(s.to_string())).collect())
        .unwrap_or_default();

    let is_bool = !arg.get_action().takes_values();
    let type_str = if is_bool { "boolean" } else { "string" };

    let repeatable = matches!(arg.get_action(), ArgAction::Append | ArgAction::Count);

    let is_global = global_ids.contains(arg.get_id().as_str());
    let is_positional = arg.is_positional();

    // Use value_name if set, otherwise derive from the arg id
    let value_name: Option<String> = arg
        .get_value_names()
        .and_then(|names| names.first().map(|s| s.to_string()));

    json!({
        "long": long,
        "short": short,
        "type": type_str,
        "default": default,
        "description": help,
        "global": is_global,
        "positional": is_positional,
        "aliases": aliases,
        "repeatable": repeatable,
        "value_name": value_name,
    })
}

fn command_to_json(cmd: &clap::Command, global_ids: &HashSet<String>) -> Value {
    let args: Vec<Value> = cmd
        .get_arguments()
        .filter(|a| a.get_id() != "help" && a.get_id() != "version")
        .map(|a| arg_to_json(a, global_ids))
        .collect();

    let subcommands: Vec<Value> = cmd
        .get_subcommands()
        .filter(|s| s.get_name() != "help")
        .map(|s| command_to_json(s, global_ids))
        .collect();

    let aliases: Vec<Value> = cmd
        .get_visible_aliases()
        .map(|s| Value::String(s.to_string()))
        .collect();

    let about = cmd
        .get_about()
        .map(|h| Value::String(h.to_string()))
        .unwrap_or(Value::Null);

    let after_help = cmd
        .get_after_long_help()
        .or_else(|| cmd.get_after_help())
        .map(|h| Value::String(h.to_string()))
        .unwrap_or(Value::Null);

    json!({
        "name": cmd.get_name(),
        "description": about,
        "aliases": aliases,
        "args": args,
        "after_help": after_help,
        "subcommands": subcommands,
    })
}

fn main() {
    let cmd = cli::Args::command();
    let global_ids = global_arg_ids();

    // Extract global args (those declared in GlobalArgs) separately
    let global_args: Vec<Value> = cmd
        .get_arguments()
        .filter(|a| global_ids.contains(a.get_id().as_str()) && a.get_id() != "help" && a.get_id() != "version")
        .map(|a| arg_to_json(a, &global_ids))
        .collect();

    let commands: Vec<Value> = cmd
        .get_subcommands()
        .filter(|s| s.get_name() != "help")
        .map(|s| command_to_json(s, &global_ids))
        .collect();

    let output = json!({
        "name": cmd.get_name(),
        "global_args": global_args,
        "commands": commands,
    });

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}
