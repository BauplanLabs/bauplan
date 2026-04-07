import React from "react";

export function CliSubcommands({ children }) {
  return (
    <div className="my-4">
      <div className="font-semibold mb-2">Subcommands</div>
      <ul>{children}</ul>
    </div>
  );
}

export function CliSubcommandLink({ name, href, children }) {
  return (
    <li>
      <a href={href}>
        <code>{name}</code>
      </a>
      {children ? <> - {children}</> : null}
    </li>
  );
}

export function CliOptions({ children }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Flag</th>
          <th>Short</th>
          <th>Default</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>{children}</tbody>
    </table>
  );
}

export function CliOption({
  flag,
  short,
  value,
  default: defaultValue,
  repeatable,
  children,
}) {
  return (
    <tr>
      <td>
        {flag === "-" ? (
          "-"
        ) : (
          <code>
            {flag}
            {value ? ` <${value}>` : ""}
          </code>
        )}
      </td>
      <td>{short ? <code>{short}</code> : "-"}</td>
      <td>{defaultValue ? <code>{defaultValue}</code> : "-"}</td>
      <td>
        {children}
        {repeatable && <> (repeatable)</>}
      </td>
    </tr>
  );
}

export function CliArguments({ children }) {
  return (
    <ul className="my-4">{children}</ul>
  );
}

export function CliArgument({ name, children }) {
  return (
    <li>
      <code>{name}</code>
      {children ? <> - {children}</> : null}
    </li>
  );
}

export function CliExamples({ children }) {
  return (
    <div className="my-4">
      <div className="font-semibold mb-2">Examples</div>
      {children}
    </div>
  );
}
