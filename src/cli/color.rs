use anstyle::{AnsiColor, Style};

pub(crate) const BOLD: Style = Style::new().bold();
pub(crate) const DIM: Style = Style::new().dimmed();
pub(crate) const RED: Style = AnsiColor::Red.on_default();
pub(crate) const GREEN: Style = AnsiColor::Green.on_default();
pub(crate) const YELLOW: Style = AnsiColor::Yellow.on_default();
pub(crate) const BLUE: Style = AnsiColor::Blue.on_default();
pub(crate) const CYAN: Style = AnsiColor::Cyan.on_default();
pub(crate) const HEADER: Style = AnsiColor::White.on_default().bold();

/// A styled static string, const-constructible and `Display`.
#[derive(Clone, Copy)]
pub(crate) struct Styled(pub Style, pub &'static str);

impl std::fmt::Display for Styled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}{:#}", self.0, self.1, self.0)
    }
}

pub(crate) struct CliExamples(pub &'static str);

impl From<CliExamples> for clap::builder::StyledStr {
    fn from(ex: CliExamples) -> Self {
        use clap::builder::styling::Style;
        use std::fmt::Write;

        const CLAP_HEADER: Style = Style::new().bold().underline();

        let mut s = clap::builder::StyledStr::new();
        write!(s, "{CLAP_HEADER}Examples{CLAP_HEADER:#}").unwrap();
        for line in ex.0.trim_matches('\n').lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with('#') {
                write!(s, "{DIM}\n{line}{DIM:#}").unwrap();
            } else if !trimmed.is_empty() {
                write!(s, "{BOLD}\n{line}{BOLD:#}").unwrap();
            } else {
                writeln!(s).unwrap();
            }
        }
        s
    }
}
