use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use yansi::Paint as _;

impl super::Cli {
    /// Creates a progress spinner that plays nicely with logging.
    pub(crate) fn new_spinner(&self) -> ProgressBar {
        fn elapsed_decimal(state: &ProgressState, w: &mut dyn std::fmt::Write) {
            let secs = state.elapsed().as_secs_f64();
            write!(w, "[{secs:.1}s]").unwrap()
        }
        fn current_timestamp(_state: &ProgressState, w: &mut dyn std::fmt::Write) {
            write!(w, "{}", chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ")).unwrap();
        }

        // This format aligns with the log output.
        let progress = ProgressBar::new_spinner().with_style(
            ProgressStyle::with_template(
                "{current_timestamp:.dim} {elapsed_decimal:<8.dim} {msg:.blue} {outcome}{spinner:.cyan/blue}",
            )
            .unwrap()
            .with_key("elapsed_decimal", elapsed_decimal)
            .with_key("current_timestamp", current_timestamp)
            .tick_strings(&["⠋", "⠙", "⠚", "⠞", "⠖", "⠦", "⠴", "⠲", "⠳", "⠓", ""]),
        );

        self.multiprogress.add(progress)
    }
}

pub(crate) trait ProgressExt {
    fn finish_with_failed(&self);
    fn finish_with_done(&self);
    fn finish_with_append(&self, msg: impl std::fmt::Display);
}

impl ProgressExt for ProgressBar {
    fn finish_with_failed(&self) {
        self.finish_with_append("failed".red())
    }

    fn finish_with_done(&self) {
        self.finish_with_append("done".green())
    }

    fn finish_with_append(&self, msg: impl std::fmt::Display) {
        self.finish_with_message(format!("{} {msg}", self.message()));
    }
}
