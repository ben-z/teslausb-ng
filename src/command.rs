use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
}

impl CommandOutput {
    pub fn success(&self) -> bool {
        self.code == Some(0) && !self.timed_out
    }

    pub fn last_error_line(&self) -> String {
        self.stderr
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .unwrap_or("unknown error")
            .trim()
            .to_string()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CommandRunner;

impl CommandRunner {
    pub fn run<I, S>(
        &self,
        program: &str,
        args: I,
        timeout: Option<Duration>,
    ) -> Result<CommandOutput>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args: Vec<String> = args.into_iter().map(|s| s.as_ref().to_string()).collect();
        let mut child = Command::new(program)
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                Error::new(format!(
                    "failed to run {}: {}",
                    display_cmd(program, &args),
                    e
                ))
            })?;

        let started = Instant::now();
        loop {
            if child.try_wait()?.is_some() {
                let output = child.wait_with_output()?;
                return Ok(CommandOutput {
                    code: output.status.code(),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                    timed_out: false,
                });
            }

            if timeout.is_some_and(|limit| started.elapsed() >= limit) {
                let _ = child.kill();
                let output = child.wait_with_output()?;
                return Ok(CommandOutput {
                    code: output.status.code(),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                    timed_out: true,
                });
            }

            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn check<I, S>(
        &self,
        program: &str,
        args: I,
        timeout: Option<Duration>,
    ) -> Result<CommandOutput>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args_vec: Vec<String> = args.into_iter().map(|s| s.as_ref().to_string()).collect();
        let output = self.run(program, args_vec.iter().map(String::as_str), timeout)?;
        if output.success() {
            Ok(output)
        } else if output.timed_out {
            Err(Error::new(format!("{} timed out", program)))
        } else {
            Err(Error::new(format!(
                "{} failed: {}",
                program,
                output.last_error_line()
            )))
        }
    }
}

pub fn display_cmd(program: &str, args: &[String]) -> String {
    let mut parts = vec![program.to_string()];
    parts.extend(args.iter().map(|arg| shell_quote(arg)));
    parts.join(" ")
}

fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | ':' | '=' | ','))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_quotes_spaces() {
        assert_eq!(
            display_cmd("rclone", &["copy".into(), "/a b".into()]),
            "rclone copy '/a b'"
        );
        assert_eq!(
            display_cmd("echo", &["safe/path".into(), "a'b".into()]),
            "echo safe/path 'a'\\''b'"
        );
    }

    #[test]
    fn output_success_and_last_error_line_are_stable() {
        let ok = CommandOutput {
            code: Some(0),
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
        };
        assert!(ok.success());
        assert_eq!(ok.last_error_line(), "unknown error");

        let failed = CommandOutput {
            code: Some(2),
            stdout: String::new(),
            stderr: "first\n\nlast\n".into(),
            timed_out: false,
        };
        assert!(!failed.success());
        assert_eq!(failed.last_error_line(), "last");
    }

    #[test]
    fn run_captures_stdout_and_stderr() {
        let runner = CommandRunner;
        let output = runner
            .run(
                "sh",
                ["-c", "printf out; printf err >&2"],
                Some(Duration::from_secs(1)),
            )
            .unwrap();

        assert!(output.success());
        assert_eq!(output.stdout, "out");
        assert_eq!(output.stderr, "err");
    }

    #[test]
    fn run_timeout_marks_output() {
        let runner = CommandRunner;
        let output = runner
            .run("sh", ["-c", "sleep 2"], Some(Duration::from_millis(100)))
            .unwrap();
        assert!(output.timed_out);
        assert!(!output.success());
    }

    #[test]
    fn check_turns_nonzero_and_timeout_into_errors() {
        let runner = CommandRunner;
        let nonzero = runner.check(
            "sh",
            ["-c", "printf nope >&2; exit 7"],
            Some(Duration::from_secs(1)),
        );
        assert!(nonzero.unwrap_err().to_string().contains("nope"));

        let timeout = runner.check("sh", ["-c", "sleep 2"], Some(Duration::from_millis(100)));
        assert!(timeout.unwrap_err().to_string().contains("timed out"));
    }

    #[test]
    fn run_missing_program_reports_command() {
        let runner = CommandRunner;
        let err = runner
            .run(
                "teslausb-definitely-missing-command",
                std::iter::empty::<&str>(),
                Some(Duration::from_secs(1)),
            )
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("teslausb-definitely-missing-command"));
    }
}
