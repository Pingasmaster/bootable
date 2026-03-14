#![forbid(unsafe_code)]

use anyhow::{anyhow, Context, Result};
use gtk::glib;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;

use crate::util::{command_exists, is_root};
use crate::writer::{UiEvent, WritePlan};

const HELPER_ARG: &str = "--helper";

pub fn helper_plan_path() -> Option<PathBuf> {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == HELPER_ARG {
            return args.next().map(PathBuf::from);
        }
    }
    None
}

pub fn run_helper(plan_path: &Path) -> glib::ExitCode {
    if !is_root() {
        eprintln!("Helper must run as root");
        return glib::ExitCode::FAILURE;
    }

    match read_plan(plan_path) {
        Ok(plan) => {
            let _ = fs::remove_file(plan_path);
            let mut stdout = std::io::stdout();
            let mut ok = true;
            let mut emit = |event: UiEvent| {
                match event {
                    UiEvent::Log(msg) => {
                        let _ = writeln!(stdout, "LOG\t{msg}");
                    }
                    UiEvent::Progress(frac) => {
                        let _ = writeln!(stdout, "PROGRESS\t{frac:.6}");
                    }
                    UiEvent::Done(result) => match result {
                        Ok(()) => {
                            let _ = writeln!(stdout, "DONE\tOK");
                        }
                        Err(err) => {
                            ok = false;
                            let _ = writeln!(stdout, "DONE\tERR\t{err}");
                        }
                    },
                }
                let _ = stdout.flush();
            };

            crate::writer::run(&plan, &mut emit);
            if ok {
                glib::ExitCode::SUCCESS
            } else {
                glib::ExitCode::FAILURE
            }
        }
        Err(err) => {
            let _ = fs::remove_file(plan_path);
            eprintln!("Failed to read plan: {err}");
            glib::ExitCode::FAILURE
        }
    }
}

pub fn run_helper_with_pkexec<F>(plan: &WritePlan, mut emit: F) -> Result<()>
where
    F: FnMut(UiEvent) + Send + 'static,
{
    if !command_exists("pkexec") {
        return Err(anyhow!("pkexec not found"));
    }

    let plan_path = write_plan(plan).context("writing helper plan")?;
    let exe = env::current_exe().context("locating executable")?;

    let mut cmd = Command::new("pkexec");
    cmd.arg("env");
    for key in [
        "DISPLAY",
        "XAUTHORITY",
        "WAYLAND_DISPLAY",
        "XDG_RUNTIME_DIR",
        "GNUPGHOME",
        "GPG_TTY",
        "LANG",
        "LC_ALL",
    ] {
        if let Ok(val) = env::var(key) {
            cmd.arg(format!("{key}={val}"));
        }
    }
    cmd.arg(exe);
    cmd.arg(HELPER_ARG);
    cmd.arg(&plan_path);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .inspect_err(|_| {
            let _ = fs::remove_file(&plan_path);
        })
        .context("spawning pkexec helper")?;
    let stdout = child.stdout.take().context("capturing helper stdout")?;
    let stderr = child.stderr.take().context("capturing helper stderr")?;

    let (tx, rx) = mpsc::channel::<UiEvent>();
    let tx_out = tx.clone();
    let out_thread = thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            if let Some(event) = parse_helper_line(&line) {
                let _ = tx_out.send(event);
            }
        }
    });

    let tx_err = tx.clone();
    let err_thread = thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            let _ = tx_err.send(UiEvent::Log(format!("helper: {line}")));
        }
    });

    drop(tx);

    let mut saw_done = false;
    for event in rx {
        if matches!(event, UiEvent::Done(_)) {
            saw_done = true;
        }
        emit(event);
    }

    let _ = out_thread.join();
    let _ = err_thread.join();

    let status = child.wait().context("waiting for helper")?;
    let _ = fs::remove_file(&plan_path);
    if !saw_done {
        if status.success() {
            emit(UiEvent::Done(Ok(())));
        } else {
            emit(UiEvent::Done(Err(format!(
                "Helper exited with status {status}"
            ))));
        }
    }

    Ok(())
}

fn read_plan(path: &Path) -> Result<WritePlan> {
    let data = fs::read(path)
        .with_context(|| format!("reading plan {path}", path = path.display()))?;
    serde_json::from_slice(&data).context("parsing plan JSON")
}

fn write_plan(plan: &WritePlan) -> Result<PathBuf> {
    let mut file = tempfile::Builder::new()
        .prefix("bootable-plan-")
        .suffix(".json")
        .tempfile_in("/tmp")
        .context("creating temp file")?;
    serde_json::to_writer(&mut file, plan).context("serializing plan")?;
    file.flush().ok();
    let (_file, path) = file.keep().context("persisting plan file")?;
    Ok(path)
}

fn parse_helper_line(line: &str) -> Option<UiEvent> {
    let mut parts = line.splitn(3, '\t');
    let tag = parts.next()?;
    match tag {
        "LOG" => Some(UiEvent::Log(parts.next().unwrap_or_default().to_string())),
        "PROGRESS" => parts
            .next()
            .and_then(|val| val.parse::<f64>().ok())
            .map(UiEvent::Progress),
        "DONE" => {
            let status = parts.next().unwrap_or_default();
            if status == "OK" {
                Some(UiEvent::Done(Ok(())))
            } else {
                let err = parts.next().unwrap_or("Helper failed").to_string();
                Some(UiEvent::Done(Err(err)))
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_log() {
        let event = parse_helper_line("LOG\tHello world").unwrap();
        match event {
            UiEvent::Log(msg) => assert_eq!(msg, "Hello world"),
            _ => panic!("expected Log"),
        }
    }

    #[test]
    fn parse_log_empty_message() {
        let event = parse_helper_line("LOG\t").unwrap();
        match event {
            UiEvent::Log(msg) => assert_eq!(msg, ""),
            _ => panic!("expected Log"),
        }
    }

    #[test]
    fn parse_log_no_tab() {
        // "LOG" alone with no tab — parts.next() for message returns None, unwrap_or_default → ""
        let event = parse_helper_line("LOG").unwrap();
        match event {
            UiEvent::Log(msg) => assert_eq!(msg, ""),
            _ => panic!("expected Log"),
        }
    }

    #[test]
    fn parse_progress() {
        let event = parse_helper_line("PROGRESS\t0.750000").unwrap();
        match event {
            UiEvent::Progress(frac) => assert!((frac - 0.75).abs() < 1e-6),
            _ => panic!("expected Progress"),
        }
    }

    #[test]
    fn parse_progress_invalid() {
        assert!(parse_helper_line("PROGRESS\tabc").is_none());
    }

    #[test]
    fn parse_done_ok() {
        let event = parse_helper_line("DONE\tOK").unwrap();
        match event {
            UiEvent::Done(Ok(())) => {}
            _ => panic!("expected Done(Ok)"),
        }
    }

    #[test]
    fn parse_done_err() {
        let event = parse_helper_line("DONE\tERR\tSomething went wrong").unwrap();
        match event {
            UiEvent::Done(Err(msg)) => assert_eq!(msg, "Something went wrong"),
            _ => panic!("expected Done(Err)"),
        }
    }

    #[test]
    fn parse_done_err_no_message() {
        let event = parse_helper_line("DONE\tERR").unwrap();
        match event {
            UiEvent::Done(Err(msg)) => assert_eq!(msg, "Helper failed"),
            _ => panic!("expected Done(Err)"),
        }
    }

    #[test]
    fn parse_unknown_tag() {
        assert!(parse_helper_line("UNKNOWN\tdata").is_none());
    }

    #[test]
    fn parse_empty_line() {
        // Empty string → parts.next() returns Some(""), which is not a known tag
        assert!(parse_helper_line("").is_none());
    }
}
