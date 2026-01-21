use std::process::Command;

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{:.1} {}", size, UNITS[unit])
    }
}

pub fn command_exists(cmd: &str) -> bool {
    let script = format!("command -v {} >/dev/null 2>&1", cmd);
    Command::new("sh")
        .arg("-c")
        .arg(script)
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

pub fn is_root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .map(|output| output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "0")
        .unwrap_or(false)
}
