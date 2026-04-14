#![forbid(unsafe_code)]

#[allow(
    clippy::cast_precision_loss,
    clippy::indexing_slicing,
    reason = "UNITS is a fixed 5-element array and `unit` is bounded by the loop invariant `unit < UNITS.len() - 1`; f64 precision is adequate for display-range byte counts"
)]
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
    which::which(cmd).is_ok()
}

pub fn is_root() -> bool {
    nix::unistd::Uid::effective().is_root()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_bytes_zero() {
        assert_eq!(format_bytes(0), "0 B");
    }

    #[test]
    fn format_bytes_small() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1023), "1023 B");
    }

    #[test]
    fn format_bytes_kib() {
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1536), "1.5 KiB");
    }

    #[test]
    fn format_bytes_mib() {
        assert_eq!(format_bytes(1024 * 1024), "1.0 MiB");
    }

    #[test]
    fn format_bytes_gib() {
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GiB");
    }

    #[test]
    fn format_bytes_tib() {
        assert_eq!(format_bytes(1024 * 1024 * 1024 * 1024), "1.0 TiB");
    }

    #[test]
    fn format_bytes_large_tib() {
        // Values beyond TiB stay in TiB
        assert_eq!(format_bytes(2 * 1024 * 1024 * 1024 * 1024), "2.0 TiB");
    }

    #[test]
    fn command_exists_known() {
        assert!(command_exists("ls"));
        assert!(command_exists("sh"));
    }

    #[test]
    fn command_exists_unknown() {
        assert!(!command_exists("zzz_no_such_command_12345"));
    }

    #[test]
    fn command_exists_rejects_shell_metacharacters() {
        // which treats metacharacters as part of the name; should return false.
        assert!(!command_exists("ls; echo pwned"));
        assert!(!command_exists("$(echo ls)"));
    }

    #[test]
    fn command_exists_empty() {
        assert!(!command_exists(""));
    }
}
