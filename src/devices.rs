#![forbid(unsafe_code)]

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::process::Command;

use crate::util::format_bytes;

#[derive(Debug, Clone)]
pub struct Device {
    pub path: String,
    pub display: String,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct MountPoint {
    pub path: String,
    pub mountpoint: String,
}

#[derive(Debug, Deserialize)]
struct LsblkOutput {
    blockdevices: Vec<BlockDevice>,
}

#[derive(Debug, Deserialize)]
struct BlockDevice {
    name: Option<String>,
    #[serde(rename = "type")]
    device_type: String,
    size: Option<u64>,
    model: Option<String>,
    tran: Option<String>,
    rm: Option<bool>,
    path: Option<String>,
    mountpoints: Option<Vec<Option<String>>>,
    children: Option<Vec<Self>>,
}

pub fn list_removable() -> Result<Vec<Device>> {
    let output = Command::new("lsblk")
        .args([
            "-J",
            "-b",
            "-o",
            "NAME,TYPE,SIZE,MODEL,TRAN,RM,PATH,MOUNTPOINTS",
        ])
        .output()
        .context("running lsblk")?;

    if !output.status.success() {
        return Err(anyhow!(
            "lsblk failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let parsed: LsblkOutput = serde_json::from_slice(&output.stdout).context("parsing lsblk JSON")?;
    let mut devices = Vec::new();

    for dev in parsed.blockdevices {
        if dev.device_type != "disk" {
            continue;
        }
        if !is_removable(&dev) {
            continue;
        }
        let path = match dev.path.clone() {
            Some(path) => path,
            None => match dev.name.as_ref() {
                Some(name) => format!("/dev/{name}"),
                None => continue,
            },
        };
        let size = dev.size;
        let size_display = size.map_or_else(|| "unknown".to_string(), format_bytes);
        let model = dev.model.unwrap_or_else(|| "Unknown".to_string());
        let tran = dev.tran.unwrap_or_default();
        let display = if tran.is_empty() {
            format!("{path} • {size_display} • {model}")
        } else {
            format!("{path} • {size_display} • {model} ({tran})")
        };
        devices.push(Device {
            path,
            display,
            size_bytes: size,
        });
    }

    Ok(devices)
}

pub fn mountpoints_for_device(device_path: &str) -> Result<Vec<String>> {
    let mounts = partitions_with_mountpoints(device_path)?;
    let mut mountpoints: Vec<String> = mounts.into_iter().map(|m| m.mountpoint).collect();
    mountpoints.sort();
    mountpoints.dedup();
    Ok(mountpoints)
}

pub fn partitions_with_mountpoints(device_path: &str) -> Result<Vec<MountPoint>> {
    let output = Command::new("lsblk")
        .args(["-J", "-o", "PATH,MOUNTPOINTS,TYPE"])
        .output()
        .context("running lsblk for mountpoints")?;

    if !output.status.success() {
        return Err(anyhow!(
            "lsblk failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let parsed: LsblkOutput = serde_json::from_slice(&output.stdout).context("parsing lsblk JSON")?;
    let mut mountpoints = Vec::new();

    if let Some(device) = find_device(&parsed.blockdevices, device_path) {
        collect_partition_mounts(device, &mut mountpoints);
    }

    mountpoints.retain(|mp| !mp.mountpoint.starts_with('['));
    mountpoints.sort_by(|a, b| a.mountpoint.cmp(&b.mountpoint));
    mountpoints.dedup_by(|a, b| a.mountpoint == b.mountpoint);
    Ok(mountpoints)
}

fn is_removable(dev: &BlockDevice) -> bool {
    if dev.rm == Some(true) {
        return true;
    }
    if let Some(tran) = &dev.tran {
        return tran.eq_ignore_ascii_case("usb");
    }
    false
}

fn find_device<'a>(devices: &'a [BlockDevice], path: &str) -> Option<&'a BlockDevice> {
    for dev in devices {
        if dev.path.as_deref() == Some(path) {
            return Some(dev);
        }
        if let Some(children) = &dev.children
            && let Some(found) = find_device(children, path)
        {
            return Some(found);
        }
    }
    None
}

fn collect_partition_mounts(device: &BlockDevice, out: &mut Vec<MountPoint>) {
    if let Some(mounts) = &device.mountpoints {
        for mount in mounts.iter().flatten() {
            if mount.is_empty() {
                continue;
            }
            let path = match device.path.clone() {
                Some(path) => path,
                None => match device.name.as_ref() {
                    Some(name) => format!("/dev/{name}"),
                    None => continue,
                },
            };
            out.push(MountPoint {
                path,
                mountpoint: mount.clone(),
            });
        }
    }

    if let Some(children) = &device.children {
        for child in children {
            collect_partition_mounts(child, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_device(
        name: &str,
        path: Option<&str>,
        rm: Option<bool>,
        tran: Option<&str>,
        mountpoints: Option<Vec<Option<String>>>,
        children: Option<Vec<BlockDevice>>,
    ) -> BlockDevice {
        BlockDevice {
            name: Some(name.to_string()),
            device_type: "disk".to_string(),
            size: Some(1_000_000_000),
            model: Some("Test".to_string()),
            tran: tran.map(str::to_string),
            rm,
            path: path.map(str::to_string),
            mountpoints,
            children,
        }
    }

    // --- is_removable ---

    #[test]
    fn removable_rm_true() {
        let dev = make_device("sda", None, Some(true), None, None, None);
        assert!(is_removable(&dev));
    }

    #[test]
    fn removable_usb_transport() {
        let dev = make_device("sda", None, Some(false), Some("usb"), None, None);
        assert!(is_removable(&dev));
    }

    #[test]
    fn removable_usb_case_insensitive() {
        let dev = make_device("sda", None, Some(false), Some("USB"), None, None);
        assert!(is_removable(&dev));
    }

    #[test]
    fn not_removable_sata() {
        let dev = make_device("sda", None, Some(false), Some("sata"), None, None);
        assert!(!is_removable(&dev));
    }

    #[test]
    fn not_removable_no_info() {
        let dev = make_device("sda", None, None, None, None, None);
        assert!(!is_removable(&dev));
    }

    // --- find_device ---

    #[test]
    fn find_device_at_root() {
        let devices = vec![
            make_device("sda", Some("/dev/sda"), None, None, None, None),
            make_device("sdb", Some("/dev/sdb"), None, None, None, None),
        ];
        let found = find_device(&devices, "/dev/sdb");
        assert!(found.is_some());
        assert_eq!(found.unwrap().path.as_deref(), Some("/dev/sdb"));
    }

    #[test]
    fn find_device_in_children() {
        let child = make_device("sda1", Some("/dev/sda1"), None, None, None, None);
        let parent = make_device("sda", Some("/dev/sda"), None, None, None, Some(vec![child]));
        let devices = [parent];
        let found = find_device(&devices, "/dev/sda1");
        assert!(found.is_some());
        assert_eq!(found.unwrap().path.as_deref(), Some("/dev/sda1"));
    }

    #[test]
    fn find_device_not_found() {
        let devices = vec![make_device("sda", Some("/dev/sda"), None, None, None, None)];
        assert!(find_device(&devices, "/dev/sdb").is_none());
    }

    #[test]
    fn find_device_empty_list() {
        assert!(find_device(&[], "/dev/sda").is_none());
    }

    // --- collect_partition_mounts ---

    #[test]
    fn collect_mounts_basic() {
        let dev = make_device(
            "sda1",
            Some("/dev/sda1"),
            None,
            None,
            Some(vec![Some("/mnt/usb".to_string())]),
            None,
        );
        let mut out = Vec::new();
        collect_partition_mounts(&dev, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].path, "/dev/sda1");
        assert_eq!(out[0].mountpoint, "/mnt/usb");
    }

    #[test]
    fn collect_mounts_nested_children() {
        let child = make_device(
            "sda1",
            Some("/dev/sda1"),
            None,
            None,
            Some(vec![Some("/mnt/data".to_string())]),
            None,
        );
        let parent = make_device("sda", Some("/dev/sda"), None, None, None, Some(vec![child]));
        let mut out = Vec::new();
        collect_partition_mounts(&parent, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].mountpoint, "/mnt/data");
    }

    #[test]
    fn collect_mounts_empty_mountpoints_skipped() {
        let dev = make_device(
            "sda1",
            Some("/dev/sda1"),
            None,
            None,
            Some(vec![Some(String::new()), None, Some("/mnt/usb".to_string())]),
            None,
        );
        let mut out = Vec::new();
        collect_partition_mounts(&dev, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].mountpoint, "/mnt/usb");
    }

    #[test]
    fn collect_mounts_no_mounts() {
        let dev = make_device("sda", Some("/dev/sda"), None, None, None, None);
        let mut out = Vec::new();
        collect_partition_mounts(&dev, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn collect_mounts_fallback_to_name() {
        let dev = make_device(
            "sda1",
            None, // no path
            None,
            None,
            Some(vec![Some("/mnt/usb".to_string())]),
            None,
        );
        let mut out = Vec::new();
        collect_partition_mounts(&dev, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].path, "/dev/sda1");
    }
}
