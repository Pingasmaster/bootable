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
    children: Option<Vec<BlockDevice>>,
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
                Some(name) => format!("/dev/{}", name),
                None => continue,
            },
        };
        let size = dev.size;
        let size_display = size.map(format_bytes).unwrap_or_else(|| "unknown".to_string());
        let model = dev.model.unwrap_or_else(|| "Unknown".to_string());
        let tran = dev.tran.unwrap_or_else(|| "".to_string());
        let display = if tran.is_empty() {
            format!("{} • {} • {}", path, size_display, model)
        } else {
            format!("{} • {} • {} ({})", path, size_display, model, tran)
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
                    Some(name) => format!("/dev/{}", name),
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
