#![forbid(unsafe_code)]

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::devices;
use crate::util::command_exists;

const FAT32_LIMIT: u64 = 4 * 1024 * 1024 * 1024 - 1;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageMode {
    Auto,
    IsoHybridDd,
    WindowsUefi,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionScheme {
    Gpt,
    Mbr,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetSystem {
    Uefi,
    Bios,
    UefiAndBios,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileSystem {
    Fat32,
    Ntfs,
    Exfat,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WritePlan {
    pub iso_path: PathBuf,
    pub device_path: String,
    pub device_size_bytes: Option<u64>,
    pub image_mode: ImageMode,
    pub partition_scheme: PartitionScheme,
    pub target_system: TargetSystem,
    pub file_system: FileSystem,
    pub volume_label: String,
    pub secure_boot_only: bool,
}

#[derive(Debug)]
pub enum UiEvent {
    Log(String),
    Progress(f64),
    Done(Result<(), String>),
}

pub fn run<F>(plan: &WritePlan, mut emit: F)
where
    F: FnMut(UiEvent),
{
    let result = (|| -> Result<()> {
        log(&mut emit, "Starting write process".to_string());
        if !plan.iso_path.exists() {
            bail!("Image path does not exist");
        }

        let mode = match plan.image_mode {
            ImageMode::Auto => detect_image_mode(&plan.iso_path, &mut emit),
            other => other,
        };

        match mode {
            ImageMode::IsoHybridDd => write_dd(plan, &mut emit),
            ImageMode::WindowsUefi => write_windows_uefi(plan, &mut emit),
            ImageMode::Auto => unreachable!(),
        }
    })();

    emit(UiEvent::Done(result.map_err(|err| err.to_string())));
}

fn detect_image_mode(path: &Path, emit: &mut dyn FnMut(UiEvent)) -> ImageMode {
    if let Some(listing) = iso_listing(path) {
        let listing_lc = listing.to_lowercase();
        if listing_lc.contains("sources/install.wim") || listing_lc.contains("sources/install.esd") {
            log(emit, "Detected Windows ISO".to_string());
            return ImageMode::WindowsUefi;
        }
    } else {
        log(
            emit,
            "Auto-detect unavailable (install 7z or bsdtar); defaulting to ISOHybrid/DD".to_string(),
        );
    }

    ImageMode::IsoHybridDd
}

fn iso_listing(path: &Path) -> Option<String> {
    if command_exists("7z") {
        let output = Command::new("7z")
            .args(["l", "-ba", path.to_string_lossy().as_ref()])
            .output()
            .ok()?;
        if output.status.success() {
            return Some(String::from_utf8_lossy(&output.stdout).to_string());
        }
    }

    if command_exists("bsdtar") {
        let output = Command::new("bsdtar")
            .args(["-tf", path.to_string_lossy().as_ref()])
            .output()
            .ok()?;
        if output.status.success() {
            return Some(String::from_utf8_lossy(&output.stdout).to_string());
        }
    }

    None
}

fn write_dd(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    log(emit, "Preparing device (unmounting)".to_string());
    unmount_device(&plan.device_path, emit)?;

    let iso_size = plan.iso_path.metadata().context("reading ISO size")?.len();
    if let Some(device_size) = plan.device_size_bytes
        && iso_size > device_size
    {
        bail!("ISO is larger than the selected device");
    }

    log(emit, "Writing ISO (DD mode)".to_string());

    let mut src = File::open(&plan.iso_path).context("opening ISO")?;
    let mut dst = OpenOptions::new()
        .write(true)
        .open(&plan.device_path)
        .with_context(|| format!("opening device {device_path}", device_path = plan.device_path))?;

    let total = iso_size.max(1);
    let mut written: u64 = 0;
    let mut buffer = vec![0u8; 4 * 1024 * 1024];
    let mut last_update = Instant::now();

    loop {
        let read = src.read(&mut buffer).context("reading ISO")?;
        if read == 0 {
            break;
        }
        dst.write_all(&buffer[..read]).context("writing device")?;
        written += read as u64;

        if last_update.elapsed() >= Duration::from_millis(200) {
            #[allow(clippy::cast_precision_loss)]
            let frac = (written as f64) / (total as f64);
            emit(UiEvent::Progress(frac));
            last_update = Instant::now();
        }
    }

    dst.sync_all().ok();
    emit(UiEvent::Progress(1.0));
    log(emit, "Syncing buffers".to_string());
    let _ = Command::new("sync").status();

    log(emit, "DD write completed".to_string());
    Ok(())
}

fn write_windows_uefi(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    if matches!(plan.target_system, TargetSystem::Bios | TargetSystem::UefiAndBios) {
        bail!("Windows BIOS/UEFI+BIOS boot is not implemented yet");
    }

    match plan.file_system {
        FileSystem::Fat32 => write_windows_uefi_fat32(plan, emit),
        FileSystem::Ntfs => write_windows_uefi_ntfs(plan, emit),
        FileSystem::Exfat => bail!("Windows mode does not support exFAT"),
    }
}

fn write_windows_uefi_fat32(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    for cmd in ["parted", "mkfs.vfat", "mount", "umount", "rsync"] {
        if !command_exists(cmd) {
            bail!("Required tool not found: {cmd}");
        }
    }

    log(emit, "Preparing device (unmounting)".to_string());
    unmount_device(&plan.device_path, emit)?;

    log(emit, "Partitioning device".to_string());
    create_partition(&plan.device_path, plan.partition_scheme, emit)?;

    let partition = partition_path(&plan.device_path);
    let label = sanitize_fat_label(&plan.volume_label);
    log(emit, "Waiting for partition device".to_string());
    wait_for_device_node(&partition).with_context(|| format!("waiting for {partition}"))?;

    log(emit, "Formatting FAT32".to_string());
    let mkfs_args = vec![
        "-F".to_string(),
        "32".to_string(),
        "-n".to_string(),
        label,
        partition.clone(),
    ];
    run_cmd(emit, "mkfs.vfat", &mkfs_args, "mkfs.vfat")?;

    let iso_dir = tempfile::tempdir().context("creating temp dir for ISO")?;
    let usb_dir = tempfile::tempdir().context("creating temp dir for USB")?;

    log(emit, "Mounting ISO".to_string());
    let mount_iso_args = vec![
        "-o".to_string(),
        "loop,ro".to_string(),
        plan.iso_path.to_string_lossy().to_string(),
        iso_dir.path().to_string_lossy().to_string(),
    ];
    run_cmd(emit, "mount", &mount_iso_args, "mount ISO")?;

    log(emit, "Mounting USB".to_string());
    let mount_usb_args = vec![
        partition,
        usb_dir.path().to_string_lossy().to_string(),
    ];
    if let Err(err) = run_cmd(emit, "mount", &mount_usb_args, "mount USB") {
        let _ = Command::new("umount").arg(iso_dir.path()).status();
        return Err(err);
    }

    let copy_result = (|| -> Result<()> {
        log(emit, "Copying files".to_string());
        let rsync_args = vec![
            "-aH".to_string(),
            "--exclude=/sources/install.wim".to_string(),
            "--exclude=/sources/install.esd".to_string(),
            format!(
                "{path}/",
                path = iso_dir.path().to_string_lossy()
            ),
            format!(
                "{path}/",
                path = usb_dir.path().to_string_lossy()
            ),
        ];
        run_cmd(emit, "rsync", &rsync_args, "rsync")?;

        let wim_path = iso_dir.path().join("sources/install.wim");
        let esd_path = iso_dir.path().join("sources/install.esd");

        if wim_path.exists() {
            handle_wim(&wim_path, usb_dir.path(), emit)?;
        } else if esd_path.exists() {
            handle_wim(&esd_path, usb_dir.path(), emit)?;
        } else {
            log(emit, "No install.wim/esd found; ISO may be non-Windows".to_string());
        }

        Ok(())
    })();

    log(emit, "Unmounting USB".to_string());
    let _ = Command::new("umount").arg(usb_dir.path()).status();

    log(emit, "Unmounting ISO".to_string());
    let _ = Command::new("umount").arg(iso_dir.path()).status();

    copy_result?;

    log(emit, "Syncing buffers".to_string());
    let _ = Command::new("sync").status();
    emit(UiEvent::Progress(1.0));
    log(emit, "Windows UEFI write completed".to_string());
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn write_windows_uefi_ntfs(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    if plan.partition_scheme != PartitionScheme::Gpt {
        bail!("UEFI:NTFS requires GPT partition scheme");
    }

    let iso_size = plan.iso_path.metadata().context("reading ISO size")?.len();
    if let Some(device_size) = plan.device_size_bytes {
        let overhead = 260 * 1024 * 1024u64;
        if iso_size + overhead > device_size {
            bail!("ISO is too large for the selected device");
        }
    }

    for cmd in ["parted", "mkfs.vfat", "mount", "umount", "rsync"] {
        if !command_exists(cmd) {
            bail!("Required tool not found: {cmd}");
        }
    }

    let signed_x64_ready = signed_bootloader_ready(BootArch::X64);
    if plan.secure_boot_only && !signed_x64_ready {
        bail!("Secure Boot is enforced but signed shim/grub + modules were not found");
    }
    if !signed_x64_ready && !command_exists("grub-mkstandalone") {
        bail!("grub-mkstandalone is required for unsigned UEFI:NTFS");
    }

    let mkfs_ntfs = if command_exists("mkfs.ntfs") {
        "mkfs.ntfs"
    } else if command_exists("mkfs.ntfs3") {
        "mkfs.ntfs3"
    } else {
        bail!("Required tool not found: mkfs.ntfs or mkfs.ntfs3");
    };

    log(emit, "Preparing device (unmounting)".to_string());
    unmount_device(&plan.device_path, emit)?;

    log(emit, "Partitioning device (ESP + NTFS)".to_string());
    create_windows_partitions_ntfs(&plan.device_path, emit)?;

    let esp_partition = partition_path_for(&plan.device_path, 1);
    let data_partition = partition_path_for(&plan.device_path, 2);
    log(emit, "Waiting for partition devices".to_string());
    wait_for_device_node(&esp_partition).with_context(|| format!("waiting for {esp_partition}"))?;
    wait_for_device_node(&data_partition).with_context(|| format!("waiting for {data_partition}"))?;

    let esp_label = "BOOT".to_string();
    let ntfs_label = sanitize_ntfs_label(&plan.volume_label);

    log(emit, "Formatting ESP (FAT32)".to_string());
    let mkfs_args = vec![
        "-F".to_string(),
        "32".to_string(),
        "-n".to_string(),
        esp_label,
        esp_partition.clone(),
    ];
    run_cmd(emit, "mkfs.vfat", &mkfs_args, "mkfs.vfat")?;

    log(emit, "Formatting NTFS".to_string());
    let mkfs_ntfs_args = vec![
        "-F".to_string(),
        "-L".to_string(),
        ntfs_label.clone(),
        data_partition.clone(),
    ];
    run_cmd(emit, mkfs_ntfs, &mkfs_ntfs_args, "mkfs.ntfs")?;

    let iso_dir = tempfile::tempdir().context("creating temp dir for ISO")?;
    let esp_dir = tempfile::tempdir().context("creating temp dir for ESP")?;
    let data_dir = tempfile::tempdir().context("creating temp dir for NTFS")?;

    log(emit, "Mounting ISO".to_string());
    let mount_iso_args = vec![
        "-o".to_string(),
        "loop,ro".to_string(),
        plan.iso_path.to_string_lossy().to_string(),
        iso_dir.path().to_string_lossy().to_string(),
    ];
    run_cmd(emit, "mount", &mount_iso_args, "mount ISO")?;

    log(emit, "Mounting ESP".to_string());
    let mount_esp_args = vec![
        esp_partition,
        esp_dir.path().to_string_lossy().to_string(),
    ];
    if let Err(err) = run_cmd(emit, "mount", &mount_esp_args, "mount ESP") {
        let _ = Command::new("umount").arg(iso_dir.path()).status();
        return Err(err);
    }

    log(emit, "Mounting NTFS".to_string());
    let mount_data_args = vec![
        data_partition,
        data_dir.path().to_string_lossy().to_string(),
    ];
    if let Err(err) = run_cmd(emit, "mount", &mount_data_args, "mount NTFS") {
        let _ = Command::new("umount").arg(esp_dir.path()).status();
        let _ = Command::new("umount").arg(iso_dir.path()).status();
        return Err(err);
    }

    let copy_result = (|| -> Result<()> {
        log(emit, "Copying files to NTFS".to_string());
        run_rsync_with_progress(emit, iso_dir.path(), data_dir.path())?;

        log(emit, "Installing UEFI:NTFS bootloader".to_string());
        let secure = install_uefi_ntfs_loaders(
            esp_dir.path(),
            &ntfs_label,
            plan.secure_boot_only,
            emit,
        )?;
        if secure {
            log(emit, "Secure Boot: signed shim/grub installed".to_string());
        } else {
            log(
                emit,
                "Secure Boot: unsigned GRUB installed (may need to disable Secure Boot)"
                    .to_string(),
            );
        }
        Ok(())
    })();

    log(emit, "Unmounting NTFS".to_string());
    let _ = Command::new("umount").arg(data_dir.path()).status();
    log(emit, "Unmounting ESP".to_string());
    let _ = Command::new("umount").arg(esp_dir.path()).status();
    log(emit, "Unmounting ISO".to_string());
    let _ = Command::new("umount").arg(iso_dir.path()).status();

    copy_result?;

    log(emit, "Syncing buffers".to_string());
    let _ = Command::new("sync").status();
    emit(UiEvent::Progress(1.0));
    log(emit, "Windows UEFI NTFS write completed".to_string());
    Ok(())
}

fn handle_wim(wim_path: &Path, usb_root: &Path, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    let size = wim_path.metadata().context("reading WIM size")?.len();
    let dest_dir = usb_root.join("sources");
    fs::create_dir_all(&dest_dir).context("creating sources directory")?;

    if size <= FAT32_LIMIT {
        log(emit, "Copying install image".to_string());
        let dest_path = dest_dir.join(
            wim_path
                .file_name()
                .ok_or_else(|| anyhow!("invalid WIM filename"))?,
        );
        copy_file_buffered(wim_path, &dest_path)?;
        return Ok(());
    }

    log(emit, "Splitting install image (WIM/ESD > 4GiB)".to_string());
    let split_target = dest_dir.join("install.swm");
    let split_target_str = split_target.to_string_lossy().to_string();

    if command_exists("wimlib-imagex") {
        let args = vec![
            "split".to_string(),
            wim_path.to_string_lossy().to_string(),
            split_target_str,
            "4000".to_string(),
        ];
        run_cmd(emit, "wimlib-imagex", &args, "wimlib-imagex split")?;
        return Ok(());
    }

    if command_exists("wimsplit") {
        let args = vec![
            wim_path.to_string_lossy().to_string(),
            split_target_str,
            "4000".to_string(),
        ];
        run_cmd(emit, "wimsplit", &args, "wimsplit")?;
        return Ok(());
    }

    bail!("wimlib-imagex or wimsplit is required to split large install.wim/esd");
}

fn copy_file_buffered(src: &Path, dst: &Path) -> Result<()> {
    let mut input =
        File::open(src).with_context(|| format!("opening {path}", path = src.display()))?;
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).ok();
    }
    let mut output =
        File::create(dst).with_context(|| format!("creating {path}", path = dst.display()))?;
    let mut buffer = vec![0u8; 4 * 1024 * 1024];
    loop {
        let read = input.read(&mut buffer).context("reading input file")?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .context("writing output file")?;
    }
    output.sync_all().ok();
    Ok(())
}

fn create_partition(
    device: &str,
    scheme: PartitionScheme,
    emit: &mut dyn FnMut(UiEvent),
) -> Result<()> {
    let mut args = vec!["-s".to_string(), device.to_string(), "mklabel".to_string()];
    match scheme {
        PartitionScheme::Gpt => args.push("gpt".to_string()),
        PartitionScheme::Mbr => args.push("msdos".to_string()),
    }
    run_cmd(emit, "parted", &args, "parted mklabel")?;

    let mkpart_args = vec![
        "-s".to_string(),
        device.to_string(),
        "mkpart".to_string(),
        "primary".to_string(),
        "fat32".to_string(),
        "1MiB".to_string(),
        "100%".to_string(),
    ];
    run_cmd(emit, "parted", &mkpart_args, "parted mkpart")?;

    let mut set_args = vec!["-s".to_string(), device.to_string(), "set".to_string()];
    set_args.push("1".to_string());
    match scheme {
        PartitionScheme::Gpt => set_args.push("esp".to_string()),
        PartitionScheme::Mbr => set_args.push("boot".to_string()),
    }
    set_args.push("on".to_string());
    run_cmd(emit, "parted", &set_args, "parted set")?;

    refresh_partition_table(device);

    Ok(())
}

fn create_windows_partitions_ntfs(device: &str, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    let args = vec!["-s".to_string(), device.to_string(), "mklabel".to_string(), "gpt".to_string()];
    run_cmd(emit, "parted", &args, "parted mklabel")?;

    let mkpart_esp = vec![
        "-s".to_string(),
        device.to_string(),
        "mkpart".to_string(),
        "ESP".to_string(),
        "fat32".to_string(),
        "1MiB".to_string(),
        "201MiB".to_string(),
    ];
    run_cmd(emit, "parted", &mkpart_esp, "parted mkpart ESP")?;

    let set_esp = vec![
        "-s".to_string(),
        device.to_string(),
        "set".to_string(),
        "1".to_string(),
        "esp".to_string(),
        "on".to_string(),
    ];
    run_cmd(emit, "parted", &set_esp, "parted set esp")?;

    let mkpart_ntfs = vec![
        "-s".to_string(),
        device.to_string(),
        "mkpart".to_string(),
        "primary".to_string(),
        "ntfs".to_string(),
        "201MiB".to_string(),
        "100%".to_string(),
    ];
    run_cmd(emit, "parted", &mkpart_ntfs, "parted mkpart ntfs")?;

    refresh_partition_table(device);

    Ok(())
}

fn unmount_device(device_path: &str, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    let mounts = devices::partitions_with_mountpoints(device_path)
        .with_context(|| format!("listing mountpoints for {device_path}"))?;
    if mounts.is_empty() {
        return Ok(());
    }

    for mount in mounts {
        log(
            emit,
            format!(
                "Unmounting {mountpoint} ({path})",
                mountpoint = mount.mountpoint,
                path = &mount.path
            ),
        );
        if command_exists("udisksctl") {
            let args = vec![
                "unmount".to_string(),
                "-b".to_string(),
                mount.path,
            ];
            if run_cmd(emit, "udisksctl", &args, "udisksctl unmount").is_ok() {
                continue;
            }
        }

        let status = Command::new("umount")
            .arg(&mount.mountpoint)
            .status()
            .with_context(|| format!("umount {mountpoint}", mountpoint = mount.mountpoint))?;
        if !status.success() {
            bail!("Failed to unmount {}", mount.mountpoint);
        }
    }

    Ok(())
}

fn refresh_partition_table(device: &str) {
    if command_exists("partprobe") {
        let _ = Command::new("partprobe").arg(device).status();
    } else if command_exists("blockdev") {
        let _ = Command::new("blockdev")
            .arg("--rereadpt")
            .arg(device)
            .status();
    }
    if command_exists("udevadm") {
        let _ = Command::new("udevadm")
            .args(["settle", "--timeout=5"])
            .status();
    }
}

fn wait_for_device_node(path: &str) -> Result<()> {
    if Path::new(path).exists() {
        return Ok(());
    }
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if Path::new(path).exists() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    bail!("Timed out waiting for {path}")
}

fn run_cmd(
    emit: &mut dyn FnMut(UiEvent),
    program: &str,
    args: &[String],
    context: &str,
) -> Result<()> {
    log(emit, format!("Running: {context}"));
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("running {program}"))?;
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(anyhow!("{context} failed: {stderr}"))
    }
}

fn run_rsync_with_progress(
    emit: &mut dyn FnMut(UiEvent),
    src: &Path,
    dst: &Path,
) -> Result<()> {
    let version = rsync_version();
    let supports_progress2 = version.is_none_or(|v| v >= (3, 1, 0));
    let supports_no_inc = version.is_none_or(|v| v >= (3, 1, 0));

    let mut args = vec!["-aH".to_string()];
    if supports_progress2 {
        args.push("--info=progress2".to_string());
        if supports_no_inc {
            args.push("--no-inc-recursive".to_string());
        }
    } else {
        args.push("--progress".to_string());
    }
    args.push(format!("{path}/", path = src.to_string_lossy()));
    args.push(format!("{path}/", path = dst.to_string_lossy()));

    let mut cmd = Command::new("rsync");
    cmd.args(&args);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    log(emit, "Running: rsync (progress)".to_string());

    let mut child = cmd.spawn().context("spawning rsync")?;
    let stdout = child.stdout.take().context("capturing rsync stdout")?;
    let stderr = child.stderr.take().context("capturing rsync stderr")?;

    let (tx, rx) = mpsc::channel::<UiEvent>();
    let tx_out = tx.clone();
    let out_thread = spawn_rsync_reader(stdout, tx_out, false);

    let tx_err = tx.clone();
    let err_thread = spawn_rsync_reader(stderr, tx_err, true);

    drop(tx);

    for event in rx {
        emit(event);
    }

    let _ = out_thread.join();
    let _ = err_thread.join();

    let status = child.wait().context("waiting for rsync")?;
    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("rsync failed: {status}"))
    }
}

fn spawn_rsync_reader<R: Read + Send + 'static>(
    reader: R,
    tx: mpsc::Sender<UiEvent>,
    emit_logs: bool,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        let mut buf = [0u8; 4096];
        let mut pending = String::new();
        let mut last_emit = Instant::now();
        loop {
            let read = reader.read(&mut buf).unwrap_or(0);
            if read == 0 {
                break;
            }
            pending.push_str(&String::from_utf8_lossy(&buf[..read]));
            while let Some(idx) = find_line_break(&pending) {
                let line = pending[..idx].to_string();
                pending = pending[idx + 1..].to_string();
                handle_rsync_line(&line, &tx, &mut last_emit, emit_logs);
            }
        }
        if !pending.is_empty() {
            handle_rsync_line(&pending, &tx, &mut last_emit, emit_logs);
        }
    })
}

fn handle_rsync_line(
    line: &str,
    tx: &mpsc::Sender<UiEvent>,
    last_emit: &mut Instant,
    emit_logs: bool,
) {
    if let Some(frac) = parse_rsync_percent(line) {
        if last_emit.elapsed() >= Duration::from_millis(200) {
            let _ = tx.send(UiEvent::Progress(frac));
            *last_emit = Instant::now();
        }
        if emit_logs && !is_rsync_progress_line(line) {
            let trimmed = line.trim_end_matches(['\r', '\n']);
            if !trimmed.is_empty() {
                let _ = tx.send(UiEvent::Log(format!("rsync: {trimmed}")));
            }
        }
        return;
    }

    if emit_logs {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if !trimmed.is_empty() {
            let _ = tx.send(UiEvent::Log(format!("rsync: {trimmed}")));
        }
    }
}

fn is_rsync_progress_line(line: &str) -> bool {
    line.contains("to-chk=")
        || line.contains("xfr#")
        || line.contains("B/s")
        || line.contains("bytes/sec")
}

fn parse_rsync_percent(line: &str) -> Option<f64> {
    for token in line.split_whitespace() {
        if let Some(raw) = token.strip_suffix('%') {
            let cleaned = raw.replace(',', "");
            if let Ok(value) = cleaned.parse::<f64>() {
                let frac = (value / 100.0).clamp(0.0, 1.0);
                return Some(frac);
            }
        }
    }
    None
}

fn find_line_break(text: &str) -> Option<usize> {
    let cr = text.find('\r');
    let lf = text.find('\n');
    match (cr, lf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn rsync_version() -> Option<(u32, u32, u32)> {
    let output = Command::new("rsync").arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let first_line = stdout.lines().next()?;
    let mut parts = first_line.split_whitespace();
    while let Some(part) = parts.next() {
        if part == "version" {
            let ver = parts.next()?;
            let mut nums = ver.split('.');
            let major = nums.next()?.parse().ok()?;
            let minor = nums.next()?.parse().ok()?;
            let patch = nums.next().unwrap_or("0").parse().ok()?;
            return Some((major, minor, patch));
        }
    }
    None
}

fn partition_path(device: &str) -> String {
    partition_path_for(device, 1)
}

fn partition_path_for(device: &str, index: u8) -> String {
    let ends_with_digit = device.chars().last().is_some_and(|c| c.is_ascii_digit());
    if ends_with_digit {
        format!("{device}p{index}")
    } else {
        format!("{device}{index}")
    }
}

fn sanitize_fat_label(label: &str) -> String {
    let mut sanitized = String::new();
    for ch in label.chars() {
        if sanitized.len() >= 11 {
            break;
        }
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            sanitized.push(ch);
        }
    }
    if sanitized.is_empty() {
        "BOOTABLE".to_string()
    } else {
        sanitized
    }
}

fn sanitize_ntfs_label(label: &str) -> String {
    let mut sanitized = String::new();
    for ch in label.chars() {
        if sanitized.len() >= 32 {
            break;
        }
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == ' ' {
            sanitized.push(ch);
        }
    }
    let trimmed = sanitized.trim();
    if trimmed.is_empty() {
        "BOOTABLE".to_string()
    } else {
        trimmed.to_string()
    }
}

#[derive(Clone, Copy, Debug)]
enum BootArch {
    X64,
    Ia32,
    Aa64,
}

impl BootArch {
    const fn label(self) -> &'static str {
        match self {
            Self::X64 => "x86_64",
            Self::Ia32 => "ia32",
            Self::Aa64 => "aa64",
        }
    }

    const fn grub_target(self) -> &'static str {
        match self {
            Self::X64 => "x86_64-efi",
            Self::Ia32 => "i386-efi",
            Self::Aa64 => "arm64-efi",
        }
    }

    const fn module_dir(self) -> &'static str {
        match self {
            Self::X64 => "x86_64-efi",
            Self::Ia32 => "i386-efi",
            Self::Aa64 => "arm64-efi",
        }
    }

    const fn boot_filename(self) -> &'static str {
        match self {
            Self::X64 => "BOOTX64.EFI",
            Self::Ia32 => "BOOTIA32.EFI",
            Self::Aa64 => "BOOTAA64.EFI",
        }
    }

    const fn grub_filename(self) -> &'static str {
        match self {
            Self::X64 => "grubx64.efi",
            Self::Ia32 => "grubia32.efi",
            Self::Aa64 => "grubaa64.efi",
        }
    }

    fn signed_shim_candidates(self) -> Vec<&'static str> {
        match self {
            Self::X64 => vec![
                "/usr/lib/shim/shimx64.efi",
                "/usr/lib/shim/shimx64.efi.signed",
                "/usr/lib64/shim/shimx64.efi",
                "/usr/lib64/shim/shimx64.efi.signed",
                "/usr/share/shim/shimx64.efi",
                "/usr/share/shim/shimx64.efi.signed",
                "/usr/share/efi/shim/shimx64.efi",
                "/usr/share/efi/shim/shimx64.efi.signed",
                "/usr/lib/shim-signed/shimx64.efi.signed",
                "/usr/lib64/shim-signed/shimx64.efi.signed",
            ],
            Self::Ia32 => vec![
                "/usr/lib/shim/shimia32.efi",
                "/usr/lib/shim/shimia32.efi.signed",
                "/usr/lib64/shim/shimia32.efi",
                "/usr/lib64/shim/shimia32.efi.signed",
                "/usr/share/shim/shimia32.efi",
                "/usr/share/shim/shimia32.efi.signed",
                "/usr/share/efi/shim/shimia32.efi",
                "/usr/share/efi/shim/shimia32.efi.signed",
                "/usr/lib/shim-signed/shimia32.efi.signed",
                "/usr/lib64/shim-signed/shimia32.efi.signed",
            ],
            Self::Aa64 => vec![
                "/usr/lib/shim/shimaa64.efi",
                "/usr/lib/shim/shimaa64.efi.signed",
                "/usr/lib64/shim/shimaa64.efi",
                "/usr/lib64/shim/shimaa64.efi.signed",
                "/usr/share/shim/shimaa64.efi",
                "/usr/share/shim/shimaa64.efi.signed",
                "/usr/share/efi/shim/shimaa64.efi",
                "/usr/share/efi/shim/shimaa64.efi.signed",
                "/usr/lib/shim-signed/shimaa64.efi.signed",
                "/usr/lib64/shim-signed/shimaa64.efi.signed",
            ],
        }
    }

    fn signed_grub_candidates(self) -> Vec<&'static str> {
        match self {
            Self::X64 => vec![
                "/usr/lib/grub/x86_64-efi-signed/grubx64.efi",
                "/usr/lib/grub/x86_64-efi-signed/grubx64.efi.signed",
                "/usr/lib64/grub/x86_64-efi-signed/grubx64.efi",
                "/usr/lib64/grub/x86_64-efi-signed/grubx64.efi.signed",
                "/usr/share/grub/x86_64-efi-signed/grubx64.efi",
                "/usr/share/grub/x86_64-efi-signed/grubx64.efi.signed",
            ],
            Self::Ia32 => vec![
                "/usr/lib/grub/i386-efi-signed/grubia32.efi",
                "/usr/lib/grub/i386-efi-signed/grubia32.efi.signed",
                "/usr/lib64/grub/i386-efi-signed/grubia32.efi",
                "/usr/lib64/grub/i386-efi-signed/grubia32.efi.signed",
                "/usr/share/grub/i386-efi-signed/grubia32.efi",
                "/usr/share/grub/i386-efi-signed/grubia32.efi.signed",
            ],
            Self::Aa64 => vec![
                "/usr/lib/grub/arm64-efi-signed/grubaa64.efi",
                "/usr/lib/grub/arm64-efi-signed/grubaa64.efi.signed",
                "/usr/lib64/grub/arm64-efi-signed/grubaa64.efi",
                "/usr/lib64/grub/arm64-efi-signed/grubaa64.efi.signed",
                "/usr/share/grub/arm64-efi-signed/grubaa64.efi",
                "/usr/share/grub/arm64-efi-signed/grubaa64.efi.signed",
            ],
        }
    }

    fn mok_manager_candidates(self) -> Vec<&'static str> {
        match self {
            Self::X64 => vec![
                "/usr/lib/shim/mmx64.efi",
                "/usr/lib64/shim/mmx64.efi",
                "/usr/share/shim/mmx64.efi",
                "/usr/share/efi/shim/mmx64.efi",
            ],
            Self::Ia32 => vec![
                "/usr/lib/shim/mmia32.efi",
                "/usr/lib64/shim/mmia32.efi",
                "/usr/share/shim/mmia32.efi",
                "/usr/share/efi/shim/mmia32.efi",
            ],
            Self::Aa64 => vec![
                "/usr/lib/shim/mmaa64.efi",
                "/usr/lib64/shim/mmaa64.efi",
                "/usr/share/shim/mmaa64.efi",
                "/usr/share/efi/shim/mmaa64.efi",
            ],
        }
    }
}

#[derive(Clone, Debug)]
struct SignedBootloader {
    shim: PathBuf,
    grub: PathBuf,
    mok: Option<PathBuf>,
}

fn find_signed_bootloader(arch: BootArch) -> Option<SignedBootloader> {
    let shim = find_first_existing(&arch.signed_shim_candidates())?;
    let grub = find_first_existing(&arch.signed_grub_candidates())?;
    let mok = find_first_existing(&arch.mok_manager_candidates());
    Some(SignedBootloader { shim, grub, mok })
}

fn find_first_existing(candidates: &[&str]) -> Option<PathBuf> {
    for path in candidates {
        let candidate = Path::new(path);
        if candidate.exists() {
            return Some(candidate.to_path_buf());
        }
    }
    None
}

fn find_grub_module_dir(arch: BootArch) -> Option<PathBuf> {
    let candidates = [
        format!("/usr/lib/grub/{module_dir}", module_dir = arch.module_dir()),
        format!("/usr/lib64/grub/{module_dir}", module_dir = arch.module_dir()),
        format!("/usr/share/grub/{module_dir}", module_dir = arch.module_dir()),
    ];
    for path in &candidates {
        let candidate = Path::new(path);
        if candidate.is_dir() {
            return Some(candidate.to_path_buf());
        }
    }
    None
}

fn modules_present(dir: &Path) -> bool {
    let required = [
        "part_gpt.mod",
        "part_msdos.mod",
        "fat.mod",
        "ntfs.mod",
        "chain.mod",
        "search_fs_label.mod",
    ];
    required
        .iter()
        .all(|name| dir.join(name).exists())
}

fn signed_bootloader_ready(arch: BootArch) -> bool {
    if find_signed_bootloader(arch).is_none() {
        return false;
    }
    find_grub_module_dir(arch).is_some_and(|dir| modules_present(&dir))
}

fn copy_grub_modules(arch: BootArch, esp_root: &Path) -> Result<()> {
    let arch_label = arch.label();
    let source = find_grub_module_dir(arch)
        .ok_or_else(|| anyhow!("grub modules not found for {arch_label}"))?;
    if !modules_present(&source) {
        return Err(anyhow!(
            "grub modules missing in {path}",
            path = source.display()
        ));
    }
    let target = esp_root.join("boot/grub").join(arch.module_dir());
    fs::create_dir_all(&target).context("creating grub module dir")?;
    for entry in fs::read_dir(&source).context("reading grub module dir")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && let Some(name) = path.file_name()
        {
            let dest = target.join(name);
            fs::copy(&path, &dest).with_context(|| {
                format!(
                    "copying grub module {src} to {dst}",
                    src = path.display(),
                    dst = dest.display()
                )
            })?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn install_uefi_ntfs_loaders(
    esp_root: &Path,
    ntfs_label: &str,
    secure_only: bool,
    emit: &mut dyn FnMut(UiEvent),
) -> Result<bool> {
    let grub_dir = esp_root.join("EFI/BOOT");
    fs::create_dir_all(&grub_dir).context("creating EFI/BOOT")?;

    let grub_cfg = format!(
        "if [ \"${{grub_cpu}}\" = \"x86_64\" ]; then\n  search --no-floppy --file /EFI/BOOT/BOOTX64.EFI --set=esp\nelif [ \"${{grub_cpu}}\" = \"i386\" ]; then\n  search --no-floppy --file /EFI/BOOT/BOOTIA32.EFI --set=esp\nelif [ \"${{grub_cpu}}\" = \"arm64\" ]; then\n  search --no-floppy --file /EFI/BOOT/BOOTAA64.EFI --set=esp\nfi\nif [ -n \"$esp\" ]; then\n  set prefix=($esp)/boot/grub\nfi\ninsmod part_gpt\ninsmod part_msdos\ninsmod fat\ninsmod ntfs\ninsmod chain\ninsmod search_fs_label\nsearch --no-floppy --label \"{ntfs_label}\" --set=root\nif [ \"${{grub_cpu}}\" = \"x86_64\" ]; then\n  if [ -f /EFI/BOOT/BOOTX64.EFI ]; then\n    chainloader /EFI/BOOT/BOOTX64.EFI\n    boot\n  fi\n  if [ -f /efi/boot/bootx64.efi ]; then\n    chainloader /efi/boot/bootx64.efi\n    boot\n  fi\nelif [ \"${{grub_cpu}}\" = \"i386\" ]; then\n  if [ -f /EFI/BOOT/BOOTIA32.EFI ]; then\n    chainloader /EFI/BOOT/BOOTIA32.EFI\n    boot\n  fi\n  if [ -f /efi/boot/bootia32.efi ]; then\n    chainloader /efi/boot/bootia32.efi\n    boot\n  fi\nelif [ \"${{grub_cpu}}\" = \"arm64\" ]; then\n  if [ -f /EFI/BOOT/BOOTAA64.EFI ]; then\n    chainloader /EFI/BOOT/BOOTAA64.EFI\n    boot\n  fi\n  if [ -f /efi/boot/bootaa64.efi ]; then\n    chainloader /efi/boot/bootaa64.efi\n    boot\n  fi\nfi\necho \"Windows bootloader not found\"\nsleep 5\n"
    );

    fs::write(grub_dir.join("grub.cfg"), grub_cfg.as_bytes()).context("writing grub.cfg")?;
    let boot_grub_dir = esp_root.join("boot/grub");
    fs::create_dir_all(&boot_grub_dir).ok();
    let _ = fs::write(boot_grub_dir.join("grub.cfg"), grub_cfg.as_bytes());

    let mut cfg_file = tempfile::Builder::new()
        .prefix("grub-")
        .suffix(".cfg")
        .tempfile_in("/tmp")
        .context("creating grub cfg")?;
    cfg_file
        .write_all(grub_cfg.as_bytes())
        .context("writing grub cfg")?;
    cfg_file.flush().ok();
    let (_file, cfg_path) = cfg_file.keep().context("persisting grub cfg")?;

    let mut secure_x64 = false;
    if let Some(signed) = find_signed_bootloader(BootArch::X64) {
        if let Err(err) = copy_grub_modules(BootArch::X64, esp_root) {
            if secure_only {
                bail!("Secure Boot enforced but GRUB modules missing: {err}");
            }
            log(
                emit,
                format!("Signed GRUB modules missing, falling back to unsigned: {err}"),
            );
        } else {
            log(emit, "Using signed shim/grub for x86_64".to_string());
            copy_efi(&signed.shim, &grub_dir.join(BootArch::X64.boot_filename()))?;
            copy_efi(&signed.grub, &grub_dir.join(BootArch::X64.grub_filename()))?;
            if let Some(mok) = signed.mok {
                let target = grub_dir.join(
                    mok.file_name()
                        .unwrap_or_else(|| std::ffi::OsStr::new("mmx64.efi")),
                );
                let _ = copy_efi(&mok, &target);
            }
            secure_x64 = true;
        }
    }

    if !secure_x64 {
        if secure_only {
            bail!("Secure Boot is enforced but signed x86_64 shim/grub is missing");
        }
        build_grub_standalone(
            BootArch::X64,
            &grub_dir.join(BootArch::X64.boot_filename()),
            &cfg_path,
            emit,
        )?;
    }

    for arch in [BootArch::Ia32, BootArch::Aa64] {
        let arch_label = arch.label();
        if let Some(signed) = find_signed_bootloader(arch) {
            if let Err(err) = copy_grub_modules(arch, esp_root) {
                log(
                    emit,
                    format!(
                        "Skipping signed {arch_label} loader (modules missing: {err})"
                    ),
                );
                if secure_only {
                    continue;
                }
            } else {
                log(emit, format!("Using signed shim/grub for {arch_label}"));
                let _ = copy_efi(&signed.shim, &grub_dir.join(arch.boot_filename()));
                let _ = copy_efi(&signed.grub, &grub_dir.join(arch.grub_filename()));
                if let Some(mok) = signed.mok {
                    let target = grub_dir.join(
                        mok.file_name()
                            .unwrap_or_else(|| std::ffi::OsStr::new("mmx.efi")),
                    );
                    let _ = copy_efi(&mok, &target);
                }
                continue;
            }
        }

        if !command_exists("grub-mkstandalone") {
            log(
                emit,
                format!(
                    "Skipping {arch_label} loader (grub-mkstandalone not available)"
                ),
            );
            continue;
        }

        if secure_only {
            log(
                emit,
                format!(
                    "Secure Boot enforced; skipping unsigned {arch_label} loader"
                ),
            );
            continue;
        }

        if let Err(err) = build_grub_standalone(
            arch,
            &grub_dir.join(arch.boot_filename()),
            &cfg_path,
            emit,
        ) {
            log(emit, format!("Failed to build {arch_label} loader: {err}"));
        }
    }

    let _ = fs::remove_file(cfg_path);
    Ok(secure_x64)
}

fn build_grub_standalone(
    arch: BootArch,
    output_path: &Path,
    cfg_path: &Path,
    emit: &mut dyn FnMut(UiEvent),
) -> Result<()> {
    let args = vec![
        "-O".to_string(),
        arch.grub_target().to_string(),
        "-o".to_string(),
        output_path.to_string_lossy().to_string(),
        "--modules=part_gpt part_msdos fat ntfs chain search_fs_label".to_string(),
        format!(
            "boot/grub/grub.cfg={cfg}",
            cfg = cfg_path.to_string_lossy()
        ),
    ];
    run_cmd(emit, "grub-mkstandalone", &args, "grub-mkstandalone")
}

fn copy_efi(src: &Path, dst: &Path) -> Result<()> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).ok();
    }
    fs::copy(src, dst).with_context(|| {
        format!(
            "copying {src} to {dst}",
            src = src.display(),
            dst = dst.display()
        )
    })?;
    Ok(())
}

fn log(emit: &mut dyn FnMut(UiEvent), msg: String) {
    emit(UiEvent::Log(msg));
}
