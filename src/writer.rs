#![forbid(unsafe_code)]

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileTypeExt;
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
    pub verify_after: bool,
    pub checksum_sha256: Option<String>,
    pub signature_path: Option<PathBuf>,
    pub persistence_size_mib: u64,
    pub persistence_label: String,
    pub dry_run: bool,
}

#[derive(Debug)]
pub enum UiEvent {
    Log(String),
    Progress(f64),
    Done(Result<(), String>),
}

struct ProgressState {
    total: u64,
    completed: u64,
    last_emit: Instant,
}

impl ProgressState {
    fn new(total: u64) -> Self {
        Self {
            total: total.max(1),
            completed: 0,
            last_emit: Instant::now(),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn update(&mut self, emit: &mut dyn FnMut(UiEvent), completed: u64, force: bool) {
        self.completed = completed.min(self.total);
        if force || self.last_emit.elapsed() >= Duration::from_millis(200) {
            let frac = (self.completed as f64 / self.total as f64).clamp(0.0, 1.0);
            emit(UiEvent::Progress(frac));
            self.last_emit = Instant::now();
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn update_stage(
        &mut self,
        emit: &mut dyn FnMut(UiEvent),
        base: u64,
        stage_size: u64,
        frac: f64,
        force: bool,
    ) {
        if stage_size == 0 {
            self.update(emit, base, force);
            return;
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let stage_done = ((stage_size as f64) * frac).round() as u64;
        self.update(emit, base.saturating_add(stage_done), force);
    }

    const fn stage(&mut self, base: u64, size: u64) -> ProgressStage<'_> {
        ProgressStage::new(self, base, size)
    }
}

struct ProgressStage<'a> {
    state: &'a mut ProgressState,
    base: u64,
    size: u64,
    done: u64,
}

impl<'a> ProgressStage<'a> {
    const fn new(state: &'a mut ProgressState, base: u64, size: u64) -> Self {
        Self {
            state,
            base,
            size,
            done: 0,
        }
    }

    fn advance(&mut self, emit: &mut dyn FnMut(UiEvent), delta: u64) {
        if self.size == 0 {
            return;
        }
        self.done = self.done.saturating_add(delta).min(self.size);
        self.state
            .update(emit, self.base.saturating_add(self.done), false);
    }

    #[allow(clippy::cast_precision_loss)]
    fn set_fraction(&mut self, emit: &mut dyn FnMut(UiEvent), frac: f64) {
        if self.size == 0 {
            return;
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        { self.done = ((self.size as f64) * frac).round() as u64; }
        if self.done > self.size {
            self.done = self.size;
        }
        self.state
            .update(emit, self.base.saturating_add(self.done), false);
    }

    fn finish(&mut self, emit: &mut dyn FnMut(UiEvent)) {
        self.done = self.size;
        self.state
            .update(emit, self.base.saturating_add(self.size), true);
    }
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
        ensure_iso_is_not_device(&plan.iso_path, &plan.device_path)?;
        verify_iso(plan, &mut emit)?;

        if plan.dry_run {
            log(&mut emit, "Dry run enabled (no writes will be performed)".to_string());
        }

        let mode = match plan.image_mode {
            ImageMode::Auto => detect_image_mode(&plan.iso_path, &mut emit),
            other => other,
        };

        match mode {
            ImageMode::IsoHybridDd => write_dd(plan, &mut emit),
            ImageMode::WindowsUefi => write_windows(plan, &mut emit),
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
            .arg("l")
            .arg("-ba")
            .arg(path)
            .stdin(Stdio::null())
            .output()
            .ok()?;
        if output.status.success() {
            return Some(String::from_utf8_lossy(&output.stdout).to_string());
        }
    }

    if command_exists("bsdtar") {
        let output = Command::new("bsdtar")
            .arg("-tf")
            .arg(path)
            .stdin(Stdio::null())
            .output()
            .ok()?;
        if output.status.success() {
            return Some(String::from_utf8_lossy(&output.stdout).to_string());
        }
    }

    None
}

fn verify_iso(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    if let Some(checksum) = plan.checksum_sha256.as_deref() {
        let expected = resolve_checksum(checksum)?;
        log(emit, "Verifying SHA256 checksum".to_string());
        let actual = sha256_file(&plan.iso_path)?;
        if !actual.eq_ignore_ascii_case(&expected) {
            bail!("Checksum mismatch (expected {expected}, got {actual})");
        }
        log(emit, "Checksum verified".to_string());
    }

    if let Some(signature) = plan.signature_path.as_ref() {
        verify_signature(&plan.iso_path, signature, emit)?;
    }

    Ok(())
}

fn resolve_checksum(input: &str) -> Result<String> {
    let candidate = input.trim();
    if candidate.is_empty() {
        bail!("Checksum is empty");
    }
    if Path::new(candidate).exists() {
        let data =
            fs::read_to_string(candidate).with_context(|| format!("reading checksum {candidate}"))?;
        parse_checksum_text(&data)
    } else {
        parse_checksum_text(candidate)
    }
}

fn parse_checksum_text(text: &str) -> Result<String> {
    for token in text.split_whitespace() {
        if is_sha256_hex(token) {
            return Ok(token.to_lowercase());
        }
    }
    bail!("No valid SHA256 checksum found");
}

fn is_sha256_hex(token: &str) -> bool {
    token.len() == 64 && token.chars().all(|c| c.is_ascii_hexdigit())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("opening {path}", path = path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).context("reading file")?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    Ok(format!("{digest:x}"))
}

fn verify_signature(
    iso_path: &Path,
    signature_path: &Path,
    emit: &mut dyn FnMut(UiEvent),
) -> Result<()> {
    if !command_exists("gpg") {
        bail!("gpg not found; cannot verify signature");
    }
    log(emit, "Verifying signature".to_string());
    let output = Command::new("gpg")
        .arg("--verify")
        .arg(signature_path)
        .arg(iso_path)
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("running gpg on {sig}", sig = signature_path.display()))?;
    if output.status.success() {
        log(emit, "Signature verified".to_string());
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(anyhow!("Signature verification failed: {stderr}"))
    }
}

fn write_dd(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    let iso_size = plan.iso_path.metadata().context("reading ISO size")?.len();
    if let Some(device_size) = plan.device_size_bytes
        && iso_size > device_size
    {
        bail!("ISO is larger than the selected device");
    }

    if plan.dry_run {
        log(emit, "Dry run: would write ISO in DD mode".to_string());
        if plan.persistence_size_mib > 0 {
            validate_persistence(plan, emit)?;
        }
        return Ok(());
    }

    log(emit, "Preparing device (unmounting)".to_string());
    unmount_device(&plan.device_path, emit)?;

    log(emit, "Writing ISO (DD mode)".to_string());

    let mut src = File::open(&plan.iso_path).context("opening ISO")?;
    ensure_block_device(&plan.device_path)?;
    let mut dst = OpenOptions::new()
        .write(true)
        .open(&plan.device_path)
        .with_context(|| format!("opening device {device_path}", device_path = plan.device_path))?;

    let verify_bytes = if plan.verify_after { iso_size } else { 0 };
    let total = iso_size.saturating_add(verify_bytes).max(1);
    let mut written: u64 = 0;
    let mut buffer = vec![0u8; 4 * 1024 * 1024];
    let mut progress = ProgressState::new(total);

    loop {
        let read = src.read(&mut buffer).context("reading ISO")?;
        if read == 0 {
            break;
        }
        dst.write_all(&buffer[..read]).context("writing device")?;
        written += read as u64;

        progress.update(emit, written, false);
    }

    dst.sync_all().context("syncing device")?;
    log(emit, "Syncing buffers".to_string());
    let status = Command::new("sync").status().context("running sync")?;
    if !status.success() {
        bail!("sync failed: {status}");
    }
    progress.update(emit, written, true);

    if plan.verify_after {
        log(emit, "Verifying written data".to_string());
        let mut verify_stage = progress.stage(written, iso_size);
        verify_dd_write(plan, emit, Some(&mut verify_stage))?;
        verify_stage.finish(emit);
    }

    if plan.persistence_size_mib > 0 {
        log(emit, "Creating persistence partition".to_string());
        apply_persistence(plan, emit)?;
    }

    progress.update(emit, total, true);
    log(emit, "DD write completed".to_string());
    Ok(())
}

fn write_windows(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    match plan.target_system {
        TargetSystem::Uefi => write_windows_uefi(plan, emit),
        TargetSystem::Bios => write_windows_bios(plan, emit),
        TargetSystem::UefiAndBios => write_windows_uefi_bios(plan, emit),
    }
}

fn write_windows_uefi(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    match plan.file_system {
        FileSystem::Fat32 => write_windows_fat32(plan, emit, false),
        FileSystem::Ntfs => write_windows_ntfs_uefi(plan, emit),
    }
}

fn write_windows_bios(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    match plan.file_system {
        FileSystem::Fat32 => write_windows_fat32(plan, emit, true),
        FileSystem::Ntfs => write_windows_ntfs_bios(plan, emit),
    }
}

fn write_windows_uefi_bios(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    match plan.file_system {
        FileSystem::Fat32 => write_windows_fat32(plan, emit, true),
        FileSystem::Ntfs => write_windows_ntfs_uefi_bios(plan, emit),
    }
}

#[allow(clippy::too_many_lines)]
fn write_windows_fat32(
    plan: &WritePlan,
    emit: &mut dyn FnMut(UiEvent),
    install_bios: bool,
) -> Result<()> {
    let mut required = vec!["parted", "mkfs.vfat", "mount", "umount", "rsync"];
    if install_bios {
        required.push("grub-install");
    }
    for cmd in required {
        if !command_exists(cmd) {
            bail!("Required tool not found: {cmd}");
        }
    }

    let iso_size = plan.iso_path.metadata().context("reading ISO size")?.len();
    let fs_overhead = 10 * 1024 * 1024u64; // 10 MiB for partition alignment + FAT32 metadata
    if let Some(device_size) = plan.device_size_bytes
        && iso_size.saturating_add(fs_overhead) > device_size
    {
        bail!("ISO is too large for the selected device (accounting for filesystem overhead)");
    }

    if plan.dry_run {
        log(emit, "Dry run: would format and copy Windows files (FAT32)".to_string());
        return Ok(());
    }

    log(emit, "Preparing device (unmounting)".to_string());
    unmount_device(&plan.device_path, emit)?;

    log(emit, "Partitioning device".to_string());
    let scheme = if install_bios {
        if plan.partition_scheme != PartitionScheme::Mbr {
            log(
                emit,
                "BIOS support requires MBR; switching partition scheme to MBR".to_string(),
            );
        }
        PartitionScheme::Mbr
    } else {
        plan.partition_scheme
    };
    create_partition(&plan.device_path, scheme, emit)?;

    let partition = partition_path(&plan.device_path);
    let label = sanitize_fat_label(&plan.volume_label);
    if label != plan.volume_label {
        log(emit, format!("Volume label sanitized: \"{}\" → \"{}\"", plan.volume_label, label));
    }
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
            "--exclude=/sources/install.wim".to_string(),
            "--exclude=/sources/install.esd".to_string(),
        ];
        let wim_path = iso_dir.path().join("sources/install.wim");
        let esd_path = iso_dir.path().join("sources/install.esd");
        let install_image = if wim_path.exists() {
            Some(wim_path)
        } else if esd_path.exists() {
            Some(esd_path)
        } else {
            None
        };
        let install_size = match install_image.as_ref() {
            Some(path) => path.metadata().context("reading install image size")?.len(),
            None => 0,
        };
        let iso_total_bytes = match dir_size(iso_dir.path()) {
            Ok(total) => total,
            Err(err) => {
                log(
                    emit,
                    format!("Failed to size ISO contents for progress: {err}"),
                );
                iso_size
            }
        };
        let total_bytes = iso_total_bytes.max(install_size).max(1);
        let rsync_bytes = if install_image.is_some() {
            total_bytes.saturating_sub(install_size)
        } else {
            total_bytes
        };
        let mut progress = ProgressState::new(total_bytes);
        progress.update(emit, 0, true);

        {
            let mut rsync_emit = |event| match event {
                UiEvent::Progress(frac) => {
                    progress.update_stage(emit, 0, rsync_bytes, frac, false);
                }
                UiEvent::Log(msg) => emit(UiEvent::Log(msg)),
                UiEvent::Done(_) => {}
            };
            run_rsync_with_progress(&mut rsync_emit, iso_dir.path(), usb_dir.path(), &rsync_args)?;
        }
        progress.update(emit, rsync_bytes, true);

        if let Some(install_image) = install_image {
            let install_stage = progress.stage(rsync_bytes, install_size);
            handle_wim(&install_image, usb_dir.path(), emit, Some(install_stage))?;
        } else {
            log(emit, "No install.wim/esd found; ISO may be non-Windows".to_string());
        }

        if install_bios {
            install_bios_grub(&plan.device_path, usb_dir.path(), FileSystem::Fat32, emit)?;
        }

        if plan.verify_after {
            let verify_args = vec![
                "--exclude=/sources/install.wim".to_string(),
                "--exclude=/sources/install.esd".to_string(),
            ];
            verify_tree_with_rsync(emit, iso_dir.path(), usb_dir.path(), &verify_args)?;
            verify_windows_install_media(iso_dir.path(), usb_dir.path(), emit)?;
        }

        Ok(())
    })();

    log(emit, "Unmounting USB".to_string());
    let _ = Command::new("umount").arg(usb_dir.path()).status();

    log(emit, "Unmounting ISO".to_string());
    let _ = Command::new("umount").arg(iso_dir.path()).status();

    copy_result?;

    log(emit, "Syncing buffers".to_string());
    let status = Command::new("sync").status().context("running sync")?;
    if !status.success() {
        bail!("sync failed: {status}");
    }
    emit(UiEvent::Progress(1.0));
    log(emit, "Windows FAT32 write completed".to_string());
    Ok(())
}

fn write_windows_ntfs_uefi(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    let scheme = plan.partition_scheme;
    if scheme == PartitionScheme::Mbr {
        log(
            emit,
            "UEFI:NTFS on MBR is enabled (some firmware may prefer GPT)".to_string(),
        );
    }
    write_windows_ntfs_with_esp(plan, emit, scheme, false)
}

fn write_windows_ntfs_bios(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    for cmd in ["parted", "mount", "umount", "rsync", "grub-install"] {
        if !command_exists(cmd) {
            bail!("Required tool not found: {cmd}");
        }
    }

    let mkfs_ntfs = if command_exists("mkfs.ntfs") {
        "mkfs.ntfs"
    } else if command_exists("mkfs.ntfs3") {
        "mkfs.ntfs3"
    } else {
        bail!("Required tool not found: mkfs.ntfs or mkfs.ntfs3");
    };

    let iso_size = plan.iso_path.metadata().context("reading ISO size")?.len();
    let fs_overhead = 10 * 1024 * 1024u64; // 10 MiB for partition alignment + NTFS metadata
    if let Some(device_size) = plan.device_size_bytes
        && iso_size.saturating_add(fs_overhead) > device_size
    {
        bail!("ISO is too large for the selected device (accounting for filesystem overhead)");
    }

    if plan.dry_run {
        log(emit, "Dry run: would format and copy Windows files (NTFS BIOS)".to_string());
        return Ok(());
    }

    log(emit, "Preparing device (unmounting)".to_string());
    unmount_device(&plan.device_path, emit)?;

    log(emit, "Partitioning device (NTFS)".to_string());
    let scheme = if plan.partition_scheme == PartitionScheme::Mbr {
        plan.partition_scheme
    } else {
        log(
            emit,
            "BIOS support requires MBR; switching partition scheme to MBR".to_string(),
        );
        PartitionScheme::Mbr
    };
    create_ntfs_partition(&plan.device_path, scheme, emit)?;

    let partition = partition_path(&plan.device_path);
    log(emit, "Waiting for partition device".to_string());
    wait_for_device_node(&partition).with_context(|| format!("waiting for {partition}"))?;

    let ntfs_label = sanitize_ntfs_label(&plan.volume_label);
    if ntfs_label != plan.volume_label {
        log(emit, format!("Volume label sanitized: \"{}\" → \"{}\"", plan.volume_label, ntfs_label));
    }
    log(emit, "Formatting NTFS".to_string());
    let mkfs_ntfs_args = vec![
        "-F".to_string(),
        "-L".to_string(),
        ntfs_label,
        partition.clone(),
    ];
    run_cmd(emit, mkfs_ntfs, &mkfs_ntfs_args, "mkfs.ntfs")?;

    let iso_dir = tempfile::tempdir().context("creating temp dir for ISO")?;
    let usb_dir = tempfile::tempdir().context("creating temp dir for NTFS")?;

    log(emit, "Mounting ISO".to_string());
    let mount_iso_args = vec![
        "-o".to_string(),
        "loop,ro".to_string(),
        plan.iso_path.to_string_lossy().to_string(),
        iso_dir.path().to_string_lossy().to_string(),
    ];
    run_cmd(emit, "mount", &mount_iso_args, "mount ISO")?;

    log(emit, "Mounting NTFS".to_string());
    let mount_usb_args = vec![partition, usb_dir.path().to_string_lossy().to_string()];
    if let Err(err) = run_cmd(emit, "mount", &mount_usb_args, "mount NTFS") {
        let _ = Command::new("umount").arg(iso_dir.path()).status();
        return Err(err);
    }

    let copy_result = (|| -> Result<()> {
        log(emit, "Copying files to NTFS".to_string());
        let rsync_args = Vec::new();
        run_rsync_with_progress(emit, iso_dir.path(), usb_dir.path(), &rsync_args)?;
        install_bios_grub(&plan.device_path, usb_dir.path(), FileSystem::Ntfs, emit)?;
        if plan.verify_after {
            verify_tree_with_rsync(emit, iso_dir.path(), usb_dir.path(), &rsync_args)?;
        }
        Ok(())
    })();

    log(emit, "Unmounting NTFS".to_string());
    let _ = Command::new("umount").arg(usb_dir.path()).status();
    log(emit, "Unmounting ISO".to_string());
    let _ = Command::new("umount").arg(iso_dir.path()).status();

    copy_result?;

    log(emit, "Syncing buffers".to_string());
    let status = Command::new("sync").status().context("running sync")?;
    if !status.success() {
        bail!("sync failed: {status}");
    }
    emit(UiEvent::Progress(1.0));
    log(emit, "Windows BIOS NTFS write completed".to_string());
    Ok(())
}

fn write_windows_ntfs_uefi_bios(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    let scheme = if plan.partition_scheme == PartitionScheme::Mbr {
        plan.partition_scheme
    } else {
        log(
            emit,
            "UEFI+BIOS with NTFS requires MBR; switching partition scheme to MBR".to_string(),
        );
        PartitionScheme::Mbr
    };
    write_windows_ntfs_with_esp(plan, emit, scheme, true)
}

#[allow(clippy::too_many_lines)]
fn write_windows_ntfs_with_esp(
    plan: &WritePlan,
    emit: &mut dyn FnMut(UiEvent),
    scheme: PartitionScheme,
    install_bios: bool,
) -> Result<()> {
    let iso_size = plan.iso_path.metadata().context("reading ISO size")?.len();
    if let Some(device_size) = plan.device_size_bytes {
        let overhead = 260 * 1024 * 1024u64;
        if iso_size.saturating_add(overhead) > device_size {
            bail!("ISO is too large for the selected device");
        }
    }

    let mut required = vec!["parted", "mkfs.vfat", "mount", "umount", "rsync"];
    if install_bios {
        required.push("grub-install");
    }
    for cmd in required {
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

    if plan.dry_run {
        log(
            emit,
            "Dry run: would format ESP+NTFS and copy Windows files".to_string(),
        );
        return Ok(());
    }

    log(emit, "Preparing device (unmounting)".to_string());
    unmount_device(&plan.device_path, emit)?;

    log(emit, "Partitioning device (ESP + NTFS)".to_string());
    create_windows_partitions_ntfs(&plan.device_path, scheme, emit)?;

    let esp_partition = partition_path_for(&plan.device_path, 1);
    let data_partition = partition_path_for(&plan.device_path, 2);
    log(emit, "Waiting for partition devices".to_string());
    wait_for_device_node(&esp_partition).with_context(|| format!("waiting for {esp_partition}"))?;
    wait_for_device_node(&data_partition).with_context(|| format!("waiting for {data_partition}"))?;

    let esp_label = "BOOT".to_string();
    let ntfs_label = sanitize_ntfs_label(&plan.volume_label);
    if ntfs_label != plan.volume_label {
        log(emit, format!("Volume label sanitized: \"{}\" → \"{}\"", plan.volume_label, ntfs_label));
    }

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
        let rsync_args = Vec::new();
        run_rsync_with_progress(emit, iso_dir.path(), data_dir.path(), &rsync_args)?;

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

        if install_bios {
            install_bios_grub(&plan.device_path, data_dir.path(), FileSystem::Ntfs, emit)?;
        }

        if plan.verify_after {
            verify_tree_with_rsync(emit, iso_dir.path(), data_dir.path(), &rsync_args)?;
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
    let status = Command::new("sync").status().context("running sync")?;
    if !status.success() {
        bail!("sync failed: {status}");
    }
    emit(UiEvent::Progress(1.0));
    log(emit, "Windows NTFS write completed".to_string());
    Ok(())
}

fn handle_wim(
    wim_path: &Path,
    usb_root: &Path,
    emit: &mut dyn FnMut(UiEvent),
    mut stage: Option<ProgressStage<'_>>,
) -> Result<()> {
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
        copy_file_buffered(wim_path, &dest_path, emit, stage.as_mut())?;
        if let Some(stage) = stage.as_mut() {
            stage.finish(emit);
        }
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
        log(emit, "Running: wimlib-imagex split".to_string());
        {
            let mut progress_cb = |event| match event {
                CmdEvent::Progress(frac) => {
                    if let Some(stage) = stage.as_mut() {
                        stage.set_fraction(emit, frac);
                    }
                }
                CmdEvent::Log(line, _is_err) => {
                    if !line.is_empty() {
                        emit(UiEvent::Log(format!("wimlib-imagex: {line}")));
                    }
                }
            };
            run_cmd_with_progress("wimlib-imagex", &args, &mut progress_cb)?;
        }
        if let Some(stage) = stage.as_mut() {
            stage.finish(emit);
        }
        return Ok(());
    }

    if command_exists("wimsplit") {
        let args = vec![
            wim_path.to_string_lossy().to_string(),
            split_target_str,
            "4000".to_string(),
        ];
        log(emit, "Running: wimsplit".to_string());
        {
            let mut progress_cb = |event| match event {
                CmdEvent::Progress(frac) => {
                    if let Some(stage) = stage.as_mut() {
                        stage.set_fraction(emit, frac);
                    }
                }
                CmdEvent::Log(line, _is_err) => {
                    if !line.is_empty() {
                        emit(UiEvent::Log(format!("wimsplit: {line}")));
                    }
                }
            };
            run_cmd_with_progress("wimsplit", &args, &mut progress_cb)?;
        }
        if let Some(stage) = stage.as_mut() {
            stage.finish(emit);
        }
        return Ok(());
    }

    bail!("wimlib-imagex or wimsplit is required to split large install.wim/esd");
}

fn copy_file_buffered(
    src: &Path,
    dst: &Path,
    emit: &mut dyn FnMut(UiEvent),
    mut stage: Option<&mut ProgressStage<'_>>,
) -> Result<()> {
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
        if let Some(stage) = stage.as_mut() {
            stage.advance(emit, read as u64);
        }
    }
    output.sync_all().context("syncing output file")?;
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

fn create_ntfs_partition(
    device: &str,
    scheme: PartitionScheme,
    emit: &mut dyn FnMut(UiEvent),
) -> Result<()> {
    if scheme != PartitionScheme::Mbr {
        bail!("NTFS BIOS mode requires MBR partition scheme");
    }

    let args = vec![
        "-s".to_string(),
        device.to_string(),
        "mklabel".to_string(),
        "msdos".to_string(),
    ];
    run_cmd(emit, "parted", &args, "parted mklabel")?;

    let mkpart_args = vec![
        "-s".to_string(),
        device.to_string(),
        "mkpart".to_string(),
        "primary".to_string(),
        "ntfs".to_string(),
        "1MiB".to_string(),
        "100%".to_string(),
    ];
    run_cmd(emit, "parted", &mkpart_args, "parted mkpart")?;

    let set_args = vec![
        "-s".to_string(),
        device.to_string(),
        "set".to_string(),
        "1".to_string(),
        "boot".to_string(),
        "on".to_string(),
    ];
    run_cmd(emit, "parted", &set_args, "parted set boot")?;

    refresh_partition_table(device);

    Ok(())
}

fn create_windows_partitions_ntfs(
    device: &str,
    scheme: PartitionScheme,
    emit: &mut dyn FnMut(UiEvent),
) -> Result<()> {
    let label = match scheme {
        PartitionScheme::Gpt => "gpt",
        PartitionScheme::Mbr => "msdos",
    };
    let args = vec![
        "-s".to_string(),
        device.to_string(),
        "mklabel".to_string(),
        label.to_string(),
    ];
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

    let flag = match scheme {
        PartitionScheme::Gpt => "esp",
        PartitionScheme::Mbr => "boot",
    };
    let set_esp = vec![
        "-s".to_string(),
        device.to_string(),
        "set".to_string(),
        "1".to_string(),
        flag.to_string(),
        "on".to_string(),
    ];
    run_cmd(emit, "parted", &set_esp, "parted set flag")?;

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

struct PartedInfo {
    device_size_mib: f64,
    last_end_mib: f64,
}

fn validate_persistence(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    if plan.persistence_size_mib == 0 {
        return Ok(());
    }
    for cmd in ["parted", "mkfs.ext4"] {
        if !command_exists(cmd) {
            bail!("Persistence requires {cmd}");
        }
    }
    let info = parted_info(&plan.device_path, plan.device_size_bytes, emit)?;
    let (_start, end) = persistence_bounds(&info, plan.persistence_size_mib)?;
    if end > info.device_size_mib {
        bail!("Not enough free space for persistence");
    }
    Ok(())
}

fn apply_persistence(plan: &WritePlan, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    if plan.persistence_size_mib == 0 {
        return Ok(());
    }
    for cmd in ["parted", "mkfs.ext4"] {
        if !command_exists(cmd) {
            bail!("Persistence requires {cmd}");
        }
    }

    let info = parted_info(&plan.device_path, plan.device_size_bytes, emit)?;
    let (start, end) = persistence_bounds(&info, plan.persistence_size_mib)?;
    let start_arg = format!("{start}MiB");
    let end_arg = format!("{end}MiB");
    let mkpart_args = vec![
        "-s".to_string(),
        plan.device_path.clone(),
        "mkpart".to_string(),
        "primary".to_string(),
        "ext4".to_string(),
        start_arg,
        end_arg,
    ];
    run_cmd(emit, "parted", &mkpart_args, "parted mkpart persistence")?;
    refresh_partition_table(&plan.device_path);

    let index = find_partition_for_range(&plan.device_path, start, end)?;
    let partition = partition_path_for(&plan.device_path, index);
    log(
        emit,
        format!("Waiting for persistence device node {partition}"),
    );
    wait_for_device_node(&partition).with_context(|| format!("waiting for {partition}"))?;

    let label = sanitize_ext4_label(&plan.persistence_label);
    if label != plan.persistence_label {
        log(emit, format!("Persistence label sanitized: \"{}\" → \"{}\"", plan.persistence_label, label));
    }
    let mkfs_args = vec![
        "-F".to_string(),
        "-L".to_string(),
        label.clone(),
        partition.clone(),
    ];
    run_cmd(emit, "mkfs.ext4", &mkfs_args, "mkfs.ext4")?;

    if label == "persistence" {
        create_persistence_conf(&partition, emit)?;
    }

    Ok(())
}

fn persistence_bounds(info: &PartedInfo, size_mib: u64) -> Result<(f64, f64)> {
    if size_mib == 0 {
        bail!("Persistence size must be greater than 0");
    }
    let start = (info.last_end_mib + 1.0).ceil();
    #[allow(clippy::cast_precision_loss)]
    let end = start + size_mib as f64;
    let max_end = info.device_size_mib - 4.0;
    if end > max_end {
        bail!("Not enough free space for persistence");
    }
    Ok((start, end))
}

fn find_partition_for_range(device: &str, start_mib: f64, end_mib: f64) -> Result<u32> {
    const TOLERANCE_MIB: f64 = 2.0;
    const MAX_ATTEMPTS: usize = 50;

    let mut last_err: Option<anyhow::Error> = None;
    for _ in 0..MAX_ATTEMPTS {
        match partitions_from_parted(device) {
            Ok(partitions) => {
                for (num, part_start, part_end) in partitions {
                    let start_delta = (part_start - start_mib).abs();
                    let end_delta = (part_end - end_mib).abs();
                    if start_delta <= TOLERANCE_MIB && end_delta <= TOLERANCE_MIB {
                        return Ok(num);
                    }
                }
            }
            Err(err) => last_err = Some(err),
        }
        thread::sleep(Duration::from_millis(100));
    }

    if let Some(err) = last_err {
        bail!("Failed to determine persistence partition: {err}");
    }
    bail!("Timed out waiting for persistence partition");
}

fn partitions_from_parted(device: &str) -> Result<Vec<(u32, f64, f64)>> {
    let output = Command::new("parted")
        .args(["-ms", device, "unit", "MiB", "print"])
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("running parted on {device}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("parted failed: {stderr}"));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut partitions = Vec::new();
    for line in stdout.lines() {
        if line.trim().is_empty() || line.starts_with("BYT;") || line.starts_with(device) {
            continue;
        }
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() < 3 {
            continue;
        }
        let Ok(num) = parts[0].parse::<u32>() else {
            continue;
        };
        if let (Some(start), Some(end)) = (parse_mib(parts[1]), parse_mib(parts[2])) {
            partitions.push((num, start, end));
        }
    }
    Ok(partitions)
}

fn parted_info(
    device: &str,
    device_size_bytes: Option<u64>,
    emit: &mut dyn FnMut(UiEvent),
) -> Result<PartedInfo> {
    let output = Command::new("parted")
        .args(["-ms", device, "unit", "MiB", "print"])
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("running parted on {device}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("parted failed: {stderr}"));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut device_size_mib = None;
    let mut last_end = 1.0;

    for line in stdout.lines() {
        if line.trim().is_empty() || line.starts_with("BYT;") {
            continue;
        }
        if line.starts_with(device) {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() > 1 {
                device_size_mib = parse_mib(parts[1]);
            }
            continue;
        }
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() < 3 {
            continue;
        }
        if parts[0].parse::<u32>().is_err() {
            continue;
        }
        if let Some(end) = parse_mib(parts[2])
            && end > last_end
        {
            last_end = end;
        }
    }

    #[allow(clippy::cast_precision_loss)]
    let fallback_size = device_size_bytes.map(|bytes| bytes as f64 / 1024.0 / 1024.0);
    let device_size_mib = device_size_mib.or(fallback_size).ok_or_else(|| {
        anyhow!("Failed to determine device size for persistence")
    })?;

    log(
        emit,
        format!(
            "Persistence plan: last end {last_end:.1} MiB, device {device_size_mib:.1} MiB"
        ),
    );

    Ok(PartedInfo {
        device_size_mib,
        last_end_mib: last_end,
    })
}

fn parse_mib(text: &str) -> Option<f64> {
    let trimmed = text.trim().trim_end_matches("MiB");
    trimmed.parse::<f64>().ok()
}

fn sanitize_ext4_label(label: &str) -> String {
    let mut sanitized = String::new();
    for ch in label.chars() {
        if sanitized.len() >= 16 {
            break;
        }
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            sanitized.push(ch);
        }
    }
    let trimmed = sanitized.trim();
    if trimmed.is_empty() {
        "persistence".to_string()
    } else {
        trimmed.to_string()
    }
}

fn create_persistence_conf(partition: &str, emit: &mut dyn FnMut(UiEvent)) -> Result<()> {
    if !command_exists("mount") || !command_exists("umount") {
        bail!("Persistence label is 'persistence' but mount/umount are missing — cannot write persistence.conf");
    }
    let mount_dir = tempfile::tempdir().context("creating persistence mount dir")?;
    let mount_args = vec![
        partition.to_string(),
        mount_dir.path().to_string_lossy().to_string(),
    ];
    run_cmd(emit, "mount", &mount_args, "mount persistence")?;
    let conf_path = mount_dir.path().join("persistence.conf");
    fs::write(&conf_path, "/ union\n").context("writing persistence.conf")?;
    let umount_args = vec![mount_dir.path().to_string_lossy().to_string()];
    run_cmd(emit, "umount", &umount_args, "umount persistence")?;
    Ok(())
}

fn verify_dd_write(
    plan: &WritePlan,
    emit: &mut dyn FnMut(UiEvent),
    mut progress: Option<&mut ProgressStage<'_>>,
) -> Result<()> {
    let iso_size = plan.iso_path.metadata().context("reading ISO size")?.len();
    let mut src = File::open(&plan.iso_path).context("opening ISO for verify")?;
    let mut dst = File::open(&plan.device_path)
        .with_context(|| format!("opening device {device}", device = plan.device_path))?;
    let mut buffer_src = vec![0u8; 4 * 1024 * 1024];
    let mut buffer_dst = vec![0u8; 4 * 1024 * 1024];
    let mut compared: u64 = 0;
    let mut last_update = Instant::now();

    loop {
        let read_src = src.read(&mut buffer_src).context("reading ISO")?;
        if read_src == 0 {
            break;
        }
        dst.read_exact(&mut buffer_dst[..read_src])
            .context("reading device")?;
        if buffer_src[..read_src] != buffer_dst[..read_src] {
            bail!("Verification failed at offset {compared}");
        }
        compared += read_src as u64;
        if let Some(stage) = progress.as_mut() {
            stage.advance(emit, read_src as u64);
        }
        if last_update.elapsed() >= Duration::from_millis(500) {
            log(
                emit,
                format!(
                    "Verifying... {percent:.0}%",
                    percent = {
                        #[allow(clippy::cast_precision_loss)]
                        { (compared as f64 / iso_size.max(1) as f64) * 100.0 }
                    }
                ),
            );
            last_update = Instant::now();
        }
    }

    log(emit, "Verification completed".to_string());
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
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("running {program}"))?;
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(anyhow!("{context} failed: {stderr}"))
    }
}

enum CmdEvent {
    Progress(f64),
    Log(String, bool),
}

fn run_cmd_with_progress<F>(program: &str, args: &[String], handle: &mut F) -> Result<()>
where
    F: FnMut(CmdEvent),
{
    let mut cmd = Command::new(program);
    cmd.args(args);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().with_context(|| format!("spawning {program}"))?;
    let stdout = child.stdout.take().context("capturing command stdout")?;
    let stderr = child.stderr.take().context("capturing command stderr")?;

    let (tx, rx) = mpsc::channel::<CmdEvent>();
    let tx_out = tx.clone();
    let out_thread = spawn_cmd_reader(stdout, tx_out, false);
    let tx_err = tx.clone();
    let err_thread = spawn_cmd_reader(stderr, tx_err, true);
    drop(tx);

    let mut stderr_lines = Vec::new();
    for event in rx {
        if let CmdEvent::Log(line, true) = &event {
            stderr_lines.push(line.clone());
        }
        handle(event);
    }

    let _ = out_thread.join();
    let _ = err_thread.join();

    let status = child.wait().context("waiting for command")?;
    if status.success() {
        Ok(())
    } else if stderr_lines.is_empty() {
        Err(anyhow!("{program} failed: {status}"))
    } else {
        Err(anyhow!("{program} failed: {}", stderr_lines.join("\n")))
    }
}

fn install_bios_grub(
    device_path: &str,
    mount_root: &Path,
    file_system: FileSystem,
    emit: &mut dyn FnMut(UiEvent),
) -> Result<()> {
    if !command_exists("grub-install") {
        bail!("grub-install is required for BIOS support");
    }
    log(emit, "Installing BIOS bootloader (GRUB)".to_string());
    let boot_dir = mount_root.join("boot");
    fs::create_dir_all(&boot_dir).context("creating boot directory")?;

    let mut modules = vec!["part_msdos", "part_gpt", "chain", "search_fs_file"];
    match file_system {
        FileSystem::Fat32 => modules.push("fat"),
        FileSystem::Ntfs => modules.push("ntfs"),
    }
    let modules_arg = format!("--modules={}", modules.join(" "));

    let args = vec![
        "--target=i386-pc".to_string(),
        format!("--boot-directory={}", boot_dir.to_string_lossy()),
        "--recheck".to_string(),
        modules_arg,
        device_path.to_string(),
    ];
    run_cmd(emit, "grub-install", &args, "grub-install")?;

    let grub_dir = boot_dir.join("grub");
    fs::create_dir_all(&grub_dir).context("creating grub directory")?;
    let cfg = windows_bios_grub_cfg(file_system);
    fs::write(grub_dir.join("grub.cfg"), cfg.as_bytes()).context("writing grub.cfg")?;

    Ok(())
}

fn windows_bios_grub_cfg(file_system: FileSystem) -> String {
    let fs_module = match file_system {
        FileSystem::Fat32 => "fat",
        FileSystem::Ntfs => "ntfs",
    };
    format!(
        "set timeout=0\nset default=0\n\nmenuentry \"Windows installer\" {{\n  insmod part_msdos\n  insmod part_gpt\n  insmod {fs_module}\n  insmod chain\n  insmod search_fs_file\n  search --no-floppy --file /bootmgr --set=root\n  chainloader /bootmgr\n  boot\n}}\n"
    )
}

fn run_rsync_with_progress(
    emit: &mut dyn FnMut(UiEvent),
    src: &Path,
    dst: &Path,
    extra_args: &[String],
) -> Result<()> {
    let version = rsync_version();
    let supports_progress2 = version.is_some_and(|v| v >= (3, 1, 0));
    let supports_no_inc = supports_progress2;

    let mut args = vec!["-aH".to_string()];
    if supports_progress2 {
        args.push("--info=progress2".to_string());
        if supports_no_inc {
            args.push("--no-inc-recursive".to_string());
        }
    } else {
        args.push("--progress".to_string());
    }
    args.extend_from_slice(extra_args);
    args.push(format!("{path}/", path = src.to_string_lossy()));
    args.push(format!("{path}/", path = dst.to_string_lossy()));

    let mut cmd = Command::new("rsync");
    cmd.args(&args);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

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

fn verify_tree_with_rsync(
    emit: &mut dyn FnMut(UiEvent),
    src: &Path,
    dst: &Path,
    extra_args: &[String],
) -> Result<()> {
    if !command_exists("rsync") {
        bail!("rsync is required for verification");
    }
    log(emit, "Verifying files (rsync checksum)".to_string());
    let mut args = vec![
        "-aH".to_string(),
        "--checksum".to_string(),
        "--dry-run".to_string(),
        "--itemize-changes".to_string(),
    ];
    args.extend_from_slice(extra_args);
    args.push(format!("{path}/", path = src.to_string_lossy()));
    args.push(format!("{path}/", path = dst.to_string_lossy()));

    let output = Command::new("rsync")
        .args(&args)
        .stdin(Stdio::null())
        .output()
        .context("running rsync verification")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("rsync verification failed: {stderr}");
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut diffs = Vec::new();
    for line in stdout.lines() {
        if is_rsync_verify_diff(line) {
            diffs.push(line.to_string());
            if diffs.len() >= 5 {
                break;
            }
        }
    }
    if diffs.is_empty() {
        log(emit, "Verification OK".to_string());
        Ok(())
    } else {
        bail!("Verification failed: {}", diffs.join("; "));
    }
}

fn is_rsync_verify_diff(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.starts_with("sending incremental file list")
        || trimmed.starts_with("sending ")
        || trimmed.starts_with("sent ")
        || trimmed.starts_with("total size is")
        || trimmed.starts_with("receiving ")
        || trimmed.starts_with("created directory")
    {
        return false;
    }
    true
}

fn verify_windows_install_media(
    iso_root: &Path,
    usb_root: &Path,
    emit: &mut dyn FnMut(UiEvent),
) -> Result<()> {
    let wim_path = iso_root.join("sources/install.wim");
    let esd_path = iso_root.join("sources/install.esd");
    let dest_dir = usb_root.join("sources");
    if !wim_path.exists() && !esd_path.exists() {
        return Ok(());
    }
    let src = if wim_path.exists() { wim_path } else { esd_path };
    let size = src.metadata().context("reading install image size")?.len();
    if size <= FAT32_LIMIT {
        let dest = dest_dir.join(
            src.file_name()
                .ok_or_else(|| anyhow!("invalid install image name"))?,
        );
        if !dest.exists() {
            bail!("Install image missing after copy");
        }
        log(emit, "Verifying install image checksum".to_string());
        let src_hash = sha256_file(&src)?;
        let dst_hash = sha256_file(&dest)?;
        if src_hash != dst_hash {
            bail!("Install image checksum mismatch");
        }
    } else {
        let swm = dest_dir.join("install.swm");
        if !swm.exists() {
            bail!("Expected install.swm not found after split");
        }
    }
    Ok(())
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

fn spawn_cmd_reader<R: Read + Send + 'static>(
    reader: R,
    tx: mpsc::Sender<CmdEvent>,
    is_err: bool,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        let mut buf = [0u8; 4096];
        let mut pending = String::new();
        loop {
            let read = reader.read(&mut buf).unwrap_or(0);
            if read == 0 {
                break;
            }
            pending.push_str(&String::from_utf8_lossy(&buf[..read]));
            while let Some(idx) = find_line_break(&pending) {
                let line = pending[..idx].to_string();
                pending = pending[idx + 1..].to_string();
                handle_cmd_line(&line, &tx, is_err);
            }
        }
        if !pending.is_empty() {
            handle_cmd_line(&pending, &tx, is_err);
        }
    })
}

fn handle_cmd_line(line: &str, tx: &mpsc::Sender<CmdEvent>, is_err: bool) {
    if let Some(frac) = parse_progress_percent(line) {
        let _ = tx.send(CmdEvent::Progress(frac));
        return;
    }

    let trimmed = line.trim_end_matches(['\r', '\n']);
    if !trimmed.is_empty() {
        let _ = tx.send(CmdEvent::Log(trimmed.to_string(), is_err));
    }
}

fn handle_rsync_line(
    line: &str,
    tx: &mpsc::Sender<UiEvent>,
    last_emit: &mut Instant,
    emit_logs: bool,
) {
    if let Some(frac) = parse_progress_percent(line) {
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

fn parse_progress_percent(line: &str) -> Option<f64> {
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

fn dir_size(root: &Path) -> Result<u64> {
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("reading metadata for {}", path.display()))?;
        let file_type = metadata.file_type();
        if file_type.is_dir() {
            for entry in fs::read_dir(&path)
                .with_context(|| format!("reading directory {}", path.display()))?
            {
                let entry = entry?;
                stack.push(entry.path());
            }
        } else if file_type.is_file() || file_type.is_symlink() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn ensure_block_device(path: &str) -> Result<()> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("reading metadata for {path}"))?;
    let file_type = metadata.file_type();
    if !file_type.is_block_device() {
        bail!("{path} is not a block device");
    }
    Ok(())
}

fn ensure_iso_is_not_device(iso_path: &Path, device_path: &str) -> Result<()> {
    let canonical_iso = iso_path
        .canonicalize()
        .with_context(|| format!("resolving ISO path {iso}", iso = iso_path.display()))?;
    let canonical_device = Path::new(device_path)
        .canonicalize()
        .with_context(|| format!("resolving device path {device_path}"))?;
    if canonical_iso == canonical_device {
        bail!("ISO path and target device resolve to the same location");
    }

    // Also reject ISOs that live on any partition of the target device. If we
    // proceeded, the write flow would unmount the partition (hiding the ISO's
    // backing data) and then wipe the disk mid-read, corrupting the source
    // and producing garbage on the target.
    let mounts = devices::partitions_with_mountpoints(device_path).unwrap_or_default();
    for mp in mounts {
        let mountpoint = Path::new(&mp.mountpoint);
        if mountpoint.as_os_str().is_empty() || !mountpoint.is_absolute() {
            continue;
        }
        if canonical_iso.starts_with(mountpoint) {
            bail!(
                "ISO {iso} is on the target device {device} (mounted at {mountpoint})",
                iso = canonical_iso.display(),
                device = device_path,
                mountpoint = mountpoint.display()
            );
        }
    }

    Ok(())
}

fn partition_path(device: &str) -> String {
    partition_path_for(device, 1)
}

fn partition_path_for(device: &str, index: u32) -> String {
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

    let tmp_dir = std::env::var("XDG_RUNTIME_DIR")
        .map_or_else(|_| std::env::temp_dir(), PathBuf::from);
    let mut cfg_file = tempfile::Builder::new()
        .prefix("grub-")
        .suffix(".cfg")
        .tempfile_in(tmp_dir)
        .context("creating grub cfg")?;
    cfg_file
        .write_all(grub_cfg.as_bytes())
        .context("writing grub cfg")?;
    cfg_file.flush().context("flushing grub cfg")?;
    // Keep the NamedTempFile alive until the function returns so RAII
    // cleans up the file on every early-return path.
    let cfg_path = cfg_file.path().to_path_buf();

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

    drop(cfg_file);
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── Label sanitization ──

    #[test]
    fn fat_label_empty() {
        assert_eq!(sanitize_fat_label(""), "BOOTABLE");
    }

    #[test]
    fn fat_label_valid() {
        assert_eq!(sanitize_fat_label("BOOT"), "BOOT");
    }

    #[test]
    fn fat_label_truncates_at_11() {
        assert_eq!(sanitize_fat_label("ABCDEFGHIJKLM"), "ABCDEFGHIJK");
    }

    #[test]
    fn fat_label_strips_special() {
        assert_eq!(sanitize_fat_label("boot@disk!"), "bootdisk");
    }

    #[test]
    fn fat_label_keeps_hyphen_underscore() {
        assert_eq!(sanitize_fat_label("MY_USB-1"), "MY_USB-1");
    }

    #[test]
    fn fat_label_only_special_chars() {
        assert_eq!(sanitize_fat_label("@#$%"), "BOOTABLE");
    }

    #[test]
    fn ntfs_label_empty() {
        assert_eq!(sanitize_ntfs_label(""), "BOOTABLE");
    }

    #[test]
    fn ntfs_label_whitespace_only() {
        assert_eq!(sanitize_ntfs_label("   "), "BOOTABLE");
    }

    #[test]
    fn ntfs_label_allows_spaces() {
        assert_eq!(sanitize_ntfs_label("My Boot Disk"), "My Boot Disk");
    }

    #[test]
    fn ntfs_label_truncates_at_32() {
        let long = "A".repeat(40);
        assert_eq!(sanitize_ntfs_label(&long).len(), 32);
    }

    #[test]
    fn ntfs_label_trims_trailing_whitespace() {
        assert_eq!(sanitize_ntfs_label("  hello  "), "hello");
    }

    #[test]
    fn ntfs_label_strips_special() {
        assert_eq!(sanitize_ntfs_label("disk<>:"), "disk");
    }

    #[test]
    fn ext4_label_empty() {
        assert_eq!(sanitize_ext4_label(""), "persistence");
    }

    #[test]
    fn ext4_label_valid() {
        assert_eq!(sanitize_ext4_label("mydata"), "mydata");
    }

    #[test]
    fn ext4_label_truncates_at_16() {
        assert_eq!(sanitize_ext4_label("ABCDEFGHIJKLMNOPQRST"), "ABCDEFGHIJKLMNOP");
    }

    #[test]
    fn ext4_label_strips_special() {
        assert_eq!(sanitize_ext4_label("data@home!"), "datahome");
    }

    // ── Partition paths ──

    #[test]
    fn partition_path_sda() {
        assert_eq!(partition_path("/dev/sda"), "/dev/sda1");
    }

    #[test]
    fn partition_path_nvme() {
        assert_eq!(partition_path("/dev/nvme0n1"), "/dev/nvme0n1p1");
    }

    #[test]
    fn partition_path_for_sda_2() {
        assert_eq!(partition_path_for("/dev/sda", 2), "/dev/sda2");
    }

    #[test]
    fn partition_path_for_nvme_3() {
        assert_eq!(partition_path_for("/dev/nvme0n1", 3), "/dev/nvme0n1p3");
    }

    #[test]
    fn partition_path_for_loop() {
        assert_eq!(partition_path_for("/dev/loop0", 1), "/dev/loop0p1");
    }

    #[test]
    fn partition_path_for_mmcblk() {
        assert_eq!(partition_path_for("/dev/mmcblk0", 2), "/dev/mmcblk0p2");
    }

    #[test]
    fn partition_path_for_vda() {
        assert_eq!(partition_path_for("/dev/vda", 1), "/dev/vda1");
    }

    // ── parse_mib ──

    #[test]
    fn parse_mib_with_suffix() {
        assert_eq!(parse_mib("1024.5MiB"), Some(1024.5));
    }

    #[test]
    fn parse_mib_without_suffix() {
        assert_eq!(parse_mib("1024.5"), Some(1024.5));
    }

    #[test]
    fn parse_mib_with_whitespace() {
        assert_eq!(parse_mib("  512  "), Some(512.0));
    }

    #[test]
    fn parse_mib_invalid() {
        assert_eq!(parse_mib("abc"), None);
    }

    #[test]
    fn parse_mib_empty() {
        assert_eq!(parse_mib(""), None);
    }

    // ── is_sha256_hex ──

    #[test]
    fn sha256_hex_valid() {
        let hash = "a".repeat(64);
        assert!(is_sha256_hex(&hash));
    }

    #[test]
    fn sha256_hex_valid_mixed_case() {
        let hash = "aAbBcCdDeEfF0123456789".repeat(3)[..64].to_string();
        assert!(is_sha256_hex(&hash));
    }

    #[test]
    fn sha256_hex_too_short() {
        let hash = "a".repeat(63);
        assert!(!is_sha256_hex(&hash));
    }

    #[test]
    fn sha256_hex_too_long() {
        let hash = "a".repeat(65);
        assert!(!is_sha256_hex(&hash));
    }

    #[test]
    fn sha256_hex_non_hex() {
        let hash = "g".repeat(64);
        assert!(!is_sha256_hex(&hash));
    }

    #[test]
    fn sha256_hex_empty() {
        assert!(!is_sha256_hex(""));
    }

    // ── parse_checksum_text ──

    #[test]
    fn parse_checksum_valid() {
        let hash = "a".repeat(64);
        let text = format!("{hash}  somefile.iso");
        let result = parse_checksum_text(&text).unwrap();
        assert_eq!(result, hash);
    }

    #[test]
    fn parse_checksum_uppercase() {
        let hash = "A".repeat(64);
        let result = parse_checksum_text(&hash).unwrap();
        assert_eq!(result, "a".repeat(64));
    }

    #[test]
    fn parse_checksum_no_hash() {
        assert!(parse_checksum_text("no hash here").is_err());
    }

    #[test]
    fn parse_checksum_hash_among_tokens() {
        let hash = "b".repeat(64);
        let text = format!("SHA256 = {hash}");
        let result = parse_checksum_text(&text).unwrap();
        assert_eq!(result, hash);
    }

    // ── parse_progress_percent ──

    #[test]
    fn progress_percent_50() {
        let result = parse_progress_percent("50%").unwrap();
        assert!((result - 0.5).abs() < 1e-6);
    }

    #[test]
    fn progress_percent_100() {
        let result = parse_progress_percent("100%").unwrap();
        assert!((result - 1.0).abs() < 1e-6);
    }

    #[test]
    fn progress_percent_0() {
        let result = parse_progress_percent("0%").unwrap();
        assert!((result - 0.0).abs() < 1e-6);
    }

    #[test]
    fn progress_percent_clamped_over_100() {
        let result = parse_progress_percent("150%").unwrap();
        assert!((result - 1.0).abs() < 1e-6);
    }

    #[test]
    fn progress_percent_multi_token() {
        let result = parse_progress_percent("copying files 75%").unwrap();
        assert!((result - 0.75).abs() < 1e-6);
    }

    #[test]
    fn progress_percent_no_percent() {
        assert!(parse_progress_percent("50").is_none());
    }

    #[test]
    fn progress_percent_with_comma() {
        let result = parse_progress_percent("1,234%").unwrap();
        assert!((result - 1.0).abs() < 1e-6); // clamped
    }

    // ── find_line_break ──

    #[test]
    fn line_break_lf() {
        assert_eq!(find_line_break("hello\nworld"), Some(5));
    }

    #[test]
    fn line_break_cr() {
        assert_eq!(find_line_break("hello\rworld"), Some(5));
    }

    #[test]
    fn line_break_crlf() {
        assert_eq!(find_line_break("hello\r\nworld"), Some(5));
    }

    #[test]
    fn line_break_at_start() {
        assert_eq!(find_line_break("\nhello"), Some(0));
    }

    #[test]
    fn line_break_none() {
        assert_eq!(find_line_break("hello world"), None);
    }

    #[test]
    fn line_break_empty() {
        assert_eq!(find_line_break(""), None);
    }

    // ── is_rsync_progress_line ──

    #[test]
    fn rsync_progress_to_chk() {
        assert!(is_rsync_progress_line("file.txt to-chk=5/10"));
    }

    #[test]
    fn rsync_progress_xfr() {
        assert!(is_rsync_progress_line("  xfr#1, to-chk=0/5"));
    }

    #[test]
    fn rsync_progress_speed() {
        assert!(is_rsync_progress_line("10.5 MB/s"));
    }

    #[test]
    fn rsync_progress_bytes_sec() {
        assert!(is_rsync_progress_line("1234 bytes/sec"));
    }

    #[test]
    fn rsync_progress_plain_text() {
        assert!(!is_rsync_progress_line("file.txt"));
    }

    #[test]
    fn rsync_progress_empty() {
        assert!(!is_rsync_progress_line(""));
    }

    // ── is_rsync_verify_diff ──

    #[test]
    fn rsync_diff_actual_file() {
        assert!(is_rsync_verify_diff("sources/install.wim"));
    }

    #[test]
    fn rsync_diff_sending_incremental() {
        assert!(!is_rsync_verify_diff("sending incremental file list"));
    }

    #[test]
    fn rsync_diff_sent() {
        assert!(!is_rsync_verify_diff("sent 12345 bytes  received 100 bytes"));
    }

    #[test]
    fn rsync_diff_total_size() {
        assert!(!is_rsync_verify_diff("total size is 5678901"));
    }

    #[test]
    fn rsync_diff_receiving() {
        assert!(!is_rsync_verify_diff("receiving file list ... done"));
    }

    #[test]
    fn rsync_diff_created_directory() {
        assert!(!is_rsync_verify_diff("created directory /mnt/usb"));
    }

    #[test]
    fn rsync_diff_empty() {
        assert!(!is_rsync_verify_diff(""));
    }

    #[test]
    fn rsync_diff_whitespace() {
        assert!(!is_rsync_verify_diff("   "));
    }

    // ── BootArch methods ──

    #[test]
    fn boot_arch_labels() {
        assert_eq!(BootArch::X64.label(), "x86_64");
        assert_eq!(BootArch::Ia32.label(), "ia32");
        assert_eq!(BootArch::Aa64.label(), "aa64");
    }

    #[test]
    fn boot_arch_grub_targets() {
        assert_eq!(BootArch::X64.grub_target(), "x86_64-efi");
        assert_eq!(BootArch::Ia32.grub_target(), "i386-efi");
        assert_eq!(BootArch::Aa64.grub_target(), "arm64-efi");
    }

    #[test]
    fn boot_arch_module_dirs() {
        assert_eq!(BootArch::X64.module_dir(), "x86_64-efi");
        assert_eq!(BootArch::Ia32.module_dir(), "i386-efi");
        assert_eq!(BootArch::Aa64.module_dir(), "arm64-efi");
    }

    #[test]
    fn boot_arch_boot_filenames() {
        assert_eq!(BootArch::X64.boot_filename(), "BOOTX64.EFI");
        assert_eq!(BootArch::Ia32.boot_filename(), "BOOTIA32.EFI");
        assert_eq!(BootArch::Aa64.boot_filename(), "BOOTAA64.EFI");
    }

    #[test]
    fn boot_arch_grub_filenames() {
        assert_eq!(BootArch::X64.grub_filename(), "grubx64.efi");
        assert_eq!(BootArch::Ia32.grub_filename(), "grubia32.efi");
        assert_eq!(BootArch::Aa64.grub_filename(), "grubaa64.efi");
    }

    #[test]
    fn boot_arch_signed_shim_candidates_non_empty() {
        assert!(!BootArch::X64.signed_shim_candidates().is_empty());
        assert!(!BootArch::Ia32.signed_shim_candidates().is_empty());
        assert!(!BootArch::Aa64.signed_shim_candidates().is_empty());
    }

    #[test]
    fn boot_arch_signed_grub_candidates_non_empty() {
        assert!(!BootArch::X64.signed_grub_candidates().is_empty());
        assert!(!BootArch::Ia32.signed_grub_candidates().is_empty());
        assert!(!BootArch::Aa64.signed_grub_candidates().is_empty());
    }

    #[test]
    fn boot_arch_mok_manager_candidates_non_empty() {
        assert!(!BootArch::X64.mok_manager_candidates().is_empty());
        assert!(!BootArch::Ia32.mok_manager_candidates().is_empty());
        assert!(!BootArch::Aa64.mok_manager_candidates().is_empty());
    }

    // ── windows_bios_grub_cfg ──

    #[test]
    fn grub_cfg_fat32() {
        let cfg = windows_bios_grub_cfg(FileSystem::Fat32);
        assert!(cfg.contains("insmod fat"));
        assert!(cfg.contains("chainloader /bootmgr"));
        assert!(cfg.contains("set timeout=0"));
    }

    #[test]
    fn grub_cfg_ntfs() {
        let cfg = windows_bios_grub_cfg(FileSystem::Ntfs);
        assert!(cfg.contains("insmod ntfs"));
        assert!(cfg.contains("chainloader /bootmgr"));
    }

    // ── persistence_bounds ──

    #[test]
    fn persistence_bounds_normal() {
        let info = PartedInfo {
            device_size_mib: 1000.0,
            last_end_mib: 100.0,
        };
        let (start, end) = persistence_bounds(&info, 500).unwrap();
        assert!((start - 101.0).abs() < 1e-6);
        assert!((end - 601.0).abs() < 1e-6);
    }

    #[test]
    fn persistence_bounds_zero_size() {
        let info = PartedInfo {
            device_size_mib: 1000.0,
            last_end_mib: 100.0,
        };
        assert!(persistence_bounds(&info, 0).is_err());
    }

    #[test]
    fn persistence_bounds_too_large() {
        let info = PartedInfo {
            device_size_mib: 200.0,
            last_end_mib: 100.0,
        };
        assert!(persistence_bounds(&info, 200).is_err());
    }

    #[test]
    fn persistence_bounds_tight_fit() {
        // last_end=100, start=101, end=101+891=992, max_end=1000-4=996, 992 < 996 → OK
        let info = PartedInfo {
            device_size_mib: 1000.0,
            last_end_mib: 100.0,
        };
        assert!(persistence_bounds(&info, 891).is_ok());
    }

    #[test]
    fn persistence_bounds_exceeds_margin() {
        // last_end=100, start=101, end=101+896=997, max_end=1000-4=996, 997 > 996 → Err
        let info = PartedInfo {
            device_size_mib: 1000.0,
            last_end_mib: 100.0,
        };
        assert!(persistence_bounds(&info, 896).is_err());
    }

    // ── handle_cmd_line ──

    #[test]
    fn cmd_line_progress() {
        let (tx, rx) = mpsc::channel();
        handle_cmd_line("50%", &tx, false);
        drop(tx);
        let event = rx.recv().unwrap();
        match event {
            CmdEvent::Progress(frac) => assert!((frac - 0.5).abs() < 1e-6),
            CmdEvent::Log(..) => panic!("expected Progress"),
        }
    }

    #[test]
    fn cmd_line_log_stdout() {
        let (tx, rx) = mpsc::channel();
        handle_cmd_line("some output", &tx, false);
        drop(tx);
        let event = rx.recv().unwrap();
        match event {
            CmdEvent::Log(msg, is_err) => {
                assert_eq!(msg, "some output");
                assert!(!is_err);
            }
            CmdEvent::Progress(_) => panic!("expected Log"),
        }
    }

    #[test]
    fn cmd_line_log_stderr() {
        let (tx, rx) = mpsc::channel();
        handle_cmd_line("error msg", &tx, true);
        drop(tx);
        let event = rx.recv().unwrap();
        match event {
            CmdEvent::Log(msg, is_err) => {
                assert_eq!(msg, "error msg");
                assert!(is_err);
            }
            CmdEvent::Progress(_) => panic!("expected Log"),
        }
    }

    #[test]
    fn cmd_line_empty_no_event() {
        let (tx, rx) = mpsc::channel();
        handle_cmd_line("", &tx, false);
        drop(tx);
        assert!(rx.recv().is_err()); // no events sent
    }

    #[test]
    fn cmd_line_strips_trailing_newlines() {
        let (tx, rx) = mpsc::channel();
        handle_cmd_line("hello\r\n", &tx, false);
        drop(tx);
        let event = rx.recv().unwrap();
        match event {
            CmdEvent::Log(msg, _) => assert_eq!(msg, "hello"),
            CmdEvent::Progress(_) => panic!("expected Log"),
        }
    }

    // ── handle_rsync_line ──

    #[test]
    fn rsync_line_progress() {
        let (tx, rx) = mpsc::channel();
        // Set last_emit to long ago so throttle doesn't suppress
        let mut last_emit = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("Instant - 1s should not underflow");
        handle_rsync_line("75%", &tx, &mut last_emit, false);
        drop(tx);
        let event = rx.recv().unwrap();
        match event {
            UiEvent::Progress(frac) => assert!((frac - 0.75).abs() < 1e-6),
            UiEvent::Log(_) | UiEvent::Done(_) => panic!("expected Progress"),
        }
    }

    #[test]
    fn rsync_line_progress_throttled() {
        let (tx, rx) = mpsc::channel();
        // Set last_emit to now — should be throttled
        let mut last_emit = Instant::now();
        handle_rsync_line("75%", &tx, &mut last_emit, false);
        drop(tx);
        // No events should be sent (throttled)
        assert!(rx.recv().is_err());
    }

    #[test]
    fn rsync_line_log_when_enabled() {
        let (tx, rx) = mpsc::channel();
        let mut last_emit = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("Instant - 1s should not underflow");
        handle_rsync_line("some rsync output", &tx, &mut last_emit, true);
        drop(tx);
        let event = rx.recv().unwrap();
        match event {
            UiEvent::Log(msg) => assert_eq!(msg, "rsync: some rsync output"),
            UiEvent::Progress(_) | UiEvent::Done(_) => panic!("expected Log"),
        }
    }

    #[test]
    fn rsync_line_no_log_when_disabled() {
        let (tx, rx) = mpsc::channel();
        let mut last_emit = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("Instant - 1s should not underflow");
        handle_rsync_line("some rsync output", &tx, &mut last_emit, false);
        drop(tx);
        assert!(rx.recv().is_err());
    }

    #[test]
    fn rsync_line_empty_no_event() {
        let (tx, rx) = mpsc::channel();
        let mut last_emit = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("Instant - 1s should not underflow");
        handle_rsync_line("", &tx, &mut last_emit, true);
        drop(tx);
        assert!(rx.recv().is_err());
    }

    // ── persistence_bounds with fractional last_end ──

    #[test]
    fn persistence_bounds_fractional_last_end() {
        // last_end=100.5, start=ceil(101.5)=102, end=102+500=602, max_end=1000-4=996
        let info = PartedInfo {
            device_size_mib: 1000.0,
            last_end_mib: 100.5,
        };
        let (start, end) = persistence_bounds(&info, 500).unwrap();
        assert!((start - 102.0).abs() < 1e-6);
        assert!((end - 602.0).abs() < 1e-6);
    }

    // ── parse_mib edge cases ──

    #[test]
    fn parse_mib_only_suffix() {
        assert_eq!(parse_mib("MiB"), None);
    }

    #[test]
    fn parse_mib_zero() {
        assert_eq!(parse_mib("0MiB"), Some(0.0));
    }

    // ── resolve_checksum with file ──

    #[test]
    fn resolve_checksum_from_file() {
        let hash = "a".repeat(64);
        let content = format!("{hash}  somefile.iso\n");
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("checksum.sha256");
        fs::write(&file_path, &content).unwrap();
        let result = resolve_checksum(file_path.to_str().unwrap()).unwrap();
        assert_eq!(result, hash);
    }

    #[test]
    fn resolve_checksum_from_file_no_hash() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("bad.sha256");
        fs::write(&file_path, "no hash here\n").unwrap();
        assert!(resolve_checksum(file_path.to_str().unwrap()).is_err());
    }

    #[test]
    fn resolve_checksum_inline() {
        let hash = "b".repeat(64);
        let result = resolve_checksum(&hash).unwrap();
        assert_eq!(result, hash);
    }

    #[test]
    fn resolve_checksum_empty() {
        assert!(resolve_checksum("").is_err());
        assert!(resolve_checksum("   ").is_err());
    }

    // ── fat label does not uppercase ──

    #[test]
    fn fat_label_preserves_case() {
        assert_eq!(sanitize_fat_label("bootable"), "bootable");
    }

    // ── ext4 label allows hyphen and underscore ──

    #[test]
    fn ext4_label_keeps_hyphen_underscore() {
        assert_eq!(sanitize_ext4_label("my-data_1"), "my-data_1");
    }

    #[test]
    fn ext4_label_only_special_chars() {
        assert_eq!(sanitize_ext4_label("@#$%"), "persistence");
    }

    // ── dir_size ──

    #[test]
    fn dir_size_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(dir_size(dir.path()).unwrap(), 0);
    }

    #[test]
    fn dir_size_with_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();
        fs::write(dir.path().join("b.txt"), "world!").unwrap();
        let size = dir_size(dir.path()).unwrap();
        assert_eq!(size, 11); // 5 + 6
    }

    #[test]
    fn dir_size_nested() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("file.txt"), "data").unwrap();
        let size = dir_size(dir.path()).unwrap();
        assert_eq!(size, 4);
    }
}
