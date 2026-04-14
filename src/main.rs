#![forbid(unsafe_code)]

mod devices;
mod helper;
mod util;
mod writer;

use adw::prelude::*;
use gtk::{gio, glib};
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Duration;
use glib::types::StaticType;
use gio::prelude::VolumeMonitorExt;

use crate::writer::{FileSystem, ImageMode, PartitionScheme, TargetSystem, UiEvent, WritePlan};

fn main() -> glib::ExitCode {
    if let Some(plan_path) = helper::helper_plan_path() {
        return helper::run_helper(&plan_path);
    }

    let app = adw::Application::builder()
        .application_id("io.bootable.app")
        .build();
    app.connect_activate(build_ui);
    app.run()
}

#[allow(clippy::too_many_lines)]
fn build_ui(app: &adw::Application) {
    let window = adw::ApplicationWindow::builder()
        .application(app)
        .title("Bootable")
        .default_width(820)
        .default_height(620)
        .build();

    let header = adw::HeaderBar::builder()
        .title_widget(&gtk::Label::new(Some("Bootable")))
        .build();

    let root = gtk::Box::builder()
        .orientation(gtk::Orientation::Vertical)
        .spacing(12)
        .margin_top(12)
        .margin_bottom(12)
        .margin_start(12)
        .margin_end(12)
        .build();

    let grid = gtk::Grid::builder()
        .row_spacing(8)
        .column_spacing(12)
        .hexpand(true)
        .build();

    let device_list = gtk::StringList::new(&[]);
    let device_dropdown = gtk::DropDown::new(Some(device_list.clone()), None::<&gtk::Expression>);
    device_dropdown.set_hexpand(true);
    let refresh_button = gtk::Button::with_label("Refresh");
    let device_row = gtk::Box::builder().spacing(8).hexpand(true).build();
    device_row.append(&device_dropdown);
    device_row.append(&refresh_button);
    add_row(&grid, 0, "Device", &device_row);

    let iso_entry = gtk::Entry::builder().editable(false).hexpand(true).build();
    let browse_button = gtk::Button::with_label("Select");
    let iso_row = gtk::Box::builder().spacing(8).hexpand(true).build();
    iso_row.append(&iso_entry);
    iso_row.append(&browse_button);
    add_row(&grid, 1, "Select the image", &iso_row);

    let mode_list = gtk::StringList::new(&[
        "Auto (detect)",
        "ISOHybrid / DD",
        "Windows (UEFI/BIOS)",
    ]);
    let mode_dropdown = gtk::DropDown::new(Some(mode_list), None::<&gtk::Expression>);
    mode_dropdown.set_selected(0);
    add_row(&grid, 2, "Image mode", &mode_dropdown);

    let partition_list = gtk::StringList::new(&["GPT", "MBR"]);
    let partition_dropdown =
        gtk::DropDown::new(Some(partition_list), None::<&gtk::Expression>);
    partition_dropdown.set_selected(0);
    add_row(&grid, 3, "Partition scheme", &partition_dropdown);

    let target_list = gtk::StringList::new(&["UEFI", "BIOS", "UEFI + BIOS"]);
    let target_dropdown =
        gtk::DropDown::new(Some(target_list), None::<&gtk::Expression>);
    target_dropdown.set_selected(0);
    add_row(&grid, 4, "Target system", &target_dropdown);

    let fs_list = gtk::StringList::new(&["FAT32", "NTFS"]);
    let fs_dropdown = gtk::DropDown::new(Some(fs_list), None::<&gtk::Expression>);
    fs_dropdown.set_selected(0);
    add_row(&grid, 5, "File system", &fs_dropdown);

    let volume_entry = gtk::Entry::builder().text("BOOTABLE").build();
    add_row(&grid, 6, "Volume label", &volume_entry);

    let secure_toggle = gtk::Switch::builder().active(false).build();
    let secure_desc = gtk::Label::new(Some("Require signed shim/grub (Secure Boot only)"));
    secure_desc.set_halign(gtk::Align::Start);
    let secure_row = gtk::Box::builder()
        .orientation(gtk::Orientation::Horizontal)
        .spacing(8)
        .build();
    secure_row.append(&secure_toggle);
    secure_row.append(&secure_desc);
    add_row(&grid, 7, "Secure Boot", &secure_row);

    let checksum_entry = gtk::Entry::builder()
        .placeholder_text("SHA256 hash or .sha256 file path")
        .hexpand(true)
        .build();
    let checksum_button = gtk::Button::with_label("Select");
    let checksum_row = gtk::Box::builder().spacing(8).hexpand(true).build();
    checksum_row.append(&checksum_entry);
    checksum_row.append(&checksum_button);
    add_row(&grid, 8, "Checksum", &checksum_row);

    let signature_entry = gtk::Entry::builder()
        .placeholder_text("Signature file (.sig)")
        .hexpand(true)
        .build();
    let signature_button = gtk::Button::with_label("Select");
    let signature_row = gtk::Box::builder().spacing(8).hexpand(true).build();
    signature_row.append(&signature_entry);
    signature_row.append(&signature_button);
    add_row(&grid, 9, "Signature", &signature_row);

    let verify_toggle = gtk::Switch::builder().active(false).build();
    let verify_desc = gtk::Label::new(Some("Verify files/device after write"));
    verify_desc.set_halign(gtk::Align::Start);
    let verify_row = gtk::Box::builder()
        .orientation(gtk::Orientation::Horizontal)
        .spacing(8)
        .build();
    verify_row.append(&verify_toggle);
    verify_row.append(&verify_desc);
    add_row(&grid, 10, "Verify", &verify_row);

    let persistence_spin = gtk::SpinButton::with_range(0.0, 1_048_576.0, 64.0);
    persistence_spin.set_value(0.0);
    let persistence_label_entry = gtk::Entry::builder()
        .text("persistence")
        .placeholder_text("Label (e.g. persistence or casper-rw)")
        .hexpand(true)
        .build();
    let persistence_row = gtk::Box::builder().spacing(8).hexpand(true).build();
    persistence_row.append(&persistence_spin);
    persistence_row.append(&persistence_label_entry);
    add_row(&grid, 11, "Persistence (MiB)", &persistence_row);

    let dry_run_toggle = gtk::Switch::builder().active(false).build();
    let dry_run_desc = gtk::Label::new(Some("Dry run (no writes)"));
    dry_run_desc.set_halign(gtk::Align::Start);
    let dry_run_row = gtk::Box::builder()
        .orientation(gtk::Orientation::Horizontal)
        .spacing(8)
        .build();
    dry_run_row.append(&dry_run_toggle);
    dry_run_row.append(&dry_run_desc);
    add_row(&grid, 12, "Dry run", &dry_run_row);

    root.append(&grid);

    let start_button = gtk::Button::with_label("Start");
    start_button.set_halign(gtk::Align::End);
    root.append(&start_button);

    let progress = gtk::ProgressBar::builder().show_text(true).build();
    root.append(&progress);

    let log_view = gtk::TextView::builder()
        .editable(false)
        .cursor_visible(false)
        .monospace(true)
        .wrap_mode(gtk::WrapMode::WordChar)
        .build();
    let log_buffer = log_view.buffer();
    let scroller = gtk::ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .child(&log_view)
        .build();
    root.append(&scroller);

    let toolbar_view = adw::ToolbarView::new();
    toolbar_view.add_top_bar(&header);
    toolbar_view.set_content(Some(&root));
    window.set_content(Some(&toolbar_view));
    window.present();

    let devices_state: Rc<RefCell<Vec<devices::Device>>> = Rc::new(RefCell::new(Vec::new()));
    let flashing: Rc<RefCell<bool>> = Rc::new(RefCell::new(false));
    let dialog_open: Rc<RefCell<bool>> = Rc::new(RefCell::new(false));

    let update_controls: Rc<dyn Fn()> = {
        let mode_dropdown = mode_dropdown.clone();
        let partition_dropdown = partition_dropdown.clone();
        let target_dropdown = target_dropdown.clone();
        let fs_dropdown = fs_dropdown.clone();
        let volume_entry = volume_entry.clone();
        let secure_toggle = secure_toggle.clone();
        let persistence_spin = persistence_spin.clone();
        let persistence_label_entry = persistence_label_entry.clone();
        Rc::new(move || {
            let mode = mode_dropdown.selected();
            let dd_mode = mode == 1;
            let target = target_dropdown.selected();
            let fs_idx = fs_dropdown.selected();
            let uefi_enabled = target != 1;
            let bios_enabled = target != 0;
            let ntfs_selected = fs_idx == 1;

            if dd_mode {
                partition_dropdown.set_sensitive(false);
                target_dropdown.set_sensitive(false);
                fs_dropdown.set_sensitive(false);
                volume_entry.set_sensitive(false);
                secure_toggle.set_sensitive(false);
                secure_toggle.set_active(false);
                persistence_spin.set_sensitive(true);
                persistence_label_entry.set_sensitive(true);
                return;
            }

            target_dropdown.set_sensitive(true);
            fs_dropdown.set_sensitive(true);
            volume_entry.set_sensitive(true);
            persistence_spin.set_sensitive(false);
            persistence_label_entry.set_sensitive(false);

            if bios_enabled {
                if partition_dropdown.selected() != 1 {
                    partition_dropdown.set_selected(1);
                }
                partition_dropdown.set_sensitive(false);
            } else {
                partition_dropdown.set_sensitive(true);
            }

            let secure_allowed = ntfs_selected && uefi_enabled;
            if !secure_allowed {
                secure_toggle.set_active(false);
            }
            secure_toggle.set_sensitive(secure_allowed);
        })
    };

    update_controls();

    let update_controls_clone = update_controls.clone();
    mode_dropdown.connect_selected_notify(move |_| {
        update_controls_clone();
    });
    let update_controls_clone = update_controls.clone();
    target_dropdown.connect_selected_notify(move |_| {
        update_controls_clone();
    });
    let update_controls_clone = update_controls.clone();
    fs_dropdown.connect_selected_notify(move |_| {
        update_controls_clone();
    });

    refresh_devices_guarded(
        &device_list,
        &devices_state,
        &log_buffer,
        &flashing,
        &device_dropdown,
        false,
    );

    let device_list_clone = device_list.clone();
    let devices_state_clone = devices_state.clone();
    let log_buffer_clone = log_buffer.clone();
    let flashing_clone = flashing.clone();
    let device_dropdown_clone = device_dropdown.clone();
    refresh_button.connect_clicked(move |_| {
        refresh_devices_guarded(
            &device_list_clone,
            &devices_state_clone,
            &log_buffer_clone,
            &flashing_clone,
            &device_dropdown_clone,
            true,
        );
    });

    let refresh_timer: Rc<RefCell<Option<glib::SourceId>>> = Rc::new(RefCell::new(None));
    let schedule_refresh: Rc<dyn Fn()> = {
        let devices_state = devices_state.clone();
        let log_buffer = log_buffer.clone();
        let flashing = flashing.clone();
        let device_dropdown = device_dropdown.clone();
        Rc::new(move || {
            if let Some(id) = refresh_timer.borrow_mut().take() {
                id.remove();
            }
            let device_list = device_list.clone();
            let devices_state = devices_state.clone();
            let log_buffer = log_buffer.clone();
            let refresh_timer_for_cb = refresh_timer.clone();
            let flashing = flashing.clone();
            let device_dropdown = device_dropdown.clone();
            let id = glib::timeout_add_local_once(Duration::from_millis(400), move || {
                refresh_devices_guarded(
                    &device_list,
                    &devices_state,
                    &log_buffer,
                    &flashing,
                    &device_dropdown,
                    false,
                );
                refresh_timer_for_cb.borrow_mut().take();
            });
            *refresh_timer.borrow_mut() = Some(id);
        })
    };

    let monitor = gio::VolumeMonitor::get();
    {
        let schedule_refresh = schedule_refresh.clone();
        monitor.connect_drive_connected(move |_, _| {
            schedule_refresh();
        });
    }
    {
        let schedule_refresh = schedule_refresh.clone();
        monitor.connect_drive_disconnected(move |_, _| {
            schedule_refresh();
        });
    }
    {
        let schedule_refresh = schedule_refresh.clone();
        monitor.connect_volume_added(move |_, _| {
            schedule_refresh();
        });
    }
    {
        let schedule_refresh = schedule_refresh.clone();
        monitor.connect_volume_removed(move |_, _| {
            schedule_refresh();
        });
    }
    {
        let schedule_refresh = schedule_refresh.clone();
        monitor.connect_mount_added(move |_, _| {
            schedule_refresh();
        });
    }
    {
        let schedule_refresh = schedule_refresh.clone();
        monitor.connect_mount_removed(move |_, _| {
            schedule_refresh();
        });
    }

    let window_clone = window.clone();
    let iso_entry_clone = iso_entry.clone();
    browse_button.connect_clicked(move |_| {
        let dialog = gtk::FileDialog::builder()
            .title("Select image")
            .modal(true)
            .build();
        let image_filter = gtk::FileFilter::new();
        image_filter.set_name(Some("Disk images"));
        image_filter.add_pattern("*.iso");
        image_filter.add_pattern("*.img");
        image_filter.add_pattern("*.raw");
        image_filter.add_pattern("*.bin");
        let all_filter = gtk::FileFilter::new();
        all_filter.set_name(Some("All files"));
        all_filter.add_pattern("*");
        let filters = gio::ListStore::builder()
            .item_type(gtk::FileFilter::static_type())
            .build();
        filters.append(&image_filter);
        filters.append(&all_filter);
        dialog.set_filters(Some(&filters));
        dialog.set_default_filter(Some(&image_filter));
        let entry = iso_entry_clone.clone();
        dialog.open(Some(&window_clone), None::<&gio::Cancellable>, move |result| {
            if let Ok(file) = result
                && let Some(path) = file.path()
            {
                entry.set_text(path.to_string_lossy().as_ref());
            }
        });
    });

    let window_clone = window.clone();
    let checksum_entry_clone = checksum_entry.clone();
    checksum_button.connect_clicked(move |_| {
        let dialog = gtk::FileDialog::builder()
            .title("Select checksum file")
            .modal(true)
            .build();
        let entry = checksum_entry_clone.clone();
        dialog.open(Some(&window_clone), None::<&gio::Cancellable>, move |result| {
            if let Ok(file) = result
                && let Some(path) = file.path()
            {
                entry.set_text(path.to_string_lossy().as_ref());
            }
        });
    });

    let window_clone = window.clone();
    let signature_entry_clone = signature_entry.clone();
    signature_button.connect_clicked(move |_| {
        let dialog = gtk::FileDialog::builder()
            .title("Select signature file")
            .modal(true)
            .build();
        let entry = signature_entry_clone.clone();
        dialog.open(Some(&window_clone), None::<&gio::Cancellable>, move |result| {
            if let Ok(file) = result
                && let Some(path) = file.path()
            {
                entry.set_text(path.to_string_lossy().as_ref());
            }
        });
    });

    let (sender, receiver) = mpsc::channel::<UiEvent>();

    let controls: Vec<gtk::Widget> = vec![
        device_dropdown.clone().upcast(),
        refresh_button.upcast(),
        browse_button.upcast(),
        iso_entry.clone().upcast(),
        mode_dropdown.clone().upcast(),
        partition_dropdown.clone().upcast(),
        target_dropdown.clone().upcast(),
        fs_dropdown.clone().upcast(),
        volume_entry.clone().upcast(),
        secure_toggle.clone().upcast(),
        checksum_entry.clone().upcast(),
        checksum_button.upcast(),
        signature_entry.clone().upcast(),
        signature_button.upcast(),
        verify_toggle.clone().upcast(),
        persistence_spin.clone().upcast(),
        persistence_label_entry.clone().upcast(),
        dry_run_toggle.clone().upcast(),
        start_button.clone().upcast(),
    ];

    let progress_clone = progress.clone();
    let log_buffer_receiver = log_buffer.clone();
    let controls_receiver = controls.clone();
    let flashing_receiver = flashing.clone();
    let update_controls_receiver = update_controls.clone();

    glib::idle_add_local(move || {
        loop {
            match receiver.try_recv() {
                Ok(event) => match event {
                    UiEvent::Log(msg) => append_log(&log_buffer_receiver, &msg),
                    UiEvent::Progress(frac) => {
                        progress_clone.set_fraction(frac);
                        progress_clone.set_text(Some(&format!("{:.0}%", frac * 100.0)));
                    }
                    UiEvent::Done(result) => {
                        match result {
                            Ok(()) => append_log(&log_buffer_receiver, "Completed successfully"),
                            Err(err) => append_log(&log_buffer_receiver, &format!("Error: {err}")),
                        }
                        *flashing_receiver.borrow_mut() = false;
                        set_controls_sensitive(&controls_receiver, true);
                        update_controls_receiver();
                    }
                },
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    return glib::ControlFlow::Break;
                }
            }
        }
        glib::ControlFlow::Continue
    });

    start_button.connect_clicked(move |_| {
        if *dialog_open.borrow() {
            return;
        }
        let iso_text = iso_entry.text().to_string();
        if iso_text.trim().is_empty() {
            append_log(&log_buffer, "Select an image first");
            return;
        }

        let iso_path = PathBuf::from(iso_text);
        let selected = device_dropdown.selected() as usize;
        let devices = devices_state.borrow();
        let Some(device) = devices.get(selected) else {
            append_log(&log_buffer, "Select a target device");
            return;
        };

        let mountpoints = match devices::mountpoints_for_device(&device.path) {
            Ok(points) => points,
            Err(err) => {
                append_log(&log_buffer, &format!("Failed to read mountpoints: {err}"));
                return;
            }
        };

        if let Some(reason) = system_mount_block(&mountpoints) {
            append_log(
                &log_buffer,
                &format!(
                    "Refusing to write: {device_path} is mounted on {reason}",
                    device_path = &device.path,
                    reason = reason
                ),
            );
            return;
        }

        let dry_run = dry_run_toggle.is_active();
        if !dry_run && !util::is_root() && !util::command_exists("pkexec") {
            append_log(&log_buffer, "pkexec not found; install polkit to enable admin writes.");
            return;
        }

        let image_mode = match mode_dropdown.selected() {
            0 => ImageMode::Auto,
            1 => ImageMode::IsoHybridDd,
            _ => ImageMode::WindowsUefi,
        };

        let partition_scheme = match partition_dropdown.selected() {
            0 => PartitionScheme::Gpt,
            _ => PartitionScheme::Mbr,
        };

        let target_system = match target_dropdown.selected() {
            0 => TargetSystem::Uefi,
            1 => TargetSystem::Bios,
            _ => TargetSystem::UefiAndBios,
        };

        let file_system = if fs_dropdown.selected() == 1 {
            FileSystem::Ntfs
        } else {
            FileSystem::Fat32
        };

        let checksum_value = {
            let text = checksum_entry.text().trim().to_string();
            if text.is_empty() {
                None
            } else {
                Some(text)
            }
        };
        let signature_path = {
            let text = signature_entry.text().trim().to_string();
            if text.is_empty() {
                None
            } else {
                Some(PathBuf::from(text))
            }
        };
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let persistence_size_mib = persistence_spin.value() as u64;
        let persistence_label = persistence_label_entry.text().to_string();

        let plan = WritePlan {
            iso_path,
            device_path: device.path.clone(),
            device_size_bytes: device.size_bytes,
            image_mode,
            partition_scheme,
            target_system,
            file_system,
            volume_label: volume_entry.text().to_string(),
            secure_boot_only: secure_toggle.is_active(),
            verify_after: verify_toggle.is_active(),
            checksum_sha256: checksum_value,
            signature_path,
            persistence_size_mib,
            persistence_label,
            dry_run,
        };

        let sender = sender.clone();
        let controls = controls.clone();
        let progress = progress.clone();
        let log_buffer = log_buffer.clone();
        let flashing = flashing.clone();

        *dialog_open.borrow_mut() = true;
        let dialog_open_close = dialog_open.clone();
        show_confirmation_dialog(
            &window,
            device,
            &mountpoints,
            move || {
                append_log(
                    &log_buffer,
                    &format!(
                        "Starting write to {device_path}",
                        device_path = plan.device_path.as_str()
                    ),
                );
                if !util::is_root() && !dry_run {
                    append_log(&log_buffer, "Requesting admin access (pkexec)...");
                }
                progress.set_fraction(0.0);
                progress.set_text(Some("0%"));
                set_controls_sensitive(&controls, false);
                *flashing.borrow_mut() = true;

                let sender = sender.clone();
                let sender_panic = sender.clone();
                std::thread::spawn(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if util::is_root() || dry_run {
                            let sender = sender.clone();
                            writer::run(&plan, move |event| {
                                let _ = sender.send(event);
                            });
                        } else {
                            let sender_events = sender.clone();
                            let result = helper::run_helper_with_pkexec(&plan, move |event| {
                                let _ = sender_events.send(event);
                            });
                            if let Err(err) = result {
                                let _ = sender.send(UiEvent::Done(Err(err.to_string())));
                            }
                        }
                    }));
                    if let Err(payload) = outcome {
                        let msg = payload
                            .downcast_ref::<&'static str>()
                            .map(|s| (*s).to_string())
                            .or_else(|| payload.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "unknown panic".to_string());
                        let _ = sender_panic.send(UiEvent::Done(Err(format!(
                            "Write worker panicked: {msg}"
                        ))));
                    }
                });
            },
            move || *dialog_open_close.borrow_mut() = false,
        );
    });
}

fn add_row(grid: &gtk::Grid, row: i32, label: &str, widget: &impl IsA<gtk::Widget>) {
    let lbl = gtk::Label::new(Some(label));
    lbl.set_halign(gtk::Align::Start);
    lbl.set_valign(gtk::Align::Center);
    grid.attach(&lbl, 0, row, 1, 1);
    grid.attach(widget, 1, row, 1, 1);
}

fn append_log(buffer: &gtk::TextBuffer, msg: &str) {
    let mut end = buffer.end_iter();
    buffer.insert(&mut end, msg);
    buffer.insert(&mut end, "\n");
}

fn set_controls_sensitive(controls: &[gtk::Widget], sensitive: bool) {
    for widget in controls {
        widget.set_sensitive(sensitive);
    }
}

fn refresh_devices_guarded(
    device_list: &gtk::StringList,
    devices_state: &Rc<RefCell<Vec<devices::Device>>>,
    log_buffer: &gtk::TextBuffer,
    flashing: &Rc<RefCell<bool>>,
    device_dropdown: &gtk::DropDown,
    log_when_skipped: bool,
) {
    if *flashing.borrow() {
        if log_when_skipped {
            append_log(log_buffer, "Flash in progress; device refresh skipped");
        }
        return;
    }
    refresh_devices(device_list, devices_state, log_buffer, device_dropdown);
}

fn refresh_devices(
    device_list: &gtk::StringList,
    devices_state: &Rc<RefCell<Vec<devices::Device>>>,
    log_buffer: &gtk::TextBuffer,
    device_dropdown: &gtk::DropDown,
) {
    let previous_path = {
        let devices = devices_state.borrow();
        let selected = device_dropdown.selected() as usize;
        devices.get(selected).map(|dev| dev.path.clone())
    };

    match devices::list_removable() {
        Ok(list) => {
            devices_state.borrow_mut().clear();
            devices_state.borrow_mut().extend(list);
            let count = device_list.n_items();
            if count > 0 {
                device_list.splice(0, count, &[]);
            }
            for dev in devices_state.borrow().iter() {
                device_list.append(&dev.display);
            }
            if let Some(path) = previous_path {
                if let Some(idx) = devices_state.borrow().iter().position(|dev| dev.path == path) {
                    device_dropdown.set_selected(u32::try_from(idx).unwrap_or(gtk::INVALID_LIST_POSITION));
                } else {
                    device_dropdown.set_selected(gtk::INVALID_LIST_POSITION);
                }
            } else if device_list.n_items() == 0 {
                device_dropdown.set_selected(gtk::INVALID_LIST_POSITION);
            }
            append_log(log_buffer, "Device list refreshed");
        }
        Err(err) => {
            append_log(log_buffer, &format!("Device scan failed: {err}"));
        }
    }
}

fn system_mount_block(mountpoints: &[String]) -> Option<String> {
    let allowed_prefixes = ["/mnt/", "/media/", "/run/media/"];
    for mount in mountpoints {
        let dominated_by_allowed = allowed_prefixes
            .iter()
            .any(|prefix| mount.starts_with(prefix));
        if !dominated_by_allowed {
            return Some(mount.clone());
        }
    }
    None
}

fn show_confirmation_dialog(
    window: &adw::ApplicationWindow,
    device: &devices::Device,
    mountpoints: &[String],
    on_confirm: impl FnOnce() + 'static,
    on_close: impl Fn() + 'static,
) {
    let dialog = gtk::Window::builder()
        .transient_for(window)
        .modal(true)
        .title("Confirm erase")
        .default_width(480)
        .resizable(false)
        .build();

    let container = gtk::Box::builder()
        .orientation(gtk::Orientation::Vertical)
        .spacing(12)
        .margin_top(16)
        .margin_bottom(16)
        .margin_start(16)
        .margin_end(16)
        .build();

    let warning = gtk::Label::new(Some(&format!(
        "You are about to erase {device_display}.\nThis action cannot be undone.",
        device_display = &device.display
    )));
    warning.set_wrap(true);
    warning.set_halign(gtk::Align::Start);
    container.append(&warning);

    if !mountpoints.is_empty() {
        let mounts_text = mountpoints.join(", ");
        let mounts = gtk::Label::new(Some(&format!(
            "Currently mounted: {mounts_text}"
        )));
        mounts.set_wrap(true);
        mounts.set_halign(gtk::Align::Start);
        container.append(&mounts);
    }

    let prompt = gtk::Label::new(Some(&format!(
        "Type {device_path} to confirm:",
        device_path = &device.path
    )));
    prompt.set_halign(gtk::Align::Start);
    container.append(&prompt);

    let confirm_entry = gtk::Entry::builder()
        .placeholder_text(&device.path)
        .build();
    container.append(&confirm_entry);

    let confirm_check = gtk::CheckButton::with_label("I understand this will erase all data");
    container.append(&confirm_check);

    let error_label = gtk::Label::new(None);
    error_label.set_halign(gtk::Align::Start);
    container.append(&error_label);

    let buttons = gtk::Box::builder()
        .orientation(gtk::Orientation::Horizontal)
        .spacing(8)
        .halign(gtk::Align::End)
        .build();

    let cancel_button = gtk::Button::with_label("Cancel");
    let erase_button = gtk::Button::with_label("Erase");
    erase_button.add_css_class("destructive-action");
    buttons.append(&cancel_button);
    buttons.append(&erase_button);
    container.append(&buttons);

    dialog.set_child(Some(&container));
    dialog.connect_close_request(move |_| {
        on_close();
        glib::Propagation::Proceed
    });
    dialog.present();

    let dialog_for_cancel = dialog.clone();
    cancel_button.connect_clicked(move |_| {
        dialog_for_cancel.close();
    });

    let on_confirm = Rc::new(RefCell::new(Some(on_confirm)));
    let dialog_for_erase = dialog;
    let error_label = error_label;
    let device_path = device.path.clone();
    erase_button.connect_clicked(move |_| {
        let typed = confirm_entry.text().to_string();
        if typed.trim() != device_path {
            error_label.set_text("Device path does not match.");
            return;
        }
        if !confirm_check.is_active() {
            error_label.set_text("Please confirm the data loss checkbox.");
            return;
        }

        dialog_for_erase.close();
        if let Some(cb) = on_confirm.borrow_mut().take() {
            cb();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_mount_block_root() {
        let mounts = vec!["/".to_string()];
        assert_eq!(system_mount_block(&mounts), Some("/".to_string()));
    }

    #[test]
    fn system_mount_block_boot() {
        let mounts = vec!["/boot".to_string()];
        assert_eq!(system_mount_block(&mounts), Some("/boot".to_string()));
    }

    #[test]
    fn system_mount_block_home() {
        let mounts = vec!["/home".to_string()];
        assert_eq!(system_mount_block(&mounts), Some("/home".to_string()));
    }

    #[test]
    fn system_mount_block_safe_mount() {
        let mounts = vec!["/mnt/usb".to_string()];
        assert_eq!(system_mount_block(&mounts), None);
    }

    #[test]
    fn system_mount_block_media_mount() {
        let mounts = vec!["/media/user/USBDRIVE".to_string()];
        assert_eq!(system_mount_block(&mounts), None);
    }

    #[test]
    fn system_mount_block_run_media() {
        let mounts = vec!["/run/media/user/disk".to_string()];
        assert_eq!(system_mount_block(&mounts), None);
    }

    #[test]
    fn system_mount_block_empty() {
        let mounts: Vec<String> = vec![];
        assert_eq!(system_mount_block(&mounts), None);
    }

    #[test]
    fn system_mount_block_mixed() {
        let mounts = vec!["/mnt/usb".to_string(), "/var".to_string()];
        assert_eq!(system_mount_block(&mounts), Some("/var".to_string()));
    }

    #[test]
    fn system_mount_block_all_protected() {
        for path in ["/", "/boot", "/boot/efi", "/home", "/usr", "/var",
                     "/root", "/etc", "/opt", "/srv", "/run", "/tmp",
                     "/proc", "/sys", "/dev", "/snap", "/nix"] {
            let mounts = vec![path.to_string()];
            assert!(system_mount_block(&mounts).is_some(), "should block {path}");
        }
    }
}
