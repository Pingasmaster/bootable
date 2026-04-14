#![forbid(unsafe_code)]

mod devices;
mod helper;
mod util;
mod writer;

use adw::prelude::*;
use gio::prelude::VolumeMonitorExt;
use glib::clone;
use gtk::{gdk, gio, glib};
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use crate::writer::{FileSystem, ImageMode, PartitionScheme, TargetSystem, UiEvent, WritePlan};

fn main() -> glib::ExitCode {
    if let Some(plan_path) = helper::helper_plan_path() {
        return helper::run_helper(&plan_path);
    }

    gio::resources_register_include!("bootable.gresource")
        .expect("registering embedded gresource bundle");

    let app = adw::Application::builder()
        .application_id("io.bootable.app")
        .build();
    app.connect_startup(|_| {
        if let Some(display) = gdk::Display::default() {
            let theme = gtk::IconTheme::for_display(&display);
            theme.add_resource_path("/io/bootable/app/icons");
        }
    });
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
        .icon_name("io.bootable.app")
        .build();

    // ---------- Header bar ----------
    let title = adw::WindowTitle::new("Bootable", "");
    let header = adw::HeaderBar::builder().title_widget(&title).build();

    let menu = gio::Menu::new();
    menu.append(Some("Keyboard Shortcuts"), Some("win.show-shortcuts"));
    menu.append(Some("About Bootable"), Some("win.show-about"));
    menu.append(Some("Quit"), Some("app.quit"));
    let menu_button = gtk::MenuButton::builder()
        .icon_name("open-menu-symbolic")
        .menu_model(&menu)
        .primary(true)
        .tooltip_text("Main menu")
        .build();
    header.pack_end(&menu_button);

    // ---------- Source group ----------
    let iso_row = adw::EntryRow::builder()
        .title("Image file")
        .editable(false)
        .build();
    let iso_browse_button = build_suffix_button("document-open-symbolic", "Select image");
    iso_row.add_suffix(&iso_browse_button);

    let checksum_row = adw::EntryRow::builder()
        .title("Checksum (optional)")
        .build();
    let checksum_browse_button =
        build_suffix_button("document-open-symbolic", "Select checksum file");
    checksum_row.add_suffix(&checksum_browse_button);

    let signature_row = adw::EntryRow::builder()
        .title("Signature (optional)")
        .build();
    let signature_browse_button =
        build_suffix_button("document-open-symbolic", "Select signature file");
    signature_row.add_suffix(&signature_browse_button);

    let source_group = adw::PreferencesGroup::builder()
        .title("Image source")
        .description("The file to write and optional verification material.")
        .build();
    source_group.add(&iso_row);
    source_group.add(&checksum_row);
    source_group.add(&signature_row);

    // ---------- Target group ----------
    let device_model = gtk::StringList::new(&[]);
    let device_dropdown = gtk::DropDown::new(Some(device_model.clone()), None::<&gtk::Expression>);
    device_dropdown.set_hexpand(true);
    device_dropdown.set_valign(gtk::Align::Center);
    let device_refresh_button = build_suffix_button("view-refresh-symbolic", "Refresh (F5)");
    device_refresh_button.set_action_name(Some("win.refresh"));
    let device_row = adw::ActionRow::builder()
        .title("Device")
        .subtitle("Removable USB target")
        .build();
    device_row.add_suffix(&device_dropdown);
    device_row.add_suffix(&device_refresh_button);

    let partition_model = gtk::StringList::new(&["GPT", "MBR"]);
    let partition_row = adw::ComboRow::builder()
        .title("Partition scheme")
        .model(&partition_model)
        .selected(0)
        .use_subtitle(true)
        .build();

    let fs_model = gtk::StringList::new(&["FAT32", "NTFS"]);
    let fs_row = adw::ComboRow::builder()
        .title("File system")
        .model(&fs_model)
        .selected(0)
        .use_subtitle(true)
        .build();

    let volume_row = adw::EntryRow::builder()
        .title("Volume label")
        .text("BOOTABLE")
        .build();

    let target_group = adw::PreferencesGroup::builder()
        .title("Target device")
        .description("Where the image will be written. All data on the device will be erased.")
        .build();
    target_group.add(&device_row);
    target_group.add(&partition_row);
    target_group.add(&fs_row);
    target_group.add(&volume_row);

    // ---------- Boot config group ----------
    let mode_model =
        gtk::StringList::new(&["Auto (detect)", "ISOHybrid / DD", "Windows (UEFI/BIOS)"]);
    let mode_row = adw::ComboRow::builder()
        .title("Image mode")
        .model(&mode_model)
        .selected(0)
        .use_subtitle(true)
        .build();

    let target_system_model = gtk::StringList::new(&["UEFI", "BIOS", "UEFI + BIOS"]);
    let target_system_row = adw::ComboRow::builder()
        .title("Target system")
        .model(&target_system_model)
        .selected(0)
        .use_subtitle(true)
        .build();

    let secure_row = adw::SwitchRow::builder()
        .title("Secure Boot")
        .subtitle("Require signed shim/grub (Windows NTFS + UEFI only)")
        .active(false)
        .build();

    let boot_group = adw::PreferencesGroup::builder()
        .title("Boot configuration")
        .build();
    boot_group.add(&mode_row);
    boot_group.add(&target_system_row);
    boot_group.add(&secure_row);

    // ---------- Advanced group ----------
    let persistence_adjustment = gtk::Adjustment::new(0.0, 0.0, 1_048_576.0, 64.0, 512.0, 0.0);
    let persistence_row = adw::SpinRow::builder()
        .title("Persistence (MiB)")
        .subtitle("Live-media persistence partition size. 0 disables.")
        .adjustment(&persistence_adjustment)
        .digits(0)
        .snap_to_ticks(true)
        .build();

    let persistence_label_row = adw::EntryRow::builder()
        .title("Persistence label")
        .text("persistence")
        .build();

    let verify_row = adw::SwitchRow::builder()
        .title("Verify")
        .subtitle("Re-read the written data and compare to the source")
        .active(false)
        .build();

    let dry_run_row = adw::SwitchRow::builder()
        .title("Dry run")
        .subtitle("Plan the operation but don't write anything")
        .active(false)
        .build();

    let advanced_group = adw::PreferencesGroup::builder()
        .title("Advanced")
        .description("Live-media persistence, verification, and dry-run.")
        .build();
    advanced_group.add(&persistence_row);
    advanced_group.add(&persistence_label_row);
    advanced_group.add(&verify_row);
    advanced_group.add(&dry_run_row);

    // ---------- Start button ----------
    let start_button = gtk::Button::builder()
        .label("Start Write")
        .halign(gtk::Align::Center)
        .margin_top(8)
        .margin_bottom(8)
        .action_name("win.start")
        .build();
    start_button.add_css_class("pill");
    start_button.add_css_class("suggested-action");

    // ---------- Log view ----------
    let log_view = gtk::TextView::builder()
        .editable(false)
        .cursor_visible(false)
        .monospace(true)
        .wrap_mode(gtk::WrapMode::WordChar)
        .build();
    let log_buffer = log_view.buffer();
    let log_scroller = gtk::ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .min_content_height(160)
        .child(&log_view)
        .build();
    log_scroller.add_css_class("card");

    let log_frame = adw::PreferencesGroup::builder()
        .title("Activity log")
        .build();
    log_frame.add(&log_scroller);

    // ---------- StatusPage for empty device list ----------
    let empty_status = adw::StatusPage::builder()
        .icon_name("drive-removable-media-symbolic")
        .title("No removable devices")
        .description("Plug in a USB drive or memory card to get started.")
        .vexpand(true)
        .build();
    empty_status.add_css_class("compact");

    // ---------- Preferences page ----------
    let prefs_page = adw::PreferencesPage::new();
    prefs_page.add(&source_group);
    prefs_page.add(&target_group);
    prefs_page.add(&boot_group);
    prefs_page.add(&advanced_group);

    let start_group = adw::PreferencesGroup::new();
    start_group.add(&start_button);
    prefs_page.add(&start_group);
    prefs_page.add(&log_frame);

    // ---------- Flash-in-progress banner ----------
    let flash_banner = adw::Banner::builder()
        .title("Write in progress — device list refresh is paused")
        .revealed(false)
        .build();

    // ---------- Content stack (form vs. empty state) ----------
    let content_stack = gtk::Stack::new();
    content_stack.set_transition_type(gtk::StackTransitionType::Crossfade);
    content_stack.add_named(&prefs_page, Some("form"));
    content_stack.add_named(&empty_status, Some("empty"));
    content_stack.set_visible_child_name("form");

    let content_box = gtk::Box::builder()
        .orientation(gtk::Orientation::Vertical)
        .build();
    content_box.append(&flash_banner);
    content_box.append(&content_stack);

    // ---------- Toast overlay ----------
    let toast_overlay = adw::ToastOverlay::new();
    toast_overlay.set_child(Some(&content_box));

    // ---------- Bottom-bar progress ----------
    let progress = gtk::ProgressBar::builder()
        .show_text(true)
        .margin_top(6)
        .margin_bottom(6)
        .margin_start(12)
        .margin_end(12)
        .build();
    let bottom_bar = gtk::Box::builder()
        .orientation(gtk::Orientation::Vertical)
        .build();
    bottom_bar.append(&progress);

    // ---------- Toolbar view ----------
    let toolbar_view = adw::ToolbarView::new();
    toolbar_view.add_top_bar(&header);
    toolbar_view.set_content(Some(&toast_overlay));
    toolbar_view.add_bottom_bar(&bottom_bar);

    window.set_content(Some(&toolbar_view));

    // ---------- Adaptive breakpoint ----------
    let breakpoint = adw::Breakpoint::new(adw::BreakpointCondition::new_length(
        adw::BreakpointConditionLengthType::MaxWidth,
        500.0,
        adw::LengthUnit::Sp,
    ));
    window.add_breakpoint(breakpoint);

    window.present();

    // ---------- Shared state ----------
    let devices_state: Rc<RefCell<Vec<devices::Device>>> = Rc::new(RefCell::new(Vec::new()));
    let flashing: Rc<RefCell<bool>> = Rc::new(RefCell::new(false));

    // ---------- update_controls closure ----------
    let update_controls: Rc<dyn Fn()> = {
        let mode_row = mode_row.clone();
        let partition_row = partition_row.clone();
        let target_system_row = target_system_row.clone();
        let fs_row = fs_row.clone();
        let volume_row = volume_row.clone();
        let secure_row = secure_row.clone();
        let persistence_row = persistence_row.clone();
        let persistence_label_row = persistence_label_row.clone();
        Rc::new(move || {
            let dd_mode = mode_row.selected() == 1;
            let target = target_system_row.selected();
            let fs_idx = fs_row.selected();
            let uefi_enabled = target != 1;
            let bios_enabled = target != 0;
            let ntfs_selected = fs_idx == 1;

            if dd_mode {
                partition_row.set_sensitive(false);
                target_system_row.set_sensitive(false);
                fs_row.set_sensitive(false);
                volume_row.set_sensitive(false);
                secure_row.set_sensitive(false);
                secure_row.set_active(false);
                persistence_row.set_sensitive(true);
                persistence_label_row.set_sensitive(true);
                return;
            }

            target_system_row.set_sensitive(true);
            fs_row.set_sensitive(true);
            volume_row.set_sensitive(true);
            persistence_row.set_sensitive(false);
            persistence_label_row.set_sensitive(false);

            if bios_enabled {
                if partition_row.selected() != 1 {
                    partition_row.set_selected(1);
                }
                partition_row.set_sensitive(false);
            } else {
                partition_row.set_sensitive(true);
            }

            let secure_allowed = ntfs_selected && uefi_enabled;
            if !secure_allowed {
                secure_row.set_active(false);
            }
            secure_row.set_sensitive(secure_allowed);
        })
    };
    update_controls();

    {
        let update = update_controls.clone();
        mode_row.connect_selected_notify(move |_| update());
    }
    {
        let update = update_controls.clone();
        target_system_row.connect_selected_notify(move |_| update());
    }
    {
        let update = update_controls.clone();
        fs_row.connect_selected_notify(move |_| update());
    }

    // ---------- Device list refresh ----------
    let update_stack_visibility: Rc<dyn Fn()> = {
        let devices_state = devices_state.clone();
        Rc::new(move || {
            let name = if devices_state.borrow().is_empty() {
                "empty"
            } else {
                "form"
            };
            content_stack.set_visible_child_name(name);
        })
    };

    let refresh_devices: Rc<dyn Fn(bool, bool)> = {
        let devices_state = devices_state.clone();
        let log_buffer = log_buffer.clone();
        let device_dropdown = device_dropdown.clone();
        let flashing = flashing.clone();
        let toast_overlay = toast_overlay.clone();
        Rc::new(move |manual: bool, log_when_skipped: bool| {
            if *flashing.borrow() {
                if log_when_skipped {
                    append_log(&log_buffer, "Flash in progress; device refresh skipped");
                }
                return;
            }
            let previous_path = {
                let devices = devices_state.borrow();
                let selected = device_dropdown.selected() as usize;
                devices.get(selected).map(|dev| dev.path.clone())
            };
            match devices::list_removable() {
                Ok(list) => {
                    devices_state.borrow_mut().clear();
                    devices_state.borrow_mut().extend(list);
                    let count = device_model.n_items();
                    if count > 0 {
                        device_model.splice(0, count, &[]);
                    }
                    for dev in devices_state.borrow().iter() {
                        device_model.append(&dev.display);
                    }
                    if let Some(path) = previous_path {
                        if let Some(idx) = devices_state
                            .borrow()
                            .iter()
                            .position(|dev| dev.path == path)
                        {
                            device_dropdown.set_selected(
                                u32::try_from(idx).unwrap_or(gtk::INVALID_LIST_POSITION),
                            );
                        } else {
                            device_dropdown.set_selected(gtk::INVALID_LIST_POSITION);
                        }
                    } else if device_model.n_items() == 0 {
                        device_dropdown.set_selected(gtk::INVALID_LIST_POSITION);
                    }
                    update_stack_visibility();
                    if manual {
                        toast_overlay.add_toast(adw::Toast::new("Device list refreshed"));
                    }
                }
                Err(err) => {
                    append_log(&log_buffer, &format!("Device scan failed: {err}"));
                    toast_overlay.add_toast(adw::Toast::new("Device scan failed"));
                }
            }
        })
    };
    refresh_devices(false, false);

    // ---------- Volume monitor (debounced) ----------
    let refresh_timer: Rc<RefCell<Option<glib::SourceId>>> = Rc::new(RefCell::new(None));
    let schedule_refresh: Rc<dyn Fn()> = {
        let refresh_devices = refresh_devices.clone();
        Rc::new(move || {
            if let Some(id) = refresh_timer.borrow_mut().take() {
                id.remove();
            }
            let refresh_devices = refresh_devices.clone();
            let refresh_timer_for_cb = refresh_timer.clone();
            let id = glib::timeout_add_local_once(Duration::from_millis(400), move || {
                refresh_devices(false, false);
                refresh_timer_for_cb.borrow_mut().take();
            });
            *refresh_timer.borrow_mut() = Some(id);
        })
    };

    let monitor = gio::VolumeMonitor::get();
    for wire in [
        wire_monitor_drive_connected as fn(&gio::VolumeMonitor, Rc<dyn Fn()>),
        wire_monitor_drive_disconnected,
        wire_monitor_volume_added,
        wire_monitor_volume_removed,
        wire_monitor_mount_added,
        wire_monitor_mount_removed,
    ] {
        wire(&monitor, schedule_refresh.clone());
    }

    // ---------- async-channel wiring ----------
    let (sender, receiver) = async_channel::unbounded::<UiEvent>();

    glib::spawn_future_local(clone!(
        #[strong]
        progress,
        #[strong]
        log_buffer,
        #[strong]
        flashing,
        #[strong]
        flash_banner,
        #[strong]
        source_group,
        #[strong]
        target_group,
        #[strong]
        boot_group,
        #[strong]
        advanced_group,
        #[strong]
        start_button,
        #[strong]
        update_controls,
        #[strong]
        toast_overlay,
        async move {
            while let Ok(event) = receiver.recv().await {
                match event {
                    UiEvent::Log(msg) => append_log(&log_buffer, &msg),
                    UiEvent::Progress(frac) => {
                        progress.set_fraction(frac);
                        progress.set_text(Some(&format!("{:.0}%", frac * 100.0)));
                    }
                    UiEvent::Done(result) => {
                        match result {
                            Ok(()) => {
                                append_log(&log_buffer, "Completed successfully");
                                toast_overlay.add_toast(adw::Toast::new("Write completed"));
                            }
                            Err(err) => {
                                append_log(&log_buffer, &format!("Error: {err}"));
                                toast_overlay.add_toast(adw::Toast::new("Write failed — see log"));
                            }
                        }
                        *flashing.borrow_mut() = false;
                        flash_banner.set_revealed(false);
                        set_groups_sensitive(
                            &[&source_group, &target_group, &boot_group, &advanced_group],
                            true,
                        );
                        start_button.set_sensitive(true);
                        update_controls();
                    }
                }
            }
        }
    ));

    // ---------- File pickers ----------
    {
        let window = window.clone();
        let iso_row = iso_row.clone();
        iso_browse_button.connect_clicked(move |_| {
            pick_image_file(&window, &iso_row);
        });
    }
    {
        let window = window.clone();
        let checksum_row = checksum_row.clone();
        checksum_browse_button.connect_clicked(move |_| {
            pick_plain_file(&window, &checksum_row, "Select checksum file");
        });
    }
    {
        let window = window.clone();
        let signature_row = signature_row.clone();
        signature_browse_button.connect_clicked(move |_| {
            pick_plain_file(&window, &signature_row, "Select signature file");
        });
    }

    // ---------- Actions ----------
    let refresh_action = gio::ActionEntry::builder("refresh")
        .activate(clone!(
            #[strong]
            refresh_devices,
            move |_: &adw::ApplicationWindow, _, _| {
                refresh_devices(true, true);
            }
        ))
        .build();

    let select_image_action = gio::ActionEntry::builder("select-image")
        .activate(clone!(
            #[strong]
            iso_row,
            move |w: &adw::ApplicationWindow, _, _| {
                pick_image_file(w, &iso_row);
            }
        ))
        .build();

    let show_about_action = gio::ActionEntry::builder("show-about")
        .activate(|w: &adw::ApplicationWindow, _, _| {
            show_about_dialog(w);
        })
        .build();

    let show_shortcuts_action = gio::ActionEntry::builder("show-shortcuts")
        .activate(|w: &adw::ApplicationWindow, _, _| {
            show_shortcuts_dialog(w);
        })
        .build();

    let start_action_cb =
        move |win: &adw::ApplicationWindow, _: &gio::SimpleAction, _: Option<&glib::Variant>| {
            let iso_text = iso_row.text().to_string();
            if iso_text.trim().is_empty() {
                toast_overlay.add_toast(adw::Toast::new("Select an image first"));
                return;
            }

            let iso_path = PathBuf::from(iso_text);
            let selected = device_dropdown.selected() as usize;
            let devices = devices_state.borrow();
            let Some(device) = devices.get(selected) else {
                toast_overlay.add_toast(adw::Toast::new("Select a target device"));
                return;
            };
            let device = device.clone();
            drop(devices);

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
                    ),
                );
                toast_overlay.add_toast(adw::Toast::new("Target is mounted on a system path"));
                return;
            }

            let dry_run = dry_run_row.is_active();
            if !dry_run && !util::is_root() && !util::command_exists("pkexec") {
                append_log(
                    &log_buffer,
                    "pkexec not found; install polkit to enable admin writes.",
                );
                return;
            }

            let plan = WritePlan {
                iso_path,
                device_path: device.path.clone(),
                device_size_bytes: device.size_bytes,
                image_mode: match mode_row.selected() {
                    0 => ImageMode::Auto,
                    1 => ImageMode::IsoHybridDd,
                    _ => ImageMode::WindowsUefi,
                },
                partition_scheme: match partition_row.selected() {
                    0 => PartitionScheme::Gpt,
                    _ => PartitionScheme::Mbr,
                },
                target_system: match target_system_row.selected() {
                    0 => TargetSystem::Uefi,
                    1 => TargetSystem::Bios,
                    _ => TargetSystem::UefiAndBios,
                },
                file_system: if fs_row.selected() == 1 {
                    FileSystem::Ntfs
                } else {
                    FileSystem::Fat32
                },
                volume_label: volume_row.text().to_string(),
                secure_boot_only: secure_row.is_active(),
                verify_after: verify_row.is_active(),
                checksum_sha256: non_empty_text(checksum_row.text().as_str()),
                signature_path: non_empty_text(signature_row.text().as_str()).map(PathBuf::from),
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                persistence_size_mib: persistence_row.value() as u64,
                persistence_label: persistence_label_row.text().to_string(),
                dry_run,
            };

            let sender = sender.clone();
            let log_buffer = log_buffer.clone();
            let flashing = flashing.clone();
            let flash_banner = flash_banner.clone();
            let source_group = source_group.clone();
            let target_group = target_group.clone();
            let boot_group = boot_group.clone();
            let advanced_group = advanced_group.clone();
            let start_button = start_button.clone();
            let progress = progress.clone();
            let toast_overlay_inner = toast_overlay.clone();

            show_confirm_dialog(win, &device, &mountpoints, move || {
                append_log(
                    &log_buffer,
                    &format!(
                        "Starting write to {device_path}",
                        device_path = plan.device_path.as_str()
                    ),
                );
                toast_overlay_inner.add_toast(adw::Toast::new("Write started"));
                if !util::is_root() && !plan.dry_run {
                    append_log(&log_buffer, "Requesting admin access (pkexec)...");
                }
                progress.set_fraction(0.0);
                progress.set_text(Some("0%"));
                set_groups_sensitive(
                    &[&source_group, &target_group, &boot_group, &advanced_group],
                    false,
                );
                start_button.set_sensitive(false);
                *flashing.borrow_mut() = true;
                flash_banner.set_revealed(true);

                let sender = sender.clone();
                let sender_panic = sender.clone();
                let dry_run = plan.dry_run;
                std::thread::spawn(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if util::is_root() || dry_run {
                            let sender = sender.clone();
                            writer::run(&plan, move |event| {
                                let _ = sender.send_blocking(event);
                            });
                        } else {
                            let sender_events = sender.clone();
                            let result = helper::run_helper_with_pkexec(&plan, move |event| {
                                let _ = sender_events.send_blocking(event);
                            });
                            if let Err(err) = result {
                                let _ = sender.send_blocking(UiEvent::Done(Err(err)));
                            }
                        }
                    }));
                    if let Err(payload) = outcome {
                        let msg = payload
                            .downcast_ref::<&'static str>()
                            .map(|s| (*s).to_string())
                            .or_else(|| payload.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "unknown panic".to_string());
                        let _ = sender_panic.send_blocking(UiEvent::Done(Err(anyhow::anyhow!(
                            "Write worker panicked: {msg}"
                        ))));
                    }
                });
            });
        };
    let start_action = gio::ActionEntry::builder("start")
        .activate(start_action_cb)
        .build();

    window.add_action_entries([
        refresh_action,
        select_image_action,
        show_about_action,
        show_shortcuts_action,
        start_action,
    ]);

    let quit_action = gio::ActionEntry::builder("quit")
        .activate(|app: &adw::Application, _, _| app.quit())
        .build();
    app.add_action_entries([quit_action]);

    app.set_accels_for_action("win.refresh", &["F5"]);
    app.set_accels_for_action("win.select-image", &["<Ctrl>o"]);
    app.set_accels_for_action("win.show-about", &["<Ctrl>i"]);
    app.set_accels_for_action("win.show-shortcuts", &["<Ctrl>question"]);
    app.set_accels_for_action("app.quit", &["<Ctrl>q"]);
}

// ---------- Helpers ----------

fn build_suffix_button(icon: &str, tooltip: &str) -> gtk::Button {
    let b = gtk::Button::from_icon_name(icon);
    b.set_valign(gtk::Align::Center);
    b.set_tooltip_text(Some(tooltip));
    b.add_css_class("flat");
    b
}

fn non_empty_text(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn append_log(buffer: &gtk::TextBuffer, msg: &str) {
    let mut end = buffer.end_iter();
    buffer.insert(&mut end, msg);
    buffer.insert(&mut end, "\n");
}

fn set_groups_sensitive(groups: &[&adw::PreferencesGroup], sensitive: bool) {
    for group in groups {
        group.set_sensitive(sensitive);
    }
}

fn pick_image_file(window: &adw::ApplicationWindow, row: &adw::EntryRow) {
    let dialog = gtk::FileDialog::builder()
        .title("Select image")
        .modal(true)
        .build();
    let image_filter = gtk::FileFilter::new();
    image_filter.set_name(Some("Disk images"));
    for pat in ["*.iso", "*.img", "*.raw", "*.bin"] {
        image_filter.add_pattern(pat);
    }
    let all_filter = gtk::FileFilter::new();
    all_filter.set_name(Some("All files"));
    all_filter.add_pattern("*");
    let filters = gio::ListStore::new::<gtk::FileFilter>();
    filters.append(&image_filter);
    filters.append(&all_filter);
    dialog.set_filters(Some(&filters));
    dialog.set_default_filter(Some(&image_filter));
    let row = row.clone();
    dialog.open(Some(window), None::<&gio::Cancellable>, move |result| {
        if let Ok(file) = result
            && let Some(path) = file.path()
        {
            row.set_text(path.to_string_lossy().as_ref());
        }
    });
}

fn pick_plain_file(window: &adw::ApplicationWindow, row: &adw::EntryRow, title: &str) {
    let dialog = gtk::FileDialog::builder().title(title).modal(true).build();
    let row = row.clone();
    dialog.open(Some(window), None::<&gio::Cancellable>, move |result| {
        if let Ok(file) = result
            && let Some(path) = file.path()
        {
            row.set_text(path.to_string_lossy().as_ref());
        }
    });
}

fn show_confirm_dialog(
    window: &adw::ApplicationWindow,
    device: &devices::Device,
    mountpoints: &[String],
    on_confirm: impl FnOnce() + 'static,
) {
    let body = if mountpoints.is_empty() {
        format!(
            "You are about to erase {display}.\nThis action cannot be undone.",
            display = device.display
        )
    } else {
        format!(
            "You are about to erase {display}.\nThis action cannot be undone.\n\nCurrently mounted: {mounts}",
            display = device.display,
            mounts = mountpoints.join(", ")
        )
    };

    let dialog = adw::AlertDialog::builder()
        .heading("Confirm erase")
        .body(&body)
        .build();
    dialog.add_response("cancel", "Cancel");
    dialog.add_response("erase", "Erase");
    dialog.set_response_appearance("erase", adw::ResponseAppearance::Destructive);
    dialog.set_default_response(Some("cancel"));
    dialog.set_close_response("cancel");

    let extra = gtk::Box::builder()
        .orientation(gtk::Orientation::Vertical)
        .spacing(8)
        .build();

    let prompt = gtk::Label::new(Some(&format!(
        "Type {path} to confirm:",
        path = device.path
    )));
    prompt.set_halign(gtk::Align::Start);
    prompt.set_wrap(true);
    extra.append(&prompt);

    let confirm_entry = gtk::Entry::builder().placeholder_text(&device.path).build();
    extra.append(&confirm_entry);

    let confirm_check = gtk::CheckButton::with_label("I understand this will erase all data");
    extra.append(&confirm_check);

    let error_label = gtk::Label::new(None);
    error_label.set_halign(gtk::Align::Start);
    error_label.add_css_class("error");
    extra.append(&error_label);

    dialog.set_extra_child(Some(&extra));

    let on_confirm = Rc::new(RefCell::new(Some(on_confirm)));
    let device_path = device.path.clone();
    dialog.connect_response(None, move |d, response| {
        if response != "erase" {
            return;
        }
        let typed = confirm_entry.text().to_string();
        if typed.trim() != device_path {
            error_label.set_text("Device path does not match.");
            return;
        }
        if !confirm_check.is_active() {
            error_label.set_text("Please confirm the data loss checkbox.");
            return;
        }
        d.close();
        if let Some(cb) = on_confirm.borrow_mut().take() {
            cb();
        }
    });

    dialog.present(Some(window));
}

fn show_about_dialog(window: &adw::ApplicationWindow) {
    let dialog = adw::AboutDialog::builder()
        .application_name("Bootable")
        .application_icon("io.bootable.app")
        .version(env!("CARGO_PKG_VERSION"))
        .license_type(gtk::License::Gpl30Only)
        .website("https://github.com/Pingasmaster/bootable")
        .issue_url("https://github.com/Pingasmaster/bootable/issues")
        .developer_name("Bootable contributors")
        .build();
    dialog.present(Some(window));
}

fn show_shortcuts_dialog(window: &adw::ApplicationWindow) {
    let body = "\
F5\t\tRefresh device list
Ctrl+O\t\tSelect image
Ctrl+I\t\tAbout Bootable
Ctrl+?\t\tThis dialog
Ctrl+Q\t\tQuit";
    let dialog = adw::AlertDialog::builder()
        .heading("Keyboard shortcuts")
        .body(body)
        .build();
    dialog.add_response("ok", "Close");
    dialog.set_default_response(Some("ok"));
    dialog.set_close_response("ok");
    dialog.present(Some(window));
}

// ---------- Volume monitor wires (factored out so the loop above is typed) ----------

fn wire_monitor_drive_connected(monitor: &gio::VolumeMonitor, schedule: Rc<dyn Fn()>) {
    monitor.connect_drive_connected(move |_, _| schedule());
}
fn wire_monitor_drive_disconnected(monitor: &gio::VolumeMonitor, schedule: Rc<dyn Fn()>) {
    monitor.connect_drive_disconnected(move |_, _| schedule());
}
fn wire_monitor_volume_added(monitor: &gio::VolumeMonitor, schedule: Rc<dyn Fn()>) {
    monitor.connect_volume_added(move |_, _| schedule());
}
fn wire_monitor_volume_removed(monitor: &gio::VolumeMonitor, schedule: Rc<dyn Fn()>) {
    monitor.connect_volume_removed(move |_, _| schedule());
}
fn wire_monitor_mount_added(monitor: &gio::VolumeMonitor, schedule: Rc<dyn Fn()>) {
    monitor.connect_mount_added(move |_, _| schedule());
}
fn wire_monitor_mount_removed(monitor: &gio::VolumeMonitor, schedule: Rc<dyn Fn()>) {
    monitor.connect_mount_removed(move |_, _| schedule());
}

// ---------- System-mount block helpers ----------

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
        for path in [
            "/",
            "/boot",
            "/boot/efi",
            "/home",
            "/usr",
            "/var",
            "/root",
            "/etc",
            "/opt",
            "/srv",
            "/run",
            "/tmp",
            "/proc",
            "/sys",
            "/dev",
            "/snap",
            "/nix",
        ] {
            let mounts = vec![path.to_string()];
            assert!(system_mount_block(&mounts).is_some(), "should block {path}");
        }
    }

    #[test]
    fn non_empty_text_handles_blanks() {
        assert_eq!(non_empty_text(""), None);
        assert_eq!(non_empty_text("   "), None);
        assert_eq!(non_empty_text("value"), Some("value".to_string()));
        assert_eq!(non_empty_text("  trim  "), Some("trim".to_string()));
    }
}
