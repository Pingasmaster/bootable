# Modernization research — bootable

This document surveys where the bootable codebase is up-to-date and where it lags
behind current GTK4 / libadwaita / Rust practice. Each finding links to the exact
file and line in the current tree, names the modern replacement, and notes the
minimum crate/feature version required. Nothing in this document has been
applied — it's a punch list for future PRs.

Verified against source (read end-to-end), strict clippy (run locally against
the current `master`), `cargo info` for every crate mentioned, and the canonical
GTK4 / libadwaita / gtk-rs docs. Subagent findings that didn't hold up under
direct verification are not included.

## Baseline — what's already correct

Worth calling out so it doesn't get re-analyzed:

- **`#![forbid(unsafe_code)]`** on every source file. Keep it.
- **Rust edition 2024**. Let-chains and let-else already in use at
  `devices.rs:145`, `main.rs:391`, `main.rs:408`, `main.rs:425`, `main.rs:503`,
  `writer.rs:1304`, `writer.rs:2262`.
- **`gtk::FileDialog`** (main.rs:368–430) is the modern replacement for the
  deprecated `FileChooserDialog` / `FileChooserNative`. Good.
- **`adw::ToolbarView`** (main.rs:193) wraps content + header. Good.
- **`gio::VolumeMonitor`** (main.rs:327) is the right abstraction for device
  hot-plug in a GTK app — don't replace it with `udev` crates.
- **`lsblk -J` parsing** (devices.rs:22–39) uses a typed serde schema with
  `#[serde(rename = "type")]` — not hand-rolled.
- **`helper.rs` pkexec IPC** uses a line-oriented tab-delimited protocol with
  `sanitize_line` (helper.rs:185–207) to strip `\r\n\t` on the wire so a
  multi-line subprocess error can't inject fake protocol lines. Correct.
- **No deprecated widgets in use**: no `adw::Leaflet`, `adw::Flap`,
  `adw::Squeezer`, `adw::MessageDialog`, `adw::ViewSwitcherTitle`,
  `adw::AboutWindow`, `gtk::Dialog`, `gtk::MessageDialog`, or
  `FileChooserDialog`. The codebase already avoids every headline libadwaita 1.4
  deprecation.
- **Dependencies are recent**:
    - `gtk4 = 0.11.1` (latest 0.11.2 — patch bump available)
    - `libadwaita = 0.9.1` (latest, C library pinned at `v1_8`;
      `v1_9` is also available via feature flag)
    - `serde = 1.0.228`, `serde_json = 1.0.149`, `anyhow = 1.0.102`,
      `tempfile = 3.27.0`, `sha2 = 0.10.9` — all current.

---

## 1. Strict clippy is broken — 11 errors (highest priority)

`CLAUDE.md` mandates:

```
cargo clippy -- -D warnings -W clippy::pedantic -W clippy::nursery -W clippy::cargo
```

must be clean. It isn't. Eleven errors, all mechanical, verified locally:

| File:line | Lint | Fix |
|---|---|---|
| `devices.rs:197` | `redundant_closure_for_method_calls` | `tran.map(str::to_string)` |
| `devices.rs:199` | `redundant_closure_for_method_calls` | `path.map(str::to_string)` |
| `devices.rs:314` | `manual_string_new` | `String::new()` in the test fixture |
| `writer.rs:2951` | `match_wildcard_for_single_variants` | explicit `CmdEvent::Log(..)` arm |
| `writer.rs:2966` | `match_wildcard_for_single_variants` | explicit `CmdEvent::Progress(_)` arm |
| `writer.rs:2981` | `match_wildcard_for_single_variants` | same |
| `writer.rs:3001` | `match_wildcard_for_single_variants` | same |
| `writer.rs:3011` | `unchecked_time_subtraction` | `Instant::now().checked_sub(Duration::from_secs(1)).unwrap()` |
| `writer.rs:3035` | `unchecked_time_subtraction` | same |
| `writer.rs:3048` | `unchecked_time_subtraction` | same |
| `writer.rs:3057` | `unchecked_time_subtraction` | same |

None are architectural. `unchecked_time_subtraction` is a new nursery lint
(clippy 1.94) which is why the repo used to pass. This is the *only* finding in
this document that blocks a currently-documented policy; everything else is
improvement.

---

## 2. UI — replace the hand-rolled form with `adw::PreferencesGroup` + rows

The biggest single modernization win. The entire settings form
(`main.rs:54–168`) is built procedurally:

```rust
let grid = gtk::Grid::builder()...;
let device_list = gtk::StringList::new(&[]);
let device_dropdown = gtk::DropDown::new(...);
...
add_row(&grid, 0, "Device", &device_row);
```

With a hand-rolled `add_row()` helper at `main.rs:646` that attaches a `Label`
in column 0 and the widget in column 1. The idiomatic libadwaita way is a
`PreferencesGroup` containing specialized rows. Every row type needed is
available at the pinned `v1_8`:

| Current widget (main.rs line) | Replacement | Available since |
|---|---|---|
| `Grid` wrapper (main.rs:54) | `adw::PreferencesGroup` | 1.0 |
| `Entry` + Select button (iso, checksum, signature, volume label: 69, 102, 116, 126) | `adw::EntryRow` with `add_suffix(&button)` and optional `show_apply_button=true` | 1.2 |
| `DropDown` (mode, partition, target, fs: 81, 87, 93, 98) | `adw::ComboRow` with `use_subtitle=true` | 1.0 |
| `Switch` + `Label` (secure boot, verify, dry run: 105–168) | `adw::SwitchRow` with `subtitle` | 1.4 |
| `SpinButton` (persistence: 147) | `adw::SpinRow::new_with_range` | 1.4 |
| device row (dropdown + Refresh button: 64–67) | `adw::ActionRow` with suffixes | 1.0 |

Proposed grouping, scannable by a new user:

```
PreferencesPage
├─ Group "Image source":    iso EntryRow, checksum EntryRow, signature EntryRow
├─ Group "Target device":   device ActionRow, partition ComboRow, fs ComboRow, label EntryRow
├─ Group "Boot configuration": image-mode ComboRow, target-system ComboRow, secure-boot SwitchRow
└─ Group "Advanced":        persistence SpinRow, verify SwitchRow, dry-run SwitchRow
```

### Secondary benefits unlocked

- The `add_row()` helper can be deleted.
- Label ↔ widget accessibility association is handled by the rows themselves
  (see §7) — no manual `set_mnemonic_widget` wiring needed.
- Consistent HIG-compliant spacing, destructive/suggested styling, dark/light
  mode, RTL layout all come for free.
- The form becomes responsive inside `adw::PreferencesPage` without adding any
  breakpoint code.

### Verified

`adw::PreferencesGroup`, `EntryRow`, `ComboRow`, `SwitchRow`, `SpinRow`, and
`ActionRow` docs on gnome.pages.gitlab.gnome.org/libadwaita/doc/main/ — all
confirm the "since" versions above, all methods exist in the Rust bindings at
`libadwaita = 0.9.1` with `v1_8`.

---

## 3. UI — `main.rs:736–842` custom confirmation dialog → `adw::AlertDialog`

The erase-confirmation dialog is a 100-line hand-built `gtk::Window` containing
a warning label, a mounts-list label, a type-the-device-path `Entry`, an
"I understand" `CheckButton`, an error label, and Cancel/Erase buttons. The
Erase button gets styled with `add_css_class("destructive-action")`.

Replace with `adw::AlertDialog`:

- Introduced **libadwaita 1.5** (pinned `v1_8` has it).
- `adw::MessageDialog` is **deprecated since libadwaita 1.6**; the deprecation
  notice explicitly points at `AdwAlertDialog`.
- `set_extra_child(Option<&impl IsA<Widget>>)` hosts the typed-to-confirm entry
  + checkbox. Verified — this is the whole reason to use it.
- `add_response("erase", "Erase")` +
  `set_response_appearance("erase", ResponseAppearance::Destructive)` replaces
  the manual CSS class.
- `set_default_response(Some("cancel"))` + `set_close_response("cancel")` gives
  correct Enter / Escape semantics.
- `choose(parent, cancellable, cb)` is the GIO-async response API.
- Adaptive: renders as a centered floating dialog on wide screens and
  automatically becomes a bottom sheet on narrow widths. The current
  `gtk::Window` implementation has no such adaptivity.

```rust
let dialog = adw::AlertDialog::builder()
    .heading("Confirm erase")
    .body(&body_text)
    .build();
dialog.add_response("cancel", "Cancel");
dialog.add_response("erase",  "Erase");
dialog.set_response_appearance("erase", adw::ResponseAppearance::Destructive);
dialog.set_default_response(Some("cancel"));
dialog.set_close_response("cancel");

let extra = gtk::Box::builder().orientation(Vertical).spacing(8).build();
// prompt label, confirm_entry, confirm_check, error_label
dialog.set_extra_child(Some(&extra));

dialog.connect_response(None, move |d, response| {
    if response == "erase" {
        // validate, close, invoke on_confirm
    }
});
dialog.present(Some(window));
```

The validation-without-closing pattern (don't call `d.close()` if the typed
path doesn't match) preserves the current UX of showing an inline error.

### Follow-up that comes for free

Commit `6728e4b` recently added a `dialog_open: Rc<RefCell<bool>>` guard
(main.rs:201, 491, 601, 641) wired through `on_close` to prevent rapid-click
re-entry. Once the custom dialog is gone, `adw::AlertDialog` is inherently
modal and single-instance — the `dialog_open` flag and its plumbing can be
deleted alongside the migration.

---

## 4. UI — `mpsc` + `idle_add_local` polling → `async-channel` + `spawn_future_local`

`main.rs:432`:
```rust
let (sender, receiver) = mpsc::channel::<UiEvent>();
```

`main.rs:462–488`:
```rust
glib::idle_add_local(move || {
    loop {
        match receiver.try_recv() {
            Ok(event) => { /* handle */ }
            Err(mpsc::TryRecvError::Empty)         => break,
            Err(mpsc::TryRecvError::Disconnected)  => return glib::ControlFlow::Break,
        }
    }
    glib::ControlFlow::Continue
});
```

This polls continuously from idle callbacks — a known anti-pattern. The
gtk4-rs book
(https://gtk-rs.org/gtk4-rs/stable/latest/book/main_event_loop.html)
literally shows `async-channel` + `glib::spawn_future_local` as the idiomatic
replacement, with `send_blocking()` on the worker-thread side.

```rust
let (sender, receiver) = async_channel::unbounded::<UiEvent>();

glib::spawn_future_local(clone!(
    #[strong] progress, #[strong] log_buffer, #[strong] controls,
    #[strong] flashing, #[strong] update_controls,
    async move {
        while let Ok(event) = receiver.recv().await {
            match event {
                UiEvent::Log(msg)     => append_log(&log_buffer, &msg),
                UiEvent::Progress(f)  => { progress.set_fraction(f);
                                           progress.set_text(Some(&format!("{:.0}%", f * 100.0))); }
                UiEvent::Done(result) => {
                    match result {
                        Ok(())   => append_log(&log_buffer, "Completed successfully"),
                        Err(err) => append_log(&log_buffer, &format!("Error: {err}")),
                    }
                    *flashing.borrow_mut() = false;
                    set_controls_sensitive(&controls, true);
                    update_controls();
                }
            }
        }
    }
));
```

Worker thread (main.rs:624): replace `sender.send(event)` with
`sender.send_blocking(event)`. The helper-path worker in `helper.rs:116–143`
already uses `mpsc::channel` internally — that can stay unchanged because it
doesn't cross a thread/main-loop boundary; only the bridge back to the UI
changes.

**Dependency**: `async-channel = "2.5"`, MSRV 1.60, pure Rust, no new
feature flags. Skip `glib::MainContext::channel` — it's older and less
integrated with GLib futures.

---

## 5. UI — smaller libadwaita polish items

All available at `v1_8`:

- **`main.rs:42`**: `title_widget(&gtk::Label::new(Some("Bootable")))` →
  `adw::WindowTitle::new("Bootable", "")`. The idiomatic type; handles
  subtitle styling, RTL, and title-bar WM integration that a bare `Label`
  doesn't.
- **Responsive layout**: add a single `adw::Breakpoint`
  (`max-width: 500sp`) to the window so the form reflows usably on narrow
  widths / tiled desktops. Breakpoints landed in libadwaita 1.4. No
  `BreakpointBin` needed — a single-window app uses window-level breakpoints.
- **`adw::ToastOverlay`** wrapping the main content:
  `main.rs:608` "Starting write to …" and `main.rs:715` "Device list refreshed"
  become `adw::Toast::new("…")` / `toast_overlay.add_toast(toast)` instead of
  log-view noise. Keep the log view as the operational audit trail.
- **`adw::Banner`** for `main.rs:676` "Flash in progress; device refresh
  skipped". A persistent, dismissible warning is the right affordance — a log
  line vanishes off-screen.
- **`adw::StatusPage`** when the device list is empty: "No removable devices —
  plug in a USB drive" with a `drive-removable-media-symbolic` icon.
- **`adw::ToolbarView::add_bottom_bar(&progress_bar)`**: move the
  `ProgressBar` (main.rs:176) out of the main scroll column into a bottom
  toolbar so it stays visible while the log scrolls.

---

## 6. UI — actions, menu, keyboard shortcuts, About

The app has none of these. Every handler is a direct
`button.connect_clicked(...)`. Modernization:

- Convert button handlers on `refresh_button`, `browse_button`, `start_button`,
  `checksum_button`, `signature_button` to
  `gio::ActionEntry::builder(...).activate(...).build()` registered via
  `window.add_action_entries([...])` and wired with
  `button.set_action_name(Some("win.refresh"))`.
- Register accelerators via `app.set_accels_for_action(...)`:

  | Action             | Accel     |
  |--------------------|-----------|
  | `win.refresh`      | `F5`      |
  | `win.select-image` | `<Ctrl>O` |
  | `win.show-about`   | `<Ctrl>I` |
  | `app.quit`         | `<Ctrl>Q` |

- Hamburger menu in the header bar: `gtk::MenuButton` with a `gio::Menu`
  containing "About Bootable", "Keyboard Shortcuts", "Quit".
- `adw::AboutDialog` (libadwaita 1.4+, replaces deprecated `AdwAboutWindow`)
  populated from `Cargo.toml` metadata (name, version, license GPL-3.0-only,
  repository URL).
- `gtk::ShortcutsWindow` is being phased out in newer GTK; a small custom
  `adw::AlertDialog` listing the accels is simpler and future-proof.

---

## 7. UI — accessibility

Mostly absent today. Two categories of fix:

**Subsumed by §2 (rows):** adopting `adw::EntryRow`, `SwitchRow`, `ComboRow`,
`SpinRow`, `ActionRow` auto-associates the row title with the inner control
for screen readers, which eliminates the need for manual
`set_mnemonic_widget` wiring on ~13 form rows. This is the pragmatic path.

**Still worth doing even without §2:**

- `main.rs:646` `add_row()` should call
  `lbl.set_mnemonic_widget(Some(widget))` — one line, links all 13 form
  labels.
- Button construction: `Button::with_label("Refresh")` →
  `Button::with_mnemonic("_Refresh")` and similarly for Select, Start, Cancel,
  Erase. Alt-key navigation works immediately.
- Progress bar (`main.rs:176`) needs
  `progress.update_property(&[Property::ValueMin(0.0), Property::ValueMax(1.0), Property::ValueNow(frac)])`
  on every progress update (main.rs:468) so screen readers announce percentage.
- Confirmation dialog (`main.rs:760` warning label) should set
  `AccessibleRole::Alert` so screen readers announce the warning immediately
  when the dialog opens.
- `prompt.set_mnemonic_widget(Some(&confirm_entry))` links the "Type X to
  confirm" label to its entry (main.rs:778 → 785).

---

## 8. `writer.rs` — cleanup notes

`writer.rs` is 3168 lines. Most of it is correctly shelling out to system
tools; the concerns are narrow.

- **`writer.rs:73`** `UiEvent::Done(Result<(), String>)`. The rest of the code
  uses `anyhow::Result`. Making this consistent is a ripple through
  `helper.rs` (IPC `DONE\tERR\t…` format) and
  `main.rs:466–479` (the event handler). Low-priority cosmetic — flagging so
  it isn't forgotten.
- **Three functions carry `#[allow(clippy::too_many_lines)]`** and should be
  split: `write_windows_fat32` (writer.rs:433), `write_windows_ntfs_uefi_bios`
  (writer.rs:727), and `install_uefi_ntfs_loaders` (writer.rs:2278). The
  natural sub-phases are mount / format / copy / sync. Out of scope for this
  research doc; flagged for a follow-up.
- **Shell-outs are correct**: `grub-install`, `wimlib-imagex`/`wimsplit`,
  `mkfs.vfat`/`mkfs.ntfs`/`mkfs.ntfs3`/`mkfs.ext4`, `7z`/`bsdtar`, `rsync`,
  `gpg`, `parted`, `partprobe`/`blockdev`, `udevadm settle`, `sync`. **None
  have viable pure-Rust replacements** — don't waste time trying. The only
  shell-out this document recommends replacing lives in `util.rs` (see §9).

---

## 9. `util.rs` — low-hanging fruit

Both public functions shell out unnecessarily:

- **`util.rs:32`** `is_root()` spawns `id -u` and parses stdout. Replace with
  `nix::unistd::Uid::effective().is_root()`. `nix = "0.31.2"`, MSRV 1.69.
  Five-minute change.
- **`util.rs:21`** `command_exists()` spawns `sh -c 'command -v -- "$1"
  >/dev/null 2>&1'`. Replace with `which::which(cmd).is_ok()`.
  `which = "8.0.2"`, MSRV 1.70. Called from ~15 sites in writer.rs and main.rs,
  so the subprocess cost adds up.

Both are zero-risk: same-shape return, no change to any caller. Both crates
use unsafe internally; the `#![forbid(unsafe_code)]` on bootable's own files
is unaffected.

**Deferred**: `sys-mount = "3.1.0"` for the ~13 `umount` + 4 `mount` shell-outs
in writer.rs is tempting but is a much larger refactor, and the current
shell-based code is correct. Treat as a separate later project.

---

## 10. Dependencies and toolchain hygiene

- **`gtk4 = "0.11.2"`** — patch bump from 0.11.1. Trivial.
- **Feature flags**: stay on `v4_20` / `v1_8` for now. Only bump to `v4_22`
  or `v1_9` if a specific new widget is adopted — no gratuitous version
  chasing.
- **`Cargo.toml` `rust-version`** — currently absent. Add
  `rust-version = "1.92"` (`cargo info gtk4@0.11.1` states MSRV 1.92). This
  makes MSRV explicit and fails fast on too-old toolchains.
- **`rust-toolchain.toml`** — currently absent. Pin a stable channel for
  reproducible local builds.
- **No CI** — this is the cheapest durable quality lever. Add
  `.github/workflows/ci.yml` running, on push and PR:
    - `cargo fmt --check`
    - The strict clippy from CLAUDE.md — once §1 is fixed, lock the lint
      policy in CI so it can't regress.
    - `cargo test`
    - `cargo build --release`

---

## 11. Desktop integration (tracked for later)

Missing, all optional, all unlock store discoverability:

- `data/io.bootable.app.desktop` (freedesktop .desktop entry)
- `data/io.bootable.app.metainfo.xml` (AppStream for Flathub / GNOME Software)
- `data/icons/hicolor/scalable/apps/io.bootable.app.svg`
- GResource bundle compiled via `build.rs` + `glib-build-tools = "0.22"`, so
  icons and any custom CSS ship inside the binary.

Cargo-based install scripts or distro packaging can drop these files in place.
Low priority, but mentioned so it isn't rediscovered.

---

## 12. Explicitly *not* recommended

Research-validated dead ends; recording them here stops them being
re-proposed in future sessions.

- **`gtk::CompositeTemplate` / Blueprint** — overkill for a single-window
  utility. CompositeTemplate forces GObject subclassing, `imp` modules,
  GResource bundling, and `build.rs`. Blueprint is still experimental per its
  own docs. The right move for `build_ui()` is to split it into focused
  builder functions (`build_source_group`, `build_target_group`,
  `build_boot_group`, `build_advanced_group`, `setup_actions`,
  `setup_write_workflow`) and stop there.
- **`gptman` / `mbrman`** for replacing `parted` — parted is battle-tested,
  the pure-Rust alternatives don't cover the full GPT+MBR flag-setting flow
  (esp, boot, legacy_boot), and regression risk is real for a disk-writing
  tool.
- **`fs_extra`** for replacing `rsync` — `fs_extra` can't match rsync's
  `--checksum` verification path, which the post-write verifier depends on
  (writer.rs:1717).
- **`adw::MultiLayoutView`** — not needed; a simple `adw::Breakpoint` on
  the window plus `PreferencesPage` already handle the one form bootable has.
- **`glib::MainContext::channel`** — older and less integrated than
  `async-channel`; skip directly to `async-channel` per §4.
- **`udev` crate** for hot-plug — `gio::VolumeMonitor` is already the right
  abstraction for a GTK app.

---

## Priority ordering for follow-up PRs

1. **Fix strict clippy (§1)** — unblocks the repo's own documented lint
   policy. ~30 minutes.
2. **`util.rs` nix + which (§9)** — two tiny, zero-risk changes that remove
   per-call subprocess overhead. ~15 minutes.
3. **Migrate form to `PreferencesGroup` + rows (§2)** — biggest UX win,
   unlocks accessibility §7, responsive layout §5. Estimated half a day.
4. **Custom confirmation dialog → `adw::AlertDialog` (§3)** — smaller and
   well-scoped; drop `dialog_open` bookkeeping while there. ~1–2 hours.
5. **`mpsc` + `idle_add_local` → `async-channel` + `spawn_future_local`
   (§4)** — single-function change, adds one dependency. ~1 hour.
6. **`adw::WindowTitle`, `ToastOverlay`, `Banner`, `StatusPage`,
   bottom-bar progress (§5)** — polish pass after the form migration
   settles. Half a day.
7. **Actions + shortcuts + hamburger menu + `adw::AboutDialog` (§6)** —
   requires deciding on a menu structure. ~half a day.
8. **Split the three `too_many_lines` functions in writer.rs (§8)** — not
   urgent, but cleans up the `#[allow]` annotations.
9. **CI workflow (§10)** — only valuable *after* §1 so the pipeline doesn't
   immediately go red. ~1 hour.
10. **Desktop integration (§11)** — whenever packaging for Flathub becomes a
    goal.

## Files referenced

- `Cargo.toml`
- `src/main.rs` (905 lines — procedural UI, worker spawn, event loop)
- `src/writer.rs` (3168 lines — pipeline, 79 tests)
- `src/helper.rs` (335 lines — pkexec IPC)
- `src/devices.rs` (346 lines — typed `lsblk -J` parse)
- `src/util.rs` (104 lines — shell-out helpers)
- `CLAUDE.md` (lint policy the repo currently fails)
