# Bootable

Bootable is a simple graphical app for writing bootable USB drives from ISO
or disk image files.

## Build and run

Requirements:
- Rust (stable) and Cargo
- GTK4 + libadwaita (development packages for building)
- GTK4 + libadwaita (runtime libraries for running)
- A system that supports GTK4 apps

Build (release):
```
cargo build --release
```

Run:
```
./target/release/bootable
```

Install (system-wide):
```
sudo install -Dm755 target/release/bootable /usr/local/bin/bootable
```

## Strict clippy

This repo is kept compatible with strict clippy settings:
```
cargo clippy -- -D warnings -W clippy::pedantic -W clippy::nursery -W clippy::cargo
```

## Linux packages (build vs run)

Below are distro-specific commands. Build packages include development headers;
runtime packages include shared libraries only.

### Arch Linux

Build:
```
sudo pacman -S --needed base-devel pkgconf gtk4 libadwaita
```

Run:
```
sudo pacman -S --needed gtk4 libadwaita
```

### Ubuntu

Build:
```
sudo apt update
sudo apt install -y build-essential pkg-config libgtk-4-dev libadwaita-1-dev
```

Run:
```
sudo apt install -y libgtk-4-1 libadwaita-1-0
```

### Debian

Build:
```
sudo apt update
sudo apt install -y build-essential pkg-config libgtk-4-dev libadwaita-1-dev
```

Run:
```
sudo apt install -y libgtk-4-1 libadwaita-1-0
```

### Fedora

Build:
```
sudo dnf install -y gcc pkgconf-pkg-config gtk4-devel libadwaita-devel
```

Run:
```
sudo dnf install -y gtk4 libadwaita
```

### Nix / NixOS

Build (ephemeral dev shell):
```
nix shell nixpkgs#gtk4 nixpkgs#libadwaita nixpkgs#pkg-config
```

Run (install runtime libs into your profile):
```
nix profile install nixpkgs#gtk4 nixpkgs#libadwaita
```

### OpenMandriva

Build:
```
sudo dnf install -y lib64gtk4.0-devel lib64adwaita-devel
```

Run:
```
sudo dnf install -y gtk4.0 lib64adwaita
```

## License

This project is licensed under the GPL-3.0-only license.

## Safety

This codebase forbids `unsafe` Rust. The crate uses `#![forbid(unsafe_code)]`
in all source files.
