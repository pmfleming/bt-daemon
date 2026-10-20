{
  description = "Bluetooth policy and API daemon for Shelllist";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  # Keep the framework's Rust toolchain while upgrading the coherent audio stack.
  inputs.runtimeNixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  # Co-development: one live sibling framework; no per-daemon revision pins.
  # Use ../daemon-framework/tools/local-build for Nix builds/checks.
  inputs.daemonFramework = {
    url = "git+file:../daemon-framework";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      nixpkgs,
      runtimeNixpkgs,
      daemonFramework,
    }:
    let
      systems = [ "x86_64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system:
        f system (nixpkgs.legacyPackages.${system}.extend (_final: _prev: {
          inherit (runtimeNixpkgs.legacyPackages.${system}) bluez pipewire wireplumber;
        }))
      );
    in
    {
      nixosModules.rssi-helper = import ./nix/rssi-helper.nix self;
      nixosModules.audio-stack = import ./nix/audio-stack.nix self;

      packages = forAllSystems (
        system: pkgs:
        let
          btDaemon = daemonFramework.lib.buildRustPackage pkgs {
            pname = "bt-daemon";
            version = "0.1.0";
            src = pkgs.lib.fileset.toSource {
              root = ./.;
              fileset = pkgs.lib.fileset.unions [
                ./Cargo.toml
                ./Cargo.lock
                ./src
                ./test_support
                ./resource
              ];
            };
            postUnpack = ''
              cp -R --no-preserve=mode ${daemonFramework.lib.daemonSource pkgs} "$(dirname "$sourceRoot")/daemon-framework"
            '';
            cargoLock.lockFile = ./Cargo.lock;
            nativeBuildInputs = with pkgs; [
              llvmPackages.libclang
              pkg-config
            ];
            nativeCheckInputs = [ pkgs.dbus ];
            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
            BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${pkgs.stdenv.cc.libc.dev}/include";
            buildInputs = with pkgs; [
              dbus
              pipewire
            ];
            strictDeps = true;
            postInstall = ''
              install -Dm644 ${./packaging/systemd/bt-daemon.service} $out/share/systemd/user/bt-daemon.service
              install -Dm644 ${./packaging/dbus/org.laufan.BluetoothDaemon.service} \
                $out/share/dbus-1/services/org.laufan.BluetoothDaemon.service
              install -Dm644 ${./packaging/systemd/bt-rssi-helper.service} \
                $out/lib/systemd/system/bt-rssi-helper.service
              install -Dm644 ${./packaging/dbus/org.laufan.BluetoothRssiHelper.conf} \
                $out/share/dbus-1/system.d/org.laufan.BluetoothRssiHelper.conf
              install -Dm644 ${./packaging/dbus/org.laufan.BluetoothRssiHelper.service} \
                $out/share/dbus-1/system-services/org.laufan.BluetoothRssiHelper.service
              install -Dm644 ${./packaging/polkit/org.laufan.bt-daemon.rssi.policy} \
                $out/share/polkit-1/actions/org.laufan.bt-daemon.rssi.policy
              substituteInPlace \
                $out/share/systemd/user/bt-daemon.service \
                $out/share/dbus-1/services/org.laufan.BluetoothDaemon.service \
                $out/lib/systemd/system/bt-rssi-helper.service \
                $out/share/dbus-1/system-services/org.laufan.BluetoothRssiHelper.service \
                --replace-fail @out@ $out
            '';
            meta = {
              description = "BlueZ policy and bt-api daemon for Shelllist";
              mainProgram = "bt-daemon";
              platforms = systems;
            };
          };
        in
        {
          default = btDaemon;
        }
      );

      apps = forAllSystems (
        system: pkgs: {
          default = {
            type = "app";
            program = "${self.packages.${system}.default}/bin/bt-daemon";
            meta.description = "Run the Shelllist Bluetooth backend";
          };
        }
      );

      checks = forAllSystems (
        system: pkgs: {
          default = self.packages.${system}.default;
          runtime-versions = let
            host = enabled: (nixpkgs.lib.nixosSystem {
              inherit system;
              modules = [ self.nixosModules.audio-stack {
                services.bt-daemon.audioStack.enable = enabled;
              } ];
            }).config;
            enabled = host true;
            disabled = host false;
          in
            assert pkgs.lib.versionAtLeast pkgs.bluez.version "5.87";
            assert pkgs.lib.versionAtLeast pkgs.pipewire.version "1.6.9";
            assert enabled.hardware.bluetooth.package == pkgs.bluez;
            assert enabled.services.pipewire.package == pkgs.pipewire;
            assert enabled.services.pipewire.wireplumber.package == pkgs.wireplumber;
            assert disabled.hardware.bluetooth.package == nixpkgs.legacyPackages.${system}.bluez;
            assert disabled.services.pipewire.package == nixpkgs.legacyPackages.${system}.pipewire;
            pkgs.runCommand "bt-runtime-versions" { } ''
              printf '%s\n' 'BlueZ ${pkgs.bluez.version}' 'PipeWire ${pkgs.pipewire.version}' \
                'WirePlumber ${pkgs.wireplumber.version}' > $out
            '';
        }
      );

      devShells = forAllSystems (
        system: pkgs: {
          default = pkgs.mkShell {
            packages =
              (daemonFramework.lib.toolchain pkgs).packages
              ++ (with pkgs; [
                bluez
                cargo-audit
                cargo-llvm-cov
                cargo-mutants
                dbus
                gcc
                git
                jq
                just
                llvmPackages.libclang
                llvmPackages.llvm
                pkg-config
                pipewire
                python3
                rust-analyzer
                systemd
                wireplumber
              ]);

            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
            BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${pkgs.stdenv.cc.libc.dev}/include";
            LLVM_COV = "${pkgs.llvmPackages.llvm}/bin/llvm-cov";
            LLVM_PROFDATA = "${pkgs.llvmPackages.llvm}/bin/llvm-profdata";
            RUST_BACKTRACE = "1";
            RUST_LOG = "bt_daemon=debug";
          };
        }
      );

      formatter = forAllSystems (system: pkgs: pkgs.nixpkgs-fmt);
    };
}
