{
  description = "Bluetooth policy and API daemon for Shelllist";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAllSystems (system: pkgs:
        let
          btDaemon = pkgs.rustPlatform.buildRustPackage {
            pname = "bt-daemon";
            version = "0.1.0";
            src = ./.;
            cargoLock = {
              lockFile = ./Cargo.lock;
              outputHashes = {
                "shelllist-daemon-core-0.1.0" = "sha256-3tXlcM+PtEExZ91mtTIy/3Jpcj6o9MBlypPd3Z1ioVw=";
                "shelllist-daemon-tokio-0.1.0" = "sha256-3tXlcM+PtEExZ91mtTIy/3Jpcj6o9MBlypPd3Z1ioVw=";
              };
            };
            nativeBuildInputs = with pkgs; [ llvmPackages.libclang pkg-config ];
            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
            BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${pkgs.stdenv.cc.libc.dev}/include";
            buildInputs = with pkgs; [ dbus pipewire ];
            strictDeps = true;
            postInstall = ''
              install -Dm644 ${./packaging/systemd/bt-daemon.service} $out/share/systemd/user/bt-daemon.service
              install -Dm644 ${./packaging/dbus/org.laufan.BluetoothDaemon.service} \
                $out/share/dbus-1/services/org.laufan.BluetoothDaemon.service
              substituteInPlace \
                $out/share/systemd/user/bt-daemon.service \
                $out/share/dbus-1/services/org.laufan.BluetoothDaemon.service \
                --replace-fail @out@ $out
            '';
            meta = {
              description = "BlueZ policy and bt-api daemon for Shelllist";
              mainProgram = "bt-daemon";
              platforms = pkgs.lib.platforms.linux;
            };
          };
        in
        {
          default = btDaemon;
        });

      apps = forAllSystems (system: pkgs: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/bt-daemon";
          meta.description = "Run the Shelllist Bluetooth backend";
        };
      });

      checks = forAllSystems (system: pkgs: {
        default = self.packages.${system}.default;
      });

      devShells = forAllSystems (system: pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            bluez
            cargo
            cargo-audit
            cargo-llvm-cov
            clippy
            dbus
            gcc
            jq
            just
            llvmPackages.libclang
            llvmPackages.llvm
            pkg-config
            pipewire
            rust-analyzer
            rustc
            rustfmt
            systemd
            wireplumber
          ];

          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${pkgs.stdenv.cc.libc.dev}/include";
          LLVM_COV = "${pkgs.llvmPackages.llvm}/bin/llvm-cov";
          LLVM_PROFDATA = "${pkgs.llvmPackages.llvm}/bin/llvm-profdata";
          RUST_BACKTRACE = "1";
          RUST_LOG = "bt_daemon=debug";
        };
      });

      formatter = forAllSystems (system: pkgs: pkgs.nixpkgs-fmt);
    };
}
