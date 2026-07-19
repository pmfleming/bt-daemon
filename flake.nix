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
            cargoLock.lockFile = ./Cargo.lock;
            nativeBuildInputs = with pkgs; [ pkg-config ];
            buildInputs = with pkgs; [ dbus ];
            strictDeps = true;
            meta = {
              description = "Bluetooth policy and API daemon for Shelllist";
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
            cargo-llvm-cov
            clippy
            dbus
            gcc
            jq
            just
            llvmPackages.llvm
            pkg-config
            rust-analyzer
            rustc
            rustfmt
            systemd
            wireplumber
          ];

          LLVM_COV = "${pkgs.llvmPackages.llvm}/bin/llvm-cov";
          LLVM_PROFDATA = "${pkgs.llvmPackages.llvm}/bin/llvm-profdata";
          RUST_BACKTRACE = "1";
          RUST_LOG = "bt_daemon=debug";
        };
      });

      formatter = forAllSystems (system: pkgs: pkgs.nixpkgs-fmt);
    };
}
