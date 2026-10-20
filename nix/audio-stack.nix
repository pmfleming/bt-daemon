# Opt-in host deployment. Building bt-daemon alone never replaces running services.
self:
{ config, lib, pkgs, ... }:
let
  cfg = config.services.bt-daemon.audioStack;
  stack = self.inputs.runtimeNixpkgs.legacyPackages.${pkgs.stdenv.hostPlatform.system};
in
{
  options.services.bt-daemon.audioStack.enable = lib.mkEnableOption
    "the bt-daemon tested BlueZ/PipeWire/WirePlumber package set";

  config = lib.mkIf cfg.enable {
    hardware.bluetooth.package = lib.mkDefault stack.bluez;
    services.pipewire.package = lib.mkDefault stack.pipewire;
    services.pipewire.wireplumber.package = lib.mkDefault stack.wireplumber;
    assertions = [
      {
        assertion = lib.versionAtLeast config.hardware.bluetooth.package.version "5.87";
        message = "bt-daemon audioStack requires BlueZ >= 5.87.";
      }
      {
        assertion = lib.versionAtLeast config.services.pipewire.package.version "1.6.9";
        message = "bt-daemon audioStack requires PipeWire >= 1.6.9.";
      }
    ];
  };
}
