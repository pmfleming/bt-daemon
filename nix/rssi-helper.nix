# Opt-in system service: installing the user daemon alone grants no capability.
self:
{ config, lib, pkgs, ... }:
let
  cfg = config.services.bt-rssi-helper;
in
{
  options.services.bt-rssi-helper = {
    enable = lib.mkEnableOption "the capability-scoped Bluetooth RSSI helper";
    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "inputs.bt-daemon.packages.<system>.default";
      description = "bt-daemon package containing the RSSI helper and its system service policies.";
    };
  };

  config = lib.mkIf cfg.enable {
    users.groups.bt-rssi-helper = { };
    users.users.bt-rssi-helper = {
      isSystemUser = true;
      group = "bt-rssi-helper";
      description = "Read-only Bluetooth RSSI broker";
    };
    security.polkit.enable = true;
    environment.systemPackages = [ cfg.package ];
    services.dbus.packages = [ cfg.package ];
    systemd.packages = [ cfg.package ];
    systemd.services.bt-rssi-helper.wantedBy = [ "multi-user.target" ];
  };
}
