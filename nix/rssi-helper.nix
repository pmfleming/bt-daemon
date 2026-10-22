# Opt-in system service: installing the user daemon alone grants no capability.
self:
{ config, lib, pkgs, ... }:
let
  cfg = config.services.bt-rssi-helper;
  # The user daemon owns its session activation via Home Manager. Installing
  # the helper system-wide must not introduce a second session-bus service.
  helperPackage = pkgs.symlinkJoin {
    name = "bt-rssi-helper-system";
    paths = [ cfg.package ];
    postBuild = ''
      rm -rf "$out/share/dbus-1/services" "$out/share/systemd/user"
    '';
  };
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
    environment.systemPackages = [ helperPackage ];
    services.dbus.packages = [ helperPackage ];
    systemd.packages = [ helperPackage ];
    systemd.services.bt-rssi-helper.wantedBy = [ "multi-user.target" ];
  };
}
