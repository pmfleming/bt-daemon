//! Semantic icon identity from BlueZ metadata, never product-name heuristics.
use super::{Battery, Service};
pub(crate) fn icon_id(
    kind: &str,
    icon: Option<&str>,
    battery: &[Battery],
    services: &[Service],
) -> &'static str {
    classify(
        kind,
        icon.unwrap_or(""),
        battery
            .iter()
            .any(|b| matches!(b.component.to_lowercase().as_str(), "left" | "right")),
        &services
            .iter()
            .map(|s| s.label.to_lowercase())
            .collect::<Vec<_>>()
            .join(" "),
    )
}
fn first(text: &str, rules: &[(&[&str], &'static str)], fallback: &'static str) -> &'static str {
    rules
        .iter()
        .find(|(terms, _)| terms.iter().any(|term| text.contains(term)))
        .map_or(fallback, |(_, icon)| *icon)
}
fn classify(kind: &str, icon: &str, earbuds: bool, services: &str) -> &'static str {
    if earbuds {
        return "earbuds";
    }
    let kind = kind.to_lowercase();
    if !kind.is_empty() && kind != "bluetooth device" {
        return first(
            &kind,
            &[
                (&["earbuds"], "earbuds"),
                (&["headphones"], "headphones"),
                (&["headset"], "headset"),
                (&["speaker"], "speaker"),
                (&["audio device"], "bluetoothHeadphones"),
                (&["keyboard"], "keyboard"),
                (&["mouse"], "mouse"),
                (&["game controller"], "controller"),
                (&["phone"], "phone"),
                (&["computer"], "computer"),
                (&["wearable"], "watch"),
            ],
            "bluetooth",
        );
    }
    let found = first(
        &icon.to_lowercase(),
        &[
            (&["headphones"], "headphones"),
            (&["headset"], "headset"),
            (&["speaker", "audio-card"], "speaker"),
            (&["keyboard"], "keyboard"),
            (&["mouse"], "mouse"),
            (&["gaming", "gamepad", "joystick"], "controller"),
            (&["phone"], "phone"),
            (&["computer", "laptop"], "computer"),
            (&["watch"], "watch"),
        ],
        "",
    );
    if !found.is_empty() {
        return found;
    }
    first(
        &services.to_lowercase(),
        &[
            (&["audio sink"], "bluetoothHeadphones"),
            (&["handsfree", "headset"], "headset"),
        ],
        "bluetooth",
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn precedence_and_unknown_metadata_do_not_invent_device_types() {
        assert_eq!(classify("Headset", "", true, ""), "earbuds");
        assert_eq!(
            classify("Keyboard", "audio-card", false, "audio sink"),
            "keyboard"
        );
        assert_eq!(
            classify("Future type", "audio-card", false, "audio sink"),
            "bluetooth"
        );
        assert_eq!(
            classify("Bluetooth device", "input-gaming", false, "audio sink"),
            "controller"
        );
        assert_eq!(classify("", "", false, "Audio Sink"), "bluetoothHeadphones");
        assert_eq!(classify("", "", false, ""), "bluetooth");
    }
}
