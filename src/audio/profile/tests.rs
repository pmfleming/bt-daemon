use super::{ProfileValues, endpoint_key, parse_profile, profile_codec, profile_key, profile_mode};
use pipewire::spa::{
    pod::{Object, Pod, Property, Value, serialize::PodSerializer},
    sys,
    utils::Id,
};
use std::io::Cursor;

#[test]
fn profile_properties_are_typed_and_invalid_indices_are_rejected() {
    let mut values = ProfileValues::default();
    assert!(ProfileValues::default().finish().is_none());
    values.apply(sys::SPA_PARAM_PROFILE_index, Value::Int(-1));
    assert!(values.index.is_none());
    values.apply(sys::SPA_PARAM_PROFILE_index, Value::Int(7));
    values.apply(
        sys::SPA_PARAM_PROFILE_name,
        Value::String("a2dp-sink".into()),
    );
    values.apply(
        sys::SPA_PARAM_PROFILE_description,
        Value::String("High Fidelity (codec AAC)".into()),
    );
    values.apply(sys::SPA_PARAM_PROFILE_priority, Value::Int(100));
    values.apply(
        sys::SPA_PARAM_PROFILE_available,
        Value::Id(Id(sys::SPA_PARAM_AVAILABILITY_no)),
    );
    assert!(!values.available);
    values.apply(
        sys::SPA_PARAM_PROFILE_available,
        Value::Id(Id(sys::SPA_PARAM_AVAILABILITY_yes)),
    );
    values.apply(
        sys::SPA_PARAM_PROFILE_index,
        Value::String("bad-type".into()),
    );
    values.apply(u32::MAX, Value::Int(42));
    let profile = values.finish().unwrap();
    assert_eq!(profile.index, 7);
    assert_eq!(profile.mode, "high-fidelity");
    assert_eq!(profile.codec.as_deref(), Some("AAC"));
    assert_eq!(profile.priority, 100);
    assert!(profile.available);
}

fn parse_value(value: Value) -> Option<super::AudioProfile> {
    let (bytes, _) = PodSerializer::serialize(Cursor::new(Vec::new()), &value).unwrap();
    parse_profile(Pod::from_bytes(bytes.get_ref()).unwrap())
}

#[test]
fn profile_pods_require_the_correct_object_type_and_an_index() {
    let properties = vec![Property::new(sys::SPA_PARAM_PROFILE_index, Value::Int(3))];
    let profile = parse_value(Value::Object(Object {
        type_: sys::SPA_TYPE_OBJECT_ParamProfile,
        id: sys::SPA_PARAM_EnumProfile,
        properties: properties.clone(),
    }))
    .unwrap();
    assert_eq!(profile.index, 3);
    assert_eq!(profile.mode, "other");
    assert!(profile.name.is_empty() && profile.description.is_empty());
    assert!(
        parse_value(Value::Object(Object {
            type_: sys::SPA_TYPE_OBJECT_Props,
            id: 0,
            properties
        }))
        .is_none()
    );
    assert!(
        parse_value(Value::Object(Object {
            type_: sys::SPA_TYPE_OBJECT_ParamProfile,
            id: sys::SPA_PARAM_Profile,
            properties: vec![]
        }))
        .is_none()
    );
    assert!(parse_value(Value::Int(3)).is_none());
}

#[test]
fn profile_names_codecs_and_opaque_keys_are_stable() {
    for (name, mode) in [
        ("a2dp-sink", "high-fidelity"),
        ("headset-head-unit", "headset"),
        ("handsfree-head-unit", "headset"),
        ("off", "off"),
        ("", "other"),
    ] {
        assert_eq!(profile_mode(name), mode);
    }
    for description in ["", "codec ", "codec )", "High Fidelity"] {
        assert_eq!(profile_codec(description), None);
    }
    assert_eq!(profile_codec("codec SBC (codec AAC)"), Some("AAC".into()));
    let profile = profile_key("device-private", "a2dp-sink");
    assert_eq!(profile, profile_key("device-private", "a2dp-sink"));
    assert_ne!(profile, profile_key("other-device", "a2dp-sink"));
    assert_ne!(profile, profile_key("device-private", "headset"));
    assert_ne!(profile, endpoint_key("device-private", "a2dp-sink"));
    assert!(!profile.contains("device-private"));
}
