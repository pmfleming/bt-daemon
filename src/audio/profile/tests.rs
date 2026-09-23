use super::parse_profile;
use pipewire::spa::{
    pod::{Object, Pod, Property, Value, serialize::PodSerializer},
    sys,
    utils::Id,
};
use std::io::Cursor;

fn parse_value(value: Value) -> Option<super::AudioProfile> {
    let (bytes, _) = PodSerializer::serialize(Cursor::new(Vec::new()), &value).unwrap();
    parse_profile(Pod::from_bytes(bytes.get_ref()).unwrap())
}

#[test]
fn profile_pods_expose_typed_metadata_and_reject_invalid_shapes() {
    for (name, mode, description, codec) in [
        (
            "a2dp-sink",
            "high-fidelity",
            "High Fidelity (codec AAC)",
            Some("AAC"),
        ),
        ("headset-head-unit", "headset", "Headset", None),
        ("handsfree-head-unit", "headset", "codec ", None),
        ("off", "off", "codec )", None),
        ("", "other", "", None),
    ] {
        let availability = if name == "off" {
            sys::SPA_PARAM_AVAILABILITY_no
        } else {
            sys::SPA_PARAM_AVAILABILITY_yes
        };
        let profile = parse_value(Value::Object(Object {
            type_: sys::SPA_TYPE_OBJECT_ParamProfile,
            id: sys::SPA_PARAM_EnumProfile,
            properties: vec![
                Property::new(sys::SPA_PARAM_PROFILE_index, Value::Int(7)),
                Property::new(sys::SPA_PARAM_PROFILE_name, Value::String(name.into())),
                Property::new(
                    sys::SPA_PARAM_PROFILE_description,
                    Value::String(description.into()),
                ),
                Property::new(sys::SPA_PARAM_PROFILE_priority, Value::Int(100)),
                Property::new(
                    sys::SPA_PARAM_PROFILE_available,
                    Value::Id(Id(availability)),
                ),
                Property::new(u32::MAX, Value::Int(42)),
            ],
        }))
        .unwrap();
        assert_eq!(profile.index, 7);
        assert_eq!(profile.mode, mode);
        assert_eq!(profile.codec.as_deref(), codec);
        assert_eq!(profile.priority, 100);
        assert_eq!(profile.available, name != "off");
    }
    for (object_type, properties) in [
        (
            sys::SPA_TYPE_OBJECT_Props,
            vec![Property::new(sys::SPA_PARAM_PROFILE_index, Value::Int(3))],
        ),
        (sys::SPA_TYPE_OBJECT_ParamProfile, vec![]),
        (
            sys::SPA_TYPE_OBJECT_ParamProfile,
            vec![Property::new(sys::SPA_PARAM_PROFILE_index, Value::Int(-1))],
        ),
        (
            sys::SPA_TYPE_OBJECT_ParamProfile,
            vec![Property::new(
                sys::SPA_PARAM_PROFILE_index,
                Value::String("invalid".into()),
            )],
        ),
    ] {
        assert!(
            parse_value(Value::Object(Object {
                type_: object_type,
                id: sys::SPA_PARAM_Profile,
                properties,
            }))
            .is_none()
        );
    }
    assert!(parse_value(Value::Int(3)).is_none());
}
