use super::layout::PublisherLayout;
use super::*;
#[cfg(all(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
#[test]
fn all_product_example_keeps_independent_modes() {
    let config: Config = serde_json::from_str(include_str!(
        "../../../../tools/deploy/publisher/publisher.example.json"
    ))
    .unwrap();
    let layout = config.validated_layout().unwrap();
    assert_eq!(layout.probes.len(), 5);
    assert_eq!(layout.listeners.len(), 7);
}
#[cfg(feature = "receiver")]
#[test]
fn disabled_products_need_no_settings_or_files() {
    let config: Config = serde_json::from_value(
        serde_json::json!({"management_listen":"127.0.0.1:18080", "receiver":{"enabled":false}}),
    )
    .unwrap();
    let layout = config.validated_layout().unwrap();
    assert_eq!(layout.listeners.len(), 1);
    assert!(layout.writable_paths.is_empty());
}
#[test]
fn each_compiled_product_can_be_enabled_independently() {
    let example: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tools/deploy/publisher/publisher.example.json"
    ))
    .unwrap();
    for (product, compiled) in [
        ("enhance", cfg!(feature = "enhance")),
        ("status", cfg!(feature = "status")),
        ("transparent_history", cfg!(feature = "transparent-history")),
        ("transaction_details", cfg!(feature = "transparent-detail")),
        ("receiver", cfg!(feature = "receiver")),
    ] {
        if compiled {
            let config: Config = serde_json::from_value(serde_json::json!({
                "management_listen": "127.0.0.1:18080",
                product: example[product].clone(),
            }))
            .unwrap();
            assert_eq!(
                config.validated_layout().unwrap().probes.len(),
                1,
                "{product}"
            );
        }
    }
}
#[cfg(feature = "enhance")]
#[test]
fn embedded_status_setting_is_rejected() {
    let mut config: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tools/deploy/publisher/publisher.example.json"
    ))
    .unwrap();
    config["enhance"]["settings"]["status_config"] = serde_json::Value::Null;
    assert!(
        serde_json::from_value::<enhance_publisher::service::EnhancePublisherConfig>(
            config["enhance"]["settings"].clone()
        )
        .is_err()
    );
}
#[test]
fn overlapping_and_symlinked_state_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let mut layout = PublisherLayout::default();
    layout
        .declare_writable_path("enhance", temp.path())
        .unwrap();
    assert!(layout
        .declare_writable_path("status", &temp.path().join("child"))
        .is_err());
    #[cfg(unix)]
    {
        let link = temp.path().join("alias");
        std::os::unix::fs::symlink(temp.path(), &link).unwrap();
        assert!(layout.declare_writable_path("status", &link).is_err());
        let child = temp.path().join("real/child");
        std::fs::create_dir_all(&child).unwrap();
        let alias = temp.path().join("child-alias");
        std::os::unix::fs::symlink(&child, &alias).unwrap();
        let mut layout = PublisherLayout::default();
        layout
            .declare_writable_path("enhance", &temp.path().join("real/state"))
            .unwrap();
        assert!(layout
            .declare_writable_path("status", &alias.join("../state"))
            .is_err());
    }
}
#[test]
fn listener_conflicts_and_unknown_options_are_rejected() {
    let mut layout = PublisherLayout::default();
    let addr = "127.0.0.1:18080".parse().unwrap();
    layout.declare_listener("enhance", addr).unwrap();
    assert!(layout.declare_listener("status", addr).is_err());
    assert!(serde_json::from_str::<Config>(
        r#"{"management_listen":"127.0.0.1:18080","typo":true}"#
    )
    .is_err());
}
#[test]
fn uncompiled_sections_are_rejected_even_when_disabled() {
    for (section, compiled) in [
        ("enhance", cfg!(feature = "enhance")),
        ("status", cfg!(feature = "status")),
        ("receiver", cfg!(feature = "receiver")),
        ("transparent_history", cfg!(feature = "transparent-history")),
        ("transaction_details", cfg!(feature = "transparent-detail")),
        ("transparent_filters", cfg!(feature = "transparent-history")),
    ] {
        if !compiled {
            let raw = serde_json::json!({"management_listen":"127.0.0.1:18080", section:{"enabled":false}});
            assert!(serde_json::from_value::<Config>(raw).is_err(), "{section}");
        }
    }
}
#[cfg(feature = "transparent-detail")]
#[test]
fn replay_does_not_require_live_credentials_and_cannot_offer_http_authority() {
    let mut settings = serde_json::json!({
        "root":"/srv/display", "journal":"/srv/journal", "workers":"/etc/workers.json",
        "mode":"replay", "poll_ms":500, "status_listen":"127.0.0.1:8099"
    });
    let config: transparent_publisher::tx_detail::service::Config =
        serde_json::from_value(settings.clone()).unwrap();
    config.validate().unwrap();
    settings["http_only"] = serde_json::json!(true);
    settings["ship_runtimes"] = serde_json::json!(true);
    let config: transparent_publisher::tx_detail::service::Config =
        serde_json::from_value(settings).unwrap();
    assert!(config.validate().is_err());
}
#[cfg(feature = "transparent-history")]
#[test]
fn filter_and_history_publishers_cannot_write_the_same_state() {
    let example: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tools/deploy/publisher/publisher.example.json"
    ))
    .unwrap();
    let mut history = example["transparent_history"].clone();
    history["settings"]["follow_journal"] = serde_json::json!(false);
    let raw = serde_json::json!({"management_listen":"127.0.0.1:18080", "transparent_history":history,
    "transparent_filters":{"enabled":true,"settings":{
        "data_dir":history["settings"]["data_dir"], "zakura_cookie":"/etc/node.cookie"
    }}});
    let config: Config = serde_json::from_value(raw).unwrap();
    assert!(config.validate().is_err());
}

#[test]
fn management_requires_a_fixed_loopback_listener() {
    for address in ["0.0.0.0:18080", "10.0.0.1:18080", "127.0.0.1:0"] {
        let config: Config =
            serde_json::from_value(serde_json::json!({"management_listen":address})).unwrap();
        assert!(config.validate().is_err(), "{address}");
    }
    let config: Config =
        serde_json::from_str(r#"{"management_listen":"127.0.0.1:18080"}"#).unwrap();
    config.validate().unwrap();
}

#[test]
fn writable_path_conflicts_name_both_product_owners() {
    let directory = tempfile::tempdir().unwrap();
    let mut layout = PublisherLayout::default();
    layout
        .declare_writable_path("enhance", directory.path())
        .unwrap();
    let error = layout
        .declare_writable_path("status", &directory.path().join("nested"))
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("enhance"));
    assert!(message.contains("status"));
    assert!(message.contains("nested"));
}

#[test]
fn enabled_products_require_settings_and_identify_the_product() {
    for (product, compiled) in [
        ("enhance", cfg!(feature = "enhance")),
        ("status", cfg!(feature = "status")),
        ("transparent_history", cfg!(feature = "transparent-history")),
        ("transparent_filters", cfg!(feature = "transparent-history")),
        ("transaction_details", cfg!(feature = "transparent-detail")),
        ("receiver", cfg!(feature = "receiver")),
    ] {
        if compiled {
            let config: Config = serde_json::from_value(serde_json::json!({
                "management_listen":"127.0.0.1:18080", product:{"enabled":true},
            }))
            .unwrap();
            let error = config.validate().unwrap_err();
            assert!(error.to_string().contains(product), "{error}");
            assert!(error.to_string().contains("no settings"), "{error}");
        }
    }
}
