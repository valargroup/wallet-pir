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
                "chain": example["chain"].clone(),
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
fn chain_following_products_require_valid_root_chain_settings() {
    let example: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tools/deploy/publisher/publisher.example.json"
    ))
    .unwrap();
    let configured = |chain: serde_json::Value| {
        let mut raw = serde_json::json!({
            "management_listen": "127.0.0.1:18080",
            "enhance": example["enhance"].clone(),
        });
        if !chain.is_null() {
            raw["chain"] = chain;
        }
        serde_json::from_value::<Config>(raw).unwrap().validate()
    };
    assert!(configured(serde_json::Value::Null).is_err());
    assert!(configured(example["chain"].clone()).is_ok());
    let mut chain = example["chain"].clone();
    chain.as_object_mut().unwrap().remove("poll_ms");
    assert!(configured(chain).is_ok(), "poll_ms defaults to one second");
    for (field, value) in [
        ("poll_ms", serde_json::json!(0)),
        ("rpc_cookie", serde_json::json!("relative/cookie")),
        (
            "rpc_url",
            serde_json::json!("http://user:secret@127.0.0.1:8232"),
        ),
    ] {
        let mut chain = example["chain"].clone();
        chain[field] = value;
        assert!(configured(chain).is_err(), "{field}");
    }
    // The removed product-local source settings are refused, not ignored.
    let mut enhance = example["enhance"]["settings"].clone();
    enhance["zakura_rpc_url"] = serde_json::json!("http://127.0.0.1:8232");
    assert!(
        serde_json::from_value::<enhance_publisher::service::EnhancePublisherConfig>(enhance)
            .is_err()
    );
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
    ] {
        if !compiled {
            let raw = serde_json::json!({"management_listen":"127.0.0.1:18080", section:{"enabled":false}});
            assert!(serde_json::from_value::<Config>(raw).is_err(), "{section}");
        }
    }
}
#[cfg(feature = "transparent-detail")]
#[test]
fn detail_publication_is_live_https_only_and_rejects_retired_modes() {
    let example: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tools/deploy/publisher/publisher.example.json"
    ))
    .unwrap();
    let settings = example["transaction_details"]["settings"].clone();
    serde_json::from_value::<transparent_publisher::tx_detail::service::Config>(settings.clone())
        .unwrap();
    for (key, value) in [
        ("http_only", serde_json::json!(true)),
        ("ship_runtimes", serde_json::json!(true)),
        ("workers", serde_json::json!("/etc/workers.json")),
        ("mode", serde_json::json!("replay")),
        ("exit_when_idle", serde_json::json!(true)),
        // The node and its polling come from the root chain settings.
        ("rpc_url", serde_json::json!("http://127.0.0.1:8232")),
        ("rpc_cookie", serde_json::json!("/run/cookie")),
        ("poll_ms", serde_json::json!(1000)),
    ] {
        let mut retired = settings.clone();
        retired[key] = value;
        assert!(
            serde_json::from_value::<transparent_publisher::tx_detail::service::Config>(retired)
                .is_err(),
            "{key}"
        );
    }
}
#[cfg(feature = "transparent-history")]
#[test]
fn history_publication_rejects_retired_fleet_and_mode_keys() {
    let example: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tools/deploy/publisher/publisher.example.json"
    ))
    .unwrap();
    let settings = example["transparent_history"]["settings"].clone();
    serde_json::from_value::<transparent_publisher::service::HistoryConfig>(settings.clone())
        .unwrap();
    for (key, value) in [
        ("http_only", serde_json::json!(true)),
        ("distribute_prepared", serde_json::json!(true)),
        ("ship_runtimes", serde_json::json!(true)),
        ("shadow", serde_json::json!(false)),
        ("fleet_command", serde_json::json!("/opt/fleet.py")),
        ("fleet_config", serde_json::json!("/etc/fleet.json")),
    ] {
        let mut retired = settings.clone();
        retired[key] = value;
        assert!(
            serde_json::from_value::<transparent_publisher::service::HistoryConfig>(retired)
                .is_err(),
            "{key}"
        );
    }
}

/// History reads the node only through the root chain settings.
#[cfg(feature = "transparent-history")]
#[test]
fn history_settings_refuse_their_own_node_endpoint() {
    let example: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tools/deploy/publisher/publisher.example.json"
    ))
    .unwrap();
    let settings = example["transparent_history"]["settings"].clone();
    serde_json::from_value::<transparent_publisher::service::HistoryConfig>(settings.clone())
        .unwrap();
    for key in ["rpc_url", "rpc_cookie"] {
        let mut retired = settings.clone();
        retired[key] = serde_json::json!("http://127.0.0.1:8232");
        assert!(
            serde_json::from_value::<transparent_publisher::service::HistoryConfig>(retired)
                .is_err(),
            "{key}"
        );
    }
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
