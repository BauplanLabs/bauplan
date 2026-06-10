use anyhow::Result;

#[test]
fn config_set_writes_supported_profile_settings() -> Result<()> {
    let home = tempfile::tempdir()?;
    let api_key = "bpln_test_key";
    let api_endpoint = "https://api.use1.adev.bauplanlabs.com";
    let active_branch = "feature";

    config_set(&home, "active_branch", active_branch);
    config_set(&home, "api_endpoint", api_endpoint);
    config_set(&home, "api_key", api_key);

    let config = std::fs::read_to_string(home.path().join(".bauplan/config.yaml"))?;
    let parsed: serde_yaml::Value = serde_yaml::from_str(&config)?;
    let profile = &parsed["profiles"]["default"];

    assert_eq!(profile["api_key"].as_str(), Some(api_key));
    assert_eq!(profile["api_endpoint"].as_str(), Some(api_endpoint));
    assert_eq!(profile["active_branch"].as_str(), Some("main"));

    Ok(())
}

fn config_set(home: &tempfile::TempDir, name: &str, value: &str) {
    crate::bauplan()
        .env("HOME", home.path())
        .env("USERPROFILE", home.path())
        .env_remove("BAUPLAN_PROFILE")
        .env_remove("BAUPLAN_API_KEY")
        .env_remove("BAUPLAN_API_ENDPOINT")
        .args(["config", "set", name, value])
        .assert()
        .success();
}
