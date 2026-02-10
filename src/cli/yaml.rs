use std::path::Path;

use anyhow::bail;
use nondestructive::yaml;

/// Load a YAML file, apply edits via a closure, and write it back. The
/// closure receives a mutable reference to the parsed document.
pub(crate) fn edit_yaml(
    path: &Path,
    f: impl FnOnce(&mut yaml::Document) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(path)?;
    let mut doc = yaml::from_slice(&content)?;

    f(&mut doc)?;

    std::fs::write(path, doc.to_string())?;
    Ok(())
}

/// Navigate into a nested YAML mapping by key path, creating any missing
/// intermediate mappings along the way. Returns a `MappingMut` pointing at
/// the innermost mapping.
///
/// `path` must be non-empty. For example, `&["parameters", "location_id"]`
/// returns a `MappingMut` for the `location_id` entry.
pub(crate) fn mapping_at_path<'a>(
    doc: &'a mut yaml::Document,
    path: &[&str],
) -> anyhow::Result<yaml::MappingMut<'a>> {
    assert!(!path.is_empty());

    let Some(mut current) = doc.as_mut().into_mapping_mut() else {
        bail!("invalid file: not a dictionary");
    };

    for &key in path {
        if current.as_ref().get(key).is_none() {
            let _ = current.insert(key, yaml::Separator::Auto).make_mapping();
        }

        let Some(next) = current.get_into_mut(key).and_then(|v| v.into_mapping_mut()) else {
            bail!("key {key:?} exists, but is not a dictionary");
        };

        current = next
    }

    Ok(current)
}

pub(crate) fn upsert_str(m: &mut yaml::MappingMut<'_>, key: &str, value: &str) {
    if let Some(mut v) = m.get_mut(key) {
        v.set_string(value);
    } else {
        m.insert_str(key, value);
    }
}

pub(crate) fn upsert_bool(m: &mut yaml::MappingMut<'_>, key: &str, value: bool) {
    if let Some(mut v) = m.get_mut(key) {
        v.set_bool(value);
    } else {
        m.insert_bool(key, value);
    }
}

pub(crate) fn upsert_i64(m: &mut yaml::MappingMut<'_>, key: &str, value: i64) {
    if let Some(mut v) = m.get_mut(key) {
        v.set_i64(value);
    } else {
        m.insert_i64(key, value);
    }
}

pub(crate) fn upsert_f64(m: &mut yaml::MappingMut<'_>, key: &str, value: f64) {
    if let Some(mut v) = m.get_mut(key) {
        v.set_f64(value);
    } else {
        m.insert_f64(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_at_path_create() -> anyhow::Result<()> {
        let mut doc = yaml::from_slice("root: 1\n")?;
        let mut m = mapping_at_path(&mut doc, &["a", "b", "c"])?;
        m.insert_str("key", "value");

        let output = doc.to_string();
        let parsed: serde_yaml::Value = serde_yaml::from_str(&output)?;
        assert_eq!(parsed["a"]["b"]["c"]["key"].as_str(), Some("value"));
        assert_eq!(parsed["root"].as_i64(), Some(1));

        Ok(())
    }

    #[test]
    fn mapping_at_path_existing() -> anyhow::Result<()> {
        let input = "\
top:
  nested:
    existing: 42
";
        let mut doc = yaml::from_slice(input)?;
        let mut m = mapping_at_path(&mut doc, &["top", "nested"])?;
        m.insert_str("added", "hello");

        let output = doc.to_string();
        assert!(output.contains("existing: 42"));
        assert!(output.contains("added: hello"));

        Ok(())
    }
}
