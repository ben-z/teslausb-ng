use std::path::Path;

use crate::config::NetworkConfig;
use crate::device_config::{read_private_text, write_private_atomic};
use crate::error::{Error, Result};

pub fn apply_priorities(path: &Path, config: &NetworkConfig) -> Result<usize> {
    let original = read_private_text(path)?;
    let (updated, count) = prioritize(&original, config)?;
    if updated != original {
        write_private_atomic(path, &updated)?;
    }
    Ok(count)
}

// Netplan emits one assignment per line and escapes newlines inside values.
// Treat authentication settings as opaque text: only SSID and priority matter here.
fn prioritize(input: &str, config: &NetworkConfig) -> Result<(String, usize)> {
    if config.wifi_paused_priority >= config.wifi_default_priority {
        return Err(Error::new(
            "WIFI_PAUSED_PRIORITY must be lower than WIFI_DEFAULT_PRIORITY",
        ));
    }
    let mut output = String::with_capacity(input.len());
    let mut in_network = false;
    let mut ssid = None;
    let mut count = 0;
    for line in input.split_inclusive('\n') {
        let value = line.trim();
        if value == "network={" {
            if in_network {
                return Err(Error::new(
                    "nested network block in generated Wi-Fi configuration",
                ));
            }
            in_network = true;
        } else if value == "}" {
            if !in_network {
                return Err(Error::new(
                    "unexpected closing network block in generated Wi-Fi configuration",
                ));
            }
            let name = ssid
                .take()
                .ok_or_else(|| Error::new("network block has no SSID"))?;
            let paused = config
                .paused_ssids
                .iter()
                .any(|candidate| candidate.as_bytes() == name);
            let priority = if paused {
                config.wifi_paused_priority
            } else {
                config.wifi_default_priority
            };
            output.push_str(&format!("    priority={priority}\n"));
            count += 1;
            in_network = false;
        } else if in_network {
            if let Some(value) = value.strip_prefix("ssid=") {
                if ssid.is_some() {
                    return Err(Error::new(
                        "network block contains multiple SSID assignments",
                    ));
                }
                ssid = Some(decode_ssid(value)?);
            } else if let Some(priority) = value.strip_prefix("priority=") {
                priority
                    .parse::<i32>()
                    .map_err(|_| Error::new("invalid existing Wi-Fi priority"))?;
                continue;
            }
        }
        output.push_str(line);
    }
    if in_network {
        return Err(Error::new(
            "unterminated network block in generated Wi-Fi configuration",
        ));
    }
    if count == 0 {
        return Err(Error::new(
            "expected Netplan-generated Wi-Fi network blocks",
        ));
    }
    Ok((output, count))
}

fn decode_ssid(value: &str) -> Result<Vec<u8>> {
    let (value, escaped) = if let Some(value) = value.strip_prefix("P\"") {
        (value, true)
    } else if let Some(value) = value.strip_prefix('"') {
        (value, false)
    } else {
        return Err(Error::new("expected a Netplan quoted SSID"));
    };
    let value = value
        .strip_suffix('"')
        .ok_or_else(|| Error::new("unterminated Netplan SSID"))?;
    let mut bytes = Vec::new();
    let mut input = value.bytes();
    while let Some(byte) = input.next() {
        if escaped && byte == b'\\' {
            let escaped = input
                .next()
                .ok_or_else(|| Error::new("incomplete SSID escape"))?;
            bytes.push(match escaped {
                b'\\' | b'"' => escaped,
                b'b' => 8,
                b'f' => 12,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'v' => 11,
                b'0'..=b'7' => {
                    let middle = input.next().filter(|byte| matches!(byte, b'0'..=b'7'));
                    let last = input.next().filter(|byte| matches!(byte, b'0'..=b'7'));
                    let (Some(middle), Some(last)) = (middle, last) else {
                        return Err(Error::new("invalid Netplan octal SSID escape"));
                    };
                    let value = u16::from(escaped - b'0') * 64
                        + u16::from(middle - b'0') * 8
                        + u16::from(last - b'0');
                    u8::try_from(value)
                        .map_err(|_| Error::new("SSID octal escape exceeds one byte"))?
                }
                _ => return Err(Error::new("unsupported Netplan SSID escape")),
            });
        } else if (escaped && byte == b'"') || byte.is_ascii_control() {
            return Err(Error::new("unescaped character in Netplan SSID"));
        } else {
            bytes.push(byte);
        }
    }
    if bytes.is_empty() || bytes.len() > 32 {
        return Err(Error::new("SSID must contain 1 to 32 bytes"));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> NetworkConfig {
        NetworkConfig {
            paused_ssids: vec!["Example Mobile".into(), "Café \"#{}\\".into()],
            wifi_default_priority: 100,
            wifi_paused_priority: 0,
            ..NetworkConfig::default()
        }
    }

    #[test]
    fn every_profile_gets_a_priority_without_changing_authentication() {
        let input = "ctrl_interface=/run/wpa_supplicant\n\nnetwork={\n    ssid=P\"Example Trusted\"\n    psk=\"fictional-}#{-password\"\n    priority=-999\n}\nnetwork={\n    ssid=P\"Example Mobile\"\n    scan_ssid=1\n    psk=\"fictional\\\"password\"\n    priority=999\n}\n";
        let (output, count) = prioritize(input, &policy()).unwrap();
        assert_eq!(count, 2);
        assert_eq!(
            output,
            input
                .replace("    priority=-999\n", "")
                .replace("    priority=999\n", "")
                .replacen("}\nnetwork", "    priority=100\n}\nnetwork", 1)
                .trim_end_matches("}\n")
                .to_string()
                + "    priority=0\n}\n"
        );
        assert_eq!(prioritize(&output, &policy()).unwrap(), (output, 2));
    }

    #[test]
    fn netplan_escaped_utf8_quotes_and_punctuation_match_private_ssid_bytes() {
        let input =
            "network={\n ssid=P\"Caf\\303\\251 \\\"#{}\\\\\"\n psk=\"fictional-password\"\n}\n";
        let (output, count) = prioritize(input, &policy()).unwrap();
        assert_eq!(count, 1);
        assert!(output.ends_with("    priority=0\n}\n"));
        assert_eq!(
            decode_ssid(r#"P"s\040t\tl\nr\rb\bf\fv\v""#).unwrap(),
            b"s t\tl\nr\rb\x08f\x0cv\x0b"
        );
    }

    #[test]
    fn exact_names_and_new_unlisted_networks_use_the_normal_priority() {
        let input =
            "network={\nssid=P\"example mobile\"\n}\nnetwork={\nssid=P\"New Example Network\"\n}";
        let (output, count) = prioritize(input, &policy()).unwrap();
        assert_eq!(count, 2);
        assert_eq!(output.matches("priority=100").count(), 2);
        assert!(!output.contains("priority=0"));
    }

    #[test]
    fn older_netplan_quoted_ssids_keep_backslashes_literal() {
        let config = NetworkConfig {
            paused_ssids: vec![r"Example\040Mobile".into()],
            ..policy()
        };
        let input = "network={\nssid=\"Example\\040Mobile\"\n}\nnetwork={\nssid=P\"Example\\040Mobile\"\n}\n";
        let (output, count) = prioritize(input, &config).unwrap();
        assert_eq!(count, 2);
        assert_eq!(output.matches("priority=0").count(), 1);
        assert_eq!(output.matches("priority=100").count(), 1);
        assert!(output.starts_with("network={\nssid=\"Example\\040Mobile\"\n    priority=0\n}"));
    }

    #[test]
    fn malformed_or_unsupported_profiles_fail_without_echoing_input() {
        for input in [
            "",
            "ctrl_interface=/run/wpa_supplicant\n",
            "}\n",
            "network={\n}",
            "network={\nnetwork={\n",
            "network={\nssid=P\"PRIVATE-SENTINEL\"\n",
            "network={\nssid=P\"PRIVATE-SENTINEL\"\nssid=P\"second\"\n}\n",
            "network={\nssid=PRIVATE-SENTINEL\n}\n",
            "network={\nssid=P\"PRIVATE-SENTINEL\"\npriority=PRIVATE-SENTINEL\n}\n",
        ] {
            let error = prioritize(input, &policy()).unwrap_err().to_string();
            assert!(!error.contains("PRIVATE-SENTINEL"));
        }
        for value in [
            r#"P"""#,
            r#"P"\""#,
            r#"P"\12""#,
            r#"P"\400""#,
            r#"P"\999""#,
            r#"P"\q""#,
            r#"P"bad"quote""#,
        ] {
            assert!(
                decode_ssid(value).is_err(),
                "invalid fixture unexpectedly accepted"
            );
        }
        assert!(decode_ssid(&format!("P\"{}\"", "x".repeat(33))).is_err());
        let mut config = policy();
        config.wifi_paused_priority = config.wifi_default_priority;
        assert!(prioritize("network={\nssid=P\"Example\"\n}\n", &config).is_err());
    }
}
