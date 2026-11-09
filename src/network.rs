use std::time::Duration;

use crate::command::CommandRunner;
use crate::config::NetworkConfig;
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct NetworkPolicy {
    config: NetworkConfig,
}

impl NetworkPolicy {
    pub fn new(config: &NetworkConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    pub fn check_interval(&self) -> Duration {
        self.config.check_interval
    }

    pub fn blocked_network(&self) -> Result<Option<String>> {
        if self.config.blocked_ssids.is_empty() {
            return Ok(None);
        }
        let devices = self.iw(&["dev"])?;
        for interface in interfaces(&devices)? {
            let link = self.iw(&["dev", interface, "link"])?;
            if let Some(ssid) = connected_ssid(&link)? {
                if let Some(blocked) = self
                    .config
                    .blocked_ssids
                    .iter()
                    .find(|name| name.as_bytes() == ssid)
                {
                    return Ok(Some(blocked.clone()));
                }
            }
        }
        Ok(None)
    }

    pub fn ensure_allowed(&self) -> Result<()> {
        if let Some(ssid) = self.blocked_network()? {
            return Err(Error::new(format!(
                "uploads paused on blocked Wi-Fi network {ssid:?}"
            )));
        }
        Ok(())
    }

    fn iw(&self, args: &[&str]) -> Result<String> {
        let output =
            CommandRunner.check("iw", args.iter().copied(), Some(self.config.check_timeout))?;
        Ok(output.stdout)
    }
}

fn interfaces(output: &str) -> Result<Vec<&str>> {
    let mut interfaces = Vec::new();
    for line in output.lines() {
        let line = line.trim();
        if let Some(name) = line.strip_prefix("Interface ") {
            if name.is_empty() || name.chars().any(char::is_whitespace) {
                return Err(Error::new("invalid interface name in iw dev output"));
            }
            interfaces.push(name);
        }
    }
    if interfaces.is_empty()
        && !output.trim().is_empty()
        && !output.lines().all(|line| line.trim().starts_with("phy#"))
    {
        return Err(Error::new(
            "could not identify Wi-Fi interfaces in iw dev output",
        ));
    }
    Ok(interfaces)
}

fn connected_ssid(output: &str) -> Result<Option<Vec<u8>>> {
    if output.trim() == "Not connected." {
        return Ok(None);
    }
    if !output.starts_with("Connected to ") {
        return Err(Error::new(
            "could not identify connection state in iw link output",
        ));
    }
    let ssids: Vec<_> = output
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix("SSID: "))
        .collect();
    if ssids.len() != 1 {
        return Err(Error::new("expected one SSID in connected iw link output"));
    }
    let mut bytes = Vec::new();
    let mut input = ssids[0].as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        if byte == b'\\' {
            if input.next() != Some(b'x') {
                return Err(Error::new("invalid SSID escape in iw link output"));
            }
            let high = input.next().and_then(|b| (b as char).to_digit(16));
            let low = input.next().and_then(|b| (b as char).to_digit(16));
            match (high, low) {
                (Some(high), Some(low)) => bytes.push((high * 16 + low) as u8),
                _ => return Err(Error::new("invalid SSID hex escape in iw link output")),
            }
        } else {
            bytes.push(byte);
        }
    }
    if bytes.is_empty() || bytes.len() > 32 {
        return Err(Error::new(
            "SSID in iw link output must contain 1 to 32 bytes",
        ));
    }
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_all_interfaces_and_no_wireless_devices() {
        assert_eq!(
            interfaces("phy#1\n\tInterface wlan1\n\t\ttype managed\nphy#0\n\tInterface wlan0\n")
                .unwrap(),
            ["wlan1", "wlan0"]
        );
        assert!(interfaces("").unwrap().is_empty());
        assert!(interfaces("phy#0\n").unwrap().is_empty());
        assert!(interfaces("unexpected output").is_err());
        assert!(interfaces("\tInterface bad name\n").is_err());
    }

    #[test]
    fn decodes_ssid_bytes_without_trimming_or_normalizing_names() {
        for (encoded, expected) in [
            ("Ben's iPhone", "Ben's iPhone"),
            (r"Ben\xe2\x80\x99s iPhone", "Ben’s iPhone"),
            (r"\x20Hotspot\x20", " Hotspot "),
            (r"a\x5cb,c:d", r"a\b,c:d"),
        ] {
            assert_eq!(
                connected_ssid(&format!(
                    "Connected to aa (on wlan0)\n\tSSID: {encoded}\n\tfreq: 2412\n"
                ))
                .unwrap()
                .unwrap(),
                expected.as_bytes()
            );
        }
        assert_eq!(connected_ssid("Not connected.\n").unwrap(), None);
        assert_eq!(
            connected_ssid("Connected to aa\n\tSSID: \\xff\n").unwrap(),
            Some(vec![255])
        );
    }

    #[test]
    fn rejects_unknown_or_malformed_connection_details() {
        for output in [
            "",
            "error",
            "Connected to aa\n",
            "Connected to aa\n\tSSID: \n",
            "Connected to aa\n\tSSID: a\n\tSSID: b\n",
            "Connected to aa\n\tSSID: \\xzz\n",
            "Connected to aa\n\tSSID: \\x2\n",
            "Connected to aa\n\tSSID: \\a\n",
        ] {
            assert!(connected_ssid(output).is_err(), "{output:?}");
        }
        assert!(connected_ssid(&format!("Connected to aa\n\tSSID: {}\n", "a".repeat(33))).is_err());
    }
}
