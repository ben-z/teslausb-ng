use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::command::CommandRunner;
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LunConfig {
    pub disk_path: PathBuf,
    pub removable: bool,
    pub readonly: bool,
    pub cdrom: bool,
}

impl LunConfig {
    pub fn new(disk_path: PathBuf) -> Self {
        Self {
            disk_path,
            removable: true,
            readonly: false,
            cdrom: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UsbGadget {
    name: String,
    configfs: PathBuf,
    path: PathBuf,
    udc_path: PathBuf,
}

impl Default for UsbGadget {
    fn default() -> Self {
        Self::new("teslausb", PathBuf::from("/sys/kernel/config/usb_gadget"))
    }
}

impl UsbGadget {
    const VENDOR_ID: &'static str = "0x1d6b";
    const PRODUCT_ID: &'static str = "0x0104";

    pub fn new(name: impl Into<String>, configfs: PathBuf) -> Self {
        let name = name.into();
        let path = configfs.join(&name);
        Self {
            name,
            configfs,
            path,
            udc_path: PathBuf::from("/sys/class/udc"),
        }
    }

    pub fn initialize(&self, luns: &[(u32, LunConfig)]) -> Result<()> {
        if self.is_initialized() {
            return Ok(());
        }
        if luns.is_empty() {
            return Err(Error::new("at least one LUN must be configured"));
        }

        if !self.configfs.exists() {
            let configfs_base = Path::new("/sys/kernel/config");
            if !configfs_base.exists() {
                return Err(Error::new(
                    "configfs is not mounted; run: sudo mount -t configfs none /sys/kernel/config",
                ));
            }
            if !run_modprobe("libcomposite") || !self.configfs.exists() {
                return Err(Error::new(
                    "USB gadget configfs is unavailable; run: sudo modprobe libcomposite",
                ));
            }
        }

        for (_, lun) in luns {
            if !lun.disk_path.exists() {
                return Err(Error::new(format!(
                    "disk image not found: {}",
                    lun.disk_path.display()
                )));
            }
        }

        if let Err(err) = self.initialize_inner(luns) {
            let _ = fs::remove_dir_all(&self.path);
            return Err(err);
        }
        Ok(())
    }

    fn initialize_inner(&self, luns: &[(u32, LunConfig)]) -> Result<()> {
        fs::create_dir_all(&self.path)?;
        self.write(&self.path.join("idVendor"), Self::VENDOR_ID)?;
        self.write(&self.path.join("idProduct"), Self::PRODUCT_ID)?;
        self.write(&self.path.join("bcdDevice"), "0x0100")?;
        self.write(&self.path.join("bcdUSB"), "0x0200")?;

        let strings = self.path.join("strings/0x409");
        fs::create_dir_all(&strings)?;
        self.write(&strings.join("manufacturer"), "TeslaUSB")?;
        self.write(&strings.join("product"), "Tesla USB Drive")?;
        self.write(&strings.join("serialnumber"), "fedcba9876543210")?;

        let func = self.path.join("functions/mass_storage.0");
        fs::create_dir_all(&func)?;
        for (lun_id, config) in luns {
            self.configure_lun(&func, *lun_id, config)?;
        }

        let cfg = self.path.join("configs/c.1");
        fs::create_dir_all(cfg.join("strings/0x409"))?;
        self.write(
            &cfg.join("strings/0x409/configuration"),
            "Config 1: Mass Storage",
        )?;
        self.write(&cfg.join("MaxPower"), "250")?;

        let link = cfg.join("mass_storage.0");
        if !link.exists() {
            std::os::unix::fs::symlink(&func, &link)?;
        }
        Ok(())
    }

    fn configure_lun(&self, func: &Path, lun_id: u32, config: &LunConfig) -> Result<()> {
        let lun = func.join(format!("lun.{lun_id}"));
        fs::create_dir_all(&lun)?;
        self.write(
            &lun.join("removable"),
            if config.removable { "1" } else { "0" },
        )?;
        self.write(&lun.join("ro"), if config.readonly { "1" } else { "0" })?;
        self.write(&lun.join("cdrom"), if config.cdrom { "1" } else { "0" })?;
        self.write(&lun.join("file"), &config.disk_path.display().to_string())?;
        Ok(())
    }

    pub fn remove(&self) -> Result<()> {
        if !self.is_initialized() {
            return Ok(());
        }
        self.disable()?;

        let cfg_link = self.path.join("configs/c.1/mass_storage.0");
        if cfg_link.is_symlink() {
            fs::remove_file(cfg_link)?;
        }
        remove_dir_if_exists(&self.path.join("configs/c.1/strings/0x409"))?;
        remove_dir_if_exists(&self.path.join("configs/c.1"))?;

        let func = self.path.join("functions/mass_storage.0");
        if func.exists() {
            for entry in fs::read_dir(&func)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() && entry.file_name().to_string_lossy() != "lun.0" {
                    remove_dir_if_exists(&path)?;
                }
            }
            remove_dir_if_exists(&func)?;
        }

        remove_dir_if_exists(&self.path.join("strings/0x409"))?;
        remove_dir_if_exists(&self.path)?;
        Ok(())
    }

    pub fn enable(&self) -> Result<()> {
        if !self.is_initialized() {
            return Err(Error::new("gadget is not initialized"));
        }
        if self.is_enabled() {
            return Ok(());
        }
        let udc = self.get_udc()?;
        self.write(&self.path.join("UDC"), &udc)?;
        Ok(())
    }

    pub fn disable(&self) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        self.write(&self.path.join("UDC"), "")?;
        Ok(())
    }

    pub fn is_initialized(&self) -> bool {
        self.path.exists()
    }

    pub fn is_enabled(&self) -> bool {
        let udc = self.path.join("UDC");
        udc.exists()
            && fs::read_to_string(udc)
                .map(|content| !content.trim().is_empty())
                .unwrap_or(false)
    }

    pub fn status_json(&self) -> String {
        let enabled = self.is_enabled();
        let initialized = self.is_initialized();
        let udc = if initialized {
            fs::read_to_string(self.path.join("UDC"))
                .unwrap_or_default()
                .trim()
                .to_string()
        } else {
            String::new()
        };
        format!(
            "{{\n  \"name\": \"{}\",\n  \"initialized\": {},\n  \"enabled\": {},\n  \"udc\": {}\n}}\n",
            self.name,
            initialized,
            enabled,
            if udc.is_empty() {
                "null".to_string()
            } else {
                format!("\"{}\"", udc)
            }
        )
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn write(&self, path: &Path, value: &str) -> Result<()> {
        fs::write(path, format!("{value}\n"))?;
        Ok(())
    }

    fn get_udc(&self) -> Result<String> {
        if !self.udc_path.exists() {
            return Err(Error::new(format!(
                "UDC path {} does not exist",
                self.udc_path.display()
            )));
        }
        let mut udcs = fs::read_dir(&self.udc_path)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        udcs.sort();
        udcs.into_iter()
            .next()
            .ok_or_else(|| Error::new("no USB Device Controller found"))
    }
}

#[derive(Debug)]
pub struct GadgetDisableGuard {
    gadget: UsbGadget,
    was_enabled: bool,
}

impl GadgetDisableGuard {
    pub fn disable_if_needed(gadget: UsbGadget) -> Result<Self> {
        let was_enabled = gadget.is_enabled();
        if was_enabled {
            gadget.disable()?;
            if gadget.is_enabled() {
                return Err(Error::new("gadget is still enabled after disable"));
            }
        }
        Ok(Self {
            gadget,
            was_enabled,
        })
    }
}

impl Drop for GadgetDisableGuard {
    fn drop(&mut self) {
        if self.was_enabled {
            if let Err(err) = self.gadget.enable() {
                eprintln!("error: failed to re-enable USB gadget: {}", err);
            }
        }
    }
}

fn run_modprobe(module: &str) -> bool {
    CommandRunner
        .run("modprobe", [module], Some(Duration::from_secs(30)))
        .map(|output| output.success())
        .unwrap_or(false)
}

fn remove_dir_if_exists(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("teslausb-{name}-{suffix}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn lun_config_defaults_match_mass_storage_expectations() {
        let config = LunConfig::new(PathBuf::from("/cam.bin"));
        assert_eq!(config.disk_path, PathBuf::from("/cam.bin"));
        assert!(config.removable);
        assert!(!config.readonly);
        assert!(!config.cdrom);
    }

    #[test]
    fn initialized_and_enabled_reflect_configfs_files() {
        let configfs = temp_dir("configfs");
        let gadget = UsbGadget::new("test", configfs.clone());
        assert_eq!(gadget.name(), "test");
        assert!(!gadget.is_initialized());
        assert!(!gadget.is_enabled());

        fs::create_dir_all(configfs.join("test")).unwrap();
        fs::write(configfs.join("test/UDC"), "").unwrap();
        assert!(gadget.is_initialized());
        assert!(!gadget.is_enabled());

        fs::write(configfs.join("test/UDC"), "fake-udc\n").unwrap();
        assert!(gadget.is_enabled());

        let _ = fs::remove_dir_all(configfs);
    }

    #[test]
    fn initialize_rejects_empty_luns_and_missing_disk() {
        let configfs = temp_dir("configfs");
        let gadget = UsbGadget::new("test", configfs.clone());

        assert!(gadget.initialize(&[]).is_err());
        assert!(gadget
            .initialize(&[(0, LunConfig::new(PathBuf::from("/definitely/missing.img")))])
            .is_err());

        let _ = fs::remove_dir_all(configfs);
    }

    #[test]
    fn initialize_writes_expected_configfs_structure() {
        let root = temp_dir("gadget-init");
        let configfs = root.join("usb_gadget");
        fs::create_dir_all(&configfs).unwrap();
        let disk = root.join("cam.bin");
        fs::write(&disk, b"cam").unwrap();

        let gadget = UsbGadget::new("test", configfs.clone());
        gadget
            .initialize(&[(0, LunConfig::new(disk.clone()))])
            .unwrap();

        let gadget_path = configfs.join("test");
        assert_eq!(
            fs::read_to_string(gadget_path.join("idVendor")).unwrap(),
            "0x1d6b\n"
        );
        assert_eq!(
            fs::read_to_string(gadget_path.join("functions/mass_storage.0/lun.0/file")).unwrap(),
            format!("{}\n", disk.display())
        );
        assert!(gadget_path.join("configs/c.1/mass_storage.0").is_symlink());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn enable_requires_initialized_gadget_and_udc() {
        let root = temp_dir("gadget-enable");
        let configfs = root.join("usb_gadget");
        let udc_path = root.join("udc");
        fs::create_dir_all(&configfs).unwrap();
        fs::create_dir_all(&udc_path).unwrap();
        fs::create_dir_all(udc_path.join("fake-udc")).unwrap();
        fs::create_dir_all(configfs.join("test")).unwrap();
        fs::write(configfs.join("test/UDC"), "").unwrap();

        let mut gadget = UsbGadget::new("test", configfs.clone());
        gadget.udc_path = udc_path;
        gadget.enable().unwrap();
        assert_eq!(
            fs::read_to_string(configfs.join("test/UDC")).unwrap(),
            "fake-udc\n"
        );
        gadget.disable().unwrap();
        assert!(!gadget.is_enabled());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn enable_fails_when_not_initialized_or_no_udc_exists() {
        let root = temp_dir("gadget-enable-fail");
        let configfs = root.join("usb_gadget");
        let udc_path = root.join("udc");
        fs::create_dir_all(&configfs).unwrap();
        fs::create_dir_all(&udc_path).unwrap();

        let mut gadget = UsbGadget::new("test", configfs.clone());
        gadget.udc_path = udc_path.clone();
        assert!(gadget.enable().is_err());

        fs::create_dir_all(configfs.join("test")).unwrap();
        fs::write(configfs.join("test/UDC"), "").unwrap();
        assert!(gadget.enable().is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn status_json_reports_basic_state() {
        let configfs = temp_dir("gadget-status");
        fs::create_dir_all(configfs.join("test")).unwrap();
        fs::write(configfs.join("test/UDC"), "fake\n").unwrap();
        let gadget = UsbGadget::new("test", configfs.clone());

        let json = gadget.status_json();

        assert!(json.contains("\"name\": \"test\""));
        assert!(json.contains("\"initialized\": true"));
        assert!(json.contains("\"enabled\": true"));
        assert!(json.contains("\"udc\": \"fake\""));

        let _ = fs::remove_dir_all(configfs);
    }

    #[test]
    fn disable_guard_reenables_when_it_disabled_the_gadget() {
        let root = temp_dir("gadget-guard");
        let configfs = root.join("usb_gadget");
        let udc_path = root.join("udc");
        fs::create_dir_all(configfs.join("test")).unwrap();
        fs::write(configfs.join("test/UDC"), "fake\n").unwrap();
        fs::create_dir_all(udc_path.join("fake")).unwrap();

        let mut gadget = UsbGadget::new("test", configfs.clone());
        gadget.udc_path = udc_path;
        {
            let _guard = GadgetDisableGuard::disable_if_needed(gadget.clone()).unwrap();
            assert!(!gadget.is_enabled());
        }
        assert!(gadget.is_enabled());

        let _ = fs::remove_dir_all(root);
    }
}
