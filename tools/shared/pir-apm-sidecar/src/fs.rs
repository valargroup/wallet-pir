//! Local block filesystems and required volumes.
//!
//! Every writable local block filesystem is checked once per filesystem
//! identity (`st_dev`), so bind and alternate mounts of one device report once
//! under its shortest mount path. Availability excludes root-reserved blocks:
//! below 15% available warns immediately and exactly 15% is healthy. A required
//! volume is bound to a filesystem UUID; a missing mount, including one whose
//! path now resolves to its parent filesystem, or a different filesystem at that
//! path is critical from the first observation.
use crate::config::Volume;
use pir_host_observations::{Check, Status, Unit, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const WARN_PERCENT: u128 = 15;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mount {
    pub device: (u32, u32),
    pub mount_point: String,
    pub fstype: String,
    pub source: String,
    pub writable: bool,
}

/// Block counts from `statvfs`, in fragment-size units.
#[derive(Clone, Copy, Debug)]
pub struct Stat {
    pub frsize: u64,
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
}

/// Decode the octal escapes the kernel uses for spaces and similar characters.
fn unescape(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() {
            if let Some(v) = std::str::from_utf8(&bytes[i + 1..i + 4])
                .ok()
                .and_then(|d| u8::from_str_radix(d, 8).ok())
            {
                out.push(v);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parse `/proc/self/mountinfo`.
pub fn parse_mountinfo(text: &str) -> Vec<Mount> {
    text.lines()
        .filter_map(|line| {
            let (left, right) = line.split_once(" - ")?;
            let left: Vec<&str> = left.split(' ').collect();
            let right: Vec<&str> = right.split(' ').collect();
            let (major, minor) = left.get(2)?.split_once(':')?;
            // Superblock options, not the per-mount ones: a sandbox that remounts
            // paths read-only in this service's namespace (ProtectSystem=strict)
            // must not hide whether the host filesystem itself is writable.
            let options = right.get(2)?;
            Some(Mount {
                device: (major.parse().ok()?, minor.parse().ok()?),
                mount_point: unescape(left.get(4)?),
                fstype: right.first()?.to_string(),
                source: unescape(right.get(1)?),
                writable: options.split(',').any(|o| o == "rw"),
            })
        })
        .collect()
}

fn is_local_block(m: &Mount) -> bool {
    m.source.starts_with("/dev/")
        && !matches!(
            m.fstype.as_str(),
            "squashfs" | "iso9660" | "udf" | "overlay"
        )
        && !m.source.starts_with("/dev/loop")
}

/// Available fraction of the space unprivileged writers can use.
fn availability(stat: Stat, observed_at: u64, name: String) -> Check {
    let reserved = stat.bfree.saturating_sub(stat.bavail);
    let usable = stat.blocks.saturating_sub(reserved) as u128;
    let available = stat.bavail as u128;
    let values = vec![
        Value {
            name: "available_bytes".into(),
            value: (available * stat.frsize as u128) as f64,
            unit: Unit::Bytes,
        },
        Value {
            name: "usable_bytes".into(),
            value: (usable * stat.frsize as u128) as f64,
            unit: Unit::Bytes,
        },
    ];
    if usable == 0 {
        return Check {
            name,
            status: Status::Unknown,
            reason: "filesystem reports no usable blocks".into(),
            observed_at,
            values,
        };
    }
    let low = available * 100 < WARN_PERCENT * usable;
    Check {
        name,
        status: if low {
            Status::Warning
        } else {
            Status::Healthy
        },
        reason: format!(
            "{:.1}% available (warn below {WARN_PERCENT}%)",
            available as f64 * 100. / usable as f64
        ),
        observed_at,
        values,
    }
}

/// One check per distinct writable local block filesystem, plus a summary.
/// Paths outside the check-name alphabet use a digest; the escaped path remains
/// in the reason. The prefix cannot collide with a literal absolute mount path.
pub fn filesystems(
    mounts: &[Mount],
    stat: &dyn Fn(&str) -> Option<Stat>,
    observed_at: u64,
) -> Vec<Check> {
    let mut by_device: BTreeMap<(u32, u32), &Mount> = BTreeMap::new();
    for m in mounts.iter().filter(|m| m.writable && is_local_block(m)) {
        let keep = by_device.get(&m.device).is_none_or(|kept| {
            (m.mount_point.len(), &m.mount_point) < (kept.mount_point.len(), &kept.mount_point)
        });
        if keep {
            by_device.insert(m.device, m);
        }
    }
    let mut out = Vec::new();
    let mut unreadable = 0;
    for m in by_device.values() {
        let literal = format!("disk:{}", m.mount_point);
        let encoded = !pir_host_observations::valid_name(&literal, 128);
        let name = if encoded {
            format!(
                "disk:path-{}",
                hex::encode(Sha256::digest(m.mount_point.as_bytes()))
            )
        } else {
            literal
        };
        let mut check = match stat(&m.mount_point) {
            Some(s) => availability(s, observed_at, name),
            None => {
                unreadable += 1;
                Check {
                    name,
                    status: Status::Unknown,
                    reason: "filesystem statistics unavailable".into(),
                    observed_at,
                    values: vec![],
                }
            }
        };
        if encoded {
            // ASCII escaping and a bounded prefix keep even long/control-bearing
            // mount paths within the observation reason's 256-byte limit.
            let path: String = m
                .mount_point
                .chars()
                .flat_map(char::escape_default)
                .take(96)
                .collect();
            check.reason = format!("{path}: {}", check.reason);
        }
        out.push(check);
    }
    out.push(Check {
        name: "filesystems".into(),
        status: if by_device.is_empty() || unreadable > 0 {
            Status::Unknown
        } else {
            Status::Healthy
        },
        reason: format!("{} local block filesystems", by_device.len()),
        observed_at,
        values: vec![Value {
            name: "count".into(),
            value: by_device.len() as f64,
            unit: Unit::Count,
        }],
    });
    out
}

/// Required volumes, bound to their filesystem UUIDs.
pub fn volumes(
    required: &[Volume],
    mounts: &[Mount],
    stat: &dyn Fn(&str) -> Option<Stat>,
    uuid_device: &dyn Fn(&str) -> Option<(u32, u32)>,
    observed_at: u64,
) -> Vec<Check> {
    required
        .iter()
        .map(|v| {
            let name = format!("volume:{}", v.path);
            let critical = |reason: &str| Check {
                name: name.clone(),
                status: Status::Critical,
                reason: reason.into(),
                observed_at,
                values: vec![],
            };
            // The last mount at a path is the visible one.
            let Some(mount) = mounts.iter().rev().find(|m| m.mount_point == v.path) else {
                return critical("required volume is not mounted");
            };
            match uuid_device(&v.uuid) {
                None => return critical("required volume device is absent"),
                Some(device) if device != mount.device => {
                    return critical("a different filesystem is mounted at the required path")
                }
                Some(_) => {}
            }
            if !mount.writable {
                return critical("required volume is mounted read-only");
            }
            match stat(&v.path) {
                Some(s) => availability(s, observed_at, name),
                None => Check {
                    status: Status::Unknown,
                    reason: "filesystem statistics unavailable".into(),
                    ..critical("")
                },
            }
        })
        .collect()
}

pub fn statvfs(path: &str) -> Option<Stat> {
    let c = std::ffi::CString::new(path).ok()?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path and `s` a writable statvfs.
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return None;
    }
    Some(Stat {
        frsize: s.f_frsize as u64,
        blocks: s.f_blocks as u64,
        bfree: s.f_bfree as u64,
        bavail: s.f_bavail as u64,
    })
}

/// Device numbers from Linux udev's by-uuid links; unavailable on other platforms.
pub fn uuid_device(uuid: &str) -> Option<(u32, u32)> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(format!("/dev/disk/by-uuid/{uuid}")).ok()?;
        let rdev = meta.rdev();
        Some((libc::major(rdev), libc::minor(rdev)))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = uuid;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOUNTINFO: &str = "\
22 1 252:1 / / rw,relatime shared:1 - ext4 /dev/vda1 rw
23 22 252:15 / /boot/efi rw,relatime shared:2 - vfat /dev/vda15 rw
24 22 8:16 / /mnt/volume_data rw,relatime shared:3 - ext4 /dev/sdb rw
25 22 8:16 / /srv/data rw,relatime shared:3 - ext4 /dev/sdb rw
26 22 8:32 / /srv/cache rw,relatime shared:4 - xfs /dev/sdc rw
27 22 0:25 / /run rw,nosuid - tmpfs tmpfs rw
28 22 7:0 / /snap/core ro,relatime - squashfs /dev/loop0 ro
29 22 8:48 / /srv/ro ro,relatime - ext4 /dev/sdd ro
";

    fn stat(blocks: u64, bfree: u64, bavail: u64) -> Stat {
        Stat {
            frsize: 4096,
            blocks,
            bfree,
            bavail,
        }
    }

    fn find<'a>(checks: &'a [Check], name: &str) -> &'a Check {
        checks.iter().find(|c| c.name == name).unwrap()
    }

    #[test]
    fn root_and_data_volumes_are_independent_and_binds_deduplicate() {
        let mounts = parse_mountinfo(MOUNTINFO);
        let checks = filesystems(
            &mounts,
            &|path| match path {
                "/" => Some(stat(1000, 100, 50)),
                _ => Some(stat(1000, 900, 900)),
            },
            7,
        );
        let names: Vec<_> = checks.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "disk:/srv/data",
                "disk:/srv/cache",
                "disk:/",
                "disk:/boot/efi",
                "filesystems"
            ]
        );
        assert_eq!(find(&checks, "disk:/").status, Status::Warning);
        assert_eq!(find(&checks, "disk:/srv/data").status, Status::Healthy);
        assert_eq!(find(&checks, "filesystems").status, Status::Healthy);
    }

    #[test]
    fn unusual_mount_paths_keep_distinct_valid_disk_observations() {
        let paths = [
            "/".into(),
            "/srv/Data Set".into(),
            "/srv/data set".into(),
            "/srv/é\\mount\t".into(),
            format!("/srv/{}", "x".repeat(200)),
        ];
        let mounts: Vec<_> = paths
            .iter()
            .enumerate()
            .map(|(index, path)| Mount {
                device: (8, index as u32),
                mount_point: path.clone(),
                fstype: "ext4".into(),
                source: format!("/dev/disk{index}"),
                writable: true,
            })
            .collect();
        let checks = filesystems(&mounts, &|_| Some(stat(100, 1, 1)), 7);
        assert_eq!(
            checks.len(),
            mounts.len() + 1,
            "every writable filesystem must be observed"
        );
        assert!(checks[..mounts.len()]
            .iter()
            .all(|c| c.status == Status::Warning));
        let names: std::collections::BTreeSet<_> = checks.iter().map(|c| &c.name).collect();
        assert_eq!(names.len(), checks.len());
        assert_eq!(
            checks[0].name, "disk:/",
            "ordinary mount identifiers stay readable"
        );
        let repeated = filesystems(&mounts, &|_| None, 8);
        assert_eq!(
            checks.iter().map(|c| &c.name).collect::<Vec<_>>(),
            repeated.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
        assert!(repeated.iter().all(|c| c.status == Status::Unknown));
        for checks in [checks, repeated] {
            let doc = pir_host_observations::Observations {
                schema_version: pir_host_observations::SCHEMA_VERSION,
                host_id: "test".into(),
                incarnation: "test".into(),
                config_digest: format!("sha256:{}", "0".repeat(64)),
                generated_at: 8,
                checks,
            };
            assert!(
                doc.validate().is_ok(),
                "mount paths must not invalidate the observation document"
            );
        }
    }

    #[test]
    fn threshold_excludes_reserved_blocks_and_fifteen_percent_is_healthy() {
        // 1050 blocks, 50 reserved for root: 1000 usable.
        let edge = availability(stat(1050, 200, 150), 1, "d".into());
        assert_eq!(edge.status, Status::Healthy, "exactly 15% of 1000 usable");
        let below = availability(stat(1050, 199, 149), 1, "d".into());
        assert_eq!(below.status, Status::Warning);
        // Counting reserved blocks as usable would report 150/1050 = 14.3% and warn.
        // A resize is evaluated from current counts alone, with no remembered size.
        let resized = availability(stat(2100, 400, 300), 1, "d".into());
        assert_eq!(resized.status, Status::Healthy);
        let shrunk = availability(stat(1050, 100, 50), 1, "d".into());
        assert_eq!(shrunk.status, Status::Warning);
    }

    #[test]
    fn required_volumes_detect_absence_parent_directory_and_wrong_identity() {
        let mounts = parse_mountinfo(MOUNTINFO);
        let required = [
            Volume {
                path: "/srv/data".into(),
                uuid: "aaaa".into(),
            },
            Volume {
                path: "/srv/missing".into(),
                uuid: "bbbb".into(),
            },
            Volume {
                path: "/srv/cache".into(),
                uuid: "cccc".into(),
            },
            Volume {
                path: "/srv/ro".into(),
                uuid: "dddd".into(),
            },
        ];
        let devices = |uuid: &str| match uuid {
            "aaaa" => Some((8, 16)),
            "bbbb" => Some((8, 64)),
            "cccc" => Some((8, 99)),
            "dddd" => Some((8, 48)),
            _ => None,
        };
        let checks = volumes(&required, &mounts, &|_| Some(stat(10, 5, 5)), &devices, 3);
        assert_eq!(checks[0].status, Status::Healthy);
        // First observation: never mounted, and its path is a directory on `/`.
        assert_eq!(checks[1].status, Status::Critical);
        assert_eq!(checks[1].reason, "required volume is not mounted");
        assert_eq!(checks[2].status, Status::Critical);
        assert!(checks[2].reason.contains("different filesystem"));
        assert_eq!(checks[3].status, Status::Critical);
        // Device gone entirely.
        let checks = volumes(&required[..1], &mounts, &|_| None, &|_| None, 3);
        assert_eq!(checks[0].status, Status::Critical);
    }

    #[test]
    fn namespace_read_only_remounts_do_not_hide_writable_filesystems() {
        // As seen inside a unit with ProtectSystem=strict: per-mount `ro`.
        let sandboxed =
            "22 1 252:1 / / ro,relatime shared:1 - ext4 /dev/vda1 rw,errors=remount-ro\n\
            24 22 8:16 / /srv/data ro,relatime - ext4 /dev/sdb rw\n";
        let mounts = parse_mountinfo(sandboxed);
        assert!(mounts.iter().all(|m| m.writable));
        let checks = filesystems(&mounts, &|_| Some(stat(10, 5, 5)), 1);
        assert_eq!(checks.len(), 3);
    }

    #[test]
    fn mount_points_with_spaces_are_decoded() {
        let m = parse_mountinfo("30 22 8:1 / /srv/a\\040b rw - ext4 /dev/sde rw\n");
        assert_eq!(m[0].mount_point, "/srv/a b");
    }
}
