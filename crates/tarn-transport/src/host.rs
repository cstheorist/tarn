//! Local admission prerequisites, not a capacity or physical-failure-domain proof.
use serde::{Deserialize, Serialize};
use std::{io, path::PathBuf};
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirements {
    pub min_cpus: usize,
    pub min_memory_bytes: u64,
    pub min_free_disk_bytes: u64,
    pub data_path: PathBuf,
}
#[derive(Debug, Serialize)]
pub struct Report {
    pub cpus: usize,
    pub memory_bytes: u64,
    pub free_disk_bytes: u64,
}
impl Requirements {
    pub fn check(&self) -> io::Result<Report> {
        #[cfg(target_os = "linux")]
        {
            if self.min_cpus == 0 || self.min_memory_bytes == 0 || self.min_free_disk_bytes == 0 {
                return Err(io::Error::other("production resource minima must be positive"));
            }
            let mut cpus = std::thread::available_parallelism()?.get();
            if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
                let p: Vec<_> = s.split_whitespace().collect();
                if p.len() == 2 && p[0] != "max" {
                    let quota: u64 = p[0].parse().map_err(io::Error::other)?;
                    let period: u64 = p[1].parse().map_err(io::Error::other)?;
                    if period == 0 {
                        return Err(io::Error::other("invalid CPU quota"));
                    }
                    cpus = cpus.min((quota / period) as usize);
                }
            }
            let mem = std::fs::read_to_string("/proc/meminfo")?;
            let mut memory_bytes = mem
                .lines()
                .find_map(|l| {
                    l.strip_prefix("MemTotal:")
                        .and_then(|s| s.split_whitespace().next())
                        .and_then(|s| s.parse::<u64>().ok())
                })
                .ok_or_else(|| io::Error::other("cannot read physical memory"))?
                * 1024;
            for path in [
                "/sys/fs/cgroup/memory.max",
                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            ] {
                if let Ok(n) = std::fs::read_to_string(path) {
                    if let Ok(n) = n.trim().parse::<u64>() {
                        memory_bytes = memory_bytes.min(n);
                    }
                }
            }
            use std::os::unix::ffi::OsStrExt;
            let path = std::ffi::CString::new(self.data_path.as_os_str().as_bytes()).map_err(io::Error::other)?;
            let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
            // SAFETY: valid NUL-terminated path and writable statvfs storage.
            if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: successful statvfs initialized the structure.
            let stat = unsafe { stat.assume_init() };
            let free_disk_bytes = stat.f_bavail.saturating_mul(stat.f_frsize);
            if cpus < self.min_cpus
                || memory_bytes < self.min_memory_bytes
                || free_disk_bytes < self.min_free_disk_bytes
            {
                return Err(io::Error::other(format!(
                    "host below production minima: cpus={cpus}, memory={memory_bytes}, disk_free={free_disk_bytes}"
                )));
            }
            Ok(Report {
                cpus,
                memory_bytes,
                free_disk_bytes,
            })
        }
        #[cfg(not(target_os = "linux"))]
        Err(io::Error::other(
            "production resource preflight currently requires Linux",
        ))
    }
}
