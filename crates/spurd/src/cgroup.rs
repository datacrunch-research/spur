// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{bail, Context};
use tracing::{debug, info, warn};

use spur_core::job::JobId;

const CGROUP2_MOUNT: &str = "/sys/fs/cgroup";
const ROOT_FALLBACK: &str = "/sys/fs/cgroup/spur";
const DAEMON_SUBGROUP: &str = "spurd";
const CONTROLLERS: [&str; 4] = ["cpu", "cpuset", "memory", "pids"];

static HIERARCHY: OnceLock<Option<Hierarchy>> = OnceLock::new();

#[derive(Debug)]
struct Hierarchy {
    root: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
enum Layout {
    SystemdDelegated { root: PathBuf },
    SelfDelegated { root: PathBuf, daemon: PathBuf },
    RootFallback { root: PathBuf },
}

pub(crate) struct JobCgroup {
    path: PathBuf,
    cleanup_on_drop: bool,
}

impl JobCgroup {
    pub(crate) fn open_procs(&self) -> anyhow::Result<File> {
        let path = self.path().join("cgroup.procs");
        OpenOptions::new()
            .write(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))
    }

    pub(crate) fn into_path(mut self) -> PathBuf {
        self.cleanup_on_drop = false;
        std::mem::take(&mut self.path)
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

pub(crate) fn open_procs_path(path: &Path) -> anyhow::Result<File> {
    let procs = path.join("cgroup.procs");
    OpenOptions::new()
        .write(true)
        .open(&procs)
        .with_context(|| format!("open {}", procs.display()))
}

impl Drop for JobCgroup {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            cleanup(&self.path);
        }
    }
}

pub(crate) fn initialize() {
    hierarchy();
}

pub(crate) fn setup_job(
    job_id: JobId,
    cpus: u32,
    memory_mb: u64,
    cpu_ids: &[u32],
) -> anyhow::Result<Option<JobCgroup>> {
    let Some(hierarchy) = hierarchy() else {
        return Ok(None);
    };
    hierarchy
        .setup_job(job_id, cpus, memory_mb, cpu_ids)
        .map(Some)
}

fn hierarchy() -> Option<&'static Hierarchy> {
    HIERARCHY
        .get_or_init(|| match Hierarchy::discover() {
            Ok(hierarchy) => {
                info!(root = %hierarchy.root.display(), "cgroup-v2 job isolation enabled");
                Some(hierarchy)
            }
            Err(error) => {
                warn!(error = %error, "cgroup-v2 job isolation unavailable");
                None
            }
        })
        .as_ref()
}

impl Hierarchy {
    fn discover() -> anyhow::Result<Self> {
        let mount = Path::new(CGROUP2_MOUNT);
        let current = current_unified_cgroup(Path::new("/proc/self/cgroup"))?;
        let current = mount.join(current.strip_prefix("/").unwrap_or(&current));
        let layout = select_layout(
            mount,
            &current,
            Path::new(ROOT_FALLBACK),
            nix::unistd::geteuid().is_root(),
        )?;

        let root = match layout {
            Layout::SystemdDelegated { root } => root,
            Layout::SelfDelegated { root, daemon } => {
                std::fs::create_dir_all(&daemon)
                    .with_context(|| format!("create daemon cgroup {}", daemon.display()))?;
                std::fs::write(daemon.join("cgroup.procs"), std::process::id().to_string())
                    .with_context(|| format!("move spurd into {}", daemon.display()))?;
                root
            }
            Layout::RootFallback { root } => {
                std::fs::create_dir_all(&root)
                    .with_context(|| format!("create cgroup root {}", root.display()))?;
                root
            }
        };

        enable_controllers(&root)?;
        Ok(Self { root })
    }

    fn setup_job(
        &self,
        job_id: JobId,
        cpus: u32,
        memory_mb: u64,
        cpu_ids: &[u32],
    ) -> anyhow::Result<JobCgroup> {
        let path = self.root.join(format!("job_{job_id}"));
        if let Err(error) = std::fs::create_dir(&path) {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                bail!(
                    "job cgroup {} already exists; refusing to mix launches",
                    path.display()
                );
            }
            return Err(error).with_context(|| format!("create job cgroup {}", path.display()));
        }

        let result = configure_job(&self.root, &path, cpus, memory_mb, cpu_ids);
        if let Err(error) = result {
            cleanup(&path);
            return Err(error);
        }

        debug!(
            job_id,
            cpus,
            memory_mb,
            path = %path.display(),
            "job cgroup created"
        );
        Ok(JobCgroup {
            path,
            cleanup_on_drop: true,
        })
    }
}

fn current_unified_cgroup(proc_cgroup: &Path) -> anyhow::Result<PathBuf> {
    let contents = std::fs::read_to_string(proc_cgroup)
        .with_context(|| format!("read {}", proc_cgroup.display()))?;
    contents
        .lines()
        .find_map(|line| {
            let mut fields = line.splitn(3, ':');
            match (fields.next(), fields.next(), fields.next()) {
                (Some("0"), Some(""), Some(path)) => Some(PathBuf::from(path)),
                _ => None,
            }
        })
        .context("unified cgroup-v2 membership not found")
}

fn select_layout(
    mount: &Path,
    current: &Path,
    root_fallback: &Path,
    is_root: bool,
) -> anyhow::Result<Layout> {
    if current
        .file_name()
        .is_some_and(|name| name == DAEMON_SUBGROUP)
    {
        let root = current
            .parent()
            .context("systemd delegate subgroup has no parent")?;
        if !root.starts_with(mount) {
            bail!("systemd delegated cgroup is outside the cgroup-v2 mount");
        }
        return Ok(Layout::SystemdDelegated {
            root: root.to_path_buf(),
        });
    }

    if is_root {
        return Ok(Layout::RootFallback {
            root: root_fallback.to_path_buf(),
        });
    }

    if !current.starts_with(mount) {
        bail!("current cgroup is outside the cgroup-v2 mount");
    }
    Ok(Layout::SelfDelegated {
        root: current.to_path_buf(),
        daemon: current.join(DAEMON_SUBGROUP),
    })
}

fn enable_controllers(root: &Path) -> anyhow::Result<()> {
    let available_path = root.join("cgroup.controllers");
    let available = std::fs::read_to_string(&available_path)
        .with_context(|| format!("read {}", available_path.display()))?;
    let missing: Vec<_> = CONTROLLERS
        .iter()
        .copied()
        .filter(|controller| !available.split_whitespace().any(|item| item == *controller))
        .collect();
    if !missing.is_empty() {
        bail!(
            "required cgroup controllers are unavailable at {}: {}",
            root.display(),
            missing.join(", ")
        );
    }

    let value = CONTROLLERS
        .iter()
        .map(|controller| format!("+{controller}"))
        .collect::<Vec<_>>()
        .join(" ");
    let subtree = root.join("cgroup.subtree_control");
    std::fs::write(&subtree, value)
        .with_context(|| format!("enable controllers in {}", subtree.display()))
}

fn configure_job(
    root: &Path,
    job: &Path,
    cpus: u32,
    memory_mb: u64,
    cpu_ids: &[u32],
) -> anyhow::Result<()> {
    if cpus == 0 {
        bail!("job CPU limit must be greater than zero");
    }

    let quota = u64::from(cpus)
        .checked_mul(100_000)
        .context("job CPU quota overflow")?;
    write_control(job, "cpu.max", &format!("{quota} 100000"))?;

    if memory_mb > 0 {
        let memory_bytes = memory_mb
            .checked_mul(1024 * 1024)
            .context("job memory limit overflow")?;
        write_control(job, "memory.max", &memory_bytes.to_string())?;
    }
    write_control(job, "memory.oom.group", "1")?;

    let max_pids = (u64::from(cpus) * 256).max(1024);
    write_control(job, "pids.max", &max_pids.to_string())?;

    if !cpu_ids.is_empty() {
        let mems_path = root.join("cpuset.mems.effective");
        let mems = std::fs::read_to_string(&mems_path)
            .with_context(|| format!("read {}", mems_path.display()))?;
        let mems = mems.trim();
        if mems.is_empty() {
            bail!("effective NUMA-node mask is empty at {}", root.display());
        }
        write_control(job, "cpuset.mems", mems)?;
        let cpus = cpu_ids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        write_control(job, "cpuset.cpus", &cpus)?;
    }

    Ok(())
}

fn write_control(cgroup: &Path, control: &str, value: &str) -> anyhow::Result<()> {
    let path = cgroup.join(control);
    std::fs::write(&path, value).with_context(|| format!("write {}", path.display()))
}

pub(crate) unsafe fn attach_current_process(procs: RawFd) -> std::io::Result<()> {
    let value = b"0";
    let written = unsafe { libc::write(procs, value.as_ptr().cast(), value.len()) };
    if written == value.len() as isize {
        Ok(())
    } else if written < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "short write to cgroup.procs",
        ))
    }
}

pub(crate) fn oom_killed(cgroup: &Path) -> bool {
    let Ok(events) = std::fs::read_to_string(cgroup.join("memory.events")) else {
        return false;
    };
    events.lines().any(|line| {
        let mut fields = line.split_whitespace();
        matches!((fields.next(), fields.next()), (Some("oom_kill"), Some(count)) if count != "0")
    })
}

pub(crate) fn cleanup(cgroup: &Path) {
    if let Ok(mut kill) = OpenOptions::new()
        .write(true)
        .open(cgroup.join("cgroup.kill"))
    {
        let _ = kill.write_all(b"1");
    } else if let Ok(pids) = std::fs::read_to_string(cgroup.join("cgroup.procs")) {
        for pid in pids.lines().filter_map(|value| value.trim().parse().ok()) {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }

    if let Err(error) = std::fs::remove_dir(cgroup) {
        warn!(error = %error, path = %cgroup.display(), "failed to remove cgroup");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;

    #[test]
    fn parses_unified_membership() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cgroup");
        std::fs::write(
            &path,
            "5:cpu:/legacy\n0::/system.slice/spurd.service/spurd\n",
        )
        .unwrap();

        assert_eq!(
            current_unified_cgroup(&path).unwrap(),
            PathBuf::from("/system.slice/spurd.service/spurd")
        );
    }

    #[test]
    fn systemd_subgroup_uses_unit_as_delegated_root() {
        let mount = Path::new("/sys/fs/cgroup");
        let current = mount.join("system.slice/spurd.service/spurd");

        assert_eq!(
            select_layout(mount, &current, &mount.join("spur"), false).unwrap(),
            Layout::SystemdDelegated {
                root: mount.join("system.slice/spurd.service")
            }
        );
    }

    #[test]
    fn unprivileged_agent_moves_to_daemon_leaf() {
        let mount = Path::new("/sys/fs/cgroup");
        let current = mount.join("system.slice/spurd.service");

        assert_eq!(
            select_layout(mount, &current, &mount.join("spur"), false).unwrap(),
            Layout::SelfDelegated {
                root: current.clone(),
                daemon: current.join("spurd")
            }
        );
    }

    #[test]
    fn configures_enforced_limits_and_cpuset_domain() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let job = root.join("job_7");
        std::fs::create_dir(&job).unwrap();
        std::fs::write(root.join("cpuset.mems.effective"), "0-3\n").unwrap();

        configure_job(root, &job, 32, 4096, &[2, 4, 6, 8]).unwrap();

        assert_eq!(
            std::fs::read_to_string(job.join("cpu.max")).unwrap(),
            "3200000 100000"
        );
        assert_eq!(
            std::fs::read_to_string(job.join("memory.max")).unwrap(),
            "4294967296"
        );
        assert_eq!(
            std::fs::read_to_string(job.join("memory.oom.group")).unwrap(),
            "1"
        );
        assert_eq!(
            std::fs::read_to_string(job.join("pids.max")).unwrap(),
            "8192"
        );
        assert_eq!(
            std::fs::read_to_string(job.join("cpuset.mems")).unwrap(),
            "0-3"
        );
        assert_eq!(
            std::fs::read_to_string(job.join("cpuset.cpus")).unwrap(),
            "2,4,6,8"
        );
    }

    #[test]
    fn requires_all_delegated_controllers() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("cgroup.controllers"), "cpu memory pids\n").unwrap();

        let error = enable_controllers(dir.path()).unwrap_err().to_string();
        assert!(error.contains("cpuset"));
    }

    #[test]
    fn attaches_current_pid_through_preopened_procs_file() {
        let file = tempfile::NamedTempFile::new().unwrap();

        unsafe { attach_current_process(file.as_file().as_raw_fd()) }.unwrap();

        assert_eq!(std::fs::read_to_string(file.path()).unwrap(), "0");
    }
}
