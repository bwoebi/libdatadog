// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

use io_lifetimes::OwnedFd;
use nix::libc;
use std::{
    env,
    os::unix::prelude::{FromRawFd, RawFd},
    path::PathBuf,
};
pub mod fork;
pub mod utils;

mod spawn;
pub use spawn::*;

// Reexport nix::WaitStatus
use crate::trampoline::Entrypoint;
pub use nix::sys::wait::WaitStatus;

impl From<Entrypoint> for spawn::Target {
    fn from(entrypoint: Entrypoint) -> Self {
        spawn::Target::Entrypoint(entrypoint)
    }
}

impl Entrypoint {
    pub fn get_fs_path(&self) -> Option<PathBuf> {
        let (path, _) = unsafe { utils::get_dl_path_raw(self.ptr as *const libc::c_void) };

        Some(PathBuf::from(path?.to_str().ok()?.to_owned()))
    }
}

pub(crate) static ENV_PASS_FD_KEY: &str = "__DD_INTERNAL_PASSED_FD";

pub fn recv_passed_fd() -> anyhow::Result<OwnedFd> {
    let val = env::var(ENV_PASS_FD_KEY)?;
    let fd: RawFd = val.parse()?;

    // check if FD number is valid
    nix::fcntl::fcntl(fd, nix::fcntl::FcntlArg::F_GETFD)?;

    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[macro_export]
macro_rules! assert_child_exit {
    ($pid:expr, $expected_exit_code:expr) => {{
        loop {
            match nix::sys::wait::waitpid(Some(nix::unistd::Pid::from_raw($pid)), None).unwrap() {
                nix::sys::wait::WaitStatus::Exited(pid, exit_code) => {
                    if exit_code != $expected_exit_code {
                        panic!(
                            "Child ({}) exited with code {} instead of expected {}",
                            pid, exit_code, $expected_exit_code
                        );
                    }
                    break;
                }
                _ => continue,
            }
        }
    }};
    ($pid:expr) => {
        assert_child_exit!($pid, 0)
    };
}
