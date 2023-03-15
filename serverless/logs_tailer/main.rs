use std::{
    fs::File,
    io::{Read, Write},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    thread::{self, JoinHandle},
    time::Duration,
};

use nix::libc::STDOUT_FILENO;

struct FileDesc(OwnedFd);

impl FromRawFd for FileDesc {
    unsafe fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
        Self(OwnedFd::from_raw_fd(fd))
    }
}

impl Write for FileDesc {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(nix::unistd::write(self.0.as_raw_fd(), buf)?)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Read for FileDesc {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(nix::unistd::read(self.0.as_raw_fd(), buf)?)
    }
}

/// check if we can hijack stdout and tee it to 2 targets
pub fn piping_poc() -> anyhow::Result<JoinHandle<()>> {
    println!("ASD");
    println!("Running2");
    let (stdout_forwarder, new_stdout) = nix::unistd::pipe().expect("Failed to create pipe");
    println!("Running2");
    let old_stdout = nix::unistd::dup(STDOUT_FILENO).expect("Failed to create pipe");
    println!("Running3");
    nix::unistd::dup2(new_stdout, STDOUT_FILENO).expect("Fail");
    println!("Running4");
    nix::unistd::close(new_stdout).expect("Failed to create pipe"); //close the duplicate
    let mut file = File::options()
        .append(true)
        .create(true)
        .write(true)
        .open("/tmp/yolo.stdout")
        .expect("Failed to create pipe");

    let mut stdout_forwarder = unsafe { FileDesc::from_raw_fd(stdout_forwarder) };
    let mut old_stdout = unsafe { FileDesc::from_raw_fd(old_stdout) };
    let join = thread::spawn(move || loop {
        loop {
            let mut buf = [0; 1000];
            let read = match stdout_forwarder.read(&mut buf) {
                Ok(s) => s,
                Err(er) => {
                    eprintln!("{}", er);
                    break;
                }
            };

            if let Err(err) = file.write_all(&buf[0..read]) {
                eprintln!("{}", err);
                break;
            };

            if let Err(err) = old_stdout.write_all(&buf[0..read]) {
                eprintln!("{}", err);
                break;
            };
            thread::sleep(Duration::from_micros(10));
        }
    });

    Ok(join)
}

fn main() {
    println!("Running");
    let jh = piping_poc().expect("Error creating pipe");
    jh.join().expect("Error joining thread");
    println!("Running");
    println!("Running");
    println!("Running");
}
