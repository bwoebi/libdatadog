// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

use std::{
    io::{self, ErrorKind, Read, Write},
    os::unix::{
        net::UnixStream,
        prelude::{AsRawFd, RawFd},
    },
    time::Duration,
};

pub mod async_channel;
pub mod metadata;

use sendfd::{RecvWithFd, SendWithFd};

use self::metadata::ChannelMetadata;

use super::{PlatformHandle, MAX_FDS};

#[derive(Debug)]
pub struct Channel {
    inner: PlatformHandle<UnixStream>,
    pub metadata: ChannelMetadata,
}

impl Clone for Channel {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metadata: Default::default(),
        }
    }
}

impl Channel {
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        let sock = self.inner.as_socketlike_view()?;
        sock.set_read_timeout(timeout)
    }

    pub fn set_write_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        let sock = self.inner.as_socketlike_view()?;
        sock.set_write_timeout(timeout)
    }

    pub fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
        let sock = self.inner.as_socketlike_view()?;
        sock.set_nonblocking(nonblocking)
    }
}

impl Read for Channel {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut fds = [0; MAX_FDS];
        let socket = self.inner.as_socketlike_view()?;
        let (n, fd_cnt) = socket.recv_with_fd(buf, &mut fds)?;
        self.metadata.receive_fds(&fds[..fd_cnt]);
        Ok(n)
    }
}

impl Write for Channel {
    fn write_all(&mut self, mut buf: &[u8]) -> Result<(), io::Error> {
        let mut socket = &*self.inner.as_socketlike_view()?;

        while !buf.is_empty() {
            let handles = self.metadata.drain_to_send();
            if handles.is_empty() {
                break;
            }

            let fds: Vec<RawFd> = handles.iter().map(AsRawFd::as_raw_fd).collect();
            match socket.send_with_fd(buf, &fds) {
                Ok(0) => {
                    self.metadata.reenqueue_for_sending(handles);
                    return Err(io::Error::new(
                        ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => {
                    self.metadata.defer_close_handles(handles);
                    buf = &buf[n..]
                }
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => {
                    self.metadata.reenqueue_for_sending(handles);

                    return Err(e);
                }
            }
        }

        while !buf.is_empty() {
            match socket.write(buf) {
                Ok(0) => {
                    return Err(io::Error::new(
                        ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => buf = &buf[n..],
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        //TODO implement partial writes
        self.write_all(buf).map(|_| buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut socket = &*self.inner.as_socketlike_view()?;
        socket.flush()
    }
}

impl From<Channel> for PlatformHandle<UnixStream> {
    fn from(c: Channel) -> Self {
        c.inner
    }
}

impl From<PlatformHandle<UnixStream>> for Channel {
    fn from(h: PlatformHandle<UnixStream>) -> Self {
        Channel {
            inner: h,
            metadata: Default::default(),
        }
    }
}

impl From<UnixStream> for Channel {
    fn from(stream: UnixStream) -> Self {
        Channel {
            inner: PlatformHandle::from(stream),
            metadata: Default::default(),
        }
    }
}