/*
 * bank-data
 * Copyright © 2023 Centre for Policy Dialogue
 *
 * bank-data is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * bank-data is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with bank-data. If not, see <https://www.gnu.org/licenses/>
 * and navigate to version 3 of the GNU General Public License.
 */

use std::fmt::Debug;
use std::pin::Pin;
use std::ptr;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use futures_io::{AsyncRead, AsyncWrite};
use async_std::net::TcpStream;
use async_std::path::{Path, PathBuf};
use async_std::{io, task};
use async_std::fs::OpenOptions;
use async_tls::TlsConnector;
use http_body_util::{BodyExt, Empty};
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::{header, Method, Request, Response, StatusCode, Uri};
use eyre::Result;
use futures::AsyncWriteExt;

static TLS_CONNECTOR: OnceLock<TlsConnector> = OnceLock::new();

pub trait DownloadHandler: Debug {
    fn destination_file(&self, url: &str) -> Result<PathBuf>;
}

pub struct Connection<'dh, DH> {
    handler: &'dh DH,
    host: (Box<str>, u16),
    sender: SendRequest<Empty<Bytes>>,
    hit_count: usize
}

impl<'dh, DH> Connection<'dh, DH> where DH: DownloadHandler {
    pub async fn open_connection(handler: &'dh DH, host: &str) -> Result<Connection<'dh, DH>> {
        let host = (Box::from(host), 443);
        Self::open_connection_internal(handler, host, 0).await
    }

    async fn open_connection_internal(handler: &'dh DH, (domain, port): (Box<str>, u16),
                                      hit_count: usize) -> Result<Connection<'dh, DH>> {
        let tls = TLS_CONNECTOR.get_or_init(|| TlsConnector::default());

        let stream = TcpStream::connect((&domain as &str, port)).await?;
        let stream = StreamWrapper(tls.connect(&domain, stream).await?);
        let (sender, connection) = hyper::client::conn::http1::handshake(stream).await?;

        log::debug!("Opened connection using {:?}", handler);
        task::spawn(async move {
            if let Err(e) = connection.await {
                log::warn!("Error while polling HTTP connection: {}", e);
            }
        });
        Ok(Connection {
            handler,
            host: (domain, port),
            sender,
            hit_count
        })
    }

    pub async fn download(&mut self, url: String) -> Result<bool> {
        log::debug!("Connecting to url {}", &url);

        let parsed_uri = url.parse::<Uri>()?;
        let authority = parsed_uri.authority().expect("No authority").clone();

        let request = Request::builder()
            .uri(parsed_uri)
            .method(Method::GET)
            .header(header::HOST, authority.as_str())
            .body(Empty::<Bytes>::new())?;

        self.sender.ready().await?;
        self.hit_count += 1;

        let response = self.sender.send_request(request).await?;
        match response.status() {
            StatusCode::NOT_FOUND | StatusCode::FOUND | StatusCode::MOVED_PERMANENTLY => Ok(false),
            StatusCode::OK => {
                let destination = self.handler.destination_file(&url)?;
                self.complete_download(response, &destination).await?;
                Ok(true)
            },
            status => Err(eyre::eyre!("Unknown status code: {}", status))
        }
    }

    async fn complete_download(&mut self, mut response: Response<Incoming>, filename: &Path) -> Result<()> {
        // Determine whether we can keep re-using the existing connection
        let refresh_connection = {
            match response.headers().get(header::CONNECTION).map(|header| header.as_bytes()) {
                Some(b"Keep-Alive") | Some(b"keep-alive") => false,
                _else => true
            }
        };
        if refresh_connection {
            let host = std::mem::replace(&mut self.host, (Box::default(), 0));
            *self = Self::open_connection_internal(self.handler, host, self.hit_count).await?;
        }
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(filename).await?;
        let mut file = io::BufWriter::new(file);
        while let Some(frame) = response.frame().await.transpose()? {
            if let Some(next_chunk) = frame.data_ref() {
                file.write_all(&next_chunk).await?;
            }
        }
        Ok(())
    }

    pub fn hit_count(self) -> usize {
        self.hit_count
    }
}

struct StreamWrapper<IO>(IO);

impl<IO> hyper::rt::Read for StreamWrapper<IO> where IO: AsyncRead + Unpin {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, mut buf: hyper::rt::ReadBufCursor<'_>) -> Poll<io::Result<()>> {
        let pinned_self = Pin::new(&mut self.0);
        unsafe {
            let buffer = buf.as_mut();
            // Initialize the buffer
            ptr::write_bytes(buffer.as_mut_ptr(), 0, buffer.len());
            // Assume initialized
            let buffer: &mut [u8] = std::mem::transmute(buffer);
            let num_bytes = task::ready!(
                AsyncRead::poll_read(pinned_self, cx, buffer)?
            );
            buf.advance(num_bytes);
        }
        Poll::Ready(Ok(()))
    }
}

impl<IO> hyper::rt::Write for StreamWrapper<IO> where IO: AsyncWrite + Unpin {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let pinned_self = Pin::new(&mut self.0);
        pinned_self.poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let pinned_self = Pin::new(&mut self.0);
        pinned_self.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let pinned_self = Pin::new(&mut self.0);
        pinned_self.poll_close(cx)
    }
}
