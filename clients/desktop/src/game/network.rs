use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};

use anyhow::{Context, Result};
use quiche::RecvInfo;
use rand::RngCore;

const MAX_DATAGRAM_SIZE: usize = 1350;

pub(super) struct IncomingPackets {
    pub(super) datagrams: Vec<Vec<u8>>,
    pub(super) streams: Vec<Vec<u8>>,
}

pub(super) struct QuicClient {
    socket: UdpSocket,
    conn: quiche::Connection,
    server_addr: SocketAddr,
    local_addr: SocketAddr,
    send_buf: [u8; MAX_DATAGRAM_SIZE],
    recv_buf: [u8; 65_535],
    app_buf: [u8; 4096],
    stream_recv_buffers: HashMap<u64, Vec<u8>>,
}

impl QuicClient {
    pub(super) fn connect(server_addr: SocketAddr) -> Result<Self> {
        log::info!("connecting to server {server_addr}...");
        let socket = UdpSocket::bind("0.0.0.0:0").context("failed to bind UDP socket")?;
        socket
            .set_nonblocking(true)
            .context("failed to set UDP socket non-blocking")?;

        let local_addr = socket.local_addr().context("failed to get local addr")?;
        log::info!("local UDP endpoint: {local_addr}");

        let mut config = build_client_quic_config()?;

        let mut scid = [0u8; quiche::MAX_CONN_ID_LEN];
        rand::thread_rng().fill_bytes(&mut scid);
        let scid = quiche::ConnectionId::from_ref(&scid);

        let conn = quiche::connect(
            Some("widev.local"),
            &scid,
            local_addr,
            server_addr,
            &mut config,
        )
        .context("failed to create QUIC connection")?;
        log::info!("QUIC connection object created, starting handshake...");

        let mut client = Self {
            socket,
            conn,
            server_addr,
            local_addr,
            send_buf: [0; MAX_DATAGRAM_SIZE],
            recv_buf: [0; 65_535],
            app_buf: [0; 4096],
            stream_recv_buffers: HashMap::new(),
        };

        client.flush_outgoing()?;
        Ok(client)
    }

    pub(super) fn is_established(&self) -> bool {
        self.conn.is_established()
    }

    pub(super) fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    pub(super) fn peer_cert_der(&self) -> Option<Vec<u8>> {
        self.conn.peer_cert().map(|bytes| bytes.to_vec())
    }

    pub(super) fn poll(&mut self) -> Result<IncomingPackets> {
        loop {
            let recv = self.socket.recv_from(&mut self.recv_buf);
            let (len, from) = match recv {
                Ok(v) => v,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err).context("socket recv_from failed"),
            };

            if from != self.server_addr {
                continue;
            }

            let recv_info = RecvInfo {
                from,
                to: self.local_addr,
            };

            if let Err(err) = self.conn.recv(&mut self.recv_buf[..len], recv_info) {
                if err != quiche::Error::Done {
                    log::warn!("client conn.recv error: {err:?}");
                }
            }
        }

        let mut datagrams = Vec::new();
        loop {
            match self.conn.dgram_recv(&mut self.app_buf) {
                Ok(len) => datagrams.push(self.app_buf[..len].to_vec()),
                Err(quiche::Error::Done) => break,
                Err(_) => break,
            }
        }

        let mut streams = Vec::new();
        for stream_id in self.conn.readable() {
            loop {
                match self.conn.stream_recv(stream_id, &mut self.app_buf) {
                    Ok((len, fin)) => {
                        let recv_buf = self.stream_recv_buffers.entry(stream_id).or_default();
                        recv_buf.extend_from_slice(&self.app_buf[..len]);
                        while let Some(frame) = pop_frame(recv_buf) {
                            streams.push(frame);
                        }
                        if fin {
                            self.stream_recv_buffers.remove(&stream_id);
                            break;
                        }
                    }
                    Err(quiche::Error::Done) => break,
                    Err(_) => break,
                }
            }
        }

        if let Some(timeout) = self.conn.timeout() {
            if timeout.is_zero() {
                self.conn.on_timeout();
            }
        }

        self.flush_outgoing()?;
        Ok(IncomingPackets { datagrams, streams })
    }

    pub(super) fn send_datagram(&mut self, payload: &[u8]) -> Result<()> {
        let _ = self.conn.dgram_send(payload);
        self.flush_outgoing()
    }

    fn flush_outgoing(&mut self) -> Result<()> {
        loop {
            match self.conn.send(&mut self.send_buf) {
                Ok((len, send_info)) => {
                    self.socket
                        .send_to(&self.send_buf[..len], send_info.to)
                        .context("socket send_to failed")?;
                }
                Err(quiche::Error::Done) => break,
                Err(err) => return Err(anyhow::anyhow!("conn.send failed: {err:?}")),
            }
        }

        Ok(())
    }
}

fn build_client_quic_config() -> Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    config.verify_peer(false);
    config
        .set_application_protos(&[b"widev-poc-quic"])
        .context("failed setting ALPN")?;
    config.set_max_idle_timeout(10_000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_stream_data_bidi_remote(1_000_000);
    config.set_initial_max_stream_data_uni(1_000_000);
    config.set_initial_max_streams_bidi(16);
    config.set_initial_max_streams_uni(16);
    config.enable_dgram(true, 64, 64);
    Ok(config)
}

fn pop_frame(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    if buffer.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
    if buffer.len() < 4 + len {
        return None;
    }
    let payload = buffer[4..4 + len].to_vec();
    buffer.drain(..4 + len);
    Some(payload)
}
