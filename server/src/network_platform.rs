use std::io::{self, IoSliceMut};
use std::net::{SocketAddr, UdpSocket};

const MAX_RECV_DATAGRAM_SIZE: usize = 65_535;
const RECV_BATCH_SIZE: usize = quinn_udp::BATCH_SIZE;

#[derive(Clone, Copy)]
struct RecvMeta {
    slot: usize,
    offset: usize,
    from: SocketAddr,
    len: usize,
}

pub(crate) struct RecvBatch {
    storage: Vec<u8>,
    metas: Vec<RecvMeta>,
}

impl RecvBatch {
    pub(crate) fn new() -> Self {
        Self {
            storage: vec![0u8; RECV_BATCH_SIZE * MAX_RECV_DATAGRAM_SIZE],
            metas: Vec::with_capacity(RECV_BATCH_SIZE),
        }
    }

    fn clear(&mut self) {
        self.metas.clear();
    }

    fn push(&mut self, slot: usize, offset: usize, from: SocketAddr, len: usize) {
        self.metas.push(RecvMeta { slot, offset, from, len });
    }

    pub(crate) fn len(&self) -> usize {
        self.metas.len()
    }

    pub(crate) fn from(&self, index: usize) -> SocketAddr {
        self.metas[index].from
    }

    pub(crate) fn packet(&self, index: usize) -> &[u8] {
        let meta = self.metas[index];
        let slot_start = meta.slot * MAX_RECV_DATAGRAM_SIZE;
        let start = slot_start + meta.offset;
        &self.storage[start..start + meta.len]
    }
}

pub(crate) struct RecvBatcher {
    socket: UdpSocket,
    udp_state: quinn_udp::UdpSocketState,
    rx_meta: Vec<quinn_udp::RecvMeta>,
}

impl RecvBatcher {
    pub(crate) fn new(socket: UdpSocket) -> io::Result<Self> {
        let udp_state = quinn_udp::UdpSocketState::new((&socket).into())?;
        let rx_meta = vec![quinn_udp::RecvMeta::default(); RECV_BATCH_SIZE];
        Ok(Self { socket, udp_state, rx_meta })
    }

    pub(crate) fn recv_next_batch(&mut self, batch: &mut RecvBatch) -> io::Result<usize> {
        batch.clear();

        let received = {
            let mut bufs: Vec<IoSliceMut<'_>> = batch
                .storage
                .chunks_mut(MAX_RECV_DATAGRAM_SIZE)
                .take(RECV_BATCH_SIZE)
                .map(IoSliceMut::new)
                .collect();

            match self.udp_state.recv((&self.socket).into(), &mut bufs, &mut self.rx_meta) {
                Ok(n) => n,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(0),
                Err(err) => return Err(err),
            }
        };

        for slot in 0..received {
            let meta = &self.rx_meta[slot];
            let stride = meta.stride.max(1);
            let mut offset = 0usize;

            while offset < meta.len {
                let len = (meta.len - offset).min(stride);
                batch.push(slot, offset, meta.addr, len);
                offset += stride;
            }
        }

        Ok(batch.len())
    }
}

pub(crate) struct UdpCapabilities {
    pub(crate) batch_size: usize,
    pub(crate) batch_recv: bool,
    pub(crate) batch_send: bool,
    pub(crate) gso_enabled: bool,
    pub(crate) gro_enabled: bool,
    pub(crate) max_gso_segments: usize,
    pub(crate) gro_segments: usize,
}

pub(crate) fn detect_udp_capabilities(socket: &UdpSocket) -> io::Result<UdpCapabilities> {
    let state = quinn_udp::UdpSocketState::new(socket.into())?;
    let max_gso_segments = state.max_gso_segments().max(1);
    let gro_segments = state.gro_segments().max(1);

    Ok(UdpCapabilities {
        batch_size: quinn_udp::BATCH_SIZE,
        batch_recv: quinn_udp::BATCH_SIZE > 1,
        batch_send: quinn_udp::BATCH_SIZE > 1,
        gso_enabled: max_gso_segments > 1,
        gro_enabled: gro_segments > 1,
        max_gso_segments,
        gro_segments,
    })
}

pub(crate) struct SendBatcher {
    socket: UdpSocket,
    udp_state: quinn_udp::UdpSocketState,
}

impl SendBatcher {
    pub(crate) fn new(socket: UdpSocket) -> io::Result<Self> {
        let udp_state = quinn_udp::UdpSocketState::new((&socket).into())?;
        Ok(Self { socket, udp_state })
    }

    pub(crate) fn max_gso_segments(&self) -> usize {
        self.udp_state.max_gso_segments().max(1)
    }

    pub(crate) fn send_transmit(&self, transmit: &quinn_udp::Transmit<'_>) -> io::Result<()> {
        self.udp_state.send((&self.socket).into(), transmit)
    }
}
