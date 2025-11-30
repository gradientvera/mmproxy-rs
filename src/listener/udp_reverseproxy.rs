use proxy_protocol::{ProxyHeader, version2};
use simple_eyre::eyre::{Result, WrapErr};

use crate::{
    args::ArgsReverseProxy,
    util::{
        self, ConnectionsHashMap, MAX_DGRAM_SIZE, UdpProxyConn,
        udp_close_after_inactivity, udp_dst_to_src, make_proxy_protocol_addresses
    },
};
use std::{
    net::SocketAddr,
    sync::{
        Arc, atomic::Ordering
    },
};
use tokio::{net::UdpSocket, sync::mpsc, task::JoinSet};

pub async fn listen(args: ArgsReverseProxy) -> Result<()> {
    let connections = Arc::new(ConnectionsHashMap::new());
    let (tx, mut rx) = mpsc::channel::<SocketAddr>(128);

    log::info!("reverse proxy listening on: {}, {} listeners", args.listen_addr, args.listeners);
    
    let mut workers = JoinSet::<Result<()>>::new();

    for _i in 0..args.listeners {
        let args = args.clone();
        let socket = util::bind_udp_socket(args.listen_addr, args.listeners).await?;
        let connections = connections.clone();
        let mut buffer = [0u8; MAX_DGRAM_SIZE];
        let tx = tx.clone();

        workers.spawn(async move {
            loop {
                let (read, addr) = socket.recv_from(&mut buffer).await.wrap_err("failed to accept connection")?;
 
                if let Err(why) = udp_handle_connection(
                    &args,
                    socket.clone(),
                    addr,
                    &buffer[..read],
                    connections.clone(),
                    tx.clone(),
                )
                .await
                {
                    log::error!("{why:#}")
                }
            }
        });
    }

    loop {
        tokio::select! {
            // close inactive connections in this branch
            addr = rx.recv() => {
                if let Some(addr) = addr {
                    if let Some((_conn, (_dst, handle))) = connections.remove(&addr) {
                        log::info!("closing {addr} due to inactivity");
                        handle.abort();
                    }
                }
            },
            Some(worker) = workers.join_next() => {
                if let Err(why) = worker {
                    log::error!("{why:#}")
                }
            }
        }
    }
}

async fn udp_handle_connection(
    args: &ArgsReverseProxy,
    src: Arc<UdpSocket>,
    src_addr: SocketAddr,
    buffer: &[u8],
    connections: Arc<ConnectionsHashMap>,
    tx: mpsc::Sender<SocketAddr>,
) -> Result<()> {
    let dst = match connections.entry(src_addr) {
        dashmap::Entry::Occupied(entry) => {
            let dst = entry.get().0.clone();
            dst.last_activity.fetch_add(1, Ordering::SeqCst);
            dst
        },
        dashmap::Entry::Vacant(entry) => {
            log::info!("[new conn] [src: {src_addr}]");

            let dst = {
                let pp_header = ProxyHeader::Version2 {
                    addresses: make_proxy_protocol_addresses(src_addr, args.forward_addr),
                    command: version2::ProxyCommand::Proxy,
                    transport_protocol: version2::ProxyTransportProtocol::Datagram,
                };
                let pp_buffer = proxy_protocol::encode(pp_header).wrap_err("failed to encode PROXY protocol header!")?.to_vec();
                let sock = util::udp_create_reverse_proxy_conn(args.forward_addr).await?;
                Arc::new(UdpProxyConn::new(sock, pp_buffer))
            };

            let src_clone = src.clone();
            let dst_clone = dst.clone();
            let handle = tokio::spawn(async move {
                if let Err(why) = udp_dst_to_src(src_addr, src_clone, dst_clone).await {
                    log::error!("{why:#}");
                };
            });
            tokio::spawn(udp_close_after_inactivity(
                src_addr,
                args.close_after,
                tx.clone(),
                dst.clone(),
            ));

            entry.insert((dst.clone(), handle));
            dst
        },
    };
    
    let mut send_buffer: Vec<u8> = Vec::with_capacity(dst.pp_header.len() + buffer.len());
    send_buffer.extend_from_slice(&dst.pp_header);
    send_buffer.extend_from_slice(buffer);

    match dst.sock.send(&send_buffer).await {
        Ok(size) => {
            if size != send_buffer.len() {
                log::warn!("sent {} bytes to [{}] but received {} bytes from [{}]!", size, dst.sock.peer_addr().unwrap(), send_buffer.len(), src_addr);
            }
            log::debug!("from [{}] to [{}], size: {}", src_addr, args.forward_addr, size);
            Ok(())
        }
        Err(err) => Err(err).wrap_err("failed to write data to the upstream connection"),
    }
}