use proxy_protocol::{ProxyHeader, version2};
use simple_eyre::eyre::{eyre, Result, WrapErr};

use crate::{
    args::{ArgsMmproxy, ArgsReverseProxy},
    util::{
        self, ConnectionsHashMap, MAX_DGRAM_SIZE, UdpProxyConn, udp_close_after_inactivity, udp_dst_to_src
    },
};
use socket2::SockRef;
use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    sync::{
        Arc, atomic::{AtomicU64, Ordering}
    },
    time::Duration,
};
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle};

pub async fn listen(args: ArgsReverseProxy) -> Result<()> {
    let socket = {
        let socket = UdpSocket::bind(args.listen_addr)
            .await
            .wrap_err_with(|| format!("failed to bind to {}", args.listen_addr))?;

        let sock_ref = SockRef::from(&socket);
        sock_ref
            .set_reuse_port(args.listeners > 1)
            .wrap_err("failed to set reuse port on listener socket")?;

        Arc::new(socket)
    };

    let mut buffer = [0u8; MAX_DGRAM_SIZE];
    let mut connections = ConnectionsHashMap::new();
    let (tx, mut rx) = mpsc::channel::<SocketAddr>(128);

    log::info!("reverse proxy listening on: {}", args.listen_addr);
    loop {
        tokio::select! {
            // close inactive connections in this branch
            addr = rx.recv() => {
                if let Some(addr) = addr {
                    if let Some((_conn, handle)) = connections.remove(&addr) {
                        log::info!("closing {addr} due to inactivity");
                        handle.abort();
                    }
                }
            }
            // handle incoming DGRAM packets in this branch
            ret = socket.recv_from(&mut buffer) => {
                let (read, addr) = ret.wrap_err("failed to accept connection")?;

                if let Err(why) = udp_handle_connection(
                    &args,
                    socket.clone(),
                    addr,
                    &mut buffer[..read],
                    &mut connections,
                    tx.clone(),
                )
                .await
                {
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
    buffer: &mut [u8],
    connections: &mut ConnectionsHashMap,
    tx: mpsc::Sender<SocketAddr>,
) -> Result<()> {
    let pp_header = ProxyHeader::Version2 {
        addresses: {
            let target_addr4 = match args.forward_addr {
                SocketAddr::V4(v4) => v4,
                SocketAddr::V6(v6) => SocketAddrV4::new(Ipv4Addr::LOCALHOST, v6.port()),
            };
            let target_addr6 = match args.forward_addr {
                SocketAddr::V4(v4) => SocketAddrV6::new(Ipv6Addr::LOCALHOST, v4.port(), 0, 0),
                SocketAddr::V6(v6) => v6,
            };

            match src_addr {
                SocketAddr::V4(src_addr4) => {
                    version2::ProxyAddresses::Ipv4 {
                        source: src_addr4,
                        destination: target_addr4
                    }
                },
                SocketAddr::V6(src_addr6) => {
                    // Ipv4-mapped
                    if let Some(mapped) = src_addr6.ip().to_ipv4_mapped() {
                        version2::ProxyAddresses::Ipv4 {
                            source: SocketAddrV4::new(mapped, src_addr.port()),
                            destination: target_addr4
                        }
                    } else {
                        version2::ProxyAddresses::Ipv6 {
                            source: src_addr6,
                            destination: target_addr6
                        }
                    }
                },
            }
        },
        command: version2::ProxyCommand::Proxy,
        transport_protocol: version2::ProxyTransportProtocol::Datagram,
    };
    let mut pp_buffer = proxy_protocol::encode(pp_header).wrap_err("failed to encode PROXY protocol header!")?.to_vec();
    pp_buffer.extend(buffer.iter().cloned());

    let dst = match connections.get(&src_addr) {
        Some((dst, _handle)) => {
            dst.last_activity.fetch_add(1, Ordering::SeqCst);
            dst.clone()
        }
        // first time connecting
        None => {
            log::info!("[new conn] [src: {src_addr}]");

            let dst = {
                let sock = util::udp_create_reverse_proxy_conn(args.forward_addr).await?;
                Arc::new(UdpProxyConn::new(sock))
            };

            let src_clone = src.clone();
            let dst_clone = dst.clone();
            let handle = tokio::spawn(async move {
                if let Err(why) = udp_dst_to_src(src_addr, src_addr, src_clone, dst_clone).await {
                    log::error!("{why:#}");
                };
            });
            tokio::spawn(udp_close_after_inactivity(
                src_addr,
                args.close_after,
                tx.clone(),
                dst.clone(),
            ));

            connections.insert(src_addr, (dst.clone(), handle));
            dst
        }
    };

    match dst.sock.send(&pp_buffer).await {
        Ok(size) => {
            log::debug!("from [{}] to [{}], size: {}", src_addr, args.forward_addr, size);
            Ok(())
        }
        Err(err) => Err(err).wrap_err("failed to write data to the upstream connection"),
    }
}