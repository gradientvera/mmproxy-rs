use proxy_protocol::{ProxyHeader, version2};
use simple_eyre::eyre::{Result, WrapErr};
use tokio_util::sync::CancellationToken;

use crate::{
    args::ArgsReverseProxy,
    util::{
        self, ConnectionsHashMap, MAX_DGRAM_SIZE,
        make_proxy_protocol_addresses
    },
};
use std::{
    net::SocketAddr,
    sync::{
        Arc, atomic::Ordering
    },
};
use tokio::{net::UdpSocket, sync::{Notify, RwLock, mpsc}, time};


pub async fn listen(args: ArgsReverseProxy) -> Result<()> {
    let socket = util::bind_udp_socket(args.listen_addr).await?;

    let mut buffer = [0u8; MAX_DGRAM_SIZE];
    let connections = Arc::new(RwLock::new(ConnectionsHashMap::new()));

    log::info!("reverse proxy listening on: {}", args.listen_addr);

    let main_loop = tokio::spawn(async move {
        loop {
            let (read, addr) = socket.recv_from(&mut buffer).await.wrap_err("failed to accept connection")?;

            if let Some(conn) = connections.read().await.get(&addr) {
                conn.send(&buffer[..read]).await.wrap_err("failed to send data to upstream socket")?;
                continue;
            }

            if let Err(why) = udp_handle_connection(&args, addr, &mut buffer[..read], connections.clone()).await {
                log::error!("{why:#}");
            };
        }
    });
    
    tokio::join!(main_loop).0?
}

async fn udp_handle_connection(
    args: &ArgsReverseProxy,
    addr: SocketAddr,
    buffer: &mut [u8],
    connections: Arc<RwLock<ConnectionsHashMap>>
) -> Result<()> {
    log::info!("[new conn] [origin: {addr}]");

    let pp_header = ProxyHeader::Version2 {
        addresses: make_proxy_protocol_addresses(addr, args.forward_addr),
        command: version2::ProxyCommand::Proxy,
        transport_protocol: version2::ProxyTransportProtocol::Datagram,
    };
    let pp_buffer = Arc::new(proxy_protocol::encode(pp_header).wrap_err("failed to encode PROXY protocol header!")?);
    let dst_sock = util::udp_create_reverse_proxy_conn(args.forward_addr).await?;
    
    let mut send_buffer: Vec<u8> = Vec::with_capacity(pp_buffer.len() + buffer.len());
    send_buffer.extend_from_slice(&pp_buffer);
    send_buffer.extend_from_slice(buffer);

    dst_sock.send(&send_buffer).await.wrap_err("failed to send initial data buffer")?;

    let activity = Arc::new(Notify::new());
    let quit = CancellationToken::new();

    for _i in 0..num_cpus::get() {
        let pp_buffer = pp_buffer.clone();
        let dst_sock = dst_sock.clone();
        let activity = activity.clone();
        let quit = quit.clone();

        let listen_addr = args.listen_addr;
        tokio::spawn(async move {
            let mut buffer_src = [0u8; MAX_DGRAM_SIZE];
            let mut buffer_dst = [0u8; MAX_DGRAM_SIZE];

            let src_sock = util::bind_udp_socket(listen_addr).await.wrap_err("failed to bind new client socket").unwrap();
            src_sock.connect(addr).await.wrap_err("failed to connect to remote client address").unwrap();

            loop {
                tokio::select! {
                    res = src_sock.recv(&mut buffer_src) => {
                        match res {
                            Ok(size) => {
                                let buffer = &buffer_src[..size];
                                let mut send_buffer: Vec<u8> = Vec::with_capacity(pp_buffer.len() + buffer.len());
                                send_buffer.extend_from_slice(&pp_buffer);
                                send_buffer.extend_from_slice(buffer);

                                if size > 0 && let Err(e) = dst_sock.send(&send_buffer).await {
                                    log::info!("closing {addr} due to destination connection error: {e:#}");
                                    quit.cancel();
                                    return;
                                }
                            },
                            Err(why) => {
                                log::info!("closing {addr} due to source connection error: {why:#}");
                                quit.cancel();
                                return;
                            },
                        }
                        activity.notify_one();
                    },
                    res = dst_sock.recv(&mut buffer_dst) => {
                        match res {
                            Ok(size) => {
                                if size > 0 && let Err(e) = src_sock.send(&buffer_dst[..size]).await {
                                    log::info!("closing {addr} due to source connection error: {e:#}");
                                    quit.cancel();
                                    return;
                                }
                            },
                            Err(why) => {
                                log::info!("closing {addr} due to destination connection error: {why:#}");
                                quit.cancel();
                                return;
                            },
                        }
                        activity.notify_one();
                    },
                    _ = quit.cancelled() => {
                        return;
                    }
                }
            }
        });
    }

    let sleep = args.close_after;
    let connections = connections.clone();
    tokio::spawn(async move {
        loop {
            let read_timeout = time::sleep(sleep);
            let activity_received = activity.notified();

            tokio::select! {
                _ = read_timeout => {
                    log::info!("closing {addr} due to inactivity");
                    connections.write().await.remove(&addr);
                    quit.cancel();
                    return;
                },
                _ = quit.cancelled() => {
                    connections.write().await.remove(&addr);
                    quit.cancel();
                    return;
                },
                _ = activity_received => {}
            }
        }
    });

    Ok(())
}