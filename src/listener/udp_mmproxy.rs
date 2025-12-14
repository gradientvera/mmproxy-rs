use simple_eyre::eyre::{eyre, Result, WrapErr};

use crate::{
    args::ArgsMmproxy,
    util::{
        self, MAX_DGRAM_SIZE, ConnectionsHashMap,
    },
};
use std::{
    net::SocketAddr,
    sync::{
        Arc, atomic::Ordering
    },
};
use tokio::{net::UdpSocket, sync::{Notify, RwLock, mpsc}, time};
use tokio_util::sync::CancellationToken;

pub async fn listen(args: ArgsMmproxy) -> Result<()> {
    let socket = util::bind_udp_socket(args.listen_addr).await?;

    let mut buffer = [0u8; MAX_DGRAM_SIZE];
    let connections = Arc::new(RwLock::new(ConnectionsHashMap::new()));

    log::info!("mmproxy listening on: {}", args.listen_addr);

    let main_loop = tokio::spawn(async move {
        loop {
            let (read, addr) = socket.recv_from(&mut buffer).await.wrap_err("failed to accept connection")?;
            
            if let Some(ref allowed_subnets) = args.allowed_subnets {
                let ip_addr = addr.ip();

                if !util::check_origin_allowed(&ip_addr, allowed_subnets) {
                    log::warn!("connection origin is not allowed: {ip_addr}");
                    continue;
                }
            }

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
    args: &ArgsMmproxy,
    addr: SocketAddr,
    buffer: &mut [u8],
    connections: Arc<RwLock<ConnectionsHashMap>>
) -> Result<()> {
    let (src_addr, _, version) = match util::parse_proxy_protocol_header(&buffer) {
        Ok((addr_pair, rest, version)) => match addr_pair {
            Some((src, _)) => (src, rest, version),
            None => (addr, rest, version),
        },
        Err(err) => return Err(err).wrap_err("failed to parse proxy protocol header"),
    };

    if version < 2 {
        return Err(eyre!(
            "proxy protocol version 1 doesn't support UDP connections"
        ));
    }

    let target_addr = match src_addr {
        SocketAddr::V4(_) => args.ipv4_fwd,
        SocketAddr::V6(_) => args.ipv6_fwd,
    };

    log::info!("[new conn] [origin: {addr}] [src: {src_addr}]");

    let dst_sock = util::udp_create_upstream_conn(src_addr, target_addr, args.mark).await.wrap_err("failed to create upstream socket")?;
    let activity = Arc::new(Notify::new());
    let quit = CancellationToken::new();

    for _i in 0..num_cpus::get() {
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
                                if size > 0 {
                                    let (new_src_addr, rest, version) = match util::parse_proxy_protocol_header(&buffer_src[..size]) {
                                        Ok((addr_pair, rest, version)) => match addr_pair {
                                            Some((src, _)) => (src, rest, version),
                                            None => (addr, rest, version),
                                        },
                                        Err(e) => {
                                            log::info!("closing {addr} due to PROXY protocol parse error: {e:#}");
                                            quit.cancel();
                                            return;
                                        },
                                    };

                                    if new_src_addr != src_addr {
                                        log::info!("closing {addr} due to source address changing");
                                        quit.cancel();
                                        return;
                                    }

                                    if version < 2 {
                                        log::info!("closing {addr} because PROXY protocol version 1 doesn't support UDP connections");
                                        quit.cancel();
                                        return;
                                    }
                                    
                                    if let Err(e) = dst_sock.send(&rest).await {
                                        log::info!("closing {addr} due to destination connection error: {e:#}");
                                        quit.cancel();
                                        return;
                                    }
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