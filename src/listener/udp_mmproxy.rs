use dashmap::Entry;
use simple_eyre::eyre::{eyre, Result, WrapErr};

use crate::{
    args::ArgsMmproxy,
    util::{
        self, MAX_DGRAM_SIZE, ConnectionsHashMap, UdpProxyConn,
        udp_dst_to_src, udp_close_after_inactivity
    },
};
use std::{
    net::SocketAddr,
    sync::{
        Arc, atomic::Ordering
    },
};
use tokio::{net::UdpSocket, sync::mpsc, task::JoinSet};

pub async fn listen(args: ArgsMmproxy) -> Result<()> {
    let connections = Arc::new(ConnectionsHashMap::new());
    let (tx, mut rx) = mpsc::channel::<SocketAddr>(128);

    log::info!("mmproxy listening on: {}, {} listeners", args.listen_addr, args.listeners);
    
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
 
                if let Some(ref allowed_subnets) = args.allowed_subnets {
                    let ip_addr = addr.ip();

                    if !util::check_origin_allowed(&ip_addr, allowed_subnets) {
                        log::warn!("connection origin is not allowed: {ip_addr}");
                        continue;
                    }
                }

                if let Err(why) = udp_handle_connection(
                    &args,
                    socket.clone(),
                    addr,
                    &mut buffer[..read],
                    connections.clone(),
                    tx.clone(),
                )
                .await
                {
                    log::error!("{why:#}");
                }
            }
        });
    }

    loop {
        tokio::select! {
            // close inactive connections in this branch
            addr = rx.recv() => {
                if let Some(addr) = addr {
                    if let Some((addr, (_dst, handle))) = connections.remove(&addr) {
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
    args: &ArgsMmproxy,
    src: Arc<UdpSocket>,
    addr: SocketAddr,
    buffer: &mut [u8],
    connections: Arc<ConnectionsHashMap>,
    tx: mpsc::Sender<SocketAddr>,
) -> Result<()> {
    let (src_addr, rest, version) = match util::parse_proxy_protocol_header(buffer) {
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

    let dst = match connections.entry(addr) {
        Entry::Occupied(entry) => {
            let dst = entry.get().0.clone();
            dst.last_activity.fetch_add(1, Ordering::SeqCst);
            dst
        }
        Entry::Vacant(entry) => {
            if src_addr == addr {
                log::debug!("unknown source, using the downstream connection address");
            }

            log::info!("[new conn] [origin: {addr}] [src: {src_addr}]");

            let dst = {
                let sock = util::udp_create_upstream_conn(src_addr, target_addr, args.mark).await?;
                Arc::new(UdpProxyConn::new(sock, vec![]))
            };

            let src_clone = src.clone();
            let dst_clone = dst.clone();

            let handle = tokio::spawn(async move {
                if let Err(why) = udp_dst_to_src(addr, src_clone, dst_clone).await {
                    log::error!("{why:#}");
                };
            });

            tokio::spawn(udp_close_after_inactivity(
                addr,
                args.close_after,
                tx.clone(),
                dst.clone(),
            ));

            entry.insert((dst.clone(), handle));
            dst
        }
    };

    match dst.sock.send(rest).await {
        Ok(size) => {
            if size != rest.len() {
                log::warn!("sent {} bytes to [{}] but received {} bytes from [{}]!", size, target_addr, rest.len(), src_addr);
            }
            log::debug!("from [{}] to [{}], size: {}", src_addr, addr, size);
            Ok(())
        }
        Err(err) => Err(err).wrap_err("failed to write data to the upstream connection"),
    }
}