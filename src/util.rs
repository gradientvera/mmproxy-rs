use simple_eyre::eyre::{Result, WrapErr, eyre};

use std::{
    collections::HashMap, fs::File, io::{self, Read}, net::{IpAddr, SocketAddr}, str::FromStr, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::Duration
};

use proxy_protocol::{version1 as v1, version2 as v2, ProxyHeader};
use socket2::{Domain, SockRef, Socket, Type};
use tokio::{net::{TcpSocket, TcpStream, UdpSocket}, sync::mpsc, task::JoinHandle};

pub const MAX_DGRAM_SIZE: usize = 65_507;

pub type ConnectionsHashMap = HashMap<SocketAddr, (Arc<UdpProxyConn>, JoinHandle<()>)>;

// this is returned from `util::parse_proxy_protocol_header` function
pub type ProxyProtocolResult<'a> = io::Result<(Option<(SocketAddr, SocketAddr)>, &'a [u8], i32)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Tcp,
    Udp,
}

impl Default for Protocol {
    fn default() -> Self {
        Self::Tcp
    }
}

pub fn check_origin_allowed(addr: &IpAddr, subnets: &[cidr::IpCidr]) -> bool {
    for net in subnets.iter() {
        if net.contains(addr) {
            return true;
        }
    }

    false
}

pub fn parse_allowed_subnets(path: &str) -> io::Result<Vec<cidr::IpCidr>> {
    let mut data = Vec::new();
    let mut file = File::open(path)?;

    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    for line in contents.lines() {
        match cidr::IpCidr::from_str(line) {
            Ok(cidr) => data.push(cidr),
            Err(why) => {
                return Err(io::Error::new(io::ErrorKind::Other, why));
            }
        }
    }

    Ok(data)
}

fn setup_socket_mmproxy(socket_ref: &SockRef, src: SocketAddr, mark: u32) -> Result<()> {
    // needs CAP_NET_ADMIN
    socket_ref
        .set_ip_transparent(true)
        .wrap_err("failed to set ip transparent on the upstream socket")?;
    socket_ref
        .set_nonblocking(true)
        .wrap_err("failed to set nonblocking on the upstream socket")?;
    socket_ref
        .set_reuse_address(true)
        .wrap_err("failed to set reuse address on the upstream socket")?;
    socket_ref
        .set_mark(mark)
        .wrap_err("failed to set mark on the upstream socket")?;
    socket_ref
        .bind(&src.into())
        .wrap_err("failed to set source address for the upstream socket")?;

    Ok(())
}

fn setup_socket_reverse_proxy(socket_ref: &SockRef) -> Result<()> {
    socket_ref
        .set_nonblocking(true)
        .wrap_err("failed to set nonblocking on the upstream socket")?;
    socket_ref
        .set_reuse_address(true)
        .wrap_err("failed to set reuse address on the upstream socket")?;

    Ok(())
}

pub async fn tcp_create_upstream_conn(
    src: SocketAddr,
    target: SocketAddr,
    mark: u32,
) -> Result<TcpStream> {
    let socket = match src {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    };
    let socket = socket.wrap_err("failed to create the upstream socket")?;
    let socket_ref = SockRef::from(&socket);

    socket_ref
        .set_nodelay(true)
        .wrap_err("failed to set nodelay on the upstream socket")?;
    setup_socket_mmproxy(&socket_ref, src, mark)?;

    socket
        .connect(target)
        .await
        .wrap_err("failed to connect to the upstream server")
}

pub async fn udp_create_upstream_conn(
    src: SocketAddr,
    target: SocketAddr,
    mark: u32,
) -> Result<UdpSocket> {
    let socket = match src {
        SocketAddr::V4(_) => Socket::new(Domain::IPV4, Type::DGRAM, None),
        SocketAddr::V6(_) => Socket::new(Domain::IPV6, Type::DGRAM, None),
    };
    let socket = socket.wrap_err("failed to create upstream socket")?;

    setup_socket_mmproxy(&SockRef::from(&socket), src, mark)?;
    let udp_socket = UdpSocket::from_std(socket.into())
        .wrap_err("failed to cast socket2 socket to tokio socket")?;

    udp_socket
        .connect(target)
        .await
        .wrap_err("failed to connecto to the upstream server")?;

    Ok(udp_socket)
}

pub async fn udp_create_reverse_proxy_conn(
    target: SocketAddr
) -> Result<UdpSocket> {
    let socket = match target {
        SocketAddr::V4(_) => Socket::new(Domain::IPV4, Type::DGRAM, None),
        SocketAddr::V6(_) => Socket::new(Domain::IPV6, Type::DGRAM, None),
    };
    let socket = socket.wrap_err("failed to create upstream socket")?;

    setup_socket_reverse_proxy(&SockRef::from(&socket))?;
    let udp_socket = UdpSocket::from_std(socket.into())
        .wrap_err("failed to cast socket2 socket to tokio socket")?;

    udp_socket
        .connect(target)
        .await
        .wrap_err("failed to connecto to the upstream server")?;

    Ok(udp_socket)
}

// TODO: revise this
pub fn parse_proxy_protocol_header(mut buffer: &[u8]) -> ProxyProtocolResult {
    match proxy_protocol::parse(&mut buffer) {
        Ok(result) => match result {
            ProxyHeader::Version1 { addresses } => match addresses {
                v1::ProxyAddresses::Unknown => Ok((None, buffer, 1)),
                v1::ProxyAddresses::Ipv4 {
                    source,
                    destination,
                } => Ok((
                    Some((SocketAddr::V4(source), SocketAddr::V4(destination))),
                    buffer,
                    1,
                )),
                v1::ProxyAddresses::Ipv6 {
                    source,
                    destination,
                } => Ok((
                    Some((SocketAddr::V6(source), SocketAddr::V6(destination))),
                    buffer,
                    1,
                )),
            },
            ProxyHeader::Version2 { addresses, .. } => match addresses {
                v2::ProxyAddresses::Unspec => Ok((None, buffer, 2)),
                v2::ProxyAddresses::Ipv4 {
                    source,
                    destination,
                } => Ok((
                    Some((SocketAddr::V4(source), SocketAddr::V4(destination))),
                    buffer,
                    2,
                )),
                v2::ProxyAddresses::Ipv6 {
                    source,
                    destination,
                } => Ok((
                    Some((SocketAddr::V6(source), SocketAddr::V6(destination))),
                    buffer,
                    2,
                )),
                v2::ProxyAddresses::Unix { .. } => Err(io::Error::new(
                    io::ErrorKind::Other,
                    "unix sockets are not supported",
                )),
            },
            _ => unreachable!(),
        },
        Err(err) => Err(io::Error::new(io::ErrorKind::Other, err)),
    }
}

#[derive(Debug)]
pub struct UdpProxyConn {
    pub sock: UdpSocket,
    pub last_activity: AtomicU64,
}

impl UdpProxyConn {
    pub fn new(sock: UdpSocket) -> Self {
        Self {
            sock,
            last_activity: AtomicU64::new(0),
        }
    }
}


pub async fn udp_dst_to_src(
    addr: SocketAddr,
    src_addr: SocketAddr,
    src: Arc<UdpSocket>,
    dst: Arc<UdpProxyConn>,
) -> Result<()> {
    let mut buffer = [0u8; MAX_DGRAM_SIZE];

    loop {
        let read_bytes = dst.sock.recv(&mut buffer).await?;
        let sent_bytes = src.send_to(&buffer[..read_bytes], addr).await?;
        if sent_bytes == 0 {
            return Err(eyre!("couldn't sent anything to downstream"));
        }
        log::debug!("from [{}] to [{}], size: {}", addr, src_addr, sent_bytes);

        dst.last_activity.fetch_add(1, Ordering::SeqCst);
    }
}

pub async fn udp_close_after_inactivity(
    addr: SocketAddr,
    close_after: Duration,
    tx: mpsc::Sender<SocketAddr>,
    dst: Arc<UdpProxyConn>,
) {
    let mut last_activity = dst.last_activity.load(Ordering::SeqCst);
    loop {
        tokio::time::sleep(close_after).await;
        if dst.last_activity.load(Ordering::SeqCst) == last_activity {
            break;
        }
        last_activity = dst.last_activity.load(Ordering::SeqCst);
    }

    if let Err(why) = tx.send(addr).await {
        log::error!("couldn't send the close command to conn channel: {why}");
    }
}
