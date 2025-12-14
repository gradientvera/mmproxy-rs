use simple_eyre::eyre::{Result, WrapErr, eyre};

use std::{
    collections::HashMap, fs::File, io::{self, Read}, net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6}, str::FromStr, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::Duration
};

use proxy_protocol::{ProxyHeader, version1 as v1, version2::{self as v2, ProxyAddresses}};
use socket2::{Domain, SockRef, Socket, Type};
use tokio::{net::{TcpSocket, TcpStream, UdpSocket}, sync::mpsc};

pub const MAX_DGRAM_SIZE: usize = 65_507;

pub type ConnectionsHashMap = HashMap<SocketAddr, Arc<UdpSocket>>;

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
        .set_reuse_port(true)
        .wrap_err("failed to set reuse port on the upstream socket")?;
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
    socket_ref
        .set_reuse_port(true)
        .wrap_err("failed to set reuse port on the upstream socket")?;

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
) -> Result<Arc<UdpSocket>> {
    let domain = Domain::for_address(target);
    let socket = Socket::new(domain, Type::DGRAM, None)
        .wrap_err("failed to create upstream socket")?;

    setup_socket_mmproxy(&SockRef::from(&socket), src, mark)?;
    let udp_socket = UdpSocket::from_std(socket.into())
        .wrap_err("failed to cast socket2 socket to tokio socket")?;

    udp_socket
        .connect(target)
        .await
        .wrap_err("failed to connect to the upstream server")?;

    Ok(Arc::new(udp_socket))
}

pub async fn udp_create_reverse_proxy_conn(
    target: SocketAddr
) -> Result<Arc<UdpSocket>> {
    let domain = Domain::for_address(target);
    let socket = Socket::new(domain, Type::DGRAM, None)
        .wrap_err("failed to create upstream socket")?;

    setup_socket_reverse_proxy(&SockRef::from(&socket))?;
    let udp_socket = UdpSocket::from_std(socket.into())
        .wrap_err("failed to cast socket2 socket to tokio socket")?;

    udp_socket
        .connect(target)
        .await
        .wrap_err("failed to connect to the upstream server")?;

    Ok(Arc::new(udp_socket))
}

// TODO: revise this
pub fn parse_proxy_protocol_header(mut buffer: &[u8]) -> ProxyProtocolResult<'_> {
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

pub fn make_proxy_protocol_addresses(src_addr: SocketAddr, forward_addr: SocketAddr) -> ProxyAddresses {
    match src_addr {
        SocketAddr::V4(src_addr4) => {
            ProxyAddresses::Ipv4 {
                source: src_addr4,
                destination: proxy_protocol_get_dest_v4(forward_addr)
            }
        },
        SocketAddr::V6(src_addr6) => {
            // Ipv4-mapped
            if let Some(mapped) = src_addr6.ip().to_ipv4_mapped() {
                ProxyAddresses::Ipv4 {
                    source: SocketAddrV4::new(mapped, src_addr.port()),
                    destination: proxy_protocol_get_dest_v4(forward_addr)
                }
            } else {
                ProxyAddresses::Ipv6 {
                    source: src_addr6,
                    destination: proxy_protocol_get_dest_v6(forward_addr)
                }
            }
        },
    }
}

fn proxy_protocol_get_dest_v4(addr: SocketAddr) -> SocketAddrV4 {
    match addr {
        SocketAddr::V4(v4) => v4,
        SocketAddr::V6(v6) => SocketAddrV4::new(v6.ip().to_ipv4_mapped().unwrap_or(Ipv4Addr::UNSPECIFIED), v6.port()),
    }
}

fn proxy_protocol_get_dest_v6(addr: SocketAddr) -> SocketAddrV6 {
    match addr {
        SocketAddr::V4(v4) => SocketAddrV6::new(v4.ip().to_ipv6_mapped(), v4.port(), 0, 0),
        SocketAddr::V6(v6) => v6,
    }
}

pub async fn bind_udp_socket(listen_addr: SocketAddr) -> Result<Arc<UdpSocket>> {
    let domain = Domain::for_address(listen_addr);
    let socket = socket2::Socket::new(domain, Type::DGRAM, Some(socket2::Protocol::UDP))
        .wrap_err("failed to create new socket")?;

    socket
        .set_reuse_address(true)
        .wrap_err("failed to set reuse address on listener socket")?;

    socket
        .set_reuse_port(true)
        .wrap_err("failed to set reuse port on listener socket")?;

    if domain == Domain::IPV6 && listen_addr.ip().is_unspecified() {
        // dual-stack
        socket
            .set_only_v6(false)
            .wrap_err("failed to set only v6 to false on listener socket")?;
    }

    socket.bind(&listen_addr.into())
        .wrap_err(format!("failed to bind socket to address {}", listen_addr))?;

    let socket = UdpSocket::from_std(socket.into())
        .wrap_err("failed to create tokio socket")?;

    Ok(Arc::new(socket))
}