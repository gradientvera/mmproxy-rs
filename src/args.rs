
use crate::util::{self, Protocol};
use std::{net::SocketAddr, num::NonZero, thread::available_parallelism, time::Duration};

#[derive(Debug, Clone)]
pub enum Command {
    mmproxy(ArgsMmproxy),
    reverseproxy(ArgsReverseProxy)
}

argwerk::define! {
    #[usage = "mmproxy [-h]"]
    #[derive(Clone)]
    pub struct Args {
        pub help: bool,
        #[required = "must specify either \"mmproxy\" or \"reverseproxy\""]
        pub command: Command
    }
    /// Prints the help string.
    ["-h" | "--help"] => {
        println!("{}", Args::help());
        help = true;
    }
    /// Subcommand for mmproxy mode.
    ["mmproxy", #[rest(os)] args] if command.is_none() => {
        command = Some(Command::mmproxy(ArgsMmproxy::parse(args)?));
    }
    /// Subcommand for reverse proxy mode.
    ["reverseproxy", #[rest(os)] args] if command.is_none() => {
        command = Some(Command::reverseproxy(ArgsReverseProxy::parse(args)?));
    }
}

argwerk::define! {
    #[usage = "mmproxy mmproxy [-h] [options]"]
    #[derive(Clone)]
    pub struct ArgsMmproxy {
        pub help: bool = false,
        pub ipv4_fwd: SocketAddr = "127.0.0.1:443".parse().unwrap(),
        pub ipv6_fwd: SocketAddr = "[::1]:443".parse().unwrap(),
        pub allowed_subnets: Option<Vec<cidr::IpCidr>> = None,
        pub close_after: Duration = Duration::from_secs(60),
        pub mark: u32 = 0,
        pub listen_addr: SocketAddr = "[::]:8443".parse().unwrap(),
        pub listeners: u32 = 0,
        pub protocol: Protocol = Protocol::Tcp
    }
    /// Prints the help string.
    ["-h" | "--help"] => {
        println!("{}", ArgsMmproxy::help());
        help = true;
    }
    /// Address to which IPv4 traffic will be forwarded to. (default: "127.0.0.1:443")
    ["-4" | "--ipv4", addr] => {
        ipv4_fwd = addr.parse()?;
    }
    /// Address to which IPv6 traffic will be forwarded to. (default: "[::1]:443")
    ["-6" | "--ipv6", addr] => {
        ipv6_fwd = addr.parse()?;
    }
    /// Path to a file that contains allowed subnets of the proxy servers.
    ["-a" | "--allowed-subnets", path] => {
        let ret = util::parse_allowed_subnets(&path)?;
        allowed_subnets = if !ret.is_empty() { Some (ret) } else { None }
    }
    /// Number of seconds after which UDP socket will be cleaned up. (default: 60)
    ["-c" | "--close-after", n] => {
        close_after = Duration::from_secs(str::parse(&n)?);
    }
    /// Address the proxy listens on. (default: "[::]:8443")
    ["-l" | "--listen-addr", string] => {
        listen_addr = string.parse()?;
    }
    /// Number of listener sockets that will be opened for the listen address. 0 to automatically choose to a reasonable number. (Linux 3.9+) (default: 0)
    ["--listeners", n] => {
        listeners = str::parse(&n)?;
        if listeners == 0 {
            listeners = available_parallelism().unwrap_or(NonZero::new(1).unwrap()).get() as u32;
        }
    }
    /// Protocol that will be proxied: tcp, udp. (default: tcp)
    ["-p" | "--protocol", p] => {
        protocol = match &p.to_lowercase()[..] {
            "tcp" => Protocol::Tcp,
            "udp" => Protocol::Udp,
            _ => return Err(format!("invalid protocol value: {p}").into()),
        };
    }
    /// The mark that will be set on outbound packets. (default: 0)
    ["-m" | "--mark", n] => {
        mark = str::parse::<u32>(&n)?;
    }
}

argwerk::define! {
    #[usage = "mmproxy reverseproxy [-h] [options]"]
    #[derive(Clone)]
    pub struct ArgsReverseProxy {
        pub help: bool = false,
        pub listen_addr: SocketAddr = "[::]:443".parse().unwrap(),
        #[required = "must specify an address to reverse proxy to"]
        pub forward_addr: SocketAddr,
        pub listeners: u32 = 0,
        pub close_after: Duration = Duration::from_secs(60),
        pub protocol: Protocol = Protocol::Tcp
    }
    /// Prints the help string.
    ["-h" | "--help"] => {
        println!("{}", ArgsReverseProxy::help());
        help = true;
    }
    /// Address the proxy listens on. "[::]" for dual-stack. (default: "[::]:443")
    ["-l" | "--listen-addr", string] => {
        listen_addr = string.parse()?;
    }
    /// Address the proxy forwards to. (example: "10.0.0.2:444")
    ["-f" | "--forward-addr", string] => {
        forward_addr = Some(string.parse()?);
    }
    /// Number of listener sockets that will be opened for the listen address. 0 to automatically choose to a reasonable number. (Linux 3.9+) (default: 0)
    ["--listeners", n] => {
        listeners = str::parse(&n)?;
        if listeners == 0 {
            listeners = available_parallelism().unwrap_or(NonZero::new(1).unwrap()).get() as u32;
        }
    }
    /// Number of seconds after which UDP socket will be cleaned up. (default: 60)
    ["-c" | "--close-after", n] => {
        close_after = Duration::from_secs(str::parse(&n)?);
    }
    /// Protocol that will be proxied: tcp, udp. (default: tcp)
    ["-p" | "--protocol", p] => {
        protocol = match &p.to_lowercase()[..] {
            "tcp" => Protocol::Tcp,
            "udp" => Protocol::Udp,
            _ => return Err(format!("invalid protocol value: {p}").into()),
        };
    }
}

pub fn parse_args() -> Result<Args, argwerk::Error> {
    match Args::args() {
        Ok(args) => {
            if args.help {
                std::process::exit(1);
            };
            match &args.command {
                Command::mmproxy(args_mmproxy) => if args_mmproxy.help { std::process::exit(1) },
                Command::reverseproxy(args_reverse_proxy) => if args_reverse_proxy.help { std::process::exit(1) },
            };
            Ok(args)
        }
        Err(err) => Err(err),
    }
}
