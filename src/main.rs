mod args;
mod listener;
mod pipe;
mod util;

use env_logger::{Env, DEFAULT_FILTER_ENV};
use listener::{tcp_mmproxy, udp_mmproxy, udp_reverseproxy};

#[tokio::main]
async fn main() {
    env_logger::init_from_env(Env::default().filter_or(DEFAULT_FILTER_ENV, "info"));

    let args = match args::parse_args() {
        Ok(args) => args,
        Err(why) => {
            log::error!("{why}");
            return;
        }
    };

    let ret = match args.command {
        args::Command::mmproxy(args_mmproxy) => {
            match args_mmproxy.protocol {
                util::Protocol::Tcp => tcp_mmproxy::listen(args_mmproxy).await,
                util::Protocol::Udp => udp_mmproxy::listen(args_mmproxy).await,
            }
        },
        args::Command::reverseproxy(args_reverseproxy) => {
            match args_reverseproxy.protocol {
                util::Protocol::Tcp => todo!("Not supported yet! Use nginx or something"),
                util::Protocol::Udp => udp_reverseproxy::listen(args_reverseproxy).await,
            }
        }
    };

    if let Err(why) = ret {
        log::error!("{why:#}");
    }
}
