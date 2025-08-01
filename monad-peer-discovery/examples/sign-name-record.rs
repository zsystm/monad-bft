// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{net::SocketAddrV4, panic, path::PathBuf};

use clap::Parser;
use monad_keystore::keystore::Keystore;
use monad_node_config::MonadNodeConfig;
use monad_peer_discovery::{MonadNameRecord, NameRecord};
use monad_secp::{KeyPair, SecpSignature};

/// Example command to run the following program:
/// sign-name-record -- --address 0.0.0.0:8888 --node-config <...> --keystore-path <...> --password ""
/// sign-name-record -- --address 0.0.0.0:8888 --direct-udp-port 9999 --node-config <...> --keystore-path <...> --password ""
#[derive(Debug, Parser)]
#[command(name = "monad-peer-discovery", about)]
struct Args {
    /// SocketV4 address in format x.x.x.x:<port>
    #[arg(long)]
    address: SocketAddrV4,

    /// Sequence number for the name record
    #[arg(long)]
    self_record_seq_num: Option<u64>,
    
    /// Direct UDP port (optional)
    #[arg(long)]
    direct_udp_port: Option<u16>,

    /// Set the node config path
    #[arg(long)]
    node_config: Option<PathBuf>,

    /// File path to secp keystore json file
    #[arg(long)]
    keystore_path: PathBuf,

    /// Keystore password
    #[arg(long)]
    password: String,
}

fn main() {
    let args = Args::parse();

    let result = Keystore::load_key(&args.keystore_path, &args.password);
    let mut private_key = match result {
        Ok(private_key) => private_key,
        Err(err) => {
            println!("Unable to read private key from keystore file: {:?}", err);
            return;
        }
    };

    let self_record_seq_num = if let Some(node_config_path) = args.node_config {
        let contents =
            std::fs::read_to_string(node_config_path).expect("Failed to read node toml file");
        let node_config: MonadNodeConfig =
            toml::from_str(&contents).expect("Invalid format in node toml file");
        node_config.peer_discovery.self_record_seq_num + 1
    } else {
        args.self_record_seq_num
            .unwrap_or_else(|| panic!("Either node_config or self_record_seq_num must be provided"))
    };
    let self_address = args.address;
    let name_record = NameRecord {
        ip: self_address.ip().clone(),
        tcp_port: self_address.port(),
        udp_port: self_address.port(),
        direct_udp_port: args.direct_udp_port,
        capabilities: 0,
        seq: self_record_seq_num,
    };
    let keypair = KeyPair::from_bytes(private_key.as_mut()).expect("Invalid keypair");
    let signed_name_record: MonadNameRecord<SecpSignature> =
        MonadNameRecord::new(name_record, &keypair);

    println!("self_address = {:?}", self_address.to_string());
    if let Some(direct_udp_port) = args.direct_udp_port {
        println!("direct_udp_port = {}", direct_udp_port);
    }
    println!("self_record_seq_num = {}", self_record_seq_num);
    println!(
        "self_name_record_sig = {:?}",
        hex::encode(signed_name_record.signature.serialize())
    );
}
