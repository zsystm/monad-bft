use std::{
    fs::File,
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
    time::Duration,
};

use account::PrivateKey;
use alloy_json_rpc::RpcReturn;
use alloy_rlp::Encodable;
use alloy_rpc_client::{ClientBuilder, ReqwestClient, RpcCall};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolEventInterface, Word};
use clap::{command, Parser};
use eyre::{Context, ContextCompat, Result};
use reth_primitives::{
    hex::{encode, FromHex},
    keccak256, AccessList, Address, Bytes, Transaction, TransactionKind, TransactionSigned,
    TxEip1559, TxHash, U256, U64,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::sleep;
use url::Url;

mod account;

#[derive(Debug, Parser)]
#[command( about, long_about = None)]
pub struct EthTxGenCli {
    // #[arg(long, default_value = "http://localhost:8080")]
    #[arg(long, default_value = "http://127.0.0.1:8545")] // anvil
    pub rpc_url: Url,

    #[arg(long, default_value = "./ERC20.json")]
    pub path_to_contract: PathBuf,

    #[arg(
        long,
        // anvil
        default_value = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )]
    pub private_key: String,

    #[arg(long, default_value = "false")]
    pub use_receipt: bool,
}

#[derive(Deserialize, Debug)]
#[repr(transparent)]
pub struct NonceResult(pub String);

impl NonceResult {
    fn nonce(self) -> Result<u64> {
        u64::from_str_radix(
            self.0
                .strip_prefix("0x")
                .context("nonce string always has 0x prefix")?,
            16,
        )
        .context("nonce string is valid u64")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = EthTxGenCli::parse();
    let client: ReqwestClient = ClientBuilder::default().http(args.rpc_url);

    let output_file = File::open(args.path_to_contract)?;
    let json = serde_json::from_reader(output_file)?;
    let bytecode: String =
        extract_bytecode_string(json).context("Couldn't parse bytecode from file")?;

    let (addr, private_key) = PrivateKey::new(args.private_key);
    println!("Using public key: {}", addr.to_string());
    let nonce = client
        .request::<_, NonceResult>(
            "eth_getTransactionCount",
            [addr.to_string(), "latest".to_string()],
        )
        .await?
        .nonce()?;

    println!("Nonce: {nonce}");
    // println!("Bytecode: {}", bytecode);

    let mut input = Bytes::from_hex(bytecode)?.to_vec();

    let tx = Transaction::Eip1559(TxEip1559 {
        chain_id: 41454,
        nonce,
        gas_limit: 800_000,
        max_fee_per_gas: 10_000,
        max_priority_fee_per_gas: 10,
        to: TransactionKind::Create,
        value: U256::ZERO.into(),
        access_list: AccessList::default(),
        input: input.into(),
    });

    let contract_address = calculate_contract_addr(&addr, nonce);
    println!("Contract Address: {contract_address}");

    // keccak256(rlp.enc)

    // let gas_estimate: u64 = client
    //     .request::<_, U64>("eth_estimateGas", [&tx])
    //     .await?
    //     .to();

    // println!("gas estimate: {}", gas_estimate);

    let sig = private_key.sign_transaction(&tx)?;
    let tx = TransactionSigned::from_transaction_and_signature(tx, sig);

    let tx_hash = client
        .request::<_, String>("eth_sendRawTransaction", [tx.envelope_encoded()])
        .await?;

    println!("Tx Hash: {tx_hash}");

    if args.use_receipt {
        let rx = loop {
            let rx = client
                .request::<_, Value>("eth_getTransactionReceipt", [&tx_hash])
                .await?;
            if !rx.is_null() {
                break rx;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        println!(
            "Construct: status {}, rx: {}",
            &rx["status"],
            serde_json::to_string(&rx)?
        );
    } else {
        sleep(Duration::from_secs(1)).await;
    }
    // let contract_address = rx
    //     .get("contractAddress")
    //     .context("Failed to parse contract address from receipt")?
    //     .as_str()
    //     .unwrap();
    // let contract_address = Address::from_hex(contract_address)?;

    // try minting
    let nonce = client
        .request::<_, U64>(
            "eth_getTransactionCount",
            [addr.to_string(), "latest".to_string()],
        )
        .await?
        .to::<u64>();

    let input = ERC20::mintCall {}.abi_encode();

    let tx = Transaction::Eip1559(TxEip1559 {
        chain_id: 41454,
        nonce,
        gas_limit: 4_000_000,
        max_fee_per_gas: 100_000,
        max_priority_fee_per_gas: 10,
        to: TransactionKind::Call(contract_address),
        value: U256::ZERO.into(),
        access_list: AccessList::default(),
        input: input.into(),
    });

    // let gas_estimate: u64 = client
    //     .request::<_, U64>("eth_estimateGas", [&tx])
    //     .await?
    //     .to();

    // println!("gas estimate: {}", gas_estimate);

    let sig = private_key.sign_transaction(&tx)?;
    let tx = TransactionSigned::from_transaction_and_signature(tx, sig);

    let tx_hash = client
        .request::<_, String>("eth_sendRawTransaction", [tx.envelope_encoded()])
        .await?;

    // let rx = loop {
    //     let rx = client
    //         .request::<_, Value>("eth_getTransactionReceipt", [&tx_hash])
    //         .await?;
    //     if !rx.is_null() {
    //         break rx;
    //     }
    //     tokio::time::sleep(Duration::from_millis(200)).await;
    // };
    // println!(
    //     "Mint: status: {}, rx: {}",
    //     rx["status"],
    //     serde_json::to_string(&rx)?
    // );

    // transfer!
    let contract = Contract {
        addr: contract_address,
        client: client.clone(),
    };

    let recipient = Address::from_str("0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f").unwrap();

    {
        let (method, params) = contract.make_tx_req(
            ERC20::transferCall {
                recipient: recipient.clone(),
                amount: U256::from(100),
            },
            nonce + 1,
            private_key,
        );

        let tx_hash: TxHash = client.request(method, params).await?;
        sleep(Duration::from_secs(1)).await;
        // let rx = loop {
        //     let rx = client
        //         .request::<_, Value>("eth_getTransactionReceipt", [&tx_hash])
        //         .await?;
        //     if !rx.is_null() {
        //         break rx;
        //     }
        //     tokio::time::sleep(Duration::from_millis(200)).await;
        // };
        // println!(
        //     "Transfer: status: {}, rx: {}",
        //     rx["status"],
        //     serde_json::to_string_pretty(&rx)?
        // );

        // let logs = &rx["logs"][0];
        // dbg!(&logs["topics"]);
        // let topics = serde_json::from_value::<Vec<String>>(logs["topics"].clone())?;
        // let topics = topics
        //     .into_iter()
        //     .map(Word::from_hex)
        //     .collect::<std::result::Result<Vec<_>, _>>()?;
        // let data = logs["data"].as_str().unwrap();
        // let data = Bytes::from_hex(data)?;
        // let log = ERC20::ERC20Events::decode_raw_log(&topics, &data, false)?;
        // match log {
        //     ERC20::ERC20Events::Transfer(transfer) => {
        //         println!("Transfer Event: {}", &transfer.value)
        //     }
        //     ERC20::ERC20Events::Approval(approval) => todo!(),
        // }
    }

    // total supply

    {
        let call = json!({
            "to": contract_address,
            "data": ERC20::totalSupplyCall {}.abi_encode()
        });

        let supply: U256 = client.request("eth_call", [call]).await?;
        println!("Supply {supply}");
    }

    {
        // balanceOf
        let balance: U256 = contract
            .call(ERC20::balanceOfCall {
                account: addr.clone(),
            })
            .await?;

        println!("Balance {balance}");
    }

    Ok(())
}

pub fn calculate_contract_addr(deployer: &Address, nonce: u64) -> Address {
    let mut out = Vec::new();
    let enc: [&dyn Encodable; 2] = [&deployer, &nonce];
    alloy_rlp::encode_list::<_, dyn Encodable>(&enc, &mut out);
    let hash = keccak256(out);
    let (_, contract_address) = hash.as_slice().split_at(12);
    Address::from_slice(contract_address)
}

struct Contract {
    pub addr: Address,
    pub client: ReqwestClient,
}

impl Contract {
    pub async fn call<R: RpcReturn, T: alloy_sol_types::SolCall>(&self, input: T) -> Result<R> {
        let call = json!({
            "to": self.addr,
            "data": input.abi_encode()
        });

        let x = self.client.request::<_, R>("eth_call", [call]).await?;
        Ok(x)
    }

    pub fn make_tx_req<T: alloy_sol_types::SolCall>(
        &self,
        input: T,
        nonce: u64,
        private_key: PrivateKey,
    ) -> (&'static str, [Bytes; 1]) {
        let input = input.abi_encode();

        let tx = Transaction::Eip1559(TxEip1559 {
            chain_id: 41454,
            nonce,
            gas_limit: 400_000,
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            to: TransactionKind::Call(self.addr),
            value: U256::ZERO.into(),
            access_list: AccessList::default(),
            input: input.into(),
        });

        let sig = private_key.sign_transaction(&tx).unwrap();
        let tx = TransactionSigned::from_transaction_and_signature(tx, sig);

        ("eth_sendRawTransaction", [tx.envelope_encoded()])
    }
}

sol! {
pragma solidity ^0.8.13;

contract ERC20 {
    // constructor(string memory _name, string memory _symbol, uint8 _decimals);

    function mint() external;
    function totalSupply() external view returns (uint256);
    function balanceOf(address account) external view returns (uint256);
    function transfer(address recipient, uint256 amount) external returns (bool);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
    function transferFrom(address sender, address recipient, uint256 amount) external returns (bool);

    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);
}}

fn extract_bytecode_string(obj: Value) -> Option<String> {
    obj.get("bytecode")?
        .get("object")?
        .as_str()
        .map(ToOwned::to_owned)
}
