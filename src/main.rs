mod archive;
mod cli;
mod command;
mod config;
mod coordinator;
mod dependencies;
mod device_config;
mod error;
mod filesystem;
mod gadget;
mod idle;
mod led;
mod mount;
mod network;
mod snapshot;
mod space;
mod temperature;
mod wifi;

fn main() {
    std::process::exit(cli::main());
}
