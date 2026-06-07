mod archive;
mod cli;
mod command;
mod config;
mod coordinator;
mod dependencies;
mod error;
mod filesystem;
mod gadget;
mod idle;
mod led;
mod mount;
mod snapshot;
mod space;
mod temperature;

fn main() {
    std::process::exit(cli::main());
}
