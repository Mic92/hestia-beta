mod cli;
mod drain;
mod gc;
mod hook;
mod serve;

use clap::Parser;

use crate::cli::{Cli, Command};

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => serve::run(&args),
        Command::Hook(args) => hook::run(&args),
        Command::Drain(args) => drain::run(&args),
        Command::Gc(args) => gc::run(&args),
    }
}
