use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// Default address the substituter listens on.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:37515";

/// Default unix socket path for the post-build-hook listener.
pub const DEFAULT_SOCKET: &str = "/tmp/hestia/hook.sock";

#[derive(Parser, Debug)]
#[command(name = "hestia", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the per-job daemon: hook listener + substituter HTTP server.
    Serve(ServeArgs),
    /// Send $OUT_PATHS from a Nix post-build-hook to the daemon.
    Hook(HookArgs),
    /// Tell the daemon to upload pending paths and commit the manifest.
    Drain(DrainArgs),
    /// Mark/sweep garbage collection over the GHA cache (cron workflow).
    Gc(GcArgs),
}

#[derive(Args, Debug)]
pub struct ServeArgs {
    /// Unix socket path for the post-build-hook listener.
    #[arg(long, default_value = DEFAULT_SOCKET)]
    pub socket: PathBuf,

    /// Address for the substituter HTTP server.
    #[arg(long, default_value = DEFAULT_LISTEN)]
    pub listen: String,

    /// Drain and exit after this many seconds without activity.
    #[arg(long, value_name = "SECONDS")]
    pub idle_exit: Option<u64>,
}

#[derive(Args, Debug)]
pub struct HookArgs {
    /// Unix socket path of the running daemon.
    #[arg(long, default_value = DEFAULT_SOCKET)]
    pub socket: PathBuf,

    /// Store paths to register; falls back to $OUT_PATHS if empty.
    #[arg(value_name = "PATH")]
    pub paths: Vec<PathBuf>,
}

#[derive(Args, Debug)]
pub struct DrainArgs {
    /// Unix socket path of the running daemon.
    #[arg(long, default_value = DEFAULT_SOCKET)]
    pub socket: PathBuf,

    /// Maximum time to wait for the upload to finish, in seconds.
    #[arg(long, value_name = "SECONDS", default_value_t = 300)]
    pub timeout: u64,
}

#[derive(Args, Debug)]
pub struct GcArgs {
    /// Plan only; do not upload, repack, or delete anything.
    #[arg(long)]
    pub dry_run: bool,

    /// Grace period in days before unreachable paths become garbage.
    #[arg(long, value_name = "DAYS", default_value_t = 7)]
    pub grace: u64,

    /// Roots not updated for this many days are dropped.
    #[arg(long, value_name = "DAYS", default_value_t = 30)]
    pub root_ttl: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("arguments should parse")
    }

    #[test]
    fn serve_defaults_and_flags() {
        let cli = parse(&["hestia", "serve"]);
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        assert_eq!(args.listen, DEFAULT_LISTEN);
        assert_eq!(args.socket, PathBuf::from(DEFAULT_SOCKET));
        assert_eq!(args.idle_exit, None);

        let cli = parse(&[
            "hestia",
            "serve",
            "--socket",
            "/run/hestia.sock",
            "--listen",
            "0.0.0.0:8080",
            "--idle-exit",
            "120",
        ]);
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        assert_eq!(args.socket, PathBuf::from("/run/hestia.sock"));
        assert_eq!(args.listen, "0.0.0.0:8080");
        assert_eq!(args.idle_exit, Some(120));
    }

    #[test]
    fn hook_paths_and_socket() {
        let cli = parse(&["hestia", "hook"]);
        let Command::Hook(args) = cli.command else {
            panic!("expected hook");
        };
        assert!(args.paths.is_empty());
        assert_eq!(args.socket, PathBuf::from(DEFAULT_SOCKET));

        let cli = parse(&[
            "hestia",
            "hook",
            "--socket",
            "/run/hestia.sock",
            "/nix/store/aaaa-foo",
            "/nix/store/bbbb-bar",
        ]);
        let Command::Hook(args) = cli.command else {
            panic!("expected hook");
        };
        assert_eq!(args.socket, PathBuf::from("/run/hestia.sock"));
        assert_eq!(
            args.paths,
            vec![
                PathBuf::from("/nix/store/aaaa-foo"),
                PathBuf::from("/nix/store/bbbb-bar"),
            ]
        );
    }

    #[test]
    fn drain_timeout() {
        let cli = parse(&["hestia", "drain"]);
        let Command::Drain(args) = cli.command else {
            panic!("expected drain");
        };
        assert_eq!(args.timeout, 300);

        let cli = parse(&["hestia", "drain", "--timeout", "60"]);
        let Command::Drain(args) = cli.command else {
            panic!("expected drain");
        };
        assert_eq!(args.timeout, 60);
    }

    #[test]
    fn gc_flags() {
        let cli = parse(&["hestia", "gc"]);
        let Command::Gc(args) = cli.command else {
            panic!("expected gc");
        };
        assert!(!args.dry_run);
        assert_eq!(args.grace, 7);
        assert_eq!(args.root_ttl, 30);

        let cli = parse(&[
            "hestia",
            "gc",
            "--dry-run",
            "--grace",
            "14",
            "--root-ttl",
            "60",
        ]);
        let Command::Gc(args) = cli.command else {
            panic!("expected gc");
        };
        assert!(args.dry_run);
        assert_eq!(args.grace, 14);
        assert_eq!(args.root_ttl, 60);
    }

    #[test]
    fn unknown_subcommand_rejected() {
        assert!(Cli::try_parse_from(["hestia", "frobnicate"]).is_err());
    }
}
