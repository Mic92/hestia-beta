use crate::cli::DrainArgs;

pub fn run(args: &DrainArgs) {
    eprintln!(
        "hestia drain: not implemented yet \
         (would ask daemon at {} to upload and commit, timeout {}s)",
        args.socket.display(),
        args.timeout,
    );
}
