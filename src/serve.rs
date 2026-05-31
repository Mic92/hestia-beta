use crate::cli::ServeArgs;

pub fn run(args: &ServeArgs) {
    eprintln!(
        "hestia serve: not implemented yet \
         (would listen on {} for substitution requests, \
         accept hook connections on {}, idle-exit: {:?})",
        args.listen,
        args.socket.display(),
        args.idle_exit,
    );
}
