use crate::cli::HookArgs;

pub fn run(args: &HookArgs) {
    // A failing post-build-hook fails the build, so this command must
    // always exit 0, even once it is implemented.
    eprintln!(
        "hestia hook: not implemented yet \
         (would send {} path(s) and $OUT_PATHS to daemon at {})",
        args.paths.len(),
        args.socket.display(),
    );
}
