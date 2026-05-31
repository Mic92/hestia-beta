use crate::cli::GcArgs;

pub fn run(args: &GcArgs) {
    eprintln!(
        "hestia gc: not implemented yet \
         (would mark/sweep the GHA cache; dry-run: {}, grace: {}d, root-ttl: {}d)",
        args.dry_run, args.grace, args.root_ttl,
    );
}
