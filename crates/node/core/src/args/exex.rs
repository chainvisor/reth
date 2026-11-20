//! clap [Args](clap::Args) for Execution Extensions (ExEx).

use clap::Args;

/// Parameters for the ExEx manager.
#[derive(Debug, Clone, Copy, Default, Args, PartialEq, Eq)]
#[command(next_help_heading = "ExEx")]
pub struct ExExArgs {
    /// Number of blocks in the ExEx write-ahead log above which the node warns that the WAL is
    /// not getting cleared.
    ///
    /// The WAL keeps every block above the lower of the finalized block and the lowest ExEx
    /// finished height, so on a healthy chain it holds about the blocks between finalized and
    /// head. Chains with short block times need a higher value than the default of 128, which
    /// suits 12-second blocks.
    #[arg(long = "exex.wal-blocks-warning", value_name = "BLOCKS", help_heading = "ExEx")]
    pub wal_blocks_warning: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// A helper type to parse Args more easily
    #[derive(Parser)]
    struct CommandParser<T: Args> {
        #[command(flatten)]
        args: T,
    }

    #[test]
    fn exex_args_default_sanity_test() {
        let args = CommandParser::<ExExArgs>::parse_from(["reth"]).args;
        assert_eq!(args, ExExArgs::default());
    }

    #[test]
    fn exex_args_wal_blocks_warning() {
        let args =
            CommandParser::<ExExArgs>::parse_from(["reth", "--exex.wal-blocks-warning", "10000"])
                .args;
        assert_eq!(args.wal_blocks_warning, Some(10_000));
    }
}
