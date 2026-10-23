//! Table identities and geometry shared by history and transaction detail.
use transparent_shard::layout::Geometry;
use zakura_pir_protocol::transparent::shard as transparent_shard;

/// Which private table a query addresses: a history shard's directory or
/// pages, or a txid display bucket's table of fixed-size entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Table {
    Directory,
    Pages,
    TxDirectory,
}

impl Table {
    /// The tables a history shard has.
    pub const HISTORY: [Self; 2] = [Self::Directory, Self::Pages];
    pub fn as_str(self) -> &'static str {
        match self {
            Table::Directory => "directory",
            Table::TxDirectory => "txdirectory",
            Table::Pages => "pages",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "directory" => Some(Table::Directory),
            "pages" => Some(Table::Pages),
            "txdirectory" => Some(Table::TxDirectory),
            _ => None,
        }
    }

    /// Rows in one segment of this table, at `geometry`.
    pub fn rows(self, geometry: &Geometry) -> u64 {
        match self {
            Table::Directory | Table::TxDirectory => geometry.directory_rows,
            Table::Pages => geometry.page_rows,
        }
    }

    /// Bytes in one row of this table, at `geometry`.
    pub fn row_bytes(self, geometry: &Geometry) -> u32 {
        match self {
            Table::Directory | Table::TxDirectory => geometry.directory_row_bytes as u32,
            Table::Pages => geometry.page_row_bytes as u32,
        }
    }
}

/// A table one history query addresses: a directory shard's directory, or
/// its page shard `j`, served as the table `pages-<j>` of the directory
/// shard's revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableRef {
    Directory,
    Pages(u32),
}

impl TableRef {
    /// The table a request path names, or `None`. One table has one name.
    pub fn parse(name: &str) -> Option<Self> {
        if name == Table::Directory.as_str() {
            return Some(Self::Directory);
        }
        transparent_shard::manifest::parse_pages_table(name).map(Self::Pages)
    }

    /// The name on the wire and in the query binding.
    pub fn name(self) -> String {
        match self {
            Self::Directory => Table::Directory.as_str().to_string(),
            Self::Pages(child) => transparent_shard::manifest::pages_table(child),
        }
    }

    /// The kind, which decides the native parameters and setup seed: every
    /// page shard of a geometry shares one parameter set.
    pub fn kind(self) -> Table {
        match self {
            Self::Directory => Table::Directory,
            Self::Pages(_) => Table::Pages,
        }
    }
}

/// The published seed a table's public query setup is derived from; see
/// [`transparent_shard::display::setup_seed_named`], which clients share.
pub fn setup_seed(geometry: &Geometry, table: Table) -> u64 {
    transparent_shard::display::setup_seed_named(geometry.name, table.as_str())
}
