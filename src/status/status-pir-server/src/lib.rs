//! The Status PIR application: the `status-pir` command lines over Status
//! serving (`status-serving`) and chain observation and publication
//! (`status-publication`).

use enhance_serving::{control, coordinator};
use status_serving as status;

pub mod cli;

mod zakura {
    pub use zakura_server_chain::NodeClient as ZakuraClient;
}
