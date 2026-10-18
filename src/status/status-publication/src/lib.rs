//! Status chain observation and publication: reads canonical Status blocks
//! through wallet-libraries' node client, builds generations and publishes them
//! to the distributed Status controller. Composed by the `status-pir` and
//! `enhance-pir-server` applications; serving lives in `status-serving`.

use pir_control::admission;
use status_serving::*;

pub mod publisher;
pub mod source;

mod zakura {
    pub use zakura_server_chain::{NodeClient as ZakuraClient, NodeError as ZakuraError};
}
