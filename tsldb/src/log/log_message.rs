use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// Struct to represent a log message with timestamp.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogMessage {
  /// Timestamp for this log message.
  time: u64,

  /// The actual log message itself.
  message: String,
}

impl LogMessage {
  /// Create a new LogMessage for given time and message.
  pub fn new(time: u64, message: &str) -> Self {
    LogMessage {
      time,
      message: message.to_owned(),
    }
  }

  /// Get the timestamp.
  pub fn get_time(&self) -> u64 {
    self.time
  }

  /// Get the message.
  pub fn get_message(&self) -> &str {
    &self.message
  }
}

impl Default for LogMessage {
  fn default() -> Self {
    Self::new(0, "")
  }
}

impl Ord for LogMessage {
  fn cmp(&self, other: &Self) -> Ordering {
    self.time.cmp(&other.time)
  }
}

impl PartialOrd for LogMessage {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::utils::sync::is_sync;

  #[test]
  fn check_new() {
    is_sync::<LogMessage>();

    let mut log = LogMessage::new(1234, "mymessage");
    assert_eq!(log.get_time(), 1234);
    assert_eq!(log.get_message(), "mymessage");

    log = LogMessage::default();
    assert_eq!(log.get_time(), 0);
    assert_eq!(log.get_message(), "")
  }
}
