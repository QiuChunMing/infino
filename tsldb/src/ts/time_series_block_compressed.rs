use log::error;
use serde::{Deserialize, Serialize};

use crate::ts::time_series_block::TimeSeriesBlock;
use crate::ts::tsutils::compress_data_point_vector;
use crate::utils::custom_serde::rwlock_serde;
use crate::utils::error::TsldbError;
use crate::utils::sync::RwLock;

/// Represents a compressed time series block.
#[derive(Debug, Deserialize, Serialize)]
pub struct TimeSeriesBlockCompressed {
  // Vector of compressed log_message_ids, wrapped in RwLock.
  #[serde(with = "rwlock_serde")]
  data_points_compressed: RwLock<Vec<u8>>,
}

impl TimeSeriesBlockCompressed {
  /// Create an empty block.
  pub fn new() -> Self {
    TimeSeriesBlockCompressed {
      data_points_compressed: RwLock::new(Vec::new()),
    }
  }

  /// Create a block from given compressed data points vector.
  pub fn new_with_data_points_compressed_vec(data_points_compressed_vec: Vec<u8>) -> Self {
    TimeSeriesBlockCompressed {
      data_points_compressed: RwLock::new(data_points_compressed_vec),
    }
  }

  /// Get the compressed vector of data points, wrapped in RwLock.
  pub fn get_data_points_compressed(&self) -> &RwLock<Vec<u8>> {
    &self.data_points_compressed
  }
}

impl PartialEq for TimeSeriesBlockCompressed {
  fn eq(&self, other: &Self) -> bool {
    let data_points_lock = self.data_points_compressed.read().unwrap();
    let other_data_points_lock = other.data_points_compressed.read().unwrap();

    *data_points_lock == *other_data_points_lock
  }
}

impl Eq for TimeSeriesBlockCompressed {}

impl TryFrom<&TimeSeriesBlock> for TimeSeriesBlockCompressed {
  type Error = TsldbError;

  /// Compress the given time series block.
  fn try_from(time_series_block: &TimeSeriesBlock) -> Result<Self, Self::Error> {
    let time_series_data_points = &*time_series_block
      .get_time_series_data_points()
      .read()
      .unwrap();

    if time_series_data_points.is_empty() {
      error!("Cannot compress an empty time series block");
      return Err(TsldbError::EmptyTimeSeriesBlock());
    }
    let data_points_compressed_vec = compress_data_point_vector(time_series_data_points);

    Ok(Self::new_with_data_points_compressed_vec(
      data_points_compressed_vec,
    ))
  }
}

impl Default for TimeSeriesBlockCompressed {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use std::mem::size_of_val;

  use super::super::constants::BLOCK_SIZE_FOR_TIME_SERIES;
  use super::*;

  use crate::utils::sync::is_sync;

  #[test]
  fn test_new() {
    // Check whether TimeSeriesBlockCompressed implements sync.
    is_sync::<TimeSeriesBlockCompressed>();

    // Check that a newly created compressed time series block is empty.
    let tsbc = TimeSeriesBlockCompressed::new();
    assert_eq!(tsbc.data_points_compressed.read().unwrap().len(), 0);
  }

  #[test]
  fn test_default() {
    // Check that a default compressed time series block is empty.
    let tsbc = TimeSeriesBlockCompressed::default();
    assert_eq!(tsbc.data_points_compressed.read().unwrap().len(), 0);
  }

  #[test]
  fn test_read_from_empty() {
    let tsb = TimeSeriesBlock::new();
    let retval = TimeSeriesBlockCompressed::try_from(&tsb);

    // We can't compress an empty time series block.
    assert!(retval.is_err());
  }

  #[test]
  fn test_all_same_values() {
    // The compression only works when the values are in monotonically increasing order.
    // When passed vector with the same elements, the returned vector is empty.
    let num_data_points = 128;
    let expected = TimeSeriesBlock::new();
    for _ in 0..num_data_points {
      expected.append(10, 10.0).unwrap();
    }
    let compressed = TimeSeriesBlockCompressed::try_from(&expected).unwrap();
    let received = TimeSeriesBlock::try_from(&compressed).unwrap();

    assert_eq!(expected, received);
  }

  #[test]
  fn test_some_same_values() {
    let num_data_points = 128;
    let expected = TimeSeriesBlock::new();
    let mut start = 10;
    for _ in 0..num_data_points / 4 {
      for _ in 0..4 {
        expected.append(start, 10.0).unwrap();
      }
      start += 1;
    }

    // Check that the time series is block remains the same after compression + decompression.
    let compressed = TimeSeriesBlockCompressed::try_from(&expected).unwrap();
    let received = TimeSeriesBlock::try_from(&compressed).unwrap();

    assert_eq!(expected, received);
  }

  #[test]
  fn test_incresing_values() {
    // When time is monotonically increasing by the same difference, and value is constant,
    // we should see significant compression.
    let expected = TimeSeriesBlock::new();
    let start = 10_000_000;
    let value = 0.0;
    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
      expected.append(start + (i as u64 * 30), value).unwrap();
    }
    let compressed = TimeSeriesBlockCompressed::try_from(&expected).unwrap();

    // Check that decompression gives back the same original datapoints.
    let received = TimeSeriesBlock::try_from(&compressed).unwrap();
    assert_eq!(expected, received);

    // Each data points takes 16 bytes, so the memory requirement would be BLOCK_SIZE_FOR_TIME_SERIES*16.
    let received_data_points = received.get_time_series_data_points().read().unwrap();
    let mem_decompressed = size_of_val(&*received_data_points.as_slice());
    assert_eq!(mem_decompressed, BLOCK_SIZE_FOR_TIME_SERIES * 16);

    // Make sure that the compressed data is at least 1/10th of the original data size.
    let mem_compressed = size_of_val(&compressed.data_points_compressed.read().unwrap()[..]);
    assert!(10 * mem_compressed < mem_decompressed);
  }
}
