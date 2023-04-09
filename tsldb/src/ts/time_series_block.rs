use log::debug;
use serde::{Deserialize, Serialize};

use crate::ts::constants::BLOCK_SIZE_FOR_TIME_SERIES;
use crate::ts::data_point::DataPoint;
use crate::ts::time_series_block_compressed::TimeSeriesBlockCompressed;
use crate::ts::tsutils::decompress_numeric_vector;
use crate::utils::custom_serde::rwlock_serde;
use crate::utils::error::TsldbError;
use crate::utils::sync::RwLock;

/// Represents a time series block.
#[derive(Debug, Deserialize, Serialize)]
pub struct TimeSeriesBlock {
  #[serde(with = "rwlock_serde")]
  /// Vector of data points, wrapped in a RwLock.
  data_points: RwLock<Vec<DataPoint>>,
}

impl TimeSeriesBlock {
  /// Create a new time series block.
  pub fn new() -> Self {
    // We allocate a fixed capacity at the beginning, so that the vector doesn't get dynamically reallocated during appends.
    let data_points_vec: Vec<DataPoint> = Vec::with_capacity(BLOCK_SIZE_FOR_TIME_SERIES);
    let data_points_lock = RwLock::new(data_points_vec);

    Self {
      data_points: data_points_lock,
    }
  }

  /// Create a time series block from the given vector of data points.
  pub fn new_with_data_points(data_points_vec: Vec<DataPoint>) -> Self {
    let data_points_lock = RwLock::new(data_points_vec);

    Self {
      data_points: data_points_lock,
    }
  }

  /// Check whether this time series block is empty.
  pub fn is_empty(&self) -> bool {
    self.data_points.read().unwrap().is_empty()
  }

  /// Get the vector of data points, wrapped in RwLock.
  pub fn get_time_series_data_points(&self) -> &RwLock<Vec<DataPoint>> {
    &self.data_points
  }

  /// Append a new data point with given time and value.
  pub fn append(&self, time: u64, value: f64) -> Result<(), TsldbError> {
    let mut data_points_lock = self.data_points.write().unwrap();

    if data_points_lock.len() >= BLOCK_SIZE_FOR_TIME_SERIES {
      debug!("Capacity full error while inserting time/value {}/{}. Typically a new block will now be created.",
             time, value);
      return Err(TsldbError::CapacityFull(BLOCK_SIZE_FOR_TIME_SERIES));
    }

    let dp = DataPoint::new(time, value);

    // Always keep data_points vector sorted (by time), as the compression needs it to be sorted.
    if data_points_lock.is_empty() || data_points_lock.last().unwrap() < &dp {
      data_points_lock.push(dp);
    } else {
      let pos = data_points_lock.binary_search(&dp).unwrap_or_else(|e| e);
      data_points_lock.insert(pos, dp);
    }

    Ok(())
  }

  /// Get the data points in the specified range (both range_start_time and range_end_time inclusive).
  pub fn get_data_points_in_range(
    &self,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<DataPoint> {
    let data_points_lock = self.data_points.read().unwrap();
    let mut retval = Vec::new();

    for dp in data_points_lock.as_slice() {
      let time = dp.get_time();

      if time >= range_start_time && time <= range_end_time {
        retval.push((*dp).clone());
      }
    }

    retval
  }

  /// Get the number of data points in this time series block.
  #[cfg(test)]
  pub fn len(&self) -> usize {
    let data_points_lock = self.data_points.read().unwrap();
    data_points_lock.len()
  }
}

impl PartialEq for TimeSeriesBlock {
  fn eq(&self, other: &Self) -> bool {
    let data_points_lock = self.data_points.read().unwrap();
    let other_data_points_lock = other.data_points.read().unwrap();

    *data_points_lock == *other_data_points_lock
  }
}

impl Eq for TimeSeriesBlock {}

impl TryFrom<&TimeSeriesBlockCompressed> for TimeSeriesBlock {
  type Error = TsldbError;

  // Decompress a compressed time series block.
  fn try_from(
    time_series_block_compressed: &TimeSeriesBlockCompressed,
  ) -> Result<Self, Self::Error> {
    let data_points_compressed_lock = time_series_block_compressed
      .get_data_points_compressed()
      .read()
      .unwrap();
    let data_points_compressed = &*data_points_compressed_lock;
    let data_points_decompressed = decompress_numeric_vector(data_points_compressed).unwrap();
    let time_series_block = TimeSeriesBlock::new_with_data_points(data_points_decompressed);

    Ok(time_series_block)
  }
}

impl Default for TimeSeriesBlock {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;
  use std::thread;

  use rand::Rng;

  use super::*;
  use crate::utils::sync::is_sync;

  #[test]
  fn test_new_time_series_block() {
    // Check that time series block implements sync.
    is_sync::<TimeSeriesBlock>();

    // Check that a new time series block is empty.
    let tsb = TimeSeriesBlock::new();
    assert_eq!(tsb.data_points.read().unwrap().len(), 0);
  }

  #[test]
  fn test_default_time_series_block() {
    // Check that a default time series block is empty.
    let tsb = TimeSeriesBlock::default();
    assert_eq!(tsb.data_points.read().unwrap().len(), 0);
  }

  #[test]
  fn test_single_append() {
    // After appending a single value, check that the time series block has that value.
    let tsb = TimeSeriesBlock::new();
    tsb.append(1000, 1.0).unwrap();
    assert_eq!(tsb.data_points.read().unwrap().len(), 1);
    assert_eq!(
      tsb.data_points.read().unwrap().get(0).unwrap().get_time(),
      1000
    );
    assert_eq!(
      tsb.data_points.read().unwrap().get(0).unwrap().get_value(),
      1.0
    );
  }

  #[test]
  fn test_block_size_appends() {
    let tsb = TimeSeriesBlock::new();
    let mut expected: Vec<DataPoint> = Vec::new();

    // Append BLOCK_SIZE_FOR_TIME_SERIES values, and check that the time series block has those values.
    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
      tsb.append(i as u64, i as f64).unwrap();
      expected.push(DataPoint::new(i as u64, i as f64));
    }
    assert_eq!(*tsb.data_points.read().unwrap(), expected);
  }

  #[test]
  fn test_data_points_in_range() {
    let tsb = TimeSeriesBlock::new();
    tsb.append(100, 1.0).unwrap();
    tsb.append(200, 1.0).unwrap();
    tsb.append(300, 1.0).unwrap();

    assert_eq!(tsb.get_data_points_in_range(50, 70).len(), 0);
    assert_eq!(tsb.get_data_points_in_range(50, 150).len(), 1);
    assert_eq!(tsb.get_data_points_in_range(50, 350).len(), 3);
    assert_eq!(tsb.get_data_points_in_range(350, 1350).len(), 0);
  }

  #[test]
  fn test_concurrent_appends() {
    // Append BLOCK_SIZE_FOR_TIME_SERIES data points in multiple threads.
    // Check that all the data points are appended in sorted order.

    let num_threads = 16;
    let num_data_points_per_thread = BLOCK_SIZE_FOR_TIME_SERIES / 16;
    let tsb = Arc::new(TimeSeriesBlock::new());

    let mut handles = Vec::new();
    let expected = Arc::new(RwLock::new(Vec::new()));
    for _ in 0..num_threads {
      let tsb_arc = tsb.clone();
      let expected_arc = expected.clone();
      let handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        for _ in 0..num_data_points_per_thread {
          let time = rng.gen_range(0..10000);
          let dp = DataPoint::new(time, 1.0);
          tsb_arc.append(time, 1.0).unwrap();
          (*(expected_arc.write().unwrap())).push(dp);
        }
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap();
    }

    // Sort the expected values, as the data points should be appended in sorted order.
    (*expected.write().unwrap()).sort();

    assert_eq!(*expected.read().unwrap(), *tsb.data_points.read().unwrap());

    // If we append more than BLOCK_SIZE, it should result in an error.
    let retval = tsb.append(1000, 1000.0);
    assert!(retval.is_err());
  }
}
