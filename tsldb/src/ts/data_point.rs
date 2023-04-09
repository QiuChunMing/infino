use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// Represents a data point in time series.
#[derive(Debug, Deserialize, Serialize)]
pub struct DataPoint {
  /// Timestamp from epoch.
  time: u64,

  /// Value for this data point.
  value: f64,
}

impl DataPoint {
  /// Create a new DataPoint from given time and value.
  pub fn new(time: u64, value: f64) -> Self {
    DataPoint { time, value }
  }

  /// Create a new DataPoint from given tsz::DataPoint.
  pub fn new_from_tsz_data_point(tsz_data_point: tsz::DataPoint) -> Self {
    DataPoint {
      time: tsz_data_point.get_time(),
      value: tsz_data_point.get_value(),
    }
  }

  /// Get time.
  pub fn get_time(&self) -> u64 {
    self.time
  }

  /// Get value.
  pub fn get_value(&self) -> f64 {
    self.value
  }

  /// Get tsz::DataPoint corresponding to this DataPoint.
  pub fn get_tsz_data_point(&self) -> tsz::DataPoint {
    tsz::DataPoint::new(self.get_time(), self.get_value())
  }
}

impl Clone for DataPoint {
  fn clone(&self) -> DataPoint {
    DataPoint {
      time: self.get_time(),
      value: self.get_value(),
    }
  }
}

impl PartialEq for DataPoint {
  #[inline]
  fn eq(&self, other: &DataPoint) -> bool {
    // Two data points are equal if their times are equal, and their values are either equal or are NaN.

    if self.time == other.time {
      if self.value.is_nan() {
        return other.value.is_nan();
      } else {
        return self.value == other.value;
      }
    }
    false
  }
}

impl Eq for DataPoint {}

impl Ord for DataPoint {
  fn cmp(&self, other: &Self) -> Ordering {
    self.time.cmp(&other.time)
  }
}

impl PartialOrd for DataPoint {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_data_point() {
    let time = 1;
    let value = 2.0;

    let dp = DataPoint::new(time, value);
    assert_eq!(dp.get_time(), time);
    assert_eq!(dp.get_value(), value);

    let tsz_dp = dp.get_tsz_data_point();
    assert_eq!(tsz_dp.get_time(), time);
    assert_eq!(tsz_dp.get_value(), value);

    let dp_from_tsz = DataPoint::new_from_tsz_data_point(tsz_dp);
    assert_eq!(dp, dp_from_tsz);
  }
}
