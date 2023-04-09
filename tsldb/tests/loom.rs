// Use loom to iterate through all permutations of thread spawning.
#[cfg(loom)]
mod loom_tests {
  use loom::sync::Arc;
  use loom::thread;

  use tempdir::TempDir;
  use tsldb::Tsldb;

  #[test]
  fn test_concurrent_append() {
    let mut builder = loom::model::Builder::new();
    builder.max_branches = 10_000;

    builder.check(|| {
      let index_dir = TempDir::new("index_test").unwrap();
      let index_dir_path = index_dir.path().to_str().unwrap();
      let index = Tsldb::new_with_max_params(index_dir_path, 20, 40);
      let arc_index = Arc::new(index);
      let num_threads = 3;
      let num_appends_per_thread = 50;

      let mut handles = Vec::new();

      // Start threads to append to the index.
      for i in 0..num_threads {
        let arc_index_clone = arc_index.clone();
        let start = i * num_appends_per_thread;
        let handle = thread::spawn(move || {
          for j in 0..num_appends_per_thread {
            let time = start + j;
            arc_index_clone.append_log_message(time as u64, "message");
            arc_index_clone.append_data_point(
              "some_name",
              &[("label1", "value1")],
              time as u64,
              1.0,
            );
          }
        });
        handles.push(handle);
      }

      for handle in handles {
        handle.join().unwrap();
      }

      // Commit and refresh.
      arc_index.commit(true);
      let tsldb = Tsldb::refresh(index_dir_path);

      let expected_len = num_threads * num_appends_per_thread;

      // Check whether the number of log messages is as expected.
      let results = tsldb.search("message", 0, expected_len as u64);
      let received_logs_len = results.len();
      assert_eq!(expected_len, received_logs_len);

      // Check whether the number of data points is as expected.
      let results = tsldb.get_time_series("label1", "value1", 0, expected_len as u64);
      let received_data_points_len = results.len();
      assert_eq!(expected_len, received_data_points_len);
    })
  }
}
