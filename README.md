# infino

## Getting started

* Install docker
* Run main.rs

## Development

### Code Coverage

Use [Tarpaulin](https://github.com/xd009642/tarpaulin) for code coverage.

```
$ cargo install cargo-tarpaulin
$ cargo tarpaulin
```

### Loom Test for Tsldb

```
$ RUSTFLAGS="--cfg loom" cargo test --test loom --release
```
