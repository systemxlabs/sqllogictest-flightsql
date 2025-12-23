# sqllogictest-flightsql
![License](https://img.shields.io/badge/license-MIT-blue.svg)
[![Crates.io](https://img.shields.io/crates/v/sqllogictest-flightsql.svg)](https://crates.io/crates/sqllogictest-flightsql)
[![Docs](https://docs.rs/sqllogictest-flightsql/badge.svg)](https://docs.rs/sqllogictest-flightsql/latest/sqllogictest_flightsql/)

A sqllogictest runner for Flight SQL protocol supported databases.

## Usage
```rust
#[tokio::test]
async fn sqllogictest() -> Result<(), Box<dyn std::error::Error>> {
    let mut tester = sqllogictest::Runner::new(|| async {
        FlightSqlDB::new_from_endpoint("demo-db", "http://localhost:50050").await
    });
    tester.with_column_validator(sqllogictest::strict_column_validator);

    let mut slts_dir = tokio::fs::read_dir(format!("tests/slts")).await?;
    while let Some(entry) = slts_dir.next_entry().await? {
        if entry.file_type().await?.is_file() {
            println!(
                "======== start to run file {} ========",
                entry.file_name().to_str().unwrap()
            );
            tester.run_file_async(entry.path()).await?;
        }
    }
    Ok(())
}
```