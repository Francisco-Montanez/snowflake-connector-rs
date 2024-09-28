mod common;

use snowflake_connector_rs::Result;
use std::time::Instant;

#[tokio::test]
async fn test_download_chunked_results() -> Result<()> {
    // Arrange
    let client = common::connect()?;

    // Act
    let session = client.create_session().await?;
    let query =
        "SELECT SEQ8() AS SEQ, RANDSTR(1000, RANDOM()) AS RAND FROM TABLE(GENERATOR(ROWCOUNT=>10000))";
    let rows = session.query(query).await?;

    // Assert
    assert_eq!(rows.len(), 10000);
    assert!(rows[0].get::<u64>("SEQ").is_ok());
    assert!(rows[0].get::<String>("RAND").is_ok());
    assert!(rows[0].column_names().contains(&"SEQ"));
    assert!(rows[0].column_names().contains(&"RAND"));

    let columns = rows[0].column_types();
    assert_eq!(
        columns[0]
            .column_type()
            .snowflake_type()
            .to_ascii_uppercase(),
        "FIXED"
    );
    assert!(!columns[0].column_type().nullable());
    assert_eq!(columns[0].index(), 0);
    assert_eq!(
        columns[1]
            .column_type()
            .snowflake_type()
            .to_ascii_uppercase(),
        "TEXT"
    );
    assert!(!columns[1].column_type().nullable());
    assert_eq!(columns[1].index(), 1);

    Ok(())
}

#[tokio::test]
async fn test_query_executor() -> Result<()> {
    // Arrange
    let client = common::connect()?;

    // Act
    let session = client.create_session().await?;
    let query =
        "SELECT SEQ8() AS SEQ, RANDSTR(1000, RANDOM()) AS RAND FROM TABLE(GENERATOR(ROWCOUNT=>10000))";

    let executor = session.execute(query).await?;
    let mut rows = Vec::with_capacity(10000);
    while let Some(mut r) = executor.fetch_next_chunk().await? {
        rows.append(&mut r);
    }

    // Assert
    assert_eq!(rows.len(), 10000);
    assert!(rows[0].get::<u64>("SEQ").is_ok());
    assert!(rows[0].get::<String>("RAND").is_ok());
    assert!(rows[0].column_names().contains(&"SEQ"));
    assert!(rows[0].column_names().contains(&"RAND"));

    Ok(())
}

#[tokio::test]
async fn test_chunk_download_performance() -> Result<()> {
    // Arrange
    let client = common::connect()?;
    let session = client.create_session().await?;
    let query = "SELECT SEQ8() AS SEQ, RANDSTR(1000, RANDOM()) AS RAND FROM TABLE(GENERATOR(ROWCOUNT=>100000))";

    // Act - New implementation (FuturesUnordered)
    let start = Instant::now();
    let executor = session.execute(query).await?;
    let new_rows = executor.fetch_all().await?;
    let new_time = start.elapsed();

    // Act - Old implementation (simulated)
    let start = Instant::now();
    let executor = session.execute(query).await?;
    let mut old_rows = Vec::new();
    let mut chunks = Vec::new();

    // Simulate fetching chunks
    while let Some(chunk) = executor.fetch_next_chunk().await? {
        chunks.push(chunk);
    }

    // Simulate old implementation
    let mut handles = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        handles.push(tokio::spawn(async move { chunk }));
    }

    for handle in handles {
        let chunk = handle.await?;
        old_rows.extend(chunk);
    }

    let old_time = start.elapsed();

    // Assert
    assert_eq!(new_rows.len(), old_rows.len());
    println!("New implementation time: {:?}", new_time);
    println!("Old implementation time: {:?}", old_time);
    assert!(
        new_time <= old_time,
        "New implementation should be at least as fast as the old one. New time: {:?}, Old time: {:?}",
        new_time,
        old_time
    );

    Ok(())
}
