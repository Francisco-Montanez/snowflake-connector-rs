mod common;

use snowflake_connector_rs::Result;

#[tokio::test]
async fn test_bool_decode() -> Result<()> {
    // Arrange
    let client = common::connect()?;
    let session = client.create_session().await?;

    // Create a temporary table with boolean values
    let create_table_query = r#"
        CREATE TEMPORARY TABLE bool_test (
            id INT,
            true_value BOOLEAN,
            false_value BOOLEAN
        )
    "#;
    session.query(create_table_query).await?;

    // Insert test data
    let insert_query = r#"
        INSERT INTO bool_test (id, true_value, false_value)
        VALUES
        (1, TRUE, FALSE),
        (2, 'T', 'F'),
        (3, 'YES', 'NO'),
        (4, 'Y', 'N'),
        (5, 'ON', 'OFF'),
        (6, '1', '0')
    "#;
    session.query(insert_query).await?;

    // Act
    let select_query = "SELECT * FROM bool_test ORDER BY id";
    let rows = session.query(select_query).await?;

    // Assert
    assert_eq!(rows.len(), 6);

    for row in rows {
        let id = row.get::<i32>("ID")?;
        let true_value = row.get::<bool>("TRUE_VALUE")?;
        let false_value = row.get::<bool>("FALSE_VALUE")?;

        assert!(true_value, "True value for id {} should be true", id);
        assert!(!false_value, "False value for id {} should be false", id);
    }

    Ok(())
}

#[tokio::test]
async fn test_bool_decode_invalid_values() -> Result<()> {
    // Arrange
    let client = common::connect()?;
    let session = client.create_session().await?;

    // Create a temporary table with an invalid boolean value
    let create_table_query = r#"
        CREATE TEMPORARY TABLE bool_test_invalid (
            id INT,
            invalid_bool VARCHAR
        )
    "#;
    session.query(create_table_query).await?;

    // Insert test data with an invalid boolean value
    let insert_query = r#"
        INSERT INTO bool_test_invalid (id, invalid_bool)
        VALUES (1, 'INVALID')
    "#;
    session.query(insert_query).await?;

    // Act
    let select_query = "SELECT * FROM bool_test_invalid";
    let rows = session.query(select_query).await?;

    // Assert
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    let result = row.get::<bool>("INVALID_BOOL");

    assert!(result.is_err(), "Decoding an invalid boolean should fail");
    if let Err(e) = result {
        assert!(
            e.to_string().contains("is not bool"),
            "Error message should indicate an invalid boolean value"
        );
    }

    Ok(())
}
