
use std::sync::Arc;

use datafusion::{arrow::{array::{ArrayRef, Int32Array, StringArray}, datatypes::{Field, SchemaBuilder}}, execution::context::SessionContext};
use learn_datafusion::plain_text_table::PlainTextTable;

#[tokio::test]
async fn test_plaint_text_table() {
    let ctx = SessionContext::new();

    let users_table = PlainTextTable::new("test_data/plain_text_table/users.txtable").await.unwrap();
    ctx.register_table("users", Arc::new(users_table)).unwrap();

    let df = ctx.sql(
        "SELECT * FROM users WHERE birth_year < 2000"
    ).await.unwrap();

    //df.clone().show().await.unwrap();

    let result =  df.collect().await.unwrap();
    let record_batch = &result[0];

    let mut expected_schema = SchemaBuilder::new();
    expected_schema.push(Field::new("id", datafusion::arrow::datatypes::DataType::Int32, false));
    expected_schema.push(Field::new("name", datafusion::arrow::datatypes::DataType::Utf8, false));
    expected_schema.push(Field::new("birth_year", datafusion::arrow::datatypes::DataType::Int32, false));
    assert_eq!(*record_batch.schema(), expected_schema.finish());

    assert_eq!(&record_batch.columns(), &[
        Arc::new(Int32Array::from(vec![1,3])) as ArrayRef,
        Arc::new(StringArray::from(vec!["John Doe".to_string(), "Кот Леопольд".to_string()])) as ArrayRef,
        Arc::new(Int32Array::from(vec![1988,1975])) as ArrayRef,
    ]);

}
