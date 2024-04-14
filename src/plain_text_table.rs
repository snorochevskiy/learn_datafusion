use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties};


use async_trait::async_trait;

#[doc = r#"
Custom table provider for text files:
| id: int | name: str |
| 1       | John Doe  |
"#]
#[derive(Debug,Clone)]
pub struct PlainTextTable {
    pub table_path: String,
    pub projected_schema: SchemaRef,
}

impl PlainTextTable {
    pub async fn new(path: &str) -> Result<PlainTextTable> {
        let projected_schema = read_schema(&path).await?;

        Ok(PlainTextTable {
            table_path: path.to_owned(),
            projected_schema: projected_schema,
        })
    }
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PlainTextTableExec::new(schema, self.clone())))
    }
}

async fn read_schema(path: &str) -> Result<SchemaRef> {
    use tokio::io::AsyncBufReadExt;

    let file = tokio::fs::File::open(path).await?;
    let mut buf_reader = tokio::io::BufReader::new(file);
    let mut buf = String::new();
    buf_reader.read_line(&mut buf).await?;

    let mut schema = SchemaBuilder::new();
    for col in split_and_trim::<'|'>(&buf) {
        schema.push(parse_field_schema(col)?)
    }

    Ok(Arc::new(schema.finish()))
}

fn split_and_trim<'a, const SEPARATOR: char>(line: &'a str) -> Vec<&'a str> {
    line.split(SEPARATOR).map(|s|s.trim()).filter(|s|!s.is_empty()).collect()
}

fn parse_field_schema(col: &str) -> Result<Field> {
    let parts: Vec<&str> = col.trim().split(":").map(|s|s.trim()).collect();
    match parts[..] {
        [col_name, col_type] => {
            Ok(Field::new(col_name.to_owned(), parse_column_type(col_type)?, false))
        },
        _ => return Err(DataFusionError::Execution(format!("Invalid column header: {}", col))),
    }
}

fn parse_column_type(col_type: &str) -> Result<DataType> {
    match col_type.to_lowercase().as_str() {
        "str" => Ok(DataType::Utf8),
        "int" => Ok(DataType::Int32),
        _ => Err(DataFusionError::Execution(format!("Invalid column type: {}", col_type)))
    }
}

#[async_trait]
impl TableProvider for PlainTextTable {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be downcast to a specific implementation.
    fn as_any(&self) ->  &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    #[must_use]
    #[allow(clippy::type_complexity,clippy::type_repetition_in_bounds)]
    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, self.schema()).await;
    }
}

#[derive(Debug)]
pub struct PlainTextTableExec {
    table: PlainTextTable,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl PlainTextTableExec {
    pub fn new(schema: SchemaRef, table: PlainTextTable) -> Self {
        let cache = Self::compute_properties(&schema);
        PlainTextTableExec{table, schema, cache}
    }

    fn compute_properties(schema: &SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }
}

impl DisplayAs for PlainTextTableExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PlainTextTableExec: ")
    }
}


impl ExecutionPlan for PlainTextTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {

        let schema = self.table.schema();
        let fields = schema.fields();
        let mut filled_cols: Vec::<ResCol> = fields.iter()
                .map(|field| match field.data_type() {
                    DataType::Int32 => ResCol::IntCol(Vec::new()),
                    DataType::Utf8 => ResCol::StrCol(Vec::new()),
                    other => unreachable!("Unexpected field type: {}", other),
                }).collect();

        let file = std::fs::File::open(&self.table.table_path)?;
        let reader = std::io::BufReader::new(file);

        use std::io::prelude::*;
        for (line_num, line_res) in reader.lines().skip(1).enumerate() {
            let line = line_res?;
            let cols = split_and_trim::<'|'>(&line);
            if cols.len() != fields.len() {
                return Err(DataFusionError::Execution(format!("Wrong row at {}: {}", line_num, line)));
            }
            for (col_num, col) in cols.into_iter().enumerate() {
                let col_type = fields[col_num].data_type();
                match col_type {
                    DataType::Int32 =>
                        match filled_cols[col_num] {
                            ResCol::IntCol(ref mut vec) =>
                                match col.parse::<i32>() {
                                    Ok(value) => vec.push(value),
                                    _ => return Err(DataFusionError::Execution(format!("Unable to parse i32: {}", col)))
                                }
                            ,
                            _ => unreachable!("Unreachable file: {}, line: {}", file!(), line!()),
                        }
                    ,
                    DataType::Utf8 =>
                        match filled_cols[col_num] {
                            ResCol::StrCol(ref mut vec) => vec.push(col.to_owned()),
                            _ => unreachable!("Unreachable file: {}, line: {}", file!(), line!()),
                        },
                    other => return Err(DataFusionError::Internal(format!("Unexpected field type: {}", other))),
                }
            }
        }
    
        let mut result: Vec<ArrayRef> = Vec::new();
        for filled_col in filled_cols {
            match filled_col {
                ResCol::StrCol(vec) => result.push(Arc::new(StringArray::from(vec))),
                ResCol::IntCol(vec) => result.push(Arc::new(Int32Array::from(vec))),
            }
        }

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.schema.clone(),
                result,
            )?],
            self.schema(),
            None,
        )?))
    }
}

enum ResCol {
    IntCol(Vec<i32>),
    StrCol(Vec<String>),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_split_and_strip() {
        let line = "|1| 2 | 3|";
        let result = split_and_trim::<'|'>(line);
        assert_eq!(result, vec!["1","2","3"]);
    }
}