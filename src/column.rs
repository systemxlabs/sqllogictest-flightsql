use std::str::FromStr;

use arrow::array::*;
use arrow::datatypes::Fields;
use arrow::{
    datatypes::{DataType, Decimal128Type, Decimal256Type, DecimalType, Schema, i256},
    util::display::{ArrayFormatter, FormatOptions},
};
use bigdecimal::BigDecimal;
use half::f16;
use sqllogictest::ColumnType;

use crate::error::FlightSqlLogicTestError;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ArrowColumnType {
    Boolean,
    DateTime,
    Integer,
    Float,
    Text,
    Timestamp,
    Another,
}

impl ColumnType for ArrowColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Boolean),
            'D' => Some(Self::DateTime),
            'I' => Some(Self::Integer),
            'P' => Some(Self::Timestamp),
            'R' => Some(Self::Float),
            'T' => Some(Self::Text),
            _ => Some(Self::Another),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Boolean => 'B',
            Self::DateTime => 'D',
            Self::Integer => 'I',
            Self::Timestamp => 'P',
            Self::Float => 'R',
            Self::Text => 'T',
            Self::Another => '?',
        }
    }
}

/// Converts columns to a result as expected by sqllogicteset.
pub fn convert_schema_to_types(columns: &Fields) -> Vec<ArrowColumnType> {
    columns
        .iter()
        .map(|f| f.data_type())
        .map(|data_type| match data_type {
            DataType::Boolean => ArrowColumnType::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => ArrowColumnType::Integer,
            DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => ArrowColumnType::Float,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ArrowColumnType::Text,
            DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) => {
                ArrowColumnType::DateTime
            }
            DataType::Timestamp(_, _) => ArrowColumnType::Timestamp,
            DataType::Dictionary(key_type, value_type) => {
                if key_type.is_integer() {
                    // mapping dictionary string types to Text
                    match value_type.as_ref() {
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                            ArrowColumnType::Text
                        }
                        _ => ArrowColumnType::Another,
                    }
                } else {
                    ArrowColumnType::Another
                }
            }
            _ => ArrowColumnType::Another,
        })
        .collect()
}

/// Converts `batches` to a result as expected by sqllogictest.
pub fn convert_batches(
    schema: &Schema,
    batches: Vec<RecordBatch>,
) -> Result<Vec<Vec<String>>, FlightSqlLogicTestError> {
    let mut rows = vec![];
    for batch in batches {
        // Verify schema
        if !schema.contains(&batch.schema()) {
            return Err(FlightSqlLogicTestError::Other(format!(
                "Schema mismatch. Previously had\n{:#?}\n\nGot:\n{:#?}",
                &schema,
                batch.schema()
            )));
        }

        // Convert a single batch to a `Vec<Vec<String>>` for comparison, flatten expanded rows, and normalize each.
        let new_rows = (0..batch.num_rows())
            .map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|col| cell_to_string(col, row))
                    .collect::<Result<Vec<String>, FlightSqlLogicTestError>>()
            })
            .collect::<Result<Vec<Vec<String>>, FlightSqlLogicTestError>>()?
            .into_iter()
            .flat_map(expand_row);
        rows.extend(new_rows);
    }
    Ok(rows)
}

macro_rules! get_row_value {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        array.value($row)
    }};
}

/// Normalizes the content of a single cell in RecordBatch prior to printing.
///
/// This is to make the output comparable to the semi-standard .slt format
///
/// Normalizations applied to [NULL Values and empty strings]
///
/// [NULL Values and empty strings]: https://duckdb.org/dev/sqllogictest/result_verification#null-values-and-empty-strings
///
/// Floating numbers are rounded to have a consistent representation with the Postgres runner.
pub fn cell_to_string(col: &ArrayRef, row: usize) -> Result<String, FlightSqlLogicTestError> {
    if !col.is_valid(row) {
        // represent any null value with the string "NULL"
        Ok(NULL_STR.to_string())
    } else {
        match col.data_type() {
            DataType::Null => Ok(NULL_STR.to_string()),
            DataType::Boolean => Ok(bool_to_str(get_row_value!(BooleanArray, col, row))),
            DataType::Float16 => Ok(f16_to_str(get_row_value!(Float16Array, col, row))),
            DataType::Float32 => Ok(f32_to_str(get_row_value!(Float32Array, col, row))),
            DataType::Float64 => {
                let result = get_row_value!(Float64Array, col, row);
                Ok(f64_to_str(result))
            }
            DataType::Decimal128(_, scale) => {
                let value = get_row_value!(Decimal128Array, col, row);
                Ok(decimal_128_to_str(value, *scale))
            }
            DataType::Decimal256(_, scale) => {
                let value = get_row_value!(Decimal256Array, col, row);
                Ok(decimal_256_to_str(value, *scale))
            }
            DataType::LargeUtf8 => Ok(varchar_to_str(get_row_value!(LargeStringArray, col, row))),
            DataType::Utf8 => Ok(varchar_to_str(get_row_value!(StringArray, col, row))),
            DataType::Utf8View => Ok(varchar_to_str(get_row_value!(StringViewArray, col, row))),
            DataType::Dictionary(_, _) => {
                let dict = col.as_any_dictionary();
                let key = dict.normalized_keys()[row];
                Ok(cell_to_string(dict.values(), key)?)
            }
            _ => {
                let format_options = FormatOptions::default();

                let f = ArrayFormatter::try_new(col.as_ref(), &format_options)?;

                Ok(f.value(row).to_string())
            }
        }
    }
}

/// Represents a constant for NULL string in your database.
pub const NULL_STR: &str = "NULL";

pub(crate) fn bool_to_str(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

pub(crate) fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        // Escape nulls so that github renders them correctly in the webui
        value.trim_end_matches('\n').replace("\u{0000}", "\\0")
    }
}

pub(crate) fn f16_to_str(value: f16) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f16::INFINITY {
        "Infinity".to_string()
    } else if value == f16::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), None)
    }
}

pub(crate) fn f32_to_str(value: f32) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f32::INFINITY {
        "Infinity".to_string()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), None)
    }
}

pub(crate) fn f64_to_str(value: f64) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), None)
    }
}

pub(crate) fn decimal_128_to_str(value: i128, scale: i8) -> String {
    let precision = u8::MAX; // does not matter
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal128Type::format_decimal(value, precision, scale)).unwrap(),
        None,
    )
}

pub(crate) fn decimal_256_to_str(value: i256, scale: i8) -> String {
    let precision = u8::MAX; // does not matter
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal256Type::format_decimal(value, precision, scale)).unwrap(),
        None,
    )
}

/// Converts a `BigDecimal` to its plain string representation, optionally rounding to a specified number of decimal places.
///
/// If `round_digits` is `None`, the value is rounded to 12 decimal places by default.
#[expect(clippy::needless_pass_by_value)]
pub(crate) fn big_decimal_to_str(value: BigDecimal, round_digits: Option<i64>) -> String {
    // Round the value to limit the number of decimal places
    let value = value.round(round_digits.unwrap_or(12)).normalized();
    // Format the value to a string
    value.to_plain_string()
}

/// special case rows that have newlines in them (like explain plans)
//
/// Transform inputs like:
/// ```text
/// [
///   "logical_plan",
///   "Sort: d.b ASC NULLS LAST\n  Projection: d.b, MAX(d.a) AS max_a",
/// ]
/// ```
///
/// Into one cell per line, adding lines if necessary
/// ```text
/// [
///   "logical_plan",
/// ]
/// [
///   "Sort: d.b ASC NULLS LAST",
/// ]
/// [ <--- newly added row
///   "|-- Projection: d.b, MAX(d.a) AS max_a",
/// ]
/// ```
fn expand_row(mut row: Vec<String>) -> impl Iterator<Item = Vec<String>> {
    use itertools::Either;
    use std::iter::once;

    // check last cell
    if let Some(cell) = row.pop() {
        let lines: Vec<_> = cell.split('\n').collect();

        // no newlines in last cell
        if lines.len() < 2 {
            row.push(cell);
            return Either::Left(once(row));
        }

        // form new rows with each additional line
        let new_lines: Vec<_> = lines
            .into_iter()
            .enumerate()
            .map(|(idx, l)| {
                // replace any leading spaces with '-' as
                // `sqllogictest` ignores whitespace differences
                //
                // See https://github.com/apache/datafusion/issues/6328
                let content = l.trim_start();
                let new_prefix = "-".repeat(l.len() - content.len());
                // maintain for each line a number, so
                // reviewing explain result changes is easier
                let line_num = idx + 1;
                vec![format!("{line_num:02}){new_prefix}{content}")]
            })
            .collect();

        Either::Right(once(row).chain(new_lines))
    } else {
        Either::Left(once(row))
    }
}
