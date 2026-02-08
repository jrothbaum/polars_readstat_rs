use crate::spss::error::{Error, Result};
use crate::spss::types::VarType;
use polars::prelude::*;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const SAV_HEADER_LEN: usize = 176;
const SAV_RECORD_VARIABLE: u32 = 2;
const SAV_RECORD_VALUE_LABEL: u32 = 3;
const SAV_RECORD_VALUE_LABEL_VARS: u32 = 4;
const SAV_RECORD_DICT_TERMINATION: u32 = 999;

const SAV_MISSING_DOUBLE: u64 = 0xFFEFFFFFFFFFFFFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpssValueLabelKey(u64);

impl SpssValueLabelKey {
    pub fn from_f64(v: f64) -> Self {
        Self(v.to_bits())
    }

    pub fn to_f64(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl From<f64> for SpssValueLabelKey {
    fn from(value: f64) -> Self {
        Self::from_f64(value)
    }
}

pub type SpssValueLabelMap = HashMap<SpssValueLabelKey, String>;
pub type SpssValueLabels = HashMap<String, SpssValueLabelMap>;
pub type SpssVariableLabels = HashMap<String, String>;

#[derive(Debug, Clone)]
pub struct SpssWriteSchema {
    pub columns: Vec<SpssWriteColumn>,
    pub row_count: Option<usize>,
    pub value_labels: Option<SpssValueLabels>,
    pub variable_labels: Option<SpssVariableLabels>,
}

#[derive(Debug, Clone)]
pub struct SpssWriteColumn {
    pub name: String,
    pub dtype: DataType,
    pub string_width_bytes: Option<usize>,
}

pub struct SpssWriter {
    path: PathBuf,
    schema: Option<SpssWriteSchema>,
    value_labels: Option<SpssValueLabels>,
    variable_labels: Option<SpssVariableLabels>,
}

impl SpssWriter {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            schema: None,
            value_labels: None,
            variable_labels: None,
        }
    }

    pub fn with_schema(mut self, schema: SpssWriteSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_value_labels(mut self, labels: SpssValueLabels) -> Self {
        self.value_labels = Some(labels);
        self
    }

    pub fn with_variable_labels(mut self, labels: SpssVariableLabels) -> Self {
        self.variable_labels = Some(labels);
        self
    }

    pub fn write_df(&self, df: &DataFrame) -> Result<()> {
        let schema = self.schema.as_ref();
        let value_labels = merge_value_labels(schema.and_then(|s| s.value_labels.clone()), self.value_labels.clone());
        let variable_labels = merge_variable_labels(schema.and_then(|s| s.variable_labels.clone()), self.variable_labels.clone());
        let columns = infer_columns(df, schema, variable_labels.as_ref())?;
        let file = File::create(&self.path)?;
        let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
        write_header(&mut writer, df.height() as i32, columns.iter().map(|c| c.width).sum())?;
        write_variable_records(&mut writer, &columns)?;
        if let Some(labels) = value_labels.as_ref() {
            write_value_labels(&mut writer, &columns, labels)?;
        }
        write_dict_termination(&mut writer)?;
        write_data(&mut writer, df, &columns)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ColumnSpec {
    name: String,
    var_type: VarType,
    string_len: usize,
    width: usize,
    offset: usize,
    label: Option<String>,
}

fn infer_columns(
    df: &DataFrame,
    schema: Option<&SpssWriteSchema>,
    variable_labels: Option<&SpssVariableLabels>,
) -> Result<Vec<ColumnSpec>> {
    if let Some(schema) = schema {
        let mut cols = Vec::with_capacity(schema.columns.len());
        let mut offset = 0usize;
        for col in &schema.columns {
            validate_name(&col.name)?;
            let (var_type, string_len, width) = dtype_to_spss(col, df)?;
            let label = variable_labels.and_then(|labels| labels.get(&col.name).cloned());
            cols.push(ColumnSpec {
                name: col.name.clone(),
                var_type,
                string_len,
                width,
                offset,
                label,
            });
            offset += width;
        }
        return Ok(cols);
    }

    let mut cols = Vec::with_capacity(df.width());
    let mut offset = 0usize;
    for column in df.get_columns() {
        let series = column.as_materialized_series();
        let name = series.name().to_string();
        validate_name(&name)?;
        let (var_type, string_len, width) = infer_series(series)?;
        let label = variable_labels.and_then(|labels| labels.get(&name).cloned());
        cols.push(ColumnSpec {
            name,
            var_type,
            string_len,
            width,
            offset,
            label,
        });
        offset += width;
    }
    Ok(cols)
}

fn dtype_to_spss(col: &SpssWriteColumn, df: &DataFrame) -> Result<(VarType, usize, usize)> {
    match col.dtype {
        DataType::String => {
            let width = if let Some(w) = col.string_width_bytes {
                w
            } else {
                let series = df
                    .column(&col.name)
                    .map_err(|e| Error::ParseError(e.to_string()))?
                    .as_materialized_series();
                max_string_width(series)?
            };
            string_layout(width)
        }
        _ => Ok((VarType::Numeric, 0, 1)),
    }
}

fn infer_series(series: &Series) -> Result<(VarType, usize, usize)> {
    match series.dtype() {
        DataType::String => {
            let width = max_string_width(series)?;
            string_layout(width)
        }
        _ => Ok((VarType::Numeric, 0, 1)),
    }
}

fn string_layout(len: usize) -> Result<(VarType, usize, usize)> {
    if len > 255 {
        return Err(Error::ParseError("SPSS writer does not support strings > 255 bytes".to_string()));
    }
    let width = (len.max(1) + 7) / 8;
    Ok((VarType::Str, len.max(1), width))
}

fn max_string_width(series: &Series) -> Result<usize> {
    let utf8 = series.str().map_err(|e| Error::ParseError(e.to_string()))?;
    let mut max_len = 0usize;
    for opt in utf8.into_iter() {
        if let Some(s) = opt {
            let s: &str = s;
            let len = s.as_bytes().len();
            if len > max_len {
                max_len = len;
            }
        }
    }
    Ok(max_len.max(1))
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 8 {
        return Err(Error::ParseError(format!("invalid SPSS variable name: {name}")));
    }
    Ok(())
}

fn merge_value_labels(base: Option<SpssValueLabels>, extra: Option<SpssValueLabels>) -> Option<SpssValueLabels> {
    match (base, extra) {
        (None, None) => None,
        (Some(mut base), Some(extra)) => {
            for (k, v) in extra {
                base.entry(k).or_insert(v);
            }
            Some(base)
        }
        (Some(base), None) => Some(base),
        (None, Some(extra)) => Some(extra),
    }
}

fn merge_variable_labels(base: Option<SpssVariableLabels>, extra: Option<SpssVariableLabels>) -> Option<SpssVariableLabels> {
    match (base, extra) {
        (None, None) => None,
        (Some(mut base), Some(extra)) => {
            for (k, v) in extra {
                base.entry(k).or_insert(v);
            }
            Some(base)
        }
        (Some(base), None) => Some(base),
        (None, Some(extra)) => Some(extra),
    }
}

fn write_header<W: Write>(writer: &mut W, row_count: i32, nominal_case_size: usize) -> Result<()> {
    let mut buf = vec![0u8; SAV_HEADER_LEN];
    buf[0..4].copy_from_slice(b"$FL2");
    buf[64..68].copy_from_slice(&2i32.to_le_bytes());
    buf[68..72].copy_from_slice(&(nominal_case_size as i32).to_le_bytes());
    buf[72..76].copy_from_slice(&0i32.to_le_bytes());
    buf[80..84].copy_from_slice(&row_count.to_le_bytes());
    buf[84..92].copy_from_slice(&100.0f64.to_le_bytes());
    writer.write_all(&buf)?;
    Ok(())
}

fn write_variable_records<W: Write>(writer: &mut W, columns: &[ColumnSpec]) -> Result<()> {
    for col in columns {
        write_variable_record(writer, col)?;
        if col.width > 1 {
            for _ in 1..col.width {
                write_variable_continuation(writer)?;
            }
        }
    }
    Ok(())
}

fn write_variable_record<W: Write>(writer: &mut W, col: &ColumnSpec) -> Result<()> {
    write_u32(writer, SAV_RECORD_VARIABLE)?;
    let typ = if col.var_type == VarType::Numeric { 0 } else { col.string_len as i32 };
    write_i32(writer, typ)?;
    let has_label = if col.label.is_some() { 1 } else { 0 };
    write_i32(writer, has_label)?;
    write_i32(writer, 0)?;
    write_i32(writer, 0)?;
    write_i32(writer, 0)?;
    write_name(writer, &col.name)?;
    if let Some(label) = &col.label {
        write_variable_label(writer, label)?;
    }
    Ok(())
}

fn write_variable_continuation<W: Write>(writer: &mut W) -> Result<()> {
    write_u32(writer, SAV_RECORD_VARIABLE)?;
    write_i32(writer, -1)?;
    write_i32(writer, 0)?;
    write_i32(writer, 0)?;
    write_i32(writer, 0)?;
    write_i32(writer, 0)?;
    write_name(writer, "")?;
    Ok(())
}

fn write_variable_label<W: Write>(writer: &mut W, label: &str) -> Result<()> {
    let (bytes, _, had_errors) = encoding_rs::WINDOWS_1252.encode(label);
    if had_errors {
        return Err(Error::ParseError("variable label not representable in Windows-1252".to_string()));
    }
    let len = bytes.len().min(255);
    write_u32(writer, len as u32)?;
    let padded = ((len + 3) / 4) * 4;
    let mut buf = vec![0u8; padded];
    buf[..len].copy_from_slice(&bytes[..len]);
    writer.write_all(&buf)?;
    Ok(())
}

fn write_value_labels<W: Write>(
    writer: &mut W,
    columns: &[ColumnSpec],
    labels: &SpssValueLabels,
) -> Result<()> {
    for col in columns {
        let Some(map) = labels.get(&col.name) else { continue };
        if col.var_type == VarType::Str {
            return Err(Error::ParseError("string value labels not supported in SPSS writer".to_string()));
        }
        if map.is_empty() {
            continue;
        }
        write_u32(writer, SAV_RECORD_VALUE_LABEL)?;
        write_u32(writer, map.len() as u32)?;
        for (value, label) in map {
            writer.write_all(&value.to_f64().to_le_bytes())?;
            let (bytes, _, had_errors) = encoding_rs::WINDOWS_1252.encode(label);
            if had_errors {
                return Err(Error::ParseError("value label not representable in Windows-1252".to_string()));
            }
            let len = bytes.len().min(255);
            writer.write_all(&[len as u8])?;
            let padded = ((len + 8) / 8) * 8 - 1;
            let mut buf = vec![0u8; padded];
            buf[..len].copy_from_slice(&bytes[..len]);
            writer.write_all(&buf)?;
        }
        write_u32(writer, SAV_RECORD_VALUE_LABEL_VARS)?;
        write_u32(writer, 1)?;
        write_u32(writer, (col.offset + 1) as u32)?;
    }
    Ok(())
}

fn write_dict_termination<W: Write>(writer: &mut W) -> Result<()> {
    write_u32(writer, SAV_RECORD_DICT_TERMINATION)?;
    write_u32(writer, 0)?;
    Ok(())
}

fn write_data<W: Write>(writer: &mut W, df: &DataFrame, columns: &[ColumnSpec]) -> Result<()> {
    let mut cols: Vec<&Series> = Vec::with_capacity(columns.len());
    for col in columns {
        let series = df.column(&col.name).map_err(|e| Error::ParseError(e.to_string()))?;
        cols.push(series.as_materialized_series());
    }

    for row_idx in 0..df.height() {
        for (col_idx, col) in columns.iter().enumerate() {
            let series = cols[col_idx];
            match col.var_type {
                VarType::Numeric => {
                    let value = series.get(row_idx).map_err(|e| Error::ParseError(e.to_string()))?;
                    if value.is_null() {
                        let bytes = SAV_MISSING_DOUBLE.to_le_bytes();
                        writer.write_all(&bytes)?;
                    } else {
                        let v = anyvalue_to_f64(value).ok_or(Error::ParseError("unsupported numeric type".to_string()))?;
                        writer.write_all(&v.to_le_bytes())?;
                    }
                }
                VarType::Str => {
                    let mut buf = vec![b' '; col.width * 8];
                    let value = series.get(row_idx).map_err(|e| Error::ParseError(e.to_string()))?;
                    if !value.is_null() {
                        let s: Cow<'_, str> = match value {
                            AnyValue::String(s) => Cow::Borrowed(s),
                            AnyValue::StringOwned(s) => Cow::Owned(s.to_string()),
                            _ => return Err(Error::ParseError("unsupported string type".to_string())),
                        };
                        let (bytes, _, had_errors) = encoding_rs::WINDOWS_1252.encode(s.as_ref());
                        if had_errors {
                            return Err(Error::ParseError("string not representable in Windows-1252".to_string()));
                        }
                        let copy_len = bytes.len().min(col.string_len);
                        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
                    }
                    writer.write_all(&buf)?;
                }
            }
        }
    }
    Ok(())
}

fn anyvalue_to_f64(v: AnyValue) -> Option<f64> {
    match v {
        AnyValue::Float64(v) => Some(v),
        AnyValue::Float32(v) => Some(v as f64),
        AnyValue::Int64(v) => Some(v as f64),
        AnyValue::Int32(v) => Some(v as f64),
        AnyValue::Int16(v) => Some(v as f64),
        AnyValue::Int8(v) => Some(v as f64),
        AnyValue::UInt64(v) => Some(v as f64),
        AnyValue::UInt32(v) => Some(v as f64),
        AnyValue::UInt16(v) => Some(v as f64),
        AnyValue::UInt8(v) => Some(v as f64),
        AnyValue::Boolean(v) => Some(if v { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn write_name<W: Write>(writer: &mut W, name: &str) -> Result<()> {
    let mut buf = [b' '; 8];
    let bytes = name.as_bytes();
    let copy_len = bytes.len().min(8);
    buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
    writer.write_all(&buf)?;
    Ok(())
}

fn write_u32<W: Write>(writer: &mut W, v: u32) -> Result<()> {
    writer.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_i32<W: Write>(writer: &mut W, v: i32) -> Result<()> {
    writer.write_all(&v.to_le_bytes())?;
    Ok(())
}
