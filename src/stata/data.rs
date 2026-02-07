use crate::stata::encoding;
use crate::stata::error::{Error, Result};
use crate::stata::types::{Metadata, VarType, NumericType, Endian};
use crate::stata::value::{missing_rules, read_i8, read_i16, read_i32, read_f32, read_f64};
use polars::prelude::*;
use byteorder::ReadBytesExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

pub fn read_data_frame(
    path: &Path,
    metadata: &Metadata,
    endian: Endian,
    ds_format: u16,
    columns: Option<&[usize]>,
    offset: usize,
    limit: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
) -> Result<DataFrame> {
    let shared = build_shared_decode(path, metadata, endian, ds_format, value_labels_as_strings)?;
    read_data_frame_range(
        path,
        metadata,
        endian,
        ds_format,
        columns,
        offset,
        limit,
        missing_string_as_null,
        value_labels_as_strings,
        &shared,
    )
}

pub struct SharedDecode {
    strls: Option<Arc<HashMap<(u32, u64), String>>>,
    label_maps: Arc<HashMap<String, Arc<LabelMap>>>,
}

pub fn build_shared_decode(
    path: &Path,
    metadata: &Metadata,
    endian: Endian,
    ds_format: u16,
    value_labels_as_strings: bool,
) -> Result<SharedDecode> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let strls = if metadata.variables.iter().any(|v| matches!(v.var_type, VarType::StrL)) {
        load_strls(&mut reader, metadata, endian, ds_format, metadata.encoding)?
    } else {
        None
    };
    let label_maps = if value_labels_as_strings {
        build_label_maps(metadata)
    } else {
        HashMap::new()
    };
    Ok(SharedDecode {
        strls: strls.map(Arc::new),
        label_maps: Arc::new(label_maps),
    })
}

pub fn read_data_frame_range(
    path: &Path,
    metadata: &Metadata,
    endian: Endian,
    ds_format: u16,
    columns: Option<&[usize]>,
    offset: usize,
    limit: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    shared: &SharedDecode,
) -> Result<DataFrame> {
    let file = File::open(path)?;
    let data_offset = metadata.data_offset.ok_or_else(|| Error::MissingMetadata)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    reader.seek(SeekFrom::Start(data_offset))?;
    if ds_format >= 117 {
        read_tag(&mut reader, b"<data>")?;
    }

    let label_maps = shared.label_maps.as_ref();

    let rules = missing_rules(ds_format);
    let (col_indices, mut builders, col_offsets, col_widths, col_labels, mut string_scratch) =
        build_column_builders(metadata, columns, limit, label_maps, value_labels_as_strings)?;
    let record_len = metadata.storage_widths.iter().map(|v| *v as usize).sum::<usize>();
    let mut row_buf = vec![0u8; record_len];

    let total_rows = metadata.row_count as usize;
    let mut rows_read = 0usize;

    let any_labels = col_labels.iter().any(|v| v.is_some());
    let numeric_only = !any_labels
        && metadata.variables.iter().all(|v| matches!(v.var_type, VarType::Numeric(_)))
        && shared.strls.is_none();

    if numeric_only {
        let plans = build_numeric_plans(&col_indices, &metadata.variables, &col_offsets, &col_widths);
        rows_read = read_numeric_only(
            &mut reader,
            &mut builders,
            &plans,
            &mut row_buf,
            total_rows,
            offset,
            limit,
            endian,
            rules,
        )?;
    } else {
    let start_row = offset;
    let end_row = (offset + limit).min(total_rows);
    if start_row > 0 {
        let byte_skip = (start_row as u64) * (record_len as u64);
        reader.seek(SeekFrom::Current(byte_skip as i64))?;
    }

    for _row_idx in start_row..end_row {
        reader.read_exact(&mut row_buf)?;

        for (i, &col_idx) in col_indices.iter().enumerate() {
            let col_offset = col_offsets[i];
            let width = col_widths[i];
            let slice = &row_buf[col_offset..col_offset + width];
            append_value(
                &mut builders[i],
                &metadata.variables[col_idx].var_type,
                slice,
                endian,
                rules,
                missing_string_as_null,
                shared.strls.as_ref().map(|v| v.as_ref()),
                ds_format,
                col_labels[i].as_deref(),
                metadata.encoding,
                string_scratch[i].as_mut(),
            )?;
        }

        rows_read += 1;
    }
    }

    if ds_format >= 117 && offset + rows_read >= total_rows {
        read_tag(&mut reader, b"</data>")?;
    }

    let mut cols: Vec<Column> = Vec::with_capacity(builders.len());
    for builder in builders {
        cols.push(builder.finish().into());
    }

    Ok(DataFrame::new(cols)?)
}

#[derive(Clone, Copy)]
enum NumericKind {
    Byte,
    Int,
    Long,
    Float,
    Double,
}

struct NumericPlan {
    builder_idx: usize,
    offset: usize,
    width: usize,
    kind: NumericKind,
}

fn build_numeric_plans(
    col_indices: &[usize],
    variables: &[crate::stata::types::Variable],
    col_offsets: &[usize],
    col_widths: &[usize],
) -> Vec<NumericPlan> {
    let mut plans = Vec::with_capacity(col_indices.len());
    for (i, &col_idx) in col_indices.iter().enumerate() {
        let kind = match variables[col_idx].var_type {
            VarType::Numeric(NumericType::Byte) => NumericKind::Byte,
            VarType::Numeric(NumericType::Int) => NumericKind::Int,
            VarType::Numeric(NumericType::Long) => NumericKind::Long,
            VarType::Numeric(NumericType::Float) => NumericKind::Float,
            VarType::Numeric(NumericType::Double) => NumericKind::Double,
            _ => continue,
        };
        plans.push(NumericPlan {
            builder_idx: i,
            offset: col_offsets[i],
            width: col_widths[i],
            kind,
        });
    }
    plans
}

fn read_numeric_only(
    reader: &mut BufReader<File>,
    builders: &mut [ColumnBuilder],
    plans: &[NumericPlan],
    row_buf: &mut [u8],
    total_rows: usize,
    offset: usize,
    limit: usize,
    endian: Endian,
    rules: crate::stata::value::MissingRules,
) -> Result<usize> {
    let mut rows_read = 0usize;
    let start_row = offset;
    let end_row = (offset + limit).min(total_rows);
    if start_row > 0 {
        let byte_skip = (start_row as u64) * (row_buf.len() as u64);
        reader.seek(SeekFrom::Current(byte_skip as i64))?;
    }
    for _row_idx in start_row..end_row {
        reader.read_exact(row_buf)?;
        for plan in plans {
            let slice = &row_buf[plan.offset..plan.offset + plan.width];
            match (&mut builders[plan.builder_idx], plan.kind) {
                (ColumnBuilder::Int8(b), NumericKind::Byte) => {
                    if let Some(v) = read_i8(slice, rules) { b.append_value(v); } else { b.append_null(); }
                }
                (ColumnBuilder::Int16(b), NumericKind::Int) => {
                    if let Some(v) = read_i16(slice, endian, rules) { b.append_value(v); } else { b.append_null(); }
                }
                (ColumnBuilder::Int32(b), NumericKind::Long) => {
                    if let Some(v) = read_i32(slice, endian, rules) { b.append_value(v); } else { b.append_null(); }
                }
                (ColumnBuilder::Float32(b), NumericKind::Float) => {
                    if let Some(v) = read_f32(slice, endian, rules) { b.append_value(v); } else { b.append_null(); }
                }
                (ColumnBuilder::Float64(b), NumericKind::Double) => {
                    if let Some(v) = read_f64(slice, endian, rules) { b.append_value(v); } else { b.append_null(); }
                }
                _ => {}
            }
        }
        rows_read += 1;
    }
    Ok(rows_read)
}

fn build_column_builders(
    metadata: &Metadata,
    columns: Option<&[usize]>,
    capacity: usize,
    label_maps: &HashMap<String, Arc<LabelMap>>,
    value_labels_as_strings: bool,
) -> Result<(Vec<usize>, Vec<ColumnBuilder>, Vec<usize>, Vec<usize>, Vec<Option<Arc<LabelMap>>>, Vec<Option<StringScratch>>)> {
    let col_indices: Vec<usize> = match columns {
        Some(cols) => cols.to_vec(),
        None => (0..metadata.variables.len()).collect(),
    };

    let mut builders = Vec::with_capacity(col_indices.len());
    let mut offsets = Vec::with_capacity(col_indices.len());
    let mut widths = Vec::with_capacity(col_indices.len());
    let mut labels = Vec::with_capacity(col_indices.len());
    let mut scratch = Vec::with_capacity(col_indices.len());

    let mut running = 0usize;
    let mut all_offsets = Vec::with_capacity(metadata.variables.len());
    for w in &metadata.storage_widths {
        all_offsets.push(running);
        running += *w as usize;
    }

    for &idx in &col_indices {
        let var = &metadata.variables[idx];
        let name = var.name.as_str();
        let label_map = if value_labels_as_strings {
            var.value_label_name
                .as_ref()
                .and_then(|name| label_maps.get(name))
                .cloned()
        } else {
            None
        };

        let builder = match var.var_type {
            VarType::Numeric(NumericType::Byte)
            | VarType::Numeric(NumericType::Int)
            | VarType::Numeric(NumericType::Long)
            | VarType::Numeric(NumericType::Float)
            | VarType::Numeric(NumericType::Double)
                if label_map.is_some() => ColumnBuilder::Utf8(StringChunkedBuilder::new(name.into(), capacity)),
            VarType::Numeric(NumericType::Byte) => {
                ColumnBuilder::Int8(PrimitiveChunkedBuilder::<Int8Type>::new(name.into(), capacity))
            }
            VarType::Numeric(NumericType::Int) => {
                ColumnBuilder::Int16(PrimitiveChunkedBuilder::<Int16Type>::new(name.into(), capacity))
            }
            VarType::Numeric(NumericType::Long) => {
                ColumnBuilder::Int32(PrimitiveChunkedBuilder::<Int32Type>::new(name.into(), capacity))
            }
            VarType::Numeric(NumericType::Float) => {
                ColumnBuilder::Float32(PrimitiveChunkedBuilder::<Float32Type>::new(name.into(), capacity))
            }
            VarType::Numeric(NumericType::Double) => {
                ColumnBuilder::Float64(PrimitiveChunkedBuilder::<Float64Type>::new(name.into(), capacity))
            }
            VarType::Str(_) | VarType::StrL => ColumnBuilder::Utf8(StringChunkedBuilder::new(name.into(), capacity)),
        };
        builders.push(builder);
        offsets.push(all_offsets[idx]);
        widths.push(metadata.storage_widths[idx] as usize);
        labels.push(label_map);
        scratch.push(match var.var_type {
            VarType::Str(_) => Some(StringScratch::new(metadata.encoding, metadata.storage_widths[idx] as usize)),
            _ => None,
        });
    }

    Ok((col_indices, builders, offsets, widths, labels, scratch))
}

fn append_value(
    builder: &mut ColumnBuilder,
    var_type: &VarType,
    buf: &[u8],
    endian: Endian,
    rules: crate::stata::value::MissingRules,
    missing_string_as_null: bool,
    strls: Option<&HashMap<(u32, u64), String>>,
    ds_format: u16,
    label_map: Option<&LabelMap>,
    encoding: &'static encoding_rs::Encoding,
    scratch: Option<&mut StringScratch>,
) -> Result<()> {
    match (builder, var_type) {
        (ColumnBuilder::Int8(b), VarType::Numeric(NumericType::Byte)) => {
            if let Some(v) = read_i8(buf, rules) {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        (ColumnBuilder::Int16(b), VarType::Numeric(NumericType::Int)) => {
            if let Some(v) = read_i16(buf, endian, rules) {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        (ColumnBuilder::Int32(b), VarType::Numeric(NumericType::Long)) => {
            if let Some(v) = read_i32(buf, endian, rules) {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        (ColumnBuilder::Utf8(b), VarType::Numeric(NumericType::Byte))
        | (ColumnBuilder::Utf8(b), VarType::Numeric(NumericType::Int))
        | (ColumnBuilder::Utf8(b), VarType::Numeric(NumericType::Long))
        | (ColumnBuilder::Utf8(b), VarType::Numeric(NumericType::Float))
        | (ColumnBuilder::Utf8(b), VarType::Numeric(NumericType::Double)) => {
            match var_type {
                VarType::Numeric(NumericType::Byte) => {
                    let v = read_i8(buf, rules).map(|v| v as i32);
                    append_labeled_int(b, v, label_map);
                }
                VarType::Numeric(NumericType::Int) => {
                    let v = read_i16(buf, endian, rules).map(|v| v as i32);
                    append_labeled_int(b, v, label_map);
                }
                VarType::Numeric(NumericType::Long) => {
                    let v = read_i32(buf, endian, rules);
                    append_labeled_int(b, v, label_map);
                }
                VarType::Numeric(NumericType::Float) => {
                    let v = read_f32(buf, endian, rules);
                    append_labeled_float(b, v.map(|v| v as f64), label_map);
                }
                VarType::Numeric(NumericType::Double) => {
                    let v = read_f64(buf, endian, rules);
                    append_labeled_float(b, v, label_map);
                }
                _ => b.append_null(),
            }
        }
        (ColumnBuilder::Float32(b), VarType::Numeric(NumericType::Float)) => {
            if let Some(v) = read_f32(buf, endian, rules) {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        (ColumnBuilder::Float64(b), VarType::Numeric(NumericType::Double)) => {
            if let Some(v) = read_f64(buf, endian, rules) {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        (ColumnBuilder::Utf8(b), VarType::Str(_)) => {
            let s = read_str_into(buf, encoding, scratch)?;
            if missing_string_as_null && s.is_empty() {
                b.append_null();
            } else {
                b.append_value(s);
            }
        }
        (ColumnBuilder::Utf8(b), VarType::StrL) => {
            let Some(strls) = strls else {
                if missing_string_as_null {
                    b.append_null();
                } else {
                    b.append_value("");
                }
                return Ok(());
            };

            let (v, o) = decode_strl_ref(buf, endian, ds_format)?;
                if let Some(s) = strls.get(&(v, o)) {
                    if missing_string_as_null && s.is_empty() {
                        b.append_null();
                    } else {
                        b.append_value(s);
                    }
            } else if missing_string_as_null {
                b.append_null();
            } else {
                b.append_value("");
            }
        }
        _ => return Err(Error::ParseError("column type mismatch".to_string())),
    }
    Ok(())
}

fn read_str_into<'a>(
    buf: &[u8],
    encoding: &'static encoding_rs::Encoding,
    scratch: Option<&'a mut StringScratch>,
) -> Result<&'a str> {
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let scratch = scratch.ok_or_else(|| Error::ParseError("missing string scratch".to_string()))?;
    scratch.buf.clear();
    let _ = scratch.decoder.decode_to_string(&buf[..len], &mut scratch.buf, true);
    scratch.reset(encoding);
    Ok(scratch.buf.as_str())
}

struct StringScratch {
    decoder: encoding_rs::Decoder,
    buf: String,
}

impl StringScratch {
    fn new(encoding: &'static encoding_rs::Encoding, capacity: usize) -> Self {
        Self {
            decoder: encoding.new_decoder_without_bom_handling(),
            buf: String::with_capacity(capacity),
        }
    }

    fn reset(&mut self, encoding: &'static encoding_rs::Encoding) {
        self.decoder = encoding.new_decoder_without_bom_handling();
    }
}

fn read_tag<R: Read>(reader: &mut R, tag: &[u8]) -> Result<()> {
    let mut buf = vec![0u8; tag.len()];
    reader.read_exact(&mut buf)?;
    if buf != tag {
        return Err(Error::ParseError(format!(
            "expected tag {:?}, got {:?}",
            String::from_utf8_lossy(tag),
            String::from_utf8_lossy(&buf)
        )));
    }
    Ok(())
}

fn load_strls(
    reader: &mut BufReader<File>,
    metadata: &Metadata,
    endian: Endian,
    ds_format: u16,
    encoding: &'static encoding_rs::Encoding,
) -> Result<Option<HashMap<(u32, u64), String>>> {
    let Some(strls_offset) = metadata.strls_offset else {
        return Ok(None);
    };
    if ds_format < 117 {
        return Ok(None);
    }

    reader.seek(SeekFrom::Start(strls_offset))?;
    read_tag(reader, b"<strls>")?;

    let mut map = HashMap::new();
    loop {
        let mut tag = [0u8; 3];
        reader.read_exact(&mut tag)?;
        if &tag == b"GSO" {
            let (v, o, data_type, len) = read_strl_header(reader, endian, ds_format)?;
            if len < 0 {
                return Err(Error::ParseError("negative strl length".to_string()));
            }
            let len = len as usize;
            if data_type == 0x82 {
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;
                let s = encoding::decode_string(&buf, encoding);
                map.insert((v, o), s);
            } else {
                reader.seek(SeekFrom::Current(len as i64))?;
            }
        } else if &tag == b"</s" {
            read_tag(reader, b"trls>")?;
            break;
        } else {
            return Err(Error::ParseError("invalid strls tag".to_string()));
        }
    }

    Ok(Some(map))
}

fn read_strl_header<R: Read>(
    reader: &mut R,
    endian: Endian,
    ds_format: u16,
) -> Result<(u32, u64, u8, i32)> {
    let v = read_u32_endian(reader, endian)?;
    let o = if ds_format >= 118 {
        read_u64_endian(reader, endian)?
    } else {
        read_u32_endian(reader, endian)? as u64
    };
    let mut typ = [0u8; 1];
    reader.read_exact(&mut typ)?;
    let len = read_i32_endian(reader, endian)?;
    Ok((v, o, typ[0], len))
}

fn decode_strl_ref(buf: &[u8], endian: Endian, ds_format: u16) -> Result<(u32, u64)> {
    if ds_format >= 118 {
        if buf.len() < 8 {
            return Err(Error::ParseError("strl ref too short".to_string()));
        }
        let v = if endian == Endian::Big {
            ((buf[0] as u16) << 8 | buf[1] as u16) as u32
        } else {
            (buf[0] as u16 | ((buf[1] as u16) << 8)) as u32
        };
        let o = if endian == Endian::Big {
            ((buf[2] as u64) << 40)
                | ((buf[3] as u64) << 32)
                | ((buf[4] as u64) << 24)
                | ((buf[5] as u64) << 16)
                | ((buf[6] as u64) << 8)
                | (buf[7] as u64)
        } else {
            (buf[2] as u64)
                | ((buf[3] as u64) << 8)
                | ((buf[4] as u64) << 16)
                | ((buf[5] as u64) << 24)
                | ((buf[6] as u64) << 32)
                | ((buf[7] as u64) << 40)
        };
        Ok((v, o))
    } else {
        let mut cursor = std::io::Cursor::new(buf);
        let v = read_u32_endian(&mut cursor, endian)?;
        let o = read_u32_endian(&mut cursor, endian)? as u64;
        Ok((v, o))
    }
}

fn read_u32_endian<R: Read>(reader: &mut R, endian: Endian) -> Result<u32> {
    Ok(match endian {
        Endian::Little => reader.read_u32::<byteorder::LittleEndian>()?,
        Endian::Big => reader.read_u32::<byteorder::BigEndian>()?,
    })
}

fn read_u64_endian<R: Read>(reader: &mut R, endian: Endian) -> Result<u64> {
    Ok(match endian {
        Endian::Little => reader.read_u64::<byteorder::LittleEndian>()?,
        Endian::Big => reader.read_u64::<byteorder::BigEndian>()?,
    })
}

fn read_i32_endian<R: Read>(reader: &mut R, endian: Endian) -> Result<i32> {
    Ok(match endian {
        Endian::Little => reader.read_i32::<byteorder::LittleEndian>()?,
        Endian::Big => reader.read_i32::<byteorder::BigEndian>()?,
    })
}

enum ColumnBuilder {
    Int8(PrimitiveChunkedBuilder<Int8Type>),
    Int16(PrimitiveChunkedBuilder<Int16Type>),
    Int32(PrimitiveChunkedBuilder<Int32Type>),
    Float32(PrimitiveChunkedBuilder<Float32Type>),
    Float64(PrimitiveChunkedBuilder<Float64Type>),
    Utf8(StringChunkedBuilder),
}

fn build_label_maps(metadata: &Metadata) -> HashMap<String, Arc<LabelMap>> {
    let mut out = HashMap::new();
    for vl in &metadata.value_labels {
        let mut map = LabelMap::default();
        for (key, value) in vl.mapping.iter() {
            if let crate::stata::types::ValueLabelKey::Integer(v) = key {
                map.insert_int(*v, value.clone());
                map.insert_float(*v as f64, value.clone());
            } else if let crate::stata::types::ValueLabelKey::Double(v) = key {
                map.insert_float(*v, value.clone());
            }
        }
        out.insert(vl.name.clone(), Arc::new(map));
    }
    out
}

#[derive(Default)]
struct LabelMap {
    int_map: HashMap<i32, String>,
    float_map: HashMap<u64, String>,
}

impl LabelMap {
    fn insert_int(&mut self, v: i32, label: String) {
        self.int_map.insert(v, label);
    }

    fn insert_float(&mut self, v: f64, label: String) {
        self.float_map.insert(v.to_bits(), label);
    }

    fn get_int(&self, v: i32) -> Option<&String> {
        self.int_map.get(&v)
    }

    fn get_float(&self, v: f64) -> Option<&String> {
        self.float_map.get(&v.to_bits())
    }
}

fn append_labeled_int(builder: &mut StringChunkedBuilder, v: Option<i32>, labels: Option<&LabelMap>) {
    if let Some(v) = v {
        if let Some(labels) = labels {
            if let Some(label) = labels.get_int(v) {
                builder.append_value(label);
                return;
            }
        }
        builder.append_value(&v.to_string());
    } else {
        builder.append_null();
    }
}

fn append_labeled_float(builder: &mut StringChunkedBuilder, v: Option<f64>, labels: Option<&LabelMap>) {
    if let Some(v) = v {
        if let Some(labels) = labels {
            if let Some(label) = labels.get_float(v) {
                builder.append_value(label);
                return;
            }
        }
        builder.append_value(&v.to_string());
    } else {
        builder.append_null();
    }
}

impl ColumnBuilder {
    fn finish(self) -> Series {
        match self {
            ColumnBuilder::Int8(b) => b.finish().into_series(),
            ColumnBuilder::Int16(b) => b.finish().into_series(),
            ColumnBuilder::Int32(b) => b.finish().into_series(),
            ColumnBuilder::Float32(b) => b.finish().into_series(),
            ColumnBuilder::Float64(b) => b.finish().into_series(),
            ColumnBuilder::Utf8(b) => b.finish().into_series(),
        }
    }
}
