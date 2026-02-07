use crate::stata::types::Endian;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

#[derive(Debug, Clone, Copy)]
pub struct MissingRules {
    pub max_int8: i8,
    pub max_int16: i16,
    pub max_int32: i32,
    pub max_float: u32,
    pub max_double: u64,
}

pub fn missing_rules(ds_format: u16) -> MissingRules {
    if ds_format < 113 {
        MissingRules {
            max_int8: 0x7e,
            max_int16: 0x7ffe,
            max_int32: 0x7ffffffe,
            max_float: 0x7effffff,
            max_double: 0x7fdfffffffffffff,
        }
    } else {
        MissingRules {
            max_int8: 0x64,
            max_int16: 0x7fe4,
            max_int32: 0x7fffffe4,
            max_float: 0x7effffff,
            max_double: 0x7fdfffffffffffff,
        }
    }
}

pub fn read_i8(buf: &[u8], rules: MissingRules) -> Option<i8> {
    let mut v = buf[0] as i8;
    v = ones_to_twos_i8(v);
    if v > rules.max_int8 {
        None
    } else {
        Some(v)
    }
}

pub fn read_i16(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<i16> {
    let mut cursor = std::io::Cursor::new(buf);
    let mut v = match endian {
        Endian::Little => cursor.read_i16::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_i16::<BigEndian>().ok()?,
    };
    v = ones_to_twos_i16(v);
    if v > rules.max_int16 {
        None
    } else {
        Some(v)
    }
}

pub fn read_i32(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<i32> {
    let mut cursor = std::io::Cursor::new(buf);
    let mut v = match endian {
        Endian::Little => cursor.read_i32::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_i32::<BigEndian>().ok()?,
    };
    v = ones_to_twos_i32(v);
    if v > rules.max_int32 {
        None
    } else {
        Some(v)
    }
}

pub fn read_f32(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<f32> {
    let mut cursor = std::io::Cursor::new(buf);
    let bits = match endian {
        Endian::Little => cursor.read_u32::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_u32::<BigEndian>().ok()?,
    };
    if bits > rules.max_float {
        None
    } else {
        Some(f32::from_bits(bits))
    }
}

pub fn read_f64(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<f64> {
    let mut cursor = std::io::Cursor::new(buf);
    let bits = match endian {
        Endian::Little => cursor.read_u64::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_u64::<BigEndian>().ok()?,
    };
    if bits > rules.max_double {
        None
    } else {
        Some(f64::from_bits(bits))
    }
}

fn ones_to_twos_i8(v: i8) -> i8 {
    if v < 0 { v + 1 } else { v }
}

fn ones_to_twos_i16(v: i16) -> i16 {
    if v < 0 { v + 1 } else { v }
}

fn ones_to_twos_i32(v: i32) -> i32 {
    if v < 0 { v + 1 } else { v }
}
