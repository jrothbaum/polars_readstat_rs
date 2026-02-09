use crate::stata::types::Endian;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

#[derive(Debug, Clone, Copy)]
pub struct MissingRules {
    pub max_int8: i8,
    pub max_int16: i16,
    pub max_int32: i32,
    pub system_missing_enabled: bool,
    pub system_missing_int8: i8,
    pub system_missing_int16: i16,
    pub system_missing_int32: i32,
    pub max_float: u32,
    pub max_double: u64,
    pub missing_float: u32,
    pub missing_double: u64,
}

pub fn missing_rules(ds_format: u16) -> MissingRules {
    if ds_format < 113 {
        MissingRules {
            max_int8: 0x7e,
            max_int16: 0x7ffe,
            max_int32: 0x7ffffffe,
            system_missing_enabled: false,
            system_missing_int8: 0,
            system_missing_int16: 0,
            system_missing_int32: 0,
            max_float: 0x7effffff,
            max_double: 0x7fdfffffffffffff,
            missing_float: 0x7f000000,
            missing_double: 0x7fe0000000000000,
        }
    } else {
        // Match readstat behavior for Stata 113+:
        // only system missing values are treated as missing for integer types.
        MissingRules {
            max_int8: 0x7f,
            max_int16: 0x7fff,
            max_int32: 0x7fffffff,
            system_missing_enabled: true,
            system_missing_int8: 0x65,
            system_missing_int16: 0x7fe5,
            system_missing_int32: 0x7fffffe5,
            max_float: 0x7effffff,
            max_double: 0x7fdfffffffffffff,
            missing_float: 0x7f000000,
            missing_double: 0x7fe0000000000000,
        }
    }
}

pub fn read_i8(buf: &[u8], rules: MissingRules) -> Option<i8> {
    let v = buf[0] as i8;
    if rules.system_missing_enabled && v == rules.system_missing_int8 {
        return None;
    }
    if v > rules.max_int8 {
        None
    } else {
        Some(v)
    }
}

pub fn read_i16(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<i16> {
    let mut cursor = std::io::Cursor::new(buf);
    let v = match endian {
        Endian::Little => cursor.read_i16::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_i16::<BigEndian>().ok()?,
    };
    if rules.system_missing_enabled && v == rules.system_missing_int16 {
        return None;
    }
    if v > rules.max_int16 {
        None
    } else {
        Some(v)
    }
}

pub fn read_i32(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<i32> {
    let mut cursor = std::io::Cursor::new(buf);
    let v = match endian {
        Endian::Little => cursor.read_i32::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_i32::<BigEndian>().ok()?,
    };
    if rules.system_missing_enabled && v == rules.system_missing_int32 {
        return None;
    }
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
    let v = f32::from_bits(bits);
    let sign = (bits & 0x8000_0000) != 0;
    if !sign && bits > rules.max_float {
        if bits == rules.missing_float {
            None
        } else {
            Some(f32::NAN)
        }
    } else {
        Some(v)
    }
}

pub fn read_f64(buf: &[u8], endian: Endian, rules: MissingRules) -> Option<f64> {
    let mut cursor = std::io::Cursor::new(buf);
    let bits = match endian {
        Endian::Little => cursor.read_u64::<LittleEndian>().ok()?,
        Endian::Big => cursor.read_u64::<BigEndian>().ok()?,
    };
    let v = f64::from_bits(bits);
    let sign = (bits & 0x8000_0000_0000_0000) != 0;
    if !sign && bits > rules.max_double {
        if bits == rules.missing_double {
            None
        } else {
            Some(f64::NAN)
        }
    } else {
        Some(v)
    }
}

// Stata numeric values are stored in two's complement in practice.
