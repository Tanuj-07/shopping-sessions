//! Minimal Parquet writer: one row group, one uncompressed PLAIN data page (V1) per column.
//! Supports exactly the column kinds the two output tables need.

use std::io::Write;

pub enum Col<'a> {
    /// Required UTF8 string.
    Str(Vec<&'a [u8]>),
    /// Optional UTF8 string (null = None).
    OptStr(Vec<Option<&'a [u8]>>),
    /// Required DATE (days since epoch).
    Date(Vec<i32>),
    /// Required INT64.
    I64(Vec<i64>),
    /// Required INT32.
    I32(Vec<i32>),
    /// Required UTF8, already PLAIN-encoded (u32 length + bytes per value), and its row count.
    StrEnc(Vec<u8>, usize),
    /// Optional UTF8: presence bits (bit i of byte i/8 = row i) and the PLAIN-encoded present
    /// values, and the row count.
    OptStrEnc { bits: Vec<u8>, vals: Vec<u8>, rows: usize },
}

impl Col<'_> {
    fn len(&self) -> usize {
        match self {
            Col::Str(v) => v.len(),
            Col::OptStr(v) => v.len(),
            Col::Date(v) => v.len(),
            Col::I64(v) => v.len(),
            Col::I32(v) => v.len(),
            Col::StrEnc(_, n) => *n,
            Col::OptStrEnc { rows, .. } => *rows,
        }
    }
    fn physical(&self) -> i32 {
        match self {
            Col::Str(_) | Col::OptStr(_) | Col::StrEnc(..) | Col::OptStrEnc { .. } => 6, // BYTE_ARRAY
            Col::Date(_) | Col::I32(_) => 1,   // INT32
            Col::I64(_) => 2,                  // INT64
        }
    }
}

// Thrift compact protocol types.
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let d = id - *last;
        if d > 0 && d <= 15 {
            self.buf.push(((d as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            let z = ((id << 1) ^ (id >> 15)) as u16 as u64;
            self.varint(z);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn bin(&mut self, id: i16, b: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    /// Start an anonymous struct (a list element).
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn stop(&mut self) {
        self.buf.push(0);
    }
}

fn encode_values(col: &Col, out: &mut Vec<u8>) {
    match col {
        Col::Str(v) => {
            for s in v {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s);
            }
        }
        Col::OptStr(v) => {
            // Definition levels: RLE/bit-packed hybrid, bit width 1, prefixed by its byte length.
            let groups = v.len().div_ceil(8);
            let mut lv = Vec::with_capacity(groups + 10);
            let mut h = ((groups as u64) << 1) | 1;
            while h >= 0x80 {
                lv.push((h as u8) | 0x80);
                h >>= 7;
            }
            lv.push(h as u8);
            for g in 0..groups {
                let mut byte = 0u8;
                for k in 0..8 {
                    let i = g * 8 + k;
                    if i < v.len() && v[i].is_some() {
                        byte |= 1 << k;
                    }
                }
                lv.push(byte);
            }
            out.extend_from_slice(&(lv.len() as u32).to_le_bytes());
            out.extend_from_slice(&lv);
            for s in v.iter().flatten() {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s);
            }
        }
        // Fixed-width columns: PLAIN is the little-endian values back to back = one copy.
        Col::Date(v) => out.extend_from_slice(le_bytes(v)),
        Col::I64(v) => out.extend_from_slice(le_bytes(v)),
        Col::I32(v) => out.extend_from_slice(le_bytes(v)),
        Col::StrEnc(..) | Col::OptStrEnc { .. } => unreachable!("written from its parts"),
    }
}

/// Definition-level block header for `rows` rows given as presence bits: the block's byte
/// length, then the header varint of one bit-packed run (the bits themselves follow).
fn def_block(bits: &[u8], rows: usize) -> Vec<u8> {
    let groups = rows.div_ceil(8);
    assert_eq!(bits.len(), groups);
    let mut h = ((groups as u64) << 1) | 1;
    let mut hv = Vec::with_capacity(10);
    while h >= 0x80 {
        hv.push((h as u8) | 0x80);
        h >>= 7;
    }
    hv.push(h as u8);
    let mut lv = Vec::with_capacity(16);
    lv.extend_from_slice(&((hv.len() + groups) as u32).to_le_bytes());
    lv.extend_from_slice(&hv);
    lv
}

/// The column's encoded page data as byte slices (no copy for the big columns).
fn parts<'b>(col: &'b Col, tmp: &'b mut Vec<u8>) -> Vec<&'b [u8]> {
    match col {
        Col::Date(v) => vec![le_bytes(v)],
        Col::I32(v) => vec![le_bytes(v)],
        Col::I64(v) => vec![le_bytes(v)],
        Col::StrEnc(b, _) => vec![&b[..]],
        Col::OptStrEnc { bits, vals, rows } => {
            *tmp = def_block(bits, *rows);
            vec![&tmp[..], &bits[..], &vals[..]]
        }
        _ => {
            tmp.clear();
            tmp.reserve(encoded_len(col));
            encode_values(col, tmp);
            vec![&tmp[..]]
        }
    }
}

/// The values' bytes as stored (x86-64 is little-endian, which is what PLAIN wants).
fn le_bytes<T: Copy>(v: &[T]) -> &[u8] {
    #[cfg(target_endian = "big")]
    compile_error!("PLAIN encoding here assumes a little-endian target");
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// Exact size of `encode_values(col)`, so the page header can go first without a temp buffer.
fn encoded_len(col: &Col) -> usize {
    match col {
        Col::Str(v) => v.iter().map(|s| 4 + s.len()).sum(),
        Col::OptStr(v) => {
            let groups = v.len().div_ceil(8);
            4 + varint_len(((groups as u64) << 1) | 1) + groups + v.iter().flatten().map(|s| 4 + s.len()).sum::<usize>()
        }
        Col::Date(v) => 4 * v.len(),
        Col::I64(v) => 8 * v.len(),
        Col::I32(v) => 4 * v.len(),
        Col::StrEnc(b, _) => b.len(),
        Col::OptStrEnc { bits, vals, rows } => def_block(bits, *rows).len() + bits.len() + vals.len(),
    }
}

pub fn write(path: &std::path::Path, cols: &[(&str, Col)]) -> std::io::Result<()> {
    let rows = cols.first().map(|c| c.1.len()).unwrap_or(0);
    // Pieces go straight to the file (big ones bypass the 64 KB buffer): no file-sized copy.
    let mut out = std::io::BufWriter::with_capacity(64 << 10, std::fs::File::create(path)?);
    out.write_all(b"PAR1")?;
    let mut pos = 4usize;
    let mut chunks: Vec<(i64, i64)> = Vec::new(); // (offset, total size)
    for (_, col) in cols.iter() {
        assert_eq!(col.len(), rows);
        let mut tmp = Vec::new();
        let ps = parts(col, &mut tmp);
        let dlen: usize = ps.iter().map(|p| p.len()).sum();
        debug_assert_eq!(dlen, encoded_len(col));
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, dlen as i32);
        h.i32(3, dlen as i32);
        h.begin(5); // DataPageHeader
        h.i32(1, rows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE def levels
        h.i32(4, 3); // RLE rep levels
        h.end();
        h.stop();
        let off = pos as i64;
        out.write_all(&h.buf)?;
        for p in &ps {
            out.write_all(p)?;
        }
        pos += h.buf.len() + dlen;
        chunks.push((off, (h.buf.len() + dlen) as i64));
    }

    let mut m = Thrift::new();
    m.i32(1, 1); // version
    m.list(2, T_STRUCT, cols.len() + 1);
    m.begin_elem();
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32);
    m.end();
    for (name, col) in cols {
        m.begin_elem();
        m.i32(1, col.physical());
        m.i32(3, if matches!(col, Col::OptStr(_) | Col::OptStrEnc { .. }) { 1 } else { 0 });
        m.bin(4, name.as_bytes());
        match col {
            Col::Str(_) | Col::OptStr(_) | Col::StrEnc(..) | Col::OptStrEnc { .. } => {
                m.i32(6, 0); // converted UTF8
                m.begin(10); // LogicalType
                m.begin(1); // STRING
                m.end();
                m.end();
            }
            Col::Date(_) => {
                m.i32(6, 6); // converted DATE
                m.begin(10);
                m.begin(6); // DATE
                m.end();
                m.end();
            }
            Col::I64(_) | Col::I32(_) => {}
        }
        m.end();
    }
    m.i64(3, rows as i64);
    m.list(4, T_STRUCT, 1);
    m.begin_elem(); // RowGroup
    m.list(1, T_STRUCT, cols.len());
    let mut total = 0i64;
    for ((name, col), (off, size)) in cols.iter().zip(&chunks) {
        total += size;
        m.begin_elem(); // ColumnChunk
        m.i64(2, *off);
        m.begin(3); // ColumnMetaData
        m.i32(1, col.physical());
        m.list(2, T_I32, 2);
        m.varint(0); // PLAIN (zigzag 0)
        m.varint(6); // RLE (zigzag 3)
        m.list(3, T_BINARY, 1);
        m.varint(name.len() as u64);
        m.buf.extend_from_slice(name.as_bytes());
        m.i32(4, 0); // UNCOMPRESSED
        m.i64(5, rows as i64);
        m.i64(6, *size);
        m.i64(7, *size);
        m.i64(9, *off);
        m.end();
        m.end();
    }
    m.i64(2, total);
    m.i64(3, rows as i64);
    m.end();
    m.bin(6, b"shopping-sessions");
    m.stop();

    out.write_all(&m.buf)?;
    out.write_all(&(m.buf.len() as u32).to_le_bytes())?;
    out.write_all(b"PAR1")?;
    out.flush()
}
