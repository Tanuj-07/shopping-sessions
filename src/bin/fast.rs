//! Shopping Sessions: parallel version (v2).
//!
//! IDs up to 15 bytes become exact 16-byte keys whose numeric order is byte-wise order
//! (bytes big-endian in the high bytes, length in the low byte). Longer IDs become a
//! 128-bit hash key with low byte 0xFF, and their bytes are kept in a registry.
//!
//! Stages:
//!  1. links  -> partitioned id tables (parallel) -> lock-free union-find (root = smallest id)
//!  2. events -> SIMD line scan in parallel chunks -> partition by event key
//!  3. dedup each event bucket (cache-sized table) -> valid -> person key -> partition by person
//!  4. clicks -> SIMD line scan -> person key -> partition by person
//!  5. each person bucket: sort, sessionize, attribute, aggregate
//!  6. write Parquet files in parallel

use std::cell::RefCell;
use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering::*};
use std::sync::Mutex;
use std::time::Instant;

use memchr::memchr;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use shopping_sessions::pq::{self, Col};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: u32 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const EB: u32 = 10; // event dedup bucket bits
const PB: u32 = 10; // person bucket bits
const LB: u32 = 8; // link id table bits
const CHUNK: usize = 4 << 20;

// ---------------------------------------------------------------- hashing

#[inline]
fn r8(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}
#[inline]
fn r4(b: &[u8], i: usize) -> u64 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as u64
}
#[inline]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}
fn hash_seed(b: &[u8], mut seed: u64) -> u64 {
    const S1: u64 = 0xe703_7ed1_a0b4_28db;
    let n = b.len();
    let (a, c);
    if n <= 16 {
        if n >= 4 {
            let q = (n >> 3) << 2;
            a = (r4(b, 0) << 32) | r4(b, q);
            c = (r4(b, n - 4) << 32) | r4(b, n - 4 - q);
        } else if n > 0 {
            a = ((b[0] as u64) << 16) | ((b[n >> 1] as u64) << 8) | (b[n - 1] as u64);
            c = 0;
        } else {
            a = 0;
            c = 0;
        }
    } else {
        let mut p = 0;
        let mut i = n;
        while i > 16 {
            seed = mum(r8(b, p) ^ S1, r8(b, p + 8) ^ seed);
            p += 16;
            i -= 16;
        }
        a = r8(b, n - 16);
        c = r8(b, n - 8);
    }
    mum(S1 ^ (n as u64), mum(a ^ S1, c ^ seed))
}

// ---------------------------------------------------------------- keys

/// An exact 8-byte ID key.
///
/// Compact form (top bit 0): an ID of 1..=12 characters from [0-9a-z] as a base-37 number,
/// big-endian, with the characters mapped '0'..'9' -> 1..10, 'a'..'z' -> 11..36 and unused
/// trailing positions 0. Numeric order is then exactly byte-wise order, and equal keys are
/// equal IDs. Every ID in the generated data has this form (<= 9 chars).
///
/// Fallback form (top bit 1): anything else (upper case, spaces, '\r', long IDs) becomes a
/// 63-bit hash; its bytes go into a registry so ordering and output stay exact.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Debug, Hash)]
struct K(u64);

const FALLBACK: u64 = 1 << 63;
const P4: u64 = 37 * 37 * 37 * 37;
const P8: u64 = P4 * P4;

impl K {
    const EMPTY: K = K(0);
    #[inline]
    fn is_empty(self) -> bool {
        self.0 == 0
    }
    #[inline]
    fn is_long(self) -> bool {
        self.0 & FALLBACK != 0
    }
    #[inline]
    fn mix(self) -> u64 {
        mum(self.0 ^ 0x9E37_79B9_7F4A_7C15, 0xD6E8_FEB8_6659_FD93)
    }
}

/// Compact key for buf[off..off+len] (len >= 1), or None if it needs the fallback form.
/// Reads 16 bytes from `off`; every caller's buffer has at least 16 bytes of slack.
#[inline(always)]
fn compact_key(buf: &[u8], off: usize, len: usize) -> Option<K> {
    if len > 12 {
        return None;
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
    unsafe {
        use std::arch::x86_64::*;
        debug_assert!(off + 16 <= buf.len());
        let v = _mm_loadu_si128(buf.as_ptr().add(off) as *const __m128i);
        let iota = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        let inlen = _mm_cmpgt_epi8(_mm_set1_epi8(len as i8), iota);
        let isd = _mm_and_si128(_mm_cmpgt_epi8(v, _mm_set1_epi8(b'0' as i8 - 1)), _mm_cmpgt_epi8(_mm_set1_epi8(b'9' as i8 + 1), v));
        let isa = _mm_and_si128(_mm_cmpgt_epi8(v, _mm_set1_epi8(b'a' as i8 - 1)), _mm_cmpgt_epi8(_mm_set1_epi8(b'z' as i8 + 1), v));
        let lm = _mm_movemask_epi8(inlen);
        if _mm_movemask_epi8(_mm_and_si128(_mm_or_si128(isd, isa), inlen)) != lm {
            return None;
        }
        // letters: c - 86 (a -> 11); digits: c - 47 (0 -> 1) = (c - 86) + 39, all mod 256.
        let d = _mm_add_epi8(_mm_sub_epi8(v, _mm_set1_epi8(86)), _mm_and_si128(isd, _mm_set1_epi8(39)));
        let d = _mm_and_si128(d, inlen);
        let x = _mm_maddubs_epi16(d, _mm_setr_epi8(37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1));
        let y = _mm_madd_epi16(x, _mm_setr_epi16(1369, 1, 1369, 1, 1369, 1, 1369, 1));
        let l0 = _mm_cvtsi128_si32(y) as u32 as u64;
        let l1 = _mm_extract_epi32(y, 1) as u32 as u64;
        let l2 = _mm_extract_epi32(y, 2) as u32 as u64;
        return Some(K(l0 * P8 + l1 * P4 + l2));
    }
    #[allow(unreachable_code)]
    {
        let mut v: u64 = 0;
        for i in 0..12 {
            let d = if i < len {
                match buf[off + i] {
                    c @ b'0'..=b'9' => (c - b'0' + 1) as u64,
                    c @ b'a'..=b'z' => (c - b'a' + 11) as u64,
                    _ => return None,
                }
            } else {
                0
            };
            v = v * 37 + d;
        }
        Some(K(v))
    }
}

fn decode_compact(k: K, out: &mut Vec<u8>) {
    let mut digs = [0u8; 12];
    let mut v = k.0;
    for i in (0..12).rev() {
        digs[i] = (v % 37) as u8;
        v /= 37;
    }
    for &d in &digs {
        if d == 0 {
            break;
        }
        out.push(if d <= 10 { b'0' + d - 1 } else { b'a' + d - 11 });
    }
}

fn fallback_key(b: &[u8]) -> K {
    K(FALLBACK | (hash_seed(b, 0x8ebc_6af0_9c88_c6e3) >> 1))
}

/// Bytes of fallback keys, for ordering and output.
struct Reg(Mutex<FxHashMap<K, Vec<u8>>>);

#[inline(always)]
fn key_of(buf: &[u8], off: usize, len: usize, reg: Option<&Reg>) -> K {
    if len == 0 {
        return K::EMPTY;
    }
    if let Some(k) = compact_key(buf, off, len) {
        return k;
    }
    let b = &buf[off..off + len];
    let k = fallback_key(b);
    if let Some(r) = reg {
        r.0.lock().unwrap().entry(k).or_insert_with(|| b.to_vec());
    }
    k
}

fn key_bytes(k: K, reg: &Reg) -> Vec<u8> {
    if k.is_long() {
        reg.0.lock().unwrap().get(&k).expect("fallback key registered").clone()
    } else {
        let mut v = Vec::with_capacity(12);
        decode_compact(k, &mut v);
        v
    }
}

#[inline]
fn cmp_keys(a: K, b: K, reg: &Reg) -> Ordering {
    if !a.is_long() && !b.is_long() {
        a.cmp(&b)
    } else {
        key_bytes(a, reg).cmp(&key_bytes(b, reg))
    }
}

// ---------------------------------------------------------------- CSV scanning

/// Slow path for lines with quotes: field ranges relative to the line, quotes stripped.
fn split<const N: usize>(line: &[u8], out: &mut [(usize, usize); N]) -> bool {
    let mut start = 0;
    for (i, slot) in out.iter_mut().enumerate() {
        let end = if i + 1 < N {
            match memchr(b',', &line[start..]) {
                Some(p) => start + p,
                None => return false,
            }
        } else {
            if memchr(b',', &line[start..]).is_some() {
                return false;
            }
            line.len()
        };
        *slot = (start, end);
        start = end + 1;
    }
    for f in out.iter_mut() {
        let (s, e) = *f;
        let v = &line[s..e];
        if memchr(b'"', v).is_some() {
            if v.len() < 2 || v[0] != b'"' || v[v.len() - 1] != b'"' || memchr(b'"', &v[1..v.len() - 1]).is_some() {
                return false;
            }
            *f = (s + 1, e - 1);
        }
    }
    true
}

/// Strip quotes from fields that are wrapped in them; false if the line breaks the quote rule.
/// A quote may only open and close a whole field. Every field that starts with a quote must
/// end with one; if the line holds any other quote, the count `nq` will not match.
/// Branch-free: every field is checked the same way, no per-field if/else.
#[inline(always)]
fn fix_quotes<const N: usize>(buf: &[u8], fb: &mut [(usize, usize); N], nq: usize) -> bool {
    let mut expect = 0usize;
    let mut bad = false;
    for x in fb.iter_mut() {
        let (s, e) = *x;
        let nonempty = (e > s) as usize;
        let qs = nonempty & (buf[s] == b'"') as usize;
        let qe = nonempty & (buf[e - 1] == b'"') as usize;
        // a quote must open and close the field, and `"` alone is not a quoted field
        bad |= (qs != qe) | ((qs == 1) & (e - s < 2));
        expect += 2 * qs;
        *x = (s + qs, e - qs);
    }
    !bad && expect == nq
}

thread_local! {
    /// Per-core scan index: separator positions, newline positions, quotes before each newline.
    static SIDX: RefCell<(Vec<u32>, Vec<u32>, Vec<u32>)> = const { RefCell::new((Vec::new(), Vec::new(), Vec::new())) };
}

static SLAB_SZ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(32 << 10); // TEMP: tuning via SS_SLAB

/// Calls `f(fields)` with absolute field ranges for every well-formed line of buf[s..e].
#[inline(always)]
fn scan_fields<const N: usize>(buf: &[u8], s: usize, e: usize, mut f: impl FnMut(&[(usize, usize); N])) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            SIDX.with(|ix| {
                let mut ix = ix.borrow_mut();
                let (seps, nls, qs) = &mut *ix;
                let mut p = s;
                while p < e {
                    // A slab ends right after a newline (or at e), so no line crosses slabs.
                    let lim = (p + SLAB_SZ.load(Relaxed)).min(e);
                    let q = if lim == e {
                        e
                    } else {
                        match memchr::memrchr(b'\n', &buf[p..lim]) {
                            Some(i) => p + i + 1,
                            None => match memchr(b'\n', &buf[lim..e]) {
                                Some(i) => lim + i + 1,
                                None => e,
                            },
                        }
                    };
                    unsafe { index_slab_avx2(buf, p, q, seps, nls, qs) };
                    walk_slab::<N>(buf, p, seps, nls, qs, &mut f);
                    p = q;
                }
            });
            return;
        }
    }
    let mut fb = [(0usize, 0usize); N];
    let mut line = |ls: usize, le: usize, cm: &[usize; N], nc: usize, nq: usize| {
        if nc != N - 1 {
            return;
        }
        fb[0] = (ls, cm[0]);
        for i in 1..N - 1 {
            fb[i] = (cm[i - 1] + 1, cm[i]);
        }
        fb[N - 1] = (cm[N - 2] + 1, le);
        if nq != 0 && !fix_quotes(buf, &mut fb, nq) {
            return;
        }
        f(&fb);
    };
    scan_scalar::<N>(buf, s, e, &mut line);
}

/// Pass 1: record every separator (comma or newline) position, and every newline position
/// with the number of quotes before it, for buf[p..q]. Positions are relative to p.
/// No per-character branching: set bits are written out unconditionally, four at a time.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn index_slab_avx2(buf: &[u8], p: usize, q: usize, seps: &mut Vec<u32>, nls: &mut Vec<u32>, qs: &mut Vec<u32>) {
    use std::arch::x86_64::*;
    let len = q - p;
    seps.clear();
    nls.clear();
    qs.clear();
    seps.reserve(len + 72);
    nls.reserve(len + 8);
    qs.reserve(len + 8);
    let sp = seps.as_mut_ptr();
    let np = nls.as_mut_ptr();
    let qp = qs.as_mut_ptr();
    let (mut ns, mut nn) = (0usize, 0usize);
    let mut qtot: u32 = 0;
    let vnl = _mm256_set1_epi8(b'\n' as i8);
    let vcm = _mm256_set1_epi8(b',' as i8);
    let vqt = _mm256_set1_epi8(b'"' as i8);
    let mut tail = [0u8; 64];
    let mut off = 0usize;
    while off < len {
        let ptr = if off + 64 <= len {
            buf.as_ptr().add(p + off)
        } else {
            let n = len - off;
            tail[..n].copy_from_slice(&buf[p + off..q]);
            tail[n..].fill(0);
            tail.as_ptr()
        };
        let a = _mm256_loadu_si256(ptr as *const __m256i);
        let b = _mm256_loadu_si256(ptr.add(32) as *const __m256i);
        let mask = |v: __m256i| -> u64 {
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64;
            lo | (hi << 32)
        };
        let m_nl = mask(vnl);
        let m_c = mask(vcm);
        let m_q = mask(vqt);
        let base = off as u32;

        let sep = m_c | m_nl;
        let cnt = sep.count_ones() as usize;
        let out = sp.add(ns);
        let mut bits = sep;
        let mut i = 0;
        while i < cnt {
            *out.add(i) = base + bits.trailing_zeros();
            bits &= bits.wrapping_sub(1);
            *out.add(i + 1) = base + bits.trailing_zeros();
            bits &= bits.wrapping_sub(1);
            *out.add(i + 2) = base + bits.trailing_zeros();
            bits &= bits.wrapping_sub(1);
            *out.add(i + 3) = base + bits.trailing_zeros();
            bits &= bits.wrapping_sub(1);
            i += 4;
        }
        ns += cnt;

        let mut bits = m_nl;
        while bits != 0 {
            let t = bits.trailing_zeros();
            *np.add(nn) = base + t;
            *qp.add(nn) = qtot + (m_q & ((1u64 << t) - 1)).count_ones();
            nn += 1;
            bits &= bits - 1;
        }
        qtot += m_q.count_ones();
        off += 64;
    }
    // The file's last line may have no newline: end it with a virtual one.
    if len > 0 && buf[q - 1] != b'\n' {
        *sp.add(ns) = len as u32;
        ns += 1;
        *np.add(nn) = len as u32;
        *qp.add(nn) = qtot;
        nn += 1;
    }
    seps.set_len(ns);
    nls.set_len(nn);
    qs.set_len(nn);
}

/// Pass 2: walk the lines. A line is well-formed exactly when its N-th separator is its newline.
#[inline(always)]
fn walk_slab<const N: usize>(buf: &[u8], p: usize, seps: &[u32], nls: &[u32], qs: &[u32], f: &mut impl FnMut(&[(usize, usize); N])) {
    let mut fb = [(0usize, 0usize); N];
    let mut k = 0usize;
    let mut ls = 0u32;
    let mut qprev = 0u32;
    for j in 0..nls.len() {
        let le = nls[j];
        let nq = (qs[j] - qprev) as usize;
        qprev = qs[j];
        if k + N - 1 < seps.len() && seps[k + N - 1] == le {
            let sv = &seps[k..k + N];
            fb[0] = (p + ls as usize, p + sv[0] as usize);
            for i in 1..N {
                fb[i] = (p + sv[i - 1] as usize + 1, p + sv[i] as usize);
            }
            k += N;
            if nq == 0 || fix_quotes(buf, &mut fb, nq) {
                f(&fb);
            }
        } else {
            // Wrong number of commas: skip to just after this line's newline.
            while seps[k] != le {
                k += 1;
            }
            k += 1;
        }
        ls = le + 1;
    }
}

fn scan_scalar<const N: usize>(buf: &[u8], s: usize, e: usize, f: &mut impl FnMut(usize, usize, &[usize; N], usize, usize)) {
    let mut cm = [0usize; N];
    let mut nc = 0usize;
    let mut nq = 0usize;
    let mut ls = s;
    for p in s..e {
        match buf[p] {
            b',' => {
                if nc < N {
                    cm[nc] = p;
                }
                nc += 1;
            }
            b'\n' => {
                f(ls, p, &cm, nc, nq);
                ls = p + 1;
                nc = 0;
                nq = 0;
            }
            b'"' => nq += 1,
            _ => {}
        }
    }
    if ls < e {
        f(ls, e, &cm, nc, nq);
    }
}

/// A byte range of one file. The chunk owns every line that starts inside [start, end)
/// (the header, which starts at 0, is never owned), reading past `end` to finish its last line.
struct Chunk {
    file: usize,
    start: usize,
    end: usize,
}

fn make_chunks(sizes: &[usize], files: std::ops::Range<usize>) -> Vec<Chunk> {
    let mut out = Vec::new();
    for f in files {
        let n = sizes[f];
        let mut s = 0;
        while s < n {
            let e = (s + CHUNK).min(n);
            out.push(Chunk { file: f, start: s, end: e });
            s = e;
        }
    }
    // Biggest first helps the work-stealing balance at the end.
    out.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
    out
}

const EXTRA: usize = 64 << 10;

thread_local! {
    /// Per-core read buffer, reused for every chunk.
    static RBUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    /// Per-core dedup table (keys, record index), reused for every bucket.
    static DSCR: RefCell<(Vec<K>, Vec<u32>)> = const { RefCell::new((Vec::new(), Vec::new())) };
    /// Per-core session sort buffers, reused for every bucket.
    static SSCR: RefCell<(Vec<PEv>, Vec<PCk>)> = const { RefCell::new((Vec::new(), Vec::new())) };
}

fn read_full(f: &fs::File, buf: &mut [u8], off: u64) -> usize {
    let mut got = 0;
    while got < buf.len() {
        #[cfg(unix)]
        let r = std::os::unix::fs::FileExt::read_at(f, &mut buf[got..], off + got as u64);
        #[cfg(windows)]
        let r = std::os::windows::fs::FileExt::seek_read(f, &mut buf[got..], off + got as u64);
        match r {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("read failed: {e}"),
        }
    }
    got
}

/// Read chunk `c` into `buf`; returns the byte range of its whole lines inside `buf`.
fn load_chunk(path: &Path, size: usize, c: &Chunk, buf: &mut Vec<u8>) -> Option<(usize, usize)> {
    // Start one byte early so we can see whether `start` begins a line.
    let r0 = c.start.max(1) - 1;
    let f = fs::File::open(path).unwrap();
    let mut want = c.end - r0 + EXTRA;
    loop {
        let len = want.min(size - r0);
        if buf.len() < len + 16 {
            buf.resize(len + 16, 0); // grows only; zero-filled once per core
        }
        let got = read_full(&f, &mut buf[..len], r0 as u64);
        let first = memchr(b'\n', &buf[..got])? + 1;
        if r0 + first >= c.end {
            return None; // no line starts inside this chunk
        }
        let from = c.end - 1 - r0;
        match memchr(b'\n', &buf[from..got]) {
            Some(i) => return Some((first, from + i + 1)),
            None if r0 + got >= size => return Some((first, got)),
            None => want *= 2, // a very long last line: read further
        }
    }
}

// ---------------------------------------------------------------- timestamps

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (a, c) = (b[i].wrapping_sub(b'0'), b[i + 1].wrapping_sub(b'0'));
    if a < 10 && c < 10 {
        Some(a as i64 * 10 + c as i64)
    } else {
        None
    }
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if (n == 20 || n == 25) && b[4] == b'-' {
        if b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
            return None;
        }
        let y = d2(b, 0)? * 100 + d2(b, 2)?;
        let mo = d2(b, 5)?;
        let d = d2(b, 8)?;
        let h = d2(b, 11)?;
        let mi = d2(b, 14)?;
        let s = d2(b, 17)?;
        if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
            return None;
        }
        let off_ms = if n == 20 {
            if b[19] != b'Z' {
                return None;
            }
            0
        } else {
            let sign = match b[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            let oh = d2(b, 20)?;
            if b[22] != b':' {
                return None;
            }
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            sign * (oh * 60 + om) * 60_000
        };
        return Some((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s) * 1000 - off_ms);
    }
    if n == 0 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // keep scanning only to reject non-digits; value is already too big
            v = 10_000_001;
        }
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

// ---------------------------------------------------------------- records

/// Event copy before dedup. ts == u32::MAX marks an invalid copy.
#[derive(Clone, Copy)]
struct ERaw {
    ek: K,
    uk: K,
    ingest: u32,
    ts: u32,
    pc: u32, // price | cat << 24; 0 for view/cart
}

#[derive(Clone, Copy)]
struct PEv {
    pk: K,
    ts: u32,
    pc: u32,
}

#[derive(Clone, Copy)]
struct PCk {
    pk: K,
    ck: K,
    ts: u32,
    camp: u32,
}

struct UdRow {
    pk: K,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: u16,
}

/// Per-thread bucketed output buffers; each rayon thread only touches its own slot.
struct PerThread<T> {
    slots: Vec<Mutex<Vec<Vec<T>>>>,
}

impl<T: Send> PerThread<T> {
    fn new(nt: usize, nb: usize, reserve: usize) -> Self {
        PerThread {
            slots: (0..nt).map(|_| Mutex::new((0..nb).map(|_| Vec::with_capacity(reserve)).collect())).collect(),
        }
    }
    fn with<R>(&self, f: impl FnOnce(&mut Vec<Vec<T>>) -> R) -> R {
        let t = rayon::current_thread_index().unwrap_or(0);
        let mut g = self.slots[t].lock().unwrap();
        f(&mut g)
    }
    fn into_inner(self) -> Vec<Vec<Vec<T>>> {
        self.slots.into_iter().map(|m| m.into_inner().unwrap()).collect()
    }
}

// ---------------------------------------------------------------- person map

struct Sub {
    tab: Vec<(K, K)>,
    mask: usize,
}

struct PMap {
    subs: Vec<Sub>,
}

impl PMap {
    #[inline]
    fn get(&self, k: K) -> K {
        let h = k.mix();
        let s = &self.subs[(h >> (64 - LB)) as usize];
        if s.tab.is_empty() {
            return k;
        }
        let mut i = (h as usize) & s.mask;
        loop {
            let e = unsafe { s.tab.get_unchecked(i) };
            if e.0 == k {
                return e.1;
            }
            if e.0.is_empty() {
                return k;
            }
            i = (i + 1) & s.mask;
        }
    }
}

fn uf_find(p: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let px = p[x as usize].load(Relaxed);
        if px == x {
            return x;
        }
        let gpx = p[px as usize].load(Relaxed);
        if gpx != px {
            let _ = p[x as usize].compare_exchange_weak(px, gpx, Relaxed, Relaxed);
        }
        x = gpx;
    }
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().map(|x| x == "csv").unwrap_or(false))
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn manifest_value(text: &str, key: &str) -> i64 {
    let k = format!("\"{key}\"");
    let i = text.find(&k).expect("manifest key") + k.len();
    let rest = &text[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = rest[a..].find('"').unwrap() + a;
    parse_ts(rest[a..b].as_bytes()).expect("manifest timestamp")
}

fn small_lines(buf: &[u8]) -> Vec<&[u8]> {
    let mut v: Vec<&[u8]> = buf.split(|&c| c == b'\n').skip(1).collect();
    if buf.last() == Some(&b'\n') {
        v.pop();
    }
    v
}

fn main() {
    let t0 = Instant::now();
    let mut last = t0;
    let mut lap = |name: &str| {
        let now = Instant::now();
        eprintln!("{:>10.3}s  {:>8.3}s  {}", (now - t0).as_secs_f64(), (now - last).as_secs_f64(), name);
        last = now;
    };

    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
        .clamp(1, 31);
    rayon::ThreadPoolBuilder::new().num_threads(nt).build_global().unwrap();
    if let Some(v) = std::env::var("SS_SLAB").ok().and_then(|s| s.parse::<usize>().ok()) {
        SLAB_SZ.store(v << 10, Relaxed);
    }

    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    let base = ws - WEEK_MS; // all stored ts are ms since base
    let base_s = base.div_euclid(1000);
    let day_lo = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - day_lo + 1).max(1) as usize;

    let ev_files = csv_files(&data.join("events"));
    let ck_files = csv_files(&data.join("clicks"));
    let ln_files = csv_files(&data.join("links"));
    let paths: Vec<PathBuf> = ev_files.iter().chain(&ck_files).chain(&ln_files).cloned().collect();
    let sizes: Vec<usize> = paths.par_iter().map(|p| fs::metadata(p).unwrap().len() as usize).collect();
    let (paths, sizes) = (&paths[..], &sizes[..]);
    let (ne, nc) = (ev_files.len(), ck_files.len());
    let ev_bytes: usize = sizes[..ne].iter().sum();
    let ck_bytes: usize = sizes[ne..ne + nc].iter().sum();
    // Run `f(buf, start, end)` over the whole lines of chunk `c`, in this core's reused buffer.
    let with_chunk = |c: &Chunk, f: &mut dyn FnMut(&[u8], usize, usize)| {
        RBUF.with(|rb| {
            let mut rb = rb.borrow_mut();
            if let Some((s, e)) = load_chunk(&paths[c.file], sizes[c.file], c, &mut rb) {
                f(&rb[..], s, e);
            }
        })
    };

    let products_buf = fs::read(data.join("products.csv")).expect("products");
    let campaigns_buf = fs::read(data.join("campaigns.csv")).expect("campaigns");
    let mut prod_raw: Vec<(&[u8], &[u8])> = Vec::new();
    let mut f2 = [(0usize, 0usize); 2];
    for line in small_lines(&products_buf) {
        if split(line, &mut f2) && f2[0].0 < f2[0].1 && f2[1].0 < f2[1].1 {
            prod_raw.push((&line[f2[0].0..f2[0].1], &line[f2[1].0..f2[1].1]));
        }
    }
    let mut cat_names: Vec<&[u8]> = prod_raw.iter().map(|p| p.1).collect();
    cat_names.push(b"unknown");
    cat_names.sort_unstable();
    cat_names.dedup();
    assert!(cat_names.len() <= 255, "too many categories");
    let ncat = cat_names.len();
    let cat_index = |c: &[u8]| cat_names.binary_search(&c).unwrap() as u32;
    let unknown_cat = cat_index(b"unknown");
    let mut prod_cat: FxHashMap<&[u8], u32> = FxHashMap::default();
    for (p, c) in &prod_raw {
        prod_cat.insert(p, cat_index(c));
    }
    let mut camp_ids: Vec<&[u8]> = Vec::new();
    let mut camp_chan: Vec<&[u8]> = Vec::new();
    let mut camp_idx: FxHashMap<&[u8], u32> = FxHashMap::default();
    for line in small_lines(&campaigns_buf) {
        if split(line, &mut f2) && f2[0].0 < f2[0].1 && f2[1].0 < f2[1].1 {
            let c = &line[f2[0].0..f2[0].1];
            if !camp_idx.contains_key(c) {
                camp_idx.insert(c, camp_ids.len() as u32);
                camp_ids.push(c);
                camp_chan.push(&line[f2[1].0..f2[1].1]);
            }
        }
    }
    let ncamp = camp_ids.len();
    let organic = ncamp as u32;
    let reg = Reg(Mutex::new(FxHashMap::default()));
    let reg = &reg;
    lap("setup");

    // ------------------------------------------------------------ 1. links
    let ln_chunks = make_chunks(sizes, ne + nc..paths.len());
    let edges: Vec<Vec<(K, K)>> = ln_chunks
        .par_iter()
        .map(|c| {
            let mut v = Vec::new();
            with_chunk(c, &mut |buf, s, e| {
                scan_fields::<2>(buf, s, e, |fb| {
                    let (a0, a1) = fb[0];
                    let (b0, b1) = fb[1];
                    if a0 == a1 || b0 == b1 {
                        return;
                    }
                    v.push((key_of(buf, a0, a1 - a0, Some(reg)), key_of(buf, b0, b1 - b0, Some(reg))));
                });
            });
            v
        })
        .collect();
    let nlb = 1usize << LB;
    let lk: PerThread<K> = PerThread::new(nt, nlb, 0);
    edges.par_iter().for_each(|v| {
        lk.with(|g| {
            for &(a, b) in v {
                g[(a.mix() >> (64 - LB)) as usize].push(a);
                g[(b.mix() >> (64 - LB)) as usize].push(b);
            }
        })
    });
    let lk = lk.into_inner();
    // Per-sub tables with local index, then offset to global ids.
    let mut built: Vec<(Sub, Vec<K>)> = (0..nlb)
        .into_par_iter()
        .map(|s| {
            let n: usize = lk.iter().map(|t| t[s].len()).sum();
            if n == 0 {
                return (Sub { tab: Vec::new(), mask: 0 }, Vec::new());
            }
            let cap = (n * 2).next_power_of_two().max(4);
            let mask = cap - 1;
            let mut tab = vec![(K::EMPTY, K::EMPTY); cap];
            let mut keys = Vec::new();
            for t in &lk {
                for &k in &t[s] {
                    let mut i = (k.mix() as usize) & mask;
                    loop {
                        let e = &mut tab[i];
                        if e.0 == k {
                            break;
                        }
                        if e.0.is_empty() {
                            *e = (k, K(keys.len() as u64));
                            keys.push(k);
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            (Sub { tab, mask }, keys)
        })
        .collect();
    std::mem::forget(lk); // freeing is pointless work: the OS reclaims it at exit
    let mut offs = Vec::with_capacity(nlb);
    let mut nid = 0usize;
    for b in &built {
        offs.push(nid);
        nid += b.1.len();
    }
    built.par_iter_mut().zip(offs.par_iter()).for_each(|(b, &o)| {
        for e in b.0.tab.iter_mut() {
            if !e.0.is_empty() {
                e.1 .0 += o as u64;
            }
        }
    });
    let mut ids: Vec<K> = Vec::with_capacity(nid);
    for b in built.iter_mut() {
        ids.append(&mut b.1);
    }
    let mut pmap = PMap { subs: built.into_iter().map(|b| b.0).collect() };
    let parent: Vec<AtomicU32> = (0..nid as u32).map(AtomicU32::new).collect();
    let ids_ref = &ids;
    // Union so that each root is the person's byte-wise smallest id.
    let less = |a: u32, b: u32| cmp_keys(ids_ref[a as usize], ids_ref[b as usize], reg) == Ordering::Less;
    edges.par_iter().for_each(|v| {
        for &(ka, kb) in v {
            let mut a = pmap.get(ka).0 as u32;
            let mut b = pmap.get(kb).0 as u32;
            loop {
                a = uf_find(&parent, a);
                b = uf_find(&parent, b);
                if a == b {
                    break;
                }
                let (child, root) = if less(a, b) { (b, a) } else { (a, b) };
                if parent[child as usize].compare_exchange(child, root, AcqRel, Relaxed).is_ok() {
                    break;
                }
            }
        }
    });
    std::mem::forget(edges); // freeing is pointless work: the OS reclaims it at exit
    pmap.subs.par_iter_mut().for_each(|s| {
        for e in s.tab.iter_mut() {
            if !e.0.is_empty() {
                let r = uf_find(&parent, e.1 .0 as u32);
                e.1 = ids_ref[r as usize];
            }
        }
    });
    std::mem::forget(parent); // freeing is pointless work: the OS reclaims it at exit
    let pmap = &pmap;
    lap(&format!("links ({nid} ids)"));

    // ------------------------------------------------------------ 2. events parse
    let ev_chunks = make_chunks(sizes, 0..ne);
    let neb = 1usize << EB;
    let reserve = (ev_bytes / 80 / neb / nt) * 5 / 4 + 16;
    let stage1: PerThread<ERaw> = PerThread::new(nt, neb, reserve);
    let exp: u32 = std::env::var("SS_EXP").ok().and_then(|s| s.parse().ok()).unwrap_or(0); // TEMP profiling switch
    ev_chunks.par_iter().for_each(|c| {
        stage1.with(|g| with_chunk(c, &mut |buf, cs, ce| {
            if exp == 4 {
                std::hint::black_box(buf.len());
                return;
            }
            scan_fields::<7>(buf, cs, ce, |fb| {
                if exp == 2 {
                    std::hint::black_box(fb);
                    return;
                }
                let (es, ee) = fb[0];
                if es == ee {
                    return;
                }
                let ek = key_of(buf, es, ee - es, None);
                let ingest_ms = if exp == 3 { std::hint::black_box(ws + 5000) } else { parse_ts(&buf[fb[3].0..fb[3].1]).unwrap_or(i64::MAX / 4) };
                let ingest = (ingest_ms.div_euclid(1000) - base_s + (1i64 << 31)).clamp(0, u32::MAX as i64) as u32;
                let mut r = ERaw { ek, uk: K::EMPTY, ingest, ts: u32::MAX, pc: 0 };
                'v: {
                    let (us, ue) = fb[1];
                    if us == ue {
                        break 'v;
                    }
                    let Some(ts) = (if exp == 3 { Some(std::hint::black_box(ws + 1000)) } else { parse_ts(&buf[fb[2].0..fb[2].1]) }) else { break 'v };
                    if ts < ws || ts >= we {
                        break 'v;
                    }
                    let purchase = match &buf[fb[4].0..fb[4].1] {
                        b"view" | b"cart" => false,
                        b"purchase" => true,
                        _ => break 'v,
                    };
                    let (ps, pe) = fb[5];
                    if ps == pe {
                        break 'v;
                    }
                    if ingest_ms - ts > DAY_MS {
                        break 'v;
                    }
                    if purchase {
                        let Some(price) = parse_price(&buf[fb[6].0..fb[6].1]) else { break 'v };
                        let cat = prod_cat.get(&buf[ps..pe]).copied().unwrap_or(unknown_cat);
                        r.pc = price | (cat << 24);
                    }
                    r.uk = key_of(buf, us, ue - us, Some(reg));
                    r.ts = (ts - base) as u32;
                }
                if exp == 1 {
                    std::hint::black_box(r);
                    return;
                }
                unsafe { g.get_unchecked_mut((ek.mix() >> (64 - EB)) as usize) }.push(r);
            });
        }));
    });
    let stage1 = stage1.into_inner();
    lap(&format!("events parse ({} chunks)", ev_chunks.len()));

    // ------------------------------------------------------------ 3. dedup -> person buckets
    let npb = 1usize << PB;
    let pev: PerThread<PEv> = PerThread::new(nt, npb, reserve);
    (0..neb).into_par_iter().for_each(|b| DSCR.with(|scr| {
        let mut scr = scr.borrow_mut();
        let (keys, idx) = &mut *scr;
        let parts: Vec<&[ERaw]> = stage1.iter().map(|t| &t[b][..]).collect();
        let n: usize = parts.iter().map(|p| p.len()).sum();
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        // Reuse this core's table: clearing warm memory beats faulting in fresh pages.
        if keys.len() < cap {
            keys.resize(cap, K::EMPTY);
            idx.resize(cap, 0);
        }
        let keys = &mut keys[..cap];
        let idx = &mut idx[..cap];
        keys.fill(K::EMPTY);
        let get = |x: u32| unsafe { parts.get_unchecked((x >> 26) as usize).get_unchecked((x & ((1 << 26) - 1)) as usize) };
        for (t, part) in parts.iter().enumerate() {
            for (i, r) in part.iter().enumerate() {
                let packed = ((t as u32) << 26) | i as u32;
                let mut s = (r.ek.mix() as usize) & mask;
                loop {
                    let k = unsafe { *keys.get_unchecked(s) };
                    if k.is_empty() {
                        keys[s] = r.ek;
                        idx[s] = packed;
                        break;
                    }
                    if k == r.ek {
                        if r.ingest < get(idx[s]).ingest {
                            idx[s] = packed;
                        }
                        break;
                    }
                    s = (s + 1) & mask;
                }
            }
        }
        pev.with(|g| {
            for s in 0..cap {
                if keys[s].is_empty() {
                    continue;
                }
                let r = get(idx[s]);
                if r.ts == u32::MAX {
                    continue;
                }
                let pk = pmap.get(r.uk);
                unsafe { g.get_unchecked_mut((pk.mix() >> (64 - PB)) as usize) }.push(PEv { pk, ts: r.ts, pc: r.pc });
            }
        });
    }));
    std::mem::forget(stage1); // freeing is pointless work: the OS reclaims it at exit
    let pev = pev.into_inner();
    lap("dedup");

    // ------------------------------------------------------------ 4. clicks
    let ck_chunks = make_chunks(sizes, ne..ne + nc);
    let pck: PerThread<PCk> = PerThread::new(nt, npb, (ck_bytes / 45 / npb / nt) * 5 / 4 + 16);
    ck_chunks.par_iter().for_each(|c| {
        pck.with(|g| with_chunk(c, &mut |buf, s0, e0| {
            scan_fields::<4>(buf, s0, e0, |fb| {
                let (cs, ce) = fb[0];
                let (us, ue) = fb[1];
                if cs == ce || us == ue {
                    return;
                }
                let Some(ts) = parse_ts(&buf[fb[3].0..fb[3].1]) else { return };
                if ts < base || ts >= we {
                    return;
                }
                let Some(&camp) = camp_idx.get(&buf[fb[2].0..fb[2].1]) else { return };
                let pk = pmap.get(key_of(buf, us, ue - us, Some(reg)));
                let ck = key_of(buf, cs, ce - cs, Some(reg));
                unsafe { g.get_unchecked_mut((pk.mix() >> (64 - PB)) as usize) }.push(PCk { pk, ck, ts: (ts - base) as u32, camp });
            });
        }));
    });
    let pck = pck.into_inner();
    lap(&format!("clicks ({} chunks)", ck_chunks.len()));

    // ------------------------------------------------------------ 5. sessions
    let camp_acc: Vec<Mutex<Vec<[i64; 4]>>> = (0..nt).map(|_| Mutex::new(vec![[0i64; 4]; (ncamp + 1) * ndays])).collect();
    let any_long = !reg.0.lock().unwrap().is_empty();
    let ud: Vec<Vec<UdRow>> = (0..npb)
        .into_par_iter()
        .map(|b| SSCR.with(|scr| {
            // This core's reused sort buffers and campaign tally.
            let mut scr = scr.borrow_mut();
            let (ev, ck) = &mut *scr;
            ev.clear();
            for t in &pev {
                ev.extend_from_slice(&t[b]);
            }
            ev.sort_unstable_by_key(|e| (e.pk, e.ts));
            ck.clear();
            for t in &pck {
                ck.extend_from_slice(&t[b]);
            }
            if any_long {
                ck.sort_unstable_by(|a, c| {
                    a.pk.cmp(&c.pk).then(a.ts.cmp(&c.ts)).then_with(|| cmp_keys(a.ck, c.ck, reg))
                });
            } else {
                ck.sort_unstable_by_key(|c| (c.pk, c.ts, c.ck));
            }

            let mut rows: Vec<UdRow> = Vec::new();
            let mut cacc_guard = camp_acc[rayon::current_thread_index().unwrap_or(0)].lock().unwrap();
            let cacc = &mut *cacc_guard;
            let mut stamp = vec![u32::MAX; ncamp];
            let mut sess_no: u32 = 0;
            let mut cats = vec![0i64; ncat];
            let mut touched: Vec<u16> = Vec::new();

            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let pk = ev[i].pk;
                let mut j = i + 1;
                while j < ev.len() && ev[j].pk == pk {
                    j += 1;
                }
                while ci < ck.len() && ck[ci].pk < pk {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < ck.len() && ck[cj].pk == pk {
                    cj += 1;
                }
                let pc = &ck[ci..cj];

                let mut row: Option<UdRow> = None;
                let mut s = i;
                while s < j {
                    let mut e = s + 1;
                    while e < j && ev[e].ts - ev[e - 1].ts <= GAP_MS {
                        e += 1;
                    }
                    let start = ev[s].ts;
                    let day = (base + start as i64).div_euclid(DAY_MS);
                    if row.as_ref().map(|r| r.day as i64 != day).unwrap_or(false) {
                        let mut r = row.take().unwrap();
                        r.top = top_cat(&mut cats, &mut touched);
                        rows.push(r);
                    }
                    let r = row.get_or_insert(UdRow { pk, day: day as i32, sessions: 0, events: 0, revenue: 0, top: u16::MAX });
                    let mut rev = 0i64;
                    let mut purchased = false;
                    for x in &ev[s..e] {
                        if x.pc != 0 {
                            purchased = true;
                            let price = (x.pc & 0xFF_FFFF) as i64;
                            let cat = (x.pc >> 24) as usize;
                            rev += price;
                            if cats[cat] == 0 {
                                touched.push(cat as u16);
                            }
                            cats[cat] += price;
                        }
                    }
                    r.sessions += 1;
                    r.events += (e - s) as i64;
                    r.revenue += rev;

                    let di = (day - day_lo) as usize;
                    let lo_ts = start - WEEK_MS as u32;
                    let hi = pc.partition_point(|c| c.ts <= start);
                    let lo = pc[..hi].partition_point(|c| c.ts < lo_ts);
                    let credited = if hi > lo {
                        let mx = pc[hi - 1].ts;
                        pc[pc[lo..hi].partition_point(|c| c.ts < mx) + lo].camp
                    } else {
                        organic
                    };
                    let a = &mut cacc[credited as usize * ndays + di];
                    a[0] += 1;
                    a[1] += purchased as i64;
                    a[2] += rev;
                    sess_no += 1;
                    for c in &pc[lo..hi] {
                        if stamp[c.camp as usize] != sess_no {
                            stamp[c.camp as usize] = sess_no;
                            cacc[c.camp as usize * ndays + di][3] += 1;
                        }
                    }
                    s = e;
                }
                if let Some(mut r) = row {
                    r.top = top_cat(&mut cats, &mut touched);
                    rows.push(r);
                }
                i = j;
            }
            rows
        }))
        .collect();
    std::mem::forget(pev); // freeing is pointless work: the OS reclaims it at exit
    std::mem::forget(pck); // freeing is pointless work: the OS reclaims it at exit
    lap("sessions");

    // ------------------------------------------------------------ 6. write
    let ud_dir = out.join("user_daily");
    let cd_dir = out.join("campaign_daily");
    fs::create_dir_all(&ud_dir).unwrap();
    fs::create_dir_all(&cd_dir).unwrap();

    let nfiles = nt.min(npb);
    let groups: Vec<Vec<&Vec<UdRow>>> = (0..nfiles).map(|k| ud.iter().skip(k).step_by(nfiles).collect()).collect();
    let cat_names_ref = &cat_names;
    let ud_dir_ref = &ud_dir;
    rayon::scope(|sc| {
        for (k, g) in groups.iter().enumerate() {
            sc.spawn(move |_| {
                let n: usize = g.iter().map(|v| v.len()).sum();
                // Names: short keys decode into an arena; long keys come from the registry.
                let mut arena: Vec<u8> = Vec::with_capacity(n * 12);
                let mut spans: Vec<(usize, usize)> = Vec::with_capacity(n);
                for v in g {
                    for r in v.iter() {
                        let s = arena.len();
                        if r.pk.is_long() {
                            arena.extend_from_slice(&key_bytes(r.pk, reg));
                        } else {
                            decode_compact(r.pk, &mut arena);
                        }
                        spans.push((s, arena.len()));
                    }
                }
                let uid: Vec<&[u8]> = spans.iter().map(|&(s, e)| &arena[s..e]).collect();
                let mut day = Vec::with_capacity(n);
                let mut ses = Vec::with_capacity(n);
                let mut evs = Vec::with_capacity(n);
                let mut rev = Vec::with_capacity(n);
                let mut top: Vec<Option<&[u8]>> = Vec::with_capacity(n);
                for v in g {
                    for r in v.iter() {
                        day.push(r.day);
                        ses.push(r.sessions);
                        evs.push(r.events);
                        rev.push(r.revenue);
                        top.push(if r.top == u16::MAX { None } else { Some(cat_names_ref[r.top as usize]) });
                    }
                }
                pq::write(
                    &ud_dir_ref.join(format!("part-{k}.parquet")),
                    &[
                        ("user_id", Col::Str(uid)),
                        ("day", Col::Date(day)),
                        ("sessions", Col::I64(ses)),
                        ("events", Col::I64(evs)),
                        ("revenue_cents", Col::I64(rev)),
                        ("top_category", Col::OptStr(top)),
                    ],
                )
                .unwrap();
            });
        }
        sc.spawn(|_| {
            let mut tot = vec![[0i64; 4]; (ncamp + 1) * ndays];
            for m in &camp_acc {
                let g = m.lock().unwrap();
                for (x, y) in tot.iter_mut().zip(g.iter()) {
                    for k in 0..4 {
                        x[k] += y[k];
                    }
                }
            }
            let mut cid: Vec<&[u8]> = Vec::new();
            let mut chan: Vec<&[u8]> = Vec::new();
            let mut day = Vec::new();
            let mut ses = Vec::new();
            let mut pur = Vec::new();
            let mut rev = Vec::new();
            let mut ast = Vec::new();
            for c in 0..=ncamp {
                for d in 0..ndays {
                    let a = tot[c * ndays + d];
                    if a[0] == 0 && a[3] == 0 {
                        continue;
                    }
                    if c == ncamp {
                        cid.push(b"organic");
                        chan.push(b"organic");
                    } else {
                        cid.push(camp_ids[c]);
                        chan.push(camp_chan[c]);
                    }
                    day.push((day_lo + d as i64) as i32);
                    ses.push(a[0]);
                    pur.push(a[1]);
                    rev.push(a[2]);
                    ast.push(a[3]);
                }
            }
            pq::write(
                &cd_dir.join("part-0.parquet"),
                &[
                    ("campaign_id", Col::Str(cid)),
                    ("channel", Col::Str(chan)),
                    ("day", Col::Date(day)),
                    ("sessions", Col::I64(ses)),
                    ("purchasing_sessions", Col::I64(pur)),
                    ("revenue_cents", Col::I64(rev)),
                    ("assisted_sessions", Col::I64(ast)),
                ],
            )
            .unwrap();
        });
    });
    lap("write");
    // Output files are fully written and closed above. Exit now instead of unmapping
    // the inputs and freeing every buffer; the OS reclaims all of it instantly.
    std::process::exit(0);
}

/// Category with the highest sum (tie -> smallest index = byte-wise smallest name); resets the sums.
fn top_cat(cats: &mut [i64], touched: &mut Vec<u16>) -> u16 {
    let mut best = u16::MAX;
    let mut best_v = 0i64;
    for &c in touched.iter() {
        let v = cats[c as usize];
        if v > best_v || (v == best_v && c < best) {
            best = c;
            best_v = v;
        }
        cats[c as usize] = 0;
    }
    touched.clear();
    best
}
