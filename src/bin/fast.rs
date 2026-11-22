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
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::*};
use std::sync::Mutex;
use std::time::Instant;

use memchr::memchr;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use shopping_sessions::pq::{self, Col};

/// Linux: back every big allocation (>= 4 MB: the record buckets, tables, sort space) with
/// 2 MB transparent huge pages. That is 512x fewer pages to fault in while running and to tear
/// down at exit. madvise is only a hint: if THP is off on the host, nothing changes.
/// Profiling build on Windows: a sampling profiler. With SS_SAMPLE=<file>, a background
/// thread suspends each pool thread ~1000 times a second, records its instruction pointer
/// (relative to the module base), and writes the samples to <file> at exit. The separate
/// `tools/symbolize` program turns them into hot source lines.
#[cfg(all(windows, feature = "profile"))]
mod sampler {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    type Handle = isize;
    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentThread() -> Handle;
        fn GetCurrentProcess() -> Handle;
        fn DuplicateHandle(sp: Handle, sh: Handle, tp: Handle, th: *mut Handle, access: u32, inherit: i32, opts: u32) -> i32;
        fn SuspendThread(h: Handle) -> u32;
        fn ResumeThread(h: Handle) -> u32;
        fn GetThreadContext(h: Handle, ctx: *mut u8) -> i32;
        fn GetModuleHandleW(name: *const u16) -> isize;
        fn Sleep(ms: u32);
    }
    #[link(name = "winmm")]
    extern "system" {
        fn timeBeginPeriod(p: u32) -> u32;
    }
    pub static THREADS: Mutex<Vec<Handle>> = Mutex::new(Vec::new());
    /// Index of the stage currently running (bumped by `lap`), recorded with every sample.
    pub static STAGE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    pub static STOP: AtomicBool = AtomicBool::new(false);
    pub fn register() {
        unsafe {
            let mut h: Handle = 0;
            DuplicateHandle(GetCurrentProcess(), GetCurrentThread(), GetCurrentProcess(), &mut h, 0, 0, 2);
            THREADS.lock().unwrap().push(h);
        }
    }
    #[repr(C, align(16))]
    struct Ctx([u8; 1232]);
    pub fn start(path: String) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || unsafe {
            timeBeginPeriod(1);
            let base = GetModuleHandleW(std::ptr::null()) as u64;
            let mut out: Vec<u64> = Vec::with_capacity(1 << 20);
            let mut ctx = Box::new(Ctx([0; 1232]));
            while !STOP.load(Ordering::Relaxed) {
                let hs = THREADS.lock().unwrap().clone();
                for h in hs {
                    if SuspendThread(h) == u32::MAX {
                        continue;
                    }
                    let flags: u32 = 0x0010_0001; // CONTEXT_AMD64 | CONTEXT_CONTROL
                    ctx.0[0x30..0x34].copy_from_slice(&flags.to_le_bytes());
                    if GetThreadContext(h, ctx.0.as_mut_ptr()) != 0 {
                        let rip = u64::from_le_bytes(ctx.0[0xF8..0x100].try_into().unwrap());
                        let off = rip.wrapping_sub(base);
                        let off = if off < (1 << 40) { off } else { (1 << 40) - 1 }; // outside our module
                        out.push((STAGE.load(Ordering::Relaxed) << 48) | off);
                    }
                    ResumeThread(h);
                }
                Sleep(1);
            }
            let txt: String = out.iter().map(|x| format!("{x:x}\n")).collect();
            std::fs::write(path, txt).unwrap();
        })
    }
}

#[cfg(target_os = "linux")]
mod hugealloc {
    use std::alloc::{GlobalAlloc, Layout};
    static MI: mimalloc::MiMalloc = mimalloc::MiMalloc;
    pub struct Huge;
    const BIG: usize = 4 << 20;
    const HP: usize = 2 << 20;
    /// SS_NO_THP=1 turns huge-page advice off. On a long-running host with fragmented memory,
    /// `defrag=madvise` makes every advised fault compact memory synchronously (a stall).
    fn enabled() -> bool {
        use std::sync::atomic::{AtomicU8, Ordering::Relaxed};
        static ON: AtomicU8 = AtomicU8::new(0); // 0 unknown, 1 on, 2 off
        match ON.load(Relaxed) {
            1 => true,
            2 => false,
            _ => {
                let on = std::env::var_os("SS_NO_THP").is_none();
                ON.store(if on { 1 } else { 2 }, Relaxed);
                on
            }
        }
    }
    unsafe fn advise(p: *mut u8, size: usize) {
        if !enabled() {
            return;
        }
        let start = (p as usize + HP - 1) & !(HP - 1);
        let end = (p as usize + size) & !(HP - 1);
        if end > start {
            libc::madvise(start as *mut libc::c_void, end - start, libc::MADV_HUGEPAGE);
        }
    }
    unsafe impl GlobalAlloc for Huge {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = MI.alloc(l);
            if !p.is_null() && l.size() >= BIG {
                advise(p, l.size());
            }
            p
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            let p = MI.alloc_zeroed(l);
            if !p.is_null() && l.size() >= BIG {
                advise(p, l.size());
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            MI.dealloc(p, l)
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
            let q = MI.realloc(p, l, new_size);
            if !q.is_null() && new_size >= BIG {
                advise(q, new_size);
            }
            q
        }
    }
}

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: hugealloc::Huge = hugealloc::Huge;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: u32 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const PB: u32 = 10; // person bucket bits
const LB: u32 = 8; // link id table bits
const CHUNK: usize = 4 << 20;
const CK_CHUNK: usize = 1 << 20;

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

/// Compact keys for two IDs at once (each 1..=12 bytes): both 16-byte windows sit in one
/// 32-byte register, so the checks and the base-37 conversion run once for the pair.
/// Each result is None when that ID needs the fallback form.
#[inline(always)]
fn compact_key2(buf: &[u8], o1: usize, l1: usize, o2: usize, l2: usize) -> (Option<K>, Option<K>) {
    debug_assert!((1..=12).contains(&l1) && (1..=12).contains(&l2));
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let v = _mm256_set_m128i(
            _mm_loadu_si128(buf.as_ptr().add(o2) as *const __m128i),
            _mm_loadu_si128(buf.as_ptr().add(o1) as *const __m128i),
        );
        let iota = _mm256_setr_epi8(
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        );
        let lens = _mm256_set_m128i(_mm_set1_epi8(l2 as i8), _mm_set1_epi8(l1 as i8));
        let inlen = _mm256_cmpgt_epi8(lens, iota);
        let isd = _mm256_and_si256(
            _mm256_cmpgt_epi8(v, _mm256_set1_epi8(b'0' as i8 - 1)),
            _mm256_cmpgt_epi8(_mm256_set1_epi8(b'9' as i8 + 1), v),
        );
        let isa = _mm256_and_si256(
            _mm256_cmpgt_epi8(v, _mm256_set1_epi8(b'a' as i8 - 1)),
            _mm256_cmpgt_epi8(_mm256_set1_epi8(b'z' as i8 + 1), v),
        );
        let lm = _mm256_movemask_epi8(inlen) as u32;
        let okm = _mm256_movemask_epi8(_mm256_and_si256(_mm256_or_si256(isd, isa), inlen)) as u32;
        let d = _mm256_add_epi8(_mm256_sub_epi8(v, _mm256_set1_epi8(86)), _mm256_and_si256(isd, _mm256_set1_epi8(39)));
        let d = _mm256_and_si256(d, inlen);
        let x = _mm256_maddubs_epi16(d, _mm256_set1_epi16(0x0125)); // byte pairs (37, 1)
        let y = _mm256_madd_epi16(x, _mm256_set1_epi32(0x0001_0559)); // word pairs (1369, 1)
        let k1 = K((_mm256_extract_epi32(y, 0) as u32 as u64) * P8
            + (_mm256_extract_epi32(y, 1) as u32 as u64) * P4
            + _mm256_extract_epi32(y, 2) as u32 as u64);
        let k2 = K((_mm256_extract_epi32(y, 4) as u32 as u64) * P8
            + (_mm256_extract_epi32(y, 5) as u32 as u64) * P4
            + _mm256_extract_epi32(y, 6) as u32 as u64);
        let ok1 = (okm & 0xFFFF) == (lm & 0xFFFF);
        let ok2 = (okm >> 16) == (lm >> 16);
        return (if ok1 { Some(k1) } else { None }, if ok2 { Some(k2) } else { None });
    }
    #[allow(unreachable_code)]
    (compact_key(buf, o1, l1), compact_key(buf, o2, l2))
}

fn decode_compact(k: K, out: &mut Vec<u8>) {
    // Two base-37 digits per step (6 divisions by 1369 instead of 12 by 37).
    #[inline(always)]
    fn ch(d: u64) -> u8 {
        if d <= 10 {
            b'0' + d as u8 - 1
        } else {
            b'a' + d as u8 - 11
        }
    }
    let mut pairs = [0u16; 6];
    let mut v = k.0;
    for i in (0..6).rev() {
        pairs[i] = (v % 1369) as u16;
        v /= 1369;
    }
    for &pr in &pairs {
        let (hi, lo) = ((pr / 37) as u64, (pr % 37) as u64);
        if hi == 0 {
            return;
        }
        out.push(ch(hi));
        if lo == 0 {
            return;
        }
        out.push(ch(lo));
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

/// Field bytes buf[r.0..r.1]. The ranges come from our own scanner and always lie inside
/// `buf`, so the hot paths skip the bounds check.
#[inline(always)]
fn fld(buf: &[u8], r: (usize, usize)) -> &[u8] {
    debug_assert!(r.0 <= r.1 && r.1 <= buf.len());
    unsafe { buf.get_unchecked(r.0..r.1) }
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
    /// Plus two bitsets indexed by separator: a quote right after it (shifted by one, bit 0 =
    /// a quote at the slab start) and a quote right before it.
    static SIDX: RefCell<(Vec<u32>, Vec<u32>, Vec<u32>, Vec<u64>, Vec<u64>)> =
        const { RefCell::new((Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new())) };
}

// Profiling/tuning knobs. Only a `--features profile` build reads them from the environment
// (SS_EXP, SS_SLAB, SS_PF); in the normal build they are constants and every check on them
// compiles away.
static SLAB_SZ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(32 << 10);
static USE512: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Diagnostic per-thread stats: chunks done and busy nanoseconds, events and clicks stages.
static TS_EV_N: [AtomicU64; 32] = [const { AtomicU64::new(0) }; 32];
static TS_EV_NS: [AtomicU64; 32] = [const { AtomicU64::new(0) }; 32];
static TS_CK_N: [AtomicU64; 32] = [const { AtomicU64::new(0) }; 32];
static TS_CK_NS: [AtomicU64; 32] = [const { AtomicU64::new(0) }; 32];
#[inline]
fn ts_add(n: &[AtomicU64; 32], ns: &[AtomicU64; 32], t: Instant) {
    let i = rayon::current_thread_index().unwrap_or(31).min(31);
    n[i].fetch_add(1, Relaxed);
    ns[i].fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
}
fn ts_line(name: &str, n: &[AtomicU64; 32], ns: &[AtomicU64; 32]) -> String {
    let parts: Vec<String> = (0..32)
        .filter(|&i| n[i].load(Relaxed) > 0)
        .map(|i| format!("{}:{}/{}", i, n[i].load(Relaxed), ns[i].load(Relaxed) / 1_000_000))
        .collect();
    format!("{name} {}", parts.join(" "))
}
/// AVX-512 compares on 256-bit registers (SS_VL=1): direct bit masks, no 512-bit clock penalty.
static USEVL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static EXP_G: AtomicU32 = AtomicU32::new(0);
static PF_G: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(32);

#[inline(always)]
fn exp_g() -> u32 {
    if cfg!(feature = "profile") {
        EXP_G.load(Relaxed)
    } else {
        0
    }
}
#[inline(always)]
fn slab_size() -> usize {
    if cfg!(feature = "profile") {
        SLAB_SZ.load(Relaxed)
    } else {
        32 << 10
    }
}
#[inline(always)]
fn prefetch_distance() -> usize {
    if cfg!(feature = "profile") {
        PF_G.load(Relaxed)
    } else {
        32
    }
}

/// Calls `f(fields)` with absolute field ranges for every well-formed line of buf[s..e].
#[inline(always)]
fn scan_fields<const N: usize>(buf: &[u8], s: usize, e: usize, mut f: impl FnMut(&[(usize, usize); N])) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("bmi2") {
            let use512 = USE512.load(Relaxed);
            let usevl = USEVL.load(Relaxed);
            SIDX.with(|ix| {
                let mut ix = ix.borrow_mut();
                let (seps, nls, qs, qa, qb) = &mut *ix;
                let mut p = s;
                while p < e {
                    // A slab ends right after a newline (or at e), so no line crosses slabs.
                    let lim = (p + slab_size()).min(e);
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
                    unsafe {
                        if usevl {
                            index_slab_avx512vl(buf, p, q, seps, nls, qs, qa, qb)
                        } else if use512 {
                            index_slab_avx512(buf, p, q, seps, nls, qs, qa, qb)
                        } else {
                            index_slab_avx2(buf, p, q, seps, nls, qs, qa, qb)
                        }
                    };
                    if exp_g() != 10 {
                        walk_slab::<N>(p, seps, nls, qs, qa, qb, &mut f);
                    } else {
                        std::hint::black_box((seps.len(), nls.len()));
                    }
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

/// Comma, newline and quote bitmasks for the 64 bytes at `ptr` (bit i = byte i).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn masks_avx2(ptr: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(ptr as *const __m256i);
    let b = _mm256_loadu_si256(ptr.add(32) as *const __m256i);
    let mask = |c: u8| -> u64 {
        let v = _mm256_set1_epi8(c as i8);
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64;
        let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64;
        lo | (hi << 32)
    };
    (mask(b'\n'), mask(b','), mask(b'"'))
}

/// Same masks with AVX-512 compares on 256-bit registers: the compare writes a bit mask
/// directly (no movemask), and 256-bit registers avoid the 512-bit frequency license.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,avx512f,avx512bw,avx512vl")]
#[inline]
unsafe fn masks_avx512vl(ptr: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(ptr as *const __m256i);
    let b = _mm256_loadu_si256(ptr.add(32) as *const __m256i);
    let mask = |c: u8| -> u64 {
        let v = _mm256_set1_epi8(c as i8);
        (_mm256_cmpeq_epi8_mask(a, v) as u64) | ((_mm256_cmpeq_epi8_mask(b, v) as u64) << 32)
    };
    (mask(b'\n'), mask(b','), mask(b'"'))
}

/// Same masks with one 64-byte AVX-512 register per compare (the scorer's Xeons have AVX-512).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
#[inline]
unsafe fn masks_avx512(ptr: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let v = _mm512_loadu_si512(ptr as *const _);
    (
        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)),
        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)),
        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)),
    )
}

/// OR `n` low bits of `bits` into bitset `bs` starting at bit `at` (bitset pre-zeroed).
#[inline(always)]
unsafe fn put_bits(bs: *mut u64, at: usize, bits: u64, n: usize) {
    if n == 0 {
        return;
    }
    let (w, o) = (at >> 6, at & 63);
    *bs.add(w) |= bits << o;
    if o != 0 && o + n > 64 {
        *bs.add(w + 1) |= bits >> (64 - o);
    }
}

/// Pass 1: record every separator (comma or newline) position, and every newline position
/// with the number of quotes before it, for buf[p..q]. Positions are relative to p.
/// Also records, per separator, whether a quote sits right before / right after it, so pass 2
/// can check quoting with bit operations instead of reading bytes.
/// No per-character branching: set bits are written out unconditionally, four at a time.
/// One body, two instruction sets: only the mask function differs.
macro_rules! define_index_slab {
    ($name:ident, $features:literal, $masks:ident) => {
        #[cfg(target_arch = "x86_64")]
        #[target_feature(enable = $features)]
        #[allow(clippy::too_many_arguments)]
        unsafe fn $name(
            buf: &[u8],
            p: usize,
            q: usize,
            seps: &mut Vec<u32>,
            nls: &mut Vec<u32>,
            qs: &mut Vec<u32>,
            qa: &mut Vec<u64>,
            qb: &mut Vec<u64>,
        ) {
            use std::arch::x86_64::_pext_u64;
            let len = q - p;
            let words = (len + 72) / 64 + 4;
            qa.clear();
            qa.resize(words, 0);
            qb.clear();
            qb.resize(words, 0);
            let qap = qa.as_mut_ptr();
            let qbp = qb.as_mut_ptr();
            if len > 0 && buf[p] == b'"' {
                *qap |= 1; // the slab's first line starts with a quote
            }
            let mut carry: u64 = 0; // quote in the last byte of the previous block
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
                let (m_nl, m_c, m_q) = $masks(ptr);
                let base = off as u32;

                let sep = m_c | m_nl;
                let cnt = sep.count_ones() as usize;
                // Quote right after / right before each byte, packed in separator order.
                let next_q = if off + 64 < len { (buf[p + off + 64] == b'"') as u64 } else { 0 };
                let after = (m_q >> 1) | (next_q << 63);
                let before = (m_q << 1) | carry;
                carry = m_q >> 63;
                put_bits(qap, ns + 1, _pext_u64(after, sep), cnt);
                put_bits(qbp, ns, _pext_u64(before, sep), cnt);
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
                put_bits(qbp, ns, (buf[q - 1] == b'"') as u64, 1);
                ns += 1;
                *np.add(nn) = len as u32;
                *qp.add(nn) = qtot;
                nn += 1;
            }
            seps.set_len(ns);
            nls.set_len(nn);
            qs.set_len(nn);
        }
    };
}
define_index_slab!(index_slab_avx2, "avx2,bmi2", masks_avx2);
define_index_slab!(index_slab_avx512, "avx2,bmi2,avx512f,avx512bw", masks_avx512);
define_index_slab!(index_slab_avx512vl, "avx2,bmi2,avx512f,avx512bw,avx512vl", masks_avx512vl);

/// Pass 2: walk the lines. A line is well-formed exactly when its N-th separator is its newline.
/// Quoting is decided per line with bit operations: field i starts with a quote iff a quote
/// follows the separator before it, and ends with one iff a quote precedes the separator after
/// it. The line is well-formed iff those two masks agree, every quoted field has at least two
/// characters, and the line holds no other quotes (2 x quoted fields == quotes on the line).
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn walk_slab<const N: usize>(
    p: usize,
    seps: &[u32],
    nls: &[u32],
    qs: &[u32],
    qa: &[u64],
    qb: &[u64],
    f: &mut impl FnMut(&[(usize, usize); N]),
) {
    #[inline(always)]
    fn bits_at<const N: usize>(bs: &[u64], at: usize) -> u64 {
        let (w, o) = (at >> 6, at & 63);
        let lo = bs[w] >> o;
        let hi = if o + N > 64 { bs[w + 1] << (64 - o) } else { 0 };
        (lo | hi) & ((1u64 << N) - 1)
    }
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
            let sm = bits_at::<N>(qa, k);
            let em = bits_at::<N>(qb, k);
            let mut bad = (sm != em) | (2 * sm.count_ones() as usize != nq);
            for i in 0..N {
                let st = if i == 0 { p + ls as usize } else { p + sv[i - 1] as usize + 1 };
                let en = p + sv[i] as usize;
                let qf = ((sm >> i) & 1) as usize;
                bad |= (qf == 1) & (en - st < 2);
                fb[i] = (st + qf, en - qf);
            }
            k += N;
            if !bad {
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
    make_chunks_sized(sizes, files, CHUNK)
}

fn make_chunks_sized(sizes: &[usize], files: std::ops::Range<usize>, chunk: usize) -> Vec<Chunk> {
    let mut out = Vec::new();
    for f in files {
        let n = sizes[f];
        let mut s = 0;
        while s < n {
            let e = (s + chunk).min(n);
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
    static DSCR: RefCell<(Vec<DSlot>, Vec<(K, u32, u32)>, Vec<u32>)> = const { RefCell::new((Vec::new(), Vec::new(), Vec::new())) };
    /// Per-core click staging buffer, reused for every chunk.
    static CSCR: RefCell<Vec<PCk>> = const { RefCell::new(Vec::new()) };
    /// Per-core session sort buffers, reused for every bucket.
    static SSCR: RefCell<SessScratch> = const { RefCell::new(SessScratch::new()) };
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

/// Linux: read input straight from the page cache through a read-only mapping instead of
/// copying it into a buffer. Each chunk asks the kernel to map its own pages in bulk first
/// (MADV_POPULATE_READ, Linux 5.14+; ignored if unsupported), so the 8 cores populate in
/// parallel and no per-page faults happen during parsing.
#[cfg(target_os = "linux")]
mod zc {
    pub struct Map {
        pub ptr: *const u8,
        pub len: usize,
    }
    unsafe impl Send for Map {}
    unsafe impl Sync for Map {}

    /// Diagnostic SS_PRELOAD: read the whole file into our own memory instead of mapping it.
    pub fn load(path: &std::path::Path, len: usize) -> Option<Map> {
        if len == 0 {
            return None;
        }
        let mut v: Vec<u8> = Vec::with_capacity(len + 256);
        let mut f = std::fs::File::open(path).ok()?;
        use std::io::Read;
        f.read_to_end(&mut v).ok()?;
        v.resize(len + 256, 0);
        let p = v.as_ptr();
        std::mem::forget(v);
        Some(Map { ptr: p, len })
    }

    pub fn map(path: &std::path::Path, len: usize) -> Option<Map> {
        use std::os::unix::io::AsRawFd;
        if len == 0 || std::env::var_os("SS_NO_MMAP").is_some() {
            return None;
        }
        let f = std::fs::File::open(path).ok()?;
        let p = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0) };
        if p == libc::MAP_FAILED {
            return None;
        }
        Some(Map { ptr: p as *const u8, len })
    }

    /// Ask the kernel to map [from, to) of the file now (whole pages).
    pub fn populate(m: &Map, from: usize, to: usize) {
        const MADV_POPULATE_READ: libc::c_int = 22;
        let page = 4096;
        let a = from & !(page - 1);
        let b = to.min(m.len);
        if b > a {
            unsafe {
                libc::madvise(m.ptr.add(a) as *mut libc::c_void, b - a, MADV_POPULATE_READ);
            }
        }
    }
}

/// Windows: event chunks read straight from the disk into our buffers (unbuffered, overlapped
/// reads, so the OS copies nothing), in chunk order and 2 x cores reads ahead of the parsing
/// cores. Any chunk this cannot serve (a read error, a line longer than the read-ahead margin)
/// goes through the normal copying path.
#[cfg(windows)]
mod uio {
    use super::{Chunk, EXTRA};
    use memchr::memchr;
    use std::cell::UnsafeCell;
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering::*};
    use std::sync::OnceLock;

    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        offset: u32,
        offset_high: u32,
        h_event: *mut c_void,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn CreateFileW(name: *const u16, access: u32, share: u32, sec: *mut c_void, disp: u32, flags: u32, tmpl: *mut c_void) -> *mut c_void;
        fn ReadFile(h: *mut c_void, buf: *mut c_void, n: u32, read: *mut u32, ov: *mut Overlapped) -> i32;
        fn GetOverlappedResult(h: *mut c_void, ov: *mut Overlapped, n: *mut u32, wait: i32) -> i32;
        fn CreateEventW(sa: *mut c_void, manual: i32, initial: i32, name: *const u16) -> *mut c_void;
        fn GetLastError() -> u32;
    }

    const ALIGN: usize = 4096;
    const ERROR_IO_PENDING: u32 = 997;

    struct Slot {
        buf: *mut u8,
        ov: UnsafeCell<Overlapped>,
        // Written by the issuing core before `issued` is published, read after.
        h: UnsafeCell<*mut c_void>,
        a0: UnsafeCell<usize>,
        ok: UnsafeCell<bool>,
        issued: AtomicUsize, // 1 + index of the chunk whose read was started in this slot
    }
    unsafe impl Sync for Slot {}
    unsafe impl Send for Slot {}

    /// Parse every chunk with `parse(buf, start, end)` over its whole lines. Returns false (and
    /// does nothing) when this reader is switched off.
    #[allow(clippy::too_many_arguments)]
    pub fn run_ordered(
        paths: &[PathBuf],
        sizes: &[usize],
        chunks: &[Chunk],
        nt: usize,
        parse: &(dyn Fn(&[u8], usize, usize) + Sync),
        fallback: &(dyn Fn(&Chunk, &mut dyn FnMut(&[u8], usize, usize)) + Sync),
    ) -> bool {
        if std::env::var_os("SS_NO_UIO").is_some() || chunks.is_empty() {
            return false;
        }
        let n = chunks.len();
        let k = (std::env::var("SS_UIO_K").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(2) * nt).clamp(2, n.max(2));
        let span = |c: &Chunk| -> (usize, usize) {
            let a0 = (c.start.max(1) - 1) & !(ALIGN - 1);
            let a1 = ((c.end + EXTRA).min(sizes[c.file]) + ALIGN - 1) & !(ALIGN - 1);
            (a0, a1 - a0)
        };
        let cap = chunks.iter().map(|c| span(c).1).max().unwrap() + ALIGN;
        let handles: Vec<OnceLock<usize>> = (0..paths.len()).map(|_| OnceLock::new()).collect();
        let open = |f: usize| -> *mut c_void {
            *handles[f].get_or_init(|| {
                let w: Vec<u16> = paths[f].as_os_str().encode_wide().chain(std::iter::once(0)).collect();
                // GENERIC_READ; share read+write; OPEN_EXISTING; NO_BUFFERING | OVERLAPPED
                unsafe { CreateFileW(w.as_ptr(), 0x8000_0000, 3, std::ptr::null_mut(), 3, 0x2000_0000 | 0x4000_0000, std::ptr::null_mut()) as usize }
            }) as *mut c_void
        };
        let slots: Vec<Slot> = (0..k)
            .map(|_| {
                let layout = std::alloc::Layout::from_size_align(cap, ALIGN).unwrap();
                let buf = unsafe { std::alloc::alloc(layout) };
                assert!(!buf.is_null(), "out of memory");
                let ev = unsafe { CreateEventW(std::ptr::null_mut(), 1, 0, std::ptr::null()) };
                Slot {
                    buf,
                    ov: UnsafeCell::new(Overlapped { internal: 0, internal_high: 0, offset: 0, offset_high: 0, h_event: ev }),
                    h: UnsafeCell::new(std::ptr::null_mut()),
                    a0: UnsafeCell::new(0),
                    ok: UnsafeCell::new(false),
                    issued: AtomicUsize::new(0),
                }
            })
            .collect();
        // Start the read of chunk j into its slot (whose previous chunk is fully parsed).
        let issue = |j: usize| {
            let sl = &slots[j % k];
            let c = &chunks[j];
            let (a0, len) = span(c);
            unsafe {
                let h = open(c.file);
                let ov = &mut *sl.ov.get();
                ov.internal = 0;
                ov.internal_high = 0;
                ov.offset = a0 as u32;
                ov.offset_high = (a0 >> 32) as u32;
                let ok = !ov.h_event.is_null()
                    && h as isize != -1
                    && !h.is_null()
                    && (ReadFile(h, sl.buf as *mut c_void, len as u32, std::ptr::null_mut(), ov) != 0 || GetLastError() == ERROR_IO_PENDING);
                *sl.h.get() = h;
                *sl.a0.get() = a0;
                *sl.ok.get() = ok;
            }
            sl.issued.store(j + 1, Release);
        };
        for j in 0..k.min(n) {
            issue(j);
        }
        let next = AtomicUsize::new(0);
        rayon::scope(|sc| {
            for _ in 0..nt {
                sc.spawn(|_| loop {
                    let j = next.fetch_add(1, Relaxed);
                    if j >= n {
                        break;
                    }
                    let sl = &slots[j % k];
                    let c = &chunks[j];
                    let mut spins = 0u32;
                    while sl.issued.load(Acquire) != j + 1 {
                        spins += 1;
                        if spins < 256 {
                            std::hint::spin_loop();
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    let (h, a0, ok) = unsafe { (*sl.h.get(), *sl.a0.get(), *sl.ok.get()) };
                    let mut got: u32 = 0;
                    let ok = ok && unsafe { GetOverlappedResult(h, sl.ov.get(), &mut got, 1) != 0 };
                    let mut done = false;
                    if ok {
                        // The chunk's whole lines, exactly as `load_chunk` finds them.
                        let buf = unsafe { std::slice::from_raw_parts(sl.buf as *const u8, cap) };
                        let got = got as usize;
                        let size = sizes[c.file];
                        let rel0 = c.start.max(1) - 1 - a0;
                        if rel0 < got {
                            match memchr(b'\n', &buf[rel0..got]) {
                                None => done = true, // no line starts inside this chunk
                                Some(i) => {
                                    let first = rel0 + i + 1;
                                    if a0 + first >= c.end {
                                        done = true;
                                    } else {
                                        let from = c.end - 1 - a0;
                                        if from < got {
                                            match memchr(b'\n', &buf[from..got]) {
                                                Some(i2) => {
                                                    parse(buf, first, from + i2 + 1);
                                                    done = true;
                                                }
                                                None if a0 + got >= size => {
                                                    parse(buf, first, got);
                                                    done = true;
                                                }
                                                None => {} // a very long last line: copying path
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if !done {
                        fallback(c, &mut |b, s0, e0| parse(b, s0, e0));
                    }
                    if j + k < n {
                        issue(j + k);
                    }
                    // Let this core pick up other pending pool work (the linked-ID tasks run
                    // alongside the parse and would otherwise wait for a free core).
                    let _ = rayon::yield_now();
                });
            }
        });
        true
    }
}

/// Diagnostics for the scoring machine (SS_DIAG=1): prints the environment and input-reading
/// costs to stderr. Only used by a deliberately failing dev-set submission to read its logs.
#[cfg(target_os = "linux")]
mod diag {
    use std::time::Instant;

    fn show(label: &str, path: &str, max_lines: usize, filter: &str) {
        match std::fs::read_to_string(path) {
            Ok(t) => {
                let lines: Vec<&str> = t.lines().filter(|l| filter.is_empty() || l.contains(filter)).take(max_lines).collect();
                eprintln!("DIAG {label}: {}", lines.join(" | "));
            }
            Err(e) => eprintln!("DIAG {label}: <{e}>"),
        }
    }

    fn vmstat(keys: &[&str]) -> String {
        let t = std::fs::read_to_string("/proc/vmstat").unwrap_or_default();
        keys.iter()
            .map(|k| format!("{k}={}", t.lines().find(|l| l.split(' ').next() == Some(*k)).and_then(|l| l.split(' ').nth(1)).unwrap_or("?")))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Touch `mb` MB of fresh memory, with or without huge-page advice; returns seconds.
    fn touch(mb: usize, huge: bool) -> f64 {
        let n = mb << 20;
        unsafe {
            let p = libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0) as *mut u8;
            if huge {
                libc::madvise(p as *mut libc::c_void, n, libc::MADV_HUGEPAGE);
            }
            let t = Instant::now();
            let mut i = 0;
            while i < n {
                *p.add(i) = 1;
                i += 4096;
            }
            let dt = t.elapsed().as_secs_f64();
            libc::munmap(p as *mut libc::c_void, n);
            dt
        }
    }

    /// Huge pages on this host: cost of faulting memory in with and without the advice, and the
    /// kernel's compaction counters around it.
    pub fn thp() {
        let keys = ["thp_fault_alloc", "thp_fault_fallback", "compact_stall", "compact_fail", "compact_success"];
        show("buddyinfo", "/proc/buddyinfo", 4, "");
        eprintln!("DIAG vmstat before: {}", vmstat(&keys));
        for round in 0..2 {
            let h = touch(512, true);
            eprintln!("DIAG vmstat after huge #{round}: {}", vmstat(&keys));
            let n = touch(512, false);
            eprintln!("DIAG touch 512 MB round {round}: with huge-page advice {:.3}s, without {:.3}s", h, n);
        }
        // 8 threads at once, like the pipeline's allocation bursts
        for huge in [true, false] {
            let t = Instant::now();
            std::thread::scope(|sc| {
                for _ in 0..8 {
                    sc.spawn(move || touch(128, huge));
                }
            });
            eprintln!("DIAG 8 threads x 128 MB, huge={huge}: {:.3}s", t.elapsed().as_secs_f64());
        }
        eprintln!("DIAG vmstat end: {}", vmstat(&keys));
    }

    /// Re-run this program (without SS_DIAG) on the same data with 1, 2, 4 and 8 threads,
    /// twice each, printing wall time and stage laps, plus the cgroup's CPU throttling counters
    /// and CPU pressure around it: shows whether our threads scale on this machine.
    pub fn scaling() {
        show("cpu.stat before", "/sys/fs/cgroup/cpu.stat", 8, "");
        show("cpu.pressure before", "/sys/fs/cgroup/cpu.pressure", 2, "");
        let exe = std::env::current_exe().unwrap();
        let scratch = std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/tmp".into());
        for &nt in &[1usize, 2, 4, 8, 8, 4, 2, 1] {
            let out = format!("{scratch}/diag_out_{nt}");
            let _ = std::fs::remove_dir_all(&out);
            let t = Instant::now();
            let r = std::process::Command::new(&exe)
                .env_remove("SS_DIAG")
                .env("NCPU", nt.to_string())
                .env("OUTPUT_DIR", &out)
                .output();
            let wall = t.elapsed().as_secs_f64();
            match r {
                Ok(o) => {
                    let laps: Vec<String> = String::from_utf8_lossy(&o.stderr)
                        .lines()
                        .filter_map(|l| {
                            let v: Vec<&str> = l.split_whitespace().collect();
                            if v.len() >= 3 && v[1].ends_with('s') { Some(format!("{} {}", v[2], v[1])) } else { None }
                        })
                        .collect();
                    eprintln!("DIAG run NCPU={nt}: wall {wall:.3}s | {}", laps.join(" | "));
                }
                Err(e) => eprintln!("DIAG run NCPU={nt}: failed {e}"),
            }
            let _ = std::fs::remove_dir_all(&out);
        }
        show("cpu.stat after", "/sys/fs/cgroup/cpu.stat", 8, "");
        show("cpu.pressure after", "/sys/fs/cgroup/cpu.pressure", 2, "");
        show("loadavg", "/proc/loadavg", 1, "");
    }

    /// Generate a 5M-event dataset with the official generator (copied into the image), then run
    /// this program on it with several thread counts / switches, one compact line each.
    pub fn bigscale() {
        let gen_s = std::env::var("SS_GEN_BIN").unwrap_or_else(|_| "/app/generate".into());
        let gen = gen_s.as_str();
        if !std::path::Path::new(gen).exists() {
            eprintln!("DIAG no generator in image");
            return;
        }
        let scratch = std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/tmp".into());
        let d = format!("{scratch}/gen5m");
        let t = Instant::now();
        let g = std::process::Command::new(gen)
            .args(["--seed", "4242", "--events", "5000000", "--clicks", "1250000", "--output", &d])
            .output();
        eprintln!("DIAG generate 5M: {:.1}s ok={}", t.elapsed().as_secs_f64(), g.map(|o| o.status.success()).unwrap_or(false));
        let exe = std::env::current_exe().unwrap();
        let runs: [(&str, &[(&str, &str)]); 6] = [
            ("8t", &[("SS_THREADS", "8"), ("SS_TSTAT", "1")]),
            ("4t", &[("SS_THREADS", "4"), ("SS_TSTAT", "1")]),
            ("1t", &[("SS_THREADS", "1"), ("SS_TSTAT", "1")]),
            ("8t ml1", &[("SS_THREADS", "8"), ("SS_TSTAT", "1"), ("SS_ML1", "1")]),
            ("8t nopin", &[("SS_THREADS", "8"), ("SS_TSTAT", "1"), ("SS_NO_PIN", "1")]),
            ("8t", &[("SS_THREADS", "8"), ("SS_TSTAT", "1")]),
        ];
        for (name, envs) in runs.iter() {
            let out = format!("{scratch}/diag_out");
            let _ = std::fs::remove_dir_all(&out);
            let mut c = std::process::Command::new(&exe);
            c.env_remove("SS_DIAG").env("DATA_DIR", &d).env("OUTPUT_DIR", &out);
            for (k, v) in envs.iter() {
                c.env(k, v);
            }
            let t = Instant::now();
            let r = c.output();
            let wall = t.elapsed().as_secs_f64();
            if let Ok(o) = r {
                let laps: Vec<String> = String::from_utf8_lossy(&o.stderr)
                    .lines()
                    .filter_map(|l| {
                        let v: Vec<&str> = l.split_whitespace().collect();
                        if v.len() >= 3 && v[1].ends_with('s') { Some(format!("{}{}", &v[2][..v[2].len().min(4)], v[1].trim_end_matches('s'))) } else { None }
                    })
                    .collect();
                eprintln!("DIAG {name}: {wall:.3} | {}", laps.join(" "));
                for l in String::from_utf8_lossy(&o.stderr).lines() {
                    if l.starts_with("EVT") || l.starts_with("CKT") {
                        eprintln!("  {l}");
                    }
                }
            }
        }
    }

    /// Machine micro-benchmarks, one compact line each.
    pub fn micro() {
        use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
        // memory bandwidth: 8 threads each sum their own 256 MB (pre-touched)
        let bufs: Vec<Vec<u64>> = (0..8).map(|_| vec![1u64; 32 << 20]).collect();
        let t = Instant::now();
        let tot = AtomicU64::new(0);
        std::thread::scope(|sc| {
            for b in &bufs {
                let tot = &tot;
                sc.spawn(move || {
                    let s: u64 = b.iter().fold(0u64, |a, &x| a.wrapping_add(x));
                    tot.fetch_add(s, Relaxed);
                });
            }
        });
        let dt = t.elapsed().as_secs_f64();
        let bw8 = (8u64 << 28) as f64 / 1e9 / dt;
        let t = Instant::now();
        let s1: u64 = bufs[0].iter().fold(0u64, |a, &x| a.wrapping_add(x));
        let bw1 = (1u64 << 28) as f64 / 1e9 / t.elapsed().as_secs_f64();
        drop(bufs);
        // random access latency: pointer chase over 256 MB
        let n = 32usize << 20;
        let mut next: Vec<u32> = (0..n as u32).collect();
        let mut x: u64 = 0x9E3779B97F4A7C15;
        for i in (1..n).rev() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            next.swap(i, (x % (i as u64)) as usize);
        }
        let t = Instant::now();
        let mut p = 0u32;
        for _ in 0..2_000_000 {
            p = next[p as usize];
        }
        let lat = t.elapsed().as_secs_f64() / 2e6 * 1e9;
        drop(next);
        // fresh-memory faults: 8 threads x 128 MB, with and without huge-page advice
        let mut fault = String::new();
        for huge in [false, true] {
            let t = Instant::now();
            std::thread::scope(|sc| {
                for _ in 0..8 {
                    sc.spawn(move || touch(128, huge));
                }
            });
            fault += &format!(" huge={huge} {:.2}GB/s", 1.0 / t.elapsed().as_secs_f64());
        }
        // waking 8 pool threads: rayon broadcast round trips
        let pool = rayon::ThreadPoolBuilder::new().num_threads(8).build().unwrap();
        pool.broadcast(|_| ());
        let t = Instant::now();
        for _ in 0..2000 {
            pool.broadcast(|_| ());
        }
        let wake = t.elapsed().as_secs_f64() / 2000.0 * 1e6;
        eprintln!("DIAG micro: bw 1t {bw1:.1}GB/s 8t {bw8:.1}GB/s | rand lat {lat:.0}ns | faults 8x128MB{fault} | wake8 {wake:.1}us | ({} {})", s1 & 1, p & 1);
    }

    pub fn environment() {
        show("cpu", "/proc/cpuinfo", 1, "model name");
        let flags = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let fl = flags.lines().find(|l| l.starts_with("flags")).unwrap_or("");
        eprintln!("DIAG avx512bw={} bmi2={} avx2={}", fl.contains("avx512bw"), fl.contains("bmi2"), fl.contains(" avx2"));
        eprintln!("DIAG nproc_online={} available_parallelism={:?} NCPU={:?} MEM_GB={:?}",
            flags.lines().filter(|l| l.starts_with("processor")).count(),
            std::thread::available_parallelism().map(|n| n.get()).ok(),
            std::env::var("NCPU").ok(), std::env::var("MEM_GB").ok());
        show("kernel", "/proc/version", 1, "");
        show("mounts /data", "/proc/mounts", 6, "/data");
        show("mounts /output", "/proc/mounts", 3, "/output");
        show("thp", "/sys/kernel/mm/transparent_hugepage/enabled", 1, "");
        show("thp defrag", "/sys/kernel/mm/transparent_hugepage/defrag", 1, "");
        show("cgroup cpu.max", "/sys/fs/cgroup/cpu.max", 1, "");
        show("cgroup cpuset", "/sys/fs/cgroup/cpuset.cpus.effective", 1, "");
        show("cgroup memory.max", "/sys/fs/cgroup/memory.max", 1, "");
        show("affinity", "/proc/self/status", 2, "Cpus_allowed_list");
        show("meminfo", "/proc/meminfo", 4, "");
    }

    /// For each input file: share already in the page cache (mincore), then mmap+populate and
    /// pread throughput.
    pub fn input(paths: &[std::path::PathBuf], sizes: &[usize]) {
        let total: usize = sizes.iter().sum();
        let mut resident = 0usize;
        for (p, &n) in paths.iter().zip(sizes) {
            if n == 0 {
                continue;
            }
            unsafe {
                use std::os::unix::io::AsRawFd;
                let f = std::fs::File::open(p).unwrap();
                let m = libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0);
                if m == libc::MAP_FAILED {
                    continue;
                }
                let pages = n.div_ceil(4096);
                let mut v = vec![0u8; pages];
                if libc::mincore(m, n, v.as_mut_ptr()) == 0 {
                    resident += v.iter().filter(|&&b| b & 1 != 0).count() * 4096;
                }
                libc::munmap(m, n);
            }
        }
        eprintln!("DIAG input {} MB in {} files, {:.1}% already in page cache", total >> 20, paths.len(), 100.0 * resident as f64 / total.max(1) as f64);
        // mmap + MADV_POPULATE_READ throughput (whole input, one thread)
        let t = Instant::now();
        let mut sum = 0u64;
        for (p, &n) in paths.iter().zip(sizes) {
            if n == 0 {
                continue;
            }
            unsafe {
                use std::os::unix::io::AsRawFd;
                let f = std::fs::File::open(p).unwrap();
                let m = libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0);
                let rc = libc::madvise(m, n, 22);
                if rc != 0 {
                    eprintln!("DIAG madvise(POPULATE_READ) failed: {}", std::io::Error::last_os_error());
                }
                let sl = std::slice::from_raw_parts(m as *const u8, n);
                let mut i = 0;
                while i < n {
                    sum += sl[i] as u64;
                    i += 4096;
                }
                libc::munmap(m, n);
            }
        }
        let dt = t.elapsed().as_secs_f64();
        eprintln!("DIAG mmap+populate 1 thread (first, cold): {:.3}s = {:.2} GB/s (checksum {})", dt, total as f64 / 1e9 / dt, sum & 1);
        // pread throughput (whole input, 4 MB reads, one thread)
        let t = Instant::now();
        let mut buf = vec![0u8; 4 << 20];
        let mut got = 0usize;
        for p in paths {
            use std::os::unix::fs::FileExt;
            let f = std::fs::File::open(p).unwrap();
            let mut off = 0u64;
            loop {
                let r = f.read_at(&mut buf, off).unwrap_or(0);
                if r == 0 {
                    break;
                }
                got += r;
                off += r as u64;
            }
        }
        let dt = t.elapsed().as_secs_f64();
        eprintln!("DIAG pread 1 thread (second, now cached): {} MB in {:.3}s = {:.2} GB/s", got >> 20, dt, got as f64 / 1e9 / dt);
    }
}

#[cfg(target_os = "linux")]
fn show_cpu_short() {
    let c = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    eprintln!("DIAG {}", c.lines().find(|l| l.starts_with("model name")).unwrap_or(""));
}

/// Zero-copy variant of `load_chunk` over a mapped file. Returns None when the chunk needs
/// the copying path: its lines run too close to the end of the file for the parsers' 16-byte
/// over-reads, or it holds no line start.
#[cfg(target_os = "linux")]
fn mapped_chunk(m: &zc::Map, c: &Chunk) -> Option<(usize, usize)> {
    let data = unsafe { std::slice::from_raw_parts(m.ptr, m.len) };
    let r0 = c.start.max(1) - 1;
    let limit = (c.end + EXTRA).min(m.len);
    zc::populate(m, r0, limit);
    let first = r0 + memchr(b'\n', &data[r0..limit])? + 1;
    if first >= c.end {
        return None;
    }
    let from = c.end - 1;
    let end = from + memchr(b'\n', &data[from..limit])? + 1;
    if end + 16 > m.len {
        return None; // too close to the end of the file: use the copying path
    }
    Some((first, end))
}

thread_local! {
    /// This core's open handle for the file it read last (consecutive chunks of a core mostly
    /// come from the same file; opening a file costs tens of microseconds on Windows).
    static FH: RefCell<Option<(usize, fs::File)>> = const { RefCell::new(None) };
}

/// Read chunk `c` into `buf`; returns the byte range of its whole lines inside `buf`.
fn load_chunk(path: &Path, size: usize, c: &Chunk, buf: &mut Vec<u8>) -> Option<(usize, usize)> {
    FH.with(|fh| {
        let mut fh = fh.borrow_mut();
        if fh.as_ref().map(|h| h.0) != Some(c.file) {
            *fh = Some((c.file, fs::File::open(path).unwrap()));
        }
        load_chunk_from(&fh.as_ref().unwrap().1, size, c, buf)
    })
}

fn load_chunk_from(f: &fs::File, size: usize, c: &Chunk, buf: &mut Vec<u8>) -> Option<(usize, usize)> {
    // Start one byte early so we can see whether `start` begins a line.
    let r0 = c.start.max(1) - 1;
    let mut want = c.end - r0 + EXTRA;
    loop {
        let len = want.min(size - r0);
        if buf.len() < len + 16 {
            buf.resize(len + 16, 0); // grows only; zero-filled once per core
        }
        let got = read_full(f, &mut buf[..len], r0 as u64);
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

// ---------------------------------------------------------------- fast timestamps
//
// `TsParser::parse` returns exactly what `parse_ts` returns, for every input; the careful
// `parse_ts` stays as the fallback for anything off the common paths (and is what the
// differential test compares against).

#[inline(always)]
fn load8(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

/// True if every byte of `x` selected by `lanes` (0xFF per lane) is an ASCII digit.
#[inline(always)]
fn digits_in(x: u64, lanes: u64) -> bool {
    let hi = 0xF0F0_F0F0_F0F0_F0F0u64 & lanes;
    let want = 0x3030_3030_3030_3030u64 & lanes;
    let xd = x & lanes;
    // high nibble 3, and low nibble <= 9 (adding 6 must not carry into the high nibble)
    (xd & hi) == want && ((xd + (0x0606_0606_0606_0606u64 & lanes)) & hi) == want
}

/// Eight ASCII digits (first char in the lowest byte) to their value. Digits must be checked.
#[inline(always)]
fn eight_digits(x: u64) -> u64 {
    let v = x - 0x3030_3030_3030_3030;
    let v = v.wrapping_mul(10) + (v >> 8);
    (((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000u64 << 32)))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32))))
        >> 32
}

/// "HH:MM:SS" at b[i..i+8] -> seconds of the day, if well-formed and in range.
#[inline(always)]
fn hms(b: &[u8], i: usize) -> Option<i64> {
    let x = load8(b, i);
    const DIG: u64 = 0xFFFF_00FF_FF00_FFFF; // digit lanes 0,1,3,4,6,7
    const COLONS: u64 = 0x0000_3A00_003A_0000; // ':' in lanes 2 and 5
    if (x & !DIG) != COLONS || !digits_in(x, DIG) {
        return None;
    }
    let d = x & 0x0F0F_000F_0F00_0F0F & DIG;
    let v = d.wrapping_mul(10) + (d >> 8); // lane k = 10*d_k + d_{k+1}
    let (h, m, s) = ((v & 0xFF) as i64, ((v >> 24) & 0xFF) as i64, ((v >> 48) & 0xFF) as i64);
    if h > 23 || m > 59 || s > 59 {
        return None;
    }
    Some(h * 3600 + m * 60 + s)
}

/// Per-core timestamp parser with a cache of "YYYY-MM-DD" -> day number, one slot per
/// (month, day) text, so the 20-30 dates of a dataset never evict each other.
struct TsParser {
    keys: [(u64, u16); 1024],
    days: [i64; 1024], // i64::MIN = invalid date
}

thread_local! {
    static TSP: RefCell<TsParser> = const { RefCell::new(TsParser { keys: [(u64::MAX, u16::MAX); 1024], days: [0; 1024] }) };
}

impl TsParser {
    fn new() -> Self {
        TsParser { keys: [(u64::MAX, u16::MAX); 1024], days: [0; 1024] }
    }

    #[inline(always)]
    fn date(&mut self, b: &[u8]) -> Option<i64> {
        let k0 = load8(b, 0);
        let k1 = u16::from_le_bytes([b[8], b[9]]);
        // Slot from the month and day digits (month 0-19 x 40 + day 0-39 < 1024): distinct dates
        // of one year never share a slot; the full text is still compared.
        let mo = (((k0 >> 40) as usize & 1) * 10) + ((k0 >> 48) as usize & 0x0F);
        let dy = ((k1 as usize & 0x03) * 10) + ((k1 as usize >> 8) & 0x0F);
        let slot = (mo * 40 + dy) & 1023;
        if self.keys[slot] != (k0, k1) {
            // Miss: the careful check of the date part, exactly as in parse_ts.
            let d = (|| {
                if b[4] != b'-' || b[7] != b'-' {
                    return None;
                }
                let y = d2(b, 0)? * 100 + d2(b, 2)?;
                let mo = d2(b, 5)?;
                let d = d2(b, 8)?;
                if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) {
                    return None;
                }
                Some(days_from_civil(y, mo, d))
            })();
            self.keys[slot] = (k0, k1);
            self.days[slot] = d.unwrap_or(i64::MIN);
        }
        let d = self.days[slot];
        if d == i64::MIN {
            None
        } else {
            Some(d)
        }
    }

    /// Event ts and ingest_ts together. The common shape is two "YYYY-MM-DDTHH:MM:SSZ" with
    /// the same date text: the date is then checked and converted once. Same results as two
    /// `parse` calls, for every input.
    #[inline(always)]
    fn parse_pair(&mut self, t: &[u8], i: &[u8]) -> (Option<i64>, Option<i64>) {
        if t.len() == 20 && i.len() == 20 && load8(t, 0) == load8(i, 0) && t[8] == i[8] && t[9] == i[9] && t[4] == b'-' {
            let day = self.date(t);
            let one = |b: &[u8]| -> Option<i64> {
                if b[10] != b'T' || b[19] != b'Z' {
                    return None;
                }
                let secs = hms(b, 11)?;
                Some((day? * 86_400 + secs) * 1000)
            };
            return (one(t), one(i));
        }
        (self.parse(t), self.parse(i))
    }

    #[inline(always)]
    fn parse(&mut self, b: &[u8]) -> Option<i64> {
        let n = b.len();
        if n == 20 || n == 25 {
            if b[4] == b'-' {
                if b[10] != b'T' {
                    return None;
                }
                let day = self.date(b)?;
                let secs = hms(b, 11)?;
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
                return Some((day * 86_400 + secs) * 1000 - off_ms);
            }
        } else if n == 13 {
            // Epoch milliseconds, the usual 13 digits: digits 0..8 and 5..13 (overlapping).
            let a = load8(b, 0);
            let c = load8(b, 5);
            if digits_in(a, u64::MAX) && digits_in(c, u64::MAX) {
                return Some(((eight_digits(a) / 1000) * 100_000_000 + eight_digits(c)) as i64);
            }
            return None;
        }
        parse_ts(b)
    }
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

// ---------------------------------------------------------------- small exact lookup table

#[inline(always)]
fn low_bytes(n: usize) -> u64 {
    if n >= 8 {
        u64::MAX
    } else {
        (1u64 << (8 * n)) - 1
    }
}

/// Exact 16-byte key of an ID of 1..=15 bytes: the bytes, zero-padded, with the length in the
/// top byte. Reads 16 bytes from `off` (callers' buffers have 16 bytes of slack).
#[inline(always)]
fn key16(buf: &[u8], off: usize, len: usize) -> (u64, u64) {
    let lo = load8(buf, off);
    let hi = load8(buf, off + 8);
    if len >= 8 {
        (lo, (hi & low_bytes(len - 8)) | ((len as u64) << 56))
    } else {
        (lo & low_bytes(len), (len as u64) << 56)
    }
}

/// Campaign ID -> index, exact. Short IDs use an open-addressing table that fits in cache;
/// anything longer than 15 bytes falls back to a hash map.
struct SmallTab {
    tab: Vec<(u64, u64, u32)>,
    mask: usize,
    long: FxHashMap<Vec<u8>, u32>,
}

impl SmallTab {
    fn new(items: &[(&[u8], u32)]) -> Self {
        let cap = (items.len() * 2).next_power_of_two().max(16);
        let mut t = SmallTab { tab: vec![(0, 0, 0); cap], mask: cap - 1, long: FxHashMap::default() };
        // Keys and home slots first, then the inserts with each slot's cache line requested
        // a few items ahead (the table is bigger than the cache).
        let mut ks: Vec<(u64, u64, u32, usize)> = Vec::with_capacity(items.len());
        for &(id, v) in items {
            if id.is_empty() {
                continue;
            }
            if id.len() > 15 {
                t.long.entry(id.to_vec()).or_insert(v);
                continue;
            }
            let mut pad = [0u8; 32];
            pad[..id.len()].copy_from_slice(id);
            let (lo, hi) = key16(&pad, 0, id.len());
            ks.push((lo, hi, v, (mum(lo ^ 0x9E37_79B9_7F4A_7C15, hi ^ 0xD6E8_FEB8_6659_FD93) as usize) & t.mask));
        }
        for j in 0..ks.len() {
            if let Some(a) = ks.get(j + 16) {
                prefetch(unsafe { t.tab.as_ptr().add(a.3) });
            }
            let (lo, hi, v, mut i) = ks[j];
            loop {
                let e = &mut t.tab[i];
                if e.1 == 0 {
                    *e = (lo, hi, v);
                    break;
                }
                if e.0 == lo && e.1 == hi {
                    break; // first occurrence wins, like the hash map it replaces
                }
                i = (i + 1) & t.mask;
            }
        }
        t
    }

    /// Start loading the slot `get` will look at first.
    #[inline(always)]
    fn prefetch(&self, buf: &[u8], off: usize, len: usize) {
        if len == 0 || len > 15 {
            return;
        }
        let (lo, hi) = key16(buf, off, len);
        let i = (mum(lo ^ 0x9E37_79B9_7F4A_7C15, hi ^ 0xD6E8_FEB8_6659_FD93) as usize) & self.mask;
        prefetch(unsafe { self.tab.as_ptr().add(i) });
    }

    #[inline(always)]
    fn get(&self, buf: &[u8], off: usize, len: usize) -> Option<u32> {
        if len == 0 {
            return None;
        }
        if len > 15 {
            return self.long.get(&buf[off..off + len]).copied();
        }
        let (lo, hi) = key16(buf, off, len);
        let mut i = (mum(lo ^ 0x9E37_79B9_7F4A_7C15, hi ^ 0xD6E8_FEB8_6659_FD93) as usize) & self.mask;
        loop {
            let e = unsafe { self.tab.get_unchecked(i) };
            if e.0 == lo && e.1 == hi {
                return Some(e.2);
            }
            if e.1 == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- records

/// Event copy before dedup (24 bytes): views, carts and every invalid copy (ts == u32::MAX).
#[derive(Clone, Copy, Default)]
struct ERaw {
    em: u64, // bmix(event key): equal iff the keys are equal; top bits pick the bucket, low bits the slot
    uk: K,
    ingest: u32,
    ts: u32,
}

/// A valid purchase copy before dedup (28 bytes): the same fields plus price and category.
/// Purchases are ~3% of events, so keeping them apart saves 4 bytes on all the others.
#[derive(Clone, Copy, Default)]
#[repr(C, packed(4))]
struct EPur {
    em: u64,
    uk: K,
    ingest: u32,
    ts: u32,
    pc: u32, // price | cat << 24 (never 0)
}

/// Invertible 64-bit mix (odd multiplies and xor-shifts), so it can stand in for the key
/// itself. Maps 0 to 0 only, and no real key is 0.
#[inline(always)]
fn bmix(x: u64) -> u64 {
    let mut m = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    m ^= m >> 32;
    m = m.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    m ^ (m >> 32)
}

/// Person key -> small dense number for one bucket (open addressing, grows when needed).
struct PIdx {
    tab: Vec<(K, u32)>,
    mask: usize,
    keys: Vec<K>,
    used: Vec<u32>,
}

impl PIdx {
    const fn new() -> Self {
        PIdx { tab: Vec::new(), mask: 0, keys: Vec::new(), used: Vec::new() }
    }
    /// Empty the table (only the slots used last time) and size it for about `hint` persons.
    fn reset(&mut self, hint: usize) {
        for &u in &self.used {
            self.tab[u as usize].0 = K::EMPTY;
        }
        self.used.clear();
        self.keys.clear();
        let cap = (hint * 2).next_power_of_two().max(64);
        if self.tab.len() < cap {
            self.tab.resize(cap, (K::EMPTY, 0));
        }
        self.mask = cap - 1;
    }
    fn grow(&mut self) {
        for &u in &self.used {
            self.tab[u as usize].0 = K::EMPTY;
        }
        self.used.clear();
        let cap = (self.mask + 1) * 2;
        if self.tab.len() < cap {
            self.tab.resize(cap, (K::EMPTY, 0));
        }
        self.mask = cap - 1;
        for (id, &k) in self.keys.iter().enumerate() {
            let mut i = (k.mix() as usize) & self.mask;
            while !self.tab[i].0.is_empty() {
                i = (i + 1) & self.mask;
            }
            self.tab[i] = (k, id as u32);
            self.used.push(i as u32);
        }
    }
    #[inline(always)]
    fn id(&mut self, k: K) -> u32 {
        let mut i = (k.mix() as usize) & self.mask;
        loop {
            let e = unsafe { self.tab.get_unchecked_mut(i) };
            if e.0 == k {
                return e.1;
            }
            if e.0.is_empty() {
                let id = self.keys.len() as u32;
                *e = (k, id);
                self.keys.push(k);
                self.used.push(i as u32);
                if self.keys.len() * 10 > (self.mask + 1) * 7 {
                    self.grow();
                }
                return id;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: K) -> Option<u32> {
        let mut i = (k.mix() as usize) & self.mask;
        loop {
            let e = unsafe { self.tab.get_unchecked(i) };
            if e.0 == k {
                return Some(e.1);
            }
            if e.0.is_empty() {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Per-core sessions scratch, reused for every bucket.
struct SessScratch {
    ea: Vec<PEv>,
    eb: Vec<PEv>,
    ca: Vec<PCk>,
    cb: Vec<PCk>,
    est: Vec<u32>,
    cst: Vec<u32>,
    pidx: PIdx,
}

impl SessScratch {
    const fn new() -> Self {
        SessScratch { ea: Vec::new(), eb: Vec::new(), ca: Vec::new(), cb: Vec::new(), est: Vec::new(), cst: Vec::new(), pidx: PIdx::new() }
    }
}

/// Profiling (SS_EXP=13): records a sessions task's duration when dropped.
struct TaskTimer<'a>(bool, Instant, usize, &'a Mutex<Vec<(f64, usize)>>);
impl Drop for TaskTimer<'_> {
    fn drop(&mut self) {
        if self.0 {
            self.3.lock().unwrap().push((self.1.elapsed().as_secs_f64(), self.2));
        }
    }
}

/// Stable LSD radix sort of `a` by a 31-bit key (3 passes of 11 + 10 + 10 bits, a pass skipped
/// when all keys share those bits). `b` is scratch. Returns true if the result is in `a`.
fn radix31<T: Copy>(a: &mut Vec<T>, b: &mut Vec<T>, key: impl Fn(&T) -> u32) -> bool {
    let n = a.len();
    b.clear();
    b.reserve(n);
    unsafe { b.set_len(n) };
    let mut c0 = [0u32; 2048];
    let mut c1 = [0u32; 1024];
    let mut c2 = [0u32; 1024];
    for x in a.iter() {
        let t = key(x);
        c0[(t & 2047) as usize] += 1;
        c1[((t >> 11) & 1023) as usize] += 1;
        c2[((t >> 21) & 1023) as usize] += 1;
    }
    let mut in_a = true;
    let passes: [(&mut [u32], u32, u32); 3] = [(&mut c0[..], 0, 2047), (&mut c1[..], 11, 1023), (&mut c2[..], 21, 1023)];
    for (c, shift, mask) in passes {
        if c.iter().any(|&x| x as usize == n) {
            continue;
        }
        let mut acc = 0u32;
        for x in c.iter_mut() {
            let v = *x;
            *x = acc;
            acc += v;
        }
        let (src, dst) = if in_a { (&a[..], &mut b[..]) } else { (&b[..], &mut a[..]) };
        for x in src.iter() {
            let d = ((key(x) >> shift) & mask) as usize;
            unsafe { *dst.get_unchecked_mut(*c.get_unchecked(d) as usize) = *x };
            c[d] += 1;
        }
        in_a = !in_a;
    }
    in_a
}

/// Dedup table slot: event key, where the kept copy is, and its ingest time (one cache access).
/// Dedup table slot (8 bytes): a 32-bit fingerprint of the event key (never 0; 0 = empty) and
/// where the kept copy is. The full key and ingest time are read from the copy itself, which
/// only happens on a fingerprint match (a real duplicate, ~4% of copies).
#[derive(Clone, Copy)]
struct DSlot {
    fp: u32,
    idx: u32,
}

impl DSlot {
    const EMPTY: DSlot = DSlot { fp: 0, idx: 0 };
}

#[derive(Clone, Copy, Default)]
struct PEv {
    pk: K,
    ts: u32,
    pc: u32,
}

#[derive(Clone, Copy, Default)]
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

// ---------------------------------------------------------------- block storage
//
// Record buckets are chains of 8 KB blocks taken from per-thread pools. Blocks come from 16 MB
// slabs (huge pages on Linux) and are recycled: when dedup has consumed an event bucket, its
// blocks go back to that thread's pool and the person / click buckets written next reuse them,
// so those writes land in memory that is already faulted in (and the process touches far less
// fresh memory, which is also what the OS tears down at exit).

const BLOCK: usize = 4 << 10;
const SLAB: usize = 16 << 20;

struct Pool {
    free: Vec<*mut u8>,
    cur: *mut u8,
    left: usize,
}

thread_local! {
    static POOL: RefCell<Pool> = const { RefCell::new(Pool { free: Vec::new(), cur: std::ptr::null_mut(), left: 0 }) };
}

#[inline]
fn pool_take() -> *mut u8 {
    POOL.with(|p| {
        let mut p = p.borrow_mut();
        if let Some(b) = p.free.pop() {
            return b;
        }
        if p.left < BLOCK {
            let layout = std::alloc::Layout::from_size_align(SLAB, 4096).unwrap();
            let m = unsafe { std::alloc::alloc(layout) };
            assert!(!m.is_null(), "out of memory");
            p.cur = m;
            p.left = SLAB;
        }
        let b = p.cur;
        p.cur = unsafe { p.cur.add(BLOCK) };
        p.left -= BLOCK;
        b
    })
}

/// Hand blocks back to this thread's pool. The caller must never read them again.
fn pool_give(blocks: &[*mut u8]) {
    POOL.with(|p| p.borrow_mut().free.extend_from_slice(blocks));
}

/// A growable sequence of `T` stored as a chain of pool blocks.
struct Stream<T> {
    blocks: Vec<*mut u8>,
    cur: *mut T, // next free slot in the last block
    end: *mut T, // end of the last block
}

/// Bytes ahead of the write position to prefetch for writing (0 = off).
const PFW: usize = 0;

unsafe impl<T: Send> Send for Stream<T> {}
unsafe impl<T: Sync> Sync for Stream<T> {}

impl<T: Copy> Stream<T> {
    const PER: usize = BLOCK / std::mem::size_of::<T>();

    fn new() -> Self {
        Stream { blocks: Vec::new(), cur: std::ptr::null_mut(), end: std::ptr::null_mut() }
    }
    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let b = pool_take();
        self.blocks.push(b);
        self.cur = b as *mut T;
        self.end = unsafe { self.cur.add(Self::PER) };
    }
    #[inline(always)]
    fn push(&mut self, v: T) {
        if self.cur == self.end {
            self.grow();
        }
        unsafe {
            if PFW > 0 {
                #[cfg(target_arch = "x86_64")]
                std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_ET0 }>((self.cur as *const i8).wrapping_add(PFW));
            }
            self.cur.write(v);
            self.cur = self.cur.add(1);
        }
    }
    /// Items in the last block.
    #[inline]
    fn last(&self) -> usize {
        match self.blocks.last() {
            Some(&b) => (self.cur as usize - b as usize) / std::mem::size_of::<T>(),
            None => 0,
        }
    }
    #[inline]
    fn len(&self) -> usize {
        if self.blocks.is_empty() {
            0
        } else {
            (self.blocks.len() - 1) * Self::PER + self.last()
        }
    }
    /// Item `i` (i < len).
    #[inline(always)]
    fn get(&self, i: usize) -> &T {
        unsafe { &*(*self.blocks.get_unchecked(i / Self::PER) as *const T).add(i % Self::PER) }
    }
    /// The items, one slice per block.
    fn chunks(&self) -> impl Iterator<Item = &[T]> + '_ {
        let n = self.blocks.len();
        let last = self.last();
        self.blocks.iter().enumerate().map(move |(k, &b)| unsafe {
            std::slice::from_raw_parts(b as *const T, if k + 1 == n { last } else { Self::PER })
        })
    }
    fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.chunks().flatten()
    }
}

impl<'a, T: Copy> IntoIterator for &'a Stream<T> {
    type Item = &'a T;
    type IntoIter = Box<dyn Iterator<Item = &'a T> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// Per-thread bucketed output streams; each rayon thread only touches its own slot.
struct PerThread<T> {
    slots: Vec<Mutex<Vec<Stream<T>>>>,
}

impl<T: Send + Copy> PerThread<T> {
    fn new(nt: usize, nb: usize, _reserve: usize) -> Self {
        PerThread { slots: (0..nt).map(|_| Mutex::new((0..nb).map(|_| Stream::new()).collect())).collect() }
    }
    fn with<R>(&self, f: impl FnOnce(&mut Vec<Stream<T>>) -> R) -> R {
        let t = rayon::current_thread_index().unwrap_or(0);
        let mut g = self.slots[t].lock().unwrap();
        f(&mut g)
    }
    fn into_inner(self) -> Vec<Vec<Stream<T>>> {
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

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0)
    }
}

impl PMap {
    /// Start loading the table slot `get(k)` will look at first.
    #[inline(always)]
    fn prefetch(&self, k: K) {
        let h = k.mix();
        let s = unsafe { self.subs.get_unchecked((h >> (64 - LB)) as usize) };
        if !s.tab.is_empty() {
            prefetch(unsafe { s.tab.as_ptr().add((h as usize) & s.mask) });
        }
    }

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

/// The `*.csv` files of `dir` with their sizes, sorted by path. The directory listing already
/// carries each entry's type (and on Windows its size), so most files need no extra lookup.
fn csv_files(dir: &Path) -> Vec<(PathBuf, usize)> {
    let mut v: Vec<(PathBuf, usize)> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let p = e.path();
                if p.extension().map(|x| x != "csv").unwrap_or(true) {
                    return None;
                }
                let ft = e.file_type().ok()?;
                let md = if ft.is_file() {
                    e.metadata().ok()?
                } else if ft.is_symlink() {
                    fs::metadata(&p).ok().filter(|m| m.is_file())?
                } else {
                    return None;
                };
                Some((p, md.len() as usize))
            })
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

/// The lines after the header (no empty piece after a final newline).
fn small_lines(buf: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = if buf.last() == Some(&b'\n') { &buf[..buf.len() - 1] } else { buf };
    body.split(|&c| c == b'\n').skip(1)
}

fn main() {
    let t0 = Instant::now();
    let mut last = t0;
    let mut lap = |name: &str| {
        #[cfg(all(windows, feature = "profile"))]
        sampler::STAGE.fetch_add(1, Relaxed);
        let now = Instant::now();
        eprintln!("{:>10.3}s  {:>8.3}s  {}", (now - t0).as_secs_f64(), (now - last).as_secs_f64(), name);
        last = now;
    };

    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    // SS_THREADS (ours) takes precedence over NCPU (set by the scorer): lets a build choose its
    // own worker count, e.g. one per physical core.
    let nt: usize = std::env::var("SS_THREADS")
        .ok()
        .or_else(|| std::env::var("NCPU").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
        .clamp(1, 31);
    #[cfg(all(windows, feature = "profile"))]
    let sample_path = std::env::var("SS_SAMPLE").ok();
    #[cfg(all(windows, feature = "profile"))]
    let sampler_thread = {
        let mut b = rayon::ThreadPoolBuilder::new().num_threads(nt);
        if sample_path.is_some() {
            b = b.start_handler(|_| sampler::register());
        }
        b.build_global().unwrap();
        sample_path.clone().map(sampler::start)
    };
    #[cfg(not(any(all(windows, feature = "profile"), target_os = "linux")))]
    rayon::ThreadPoolBuilder::new().num_threads(nt).build_global().unwrap();
    // Linux: pin worker i to the i-th CPU this process may use (the scorer gives exactly 8), so
    // no worker gets moved between CPUs and loses its cache. SS_NO_PIN=1 turns it off.
    #[cfg(target_os = "linux")]
    {
        let cpus: Vec<usize> = unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0 {
                (0..libc::CPU_SETSIZE as usize).filter(|&c| libc::CPU_ISSET(c, &set)).collect()
            } else {
                Vec::new()
            }
        };
        let pin = std::env::var_os("SS_NO_PIN").is_none() && cpus.len() >= nt;
        let mut b = rayon::ThreadPoolBuilder::new().num_threads(nt);
        if pin {
            let cpus = cpus.clone();
            b = b.start_handler(move |i| unsafe {
                let mut set: libc::cpu_set_t = std::mem::zeroed();
                libc::CPU_SET(cpus[i % cpus.len()], &mut set);
                libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
            });
        }
        b.build_global().unwrap();
    }
    #[cfg(target_arch = "x86_64")]
    USE512.store(is_x86_feature_detected!("avx512bw") && std::env::var_os("SS_NO512").is_none(), Relaxed);
    USEVL.store(
        is_x86_feature_detected!("avx512bw") && is_x86_feature_detected!("avx512vl") && std::env::var_os("SS_VL").is_some(),
        Relaxed,
    );
    if !cfg!(feature = "profile") {
        // knobs below are profiling-only
    } else if let Some(v) = std::env::var("SS_PF").ok().and_then(|s| s.parse::<usize>().ok()) {
        PF_G.store(v, Relaxed);
    }
    if !cfg!(feature = "profile") {
    } else if let Some(v) = std::env::var("SS_SLAB").ok().and_then(|s| s.parse::<usize>().ok()) {
        SLAB_SZ.store(v << 10, Relaxed);
    }

    #[cfg(target_os = "linux")]
    let diag_on = std::env::var_os("SS_DIAG").is_some();
    #[cfg(target_os = "linux")]
    if diag_on {
        show_cpu_short();
        diag::micro();
        diag::bigscale();
        eprintln!("DIAG done: failing on purpose so the dev-set logs are shown");
        std::process::exit(3);
    }
    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    let base = ws - WEEK_MS; // all stored ts are ms since base
    let base_s = base.div_euclid(1000);
    let day_lo = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - day_lo + 1).max(1) as usize;

    // Input listing on one core while another reads and indexes the small tables.
    let ((ev_files, ck_files, ln_files), (products_buf, campaigns_buf)) = rayon::join(
        || (csv_files(&data.join("events")), csv_files(&data.join("clicks")), csv_files(&data.join("links"))),
        || (fs::read(data.join("products.csv")).expect("products"), fs::read(data.join("campaigns.csv")).expect("campaigns")),
    );
    let all_files: Vec<&(PathBuf, usize)> = ev_files.iter().chain(&ck_files).chain(&ln_files).collect();
    let paths: Vec<PathBuf> = all_files.iter().map(|f| f.0.clone()).collect();
    let sizes: Vec<usize> = all_files.iter().map(|f| f.1).collect();
    let (paths, sizes) = (&paths[..], &sizes[..]);
    #[cfg(target_os = "linux")]
    if diag_on {
        diag::input(paths, sizes);
    }
    let (ne, nc) = (ev_files.len(), ck_files.len());
    let ev_bytes: usize = sizes[..ne].iter().sum();
    let ck_bytes: usize = sizes[ne..ne + nc].iter().sum();
    #[cfg(target_os = "linux")]
    let preload = std::env::var_os("SS_PRELOAD").is_some();
    #[cfg(target_os = "linux")]
    let maps: Vec<Option<zc::Map>> = paths
        .par_iter()
        .zip(sizes.par_iter())
        .map(|(p, &n)| if preload { zc::load(p, n) } else { zc::map(p, n) })
        .collect();
    let read_only = std::env::var_os("SS_RO").is_some();
    // Run `f(buf, start, end)` over the whole lines of chunk `c`: straight from the mapped file
    // on Linux, otherwise (and for each file's last lines) in this core's reused buffer.
    let with_chunk = |c: &Chunk, f: &mut dyn FnMut(&[u8], usize, usize)| {
        #[cfg(target_os = "linux")]
        if let Some(m) = &maps[c.file] {
            if let Some((s, e)) = mapped_chunk(m, c) {
                if read_only {
                    let d = unsafe { std::slice::from_raw_parts(m.ptr, m.len) };
                    let mut x = 0u64;
                    let mut i = s;
                    while i < e {
                        x = x.wrapping_add(d[i] as u64);
                        i += 64;
                    }
                    std::hint::black_box(x);
                    return;
                }
                f(unsafe { std::slice::from_raw_parts(m.ptr, m.len) }, s, e);
                return;
            }
        }
        RBUF.with(|rb| {
            let mut rb = rb.borrow_mut();
            if let Some((s, e)) = load_chunk(&paths[c.file], sizes[c.file], c, &mut rb) {
                f(&rb[..], s, e);
            }
        })
    };

    // Products: categories get numbers in first-seen order (a tiny map), then are renumbered
    // so that number order is byte-wise name order (the top-category tie-break relies on it).
    let mut prod_raw: Vec<(&[u8], u32)> = Vec::new();
    let mut seen_cats: FxHashMap<&[u8], u32> = FxHashMap::default();
    let mut seen_names: Vec<&[u8]> = Vec::new();
    let mut f2 = [(0usize, 0usize); 2];
    for line in small_lines(&products_buf) {
        if split(line, &mut f2) && f2[0].0 < f2[0].1 && f2[1].0 < f2[1].1 {
            let c = &line[f2[1].0..f2[1].1];
            let ci = *seen_cats.entry(c).or_insert_with(|| {
                seen_names.push(c);
                seen_names.len() as u32 - 1
            });
            prod_raw.push((&line[f2[0].0..f2[0].1], ci));
        }
    }
    let mut cat_names: Vec<&[u8]> = seen_names.clone();
    cat_names.push(b"unknown");
    cat_names.sort_unstable();
    cat_names.dedup();
    assert!(cat_names.len() <= 255, "too many categories");
    let ncat = cat_names.len();
    let cat_index = |c: &[u8]| cat_names.binary_search(&c).unwrap() as u32;
    let unknown_cat = cat_index(b"unknown");
    let renum: Vec<u32> = seen_names.iter().map(|c| cat_index(c)).collect();
    // product_id is unique in products.csv; if it ever were not, the last row wins as before
    // (the table keeps the first insert, so insert in reverse).
    let prod_items: Vec<(&[u8], u32)> = prod_raw.iter().rev().map(|&(p, ci)| (p, renum[ci as usize])).collect();
    let prod_tab = SmallTab::new(&prod_items);
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
    let camp_items: Vec<(&[u8], u32)> = camp_ids.iter().enumerate().map(|(i, c)| (*c, i as u32)).collect();
    let camp_tab = SmallTab::new(&camp_items);
    let reg = Reg(Mutex::new(FxHashMap::default()));
    let reg = &reg;
    lap("setup");

    // Event dedup buckets: about 2.5-5 MB of input each (512 at 20M events, 1,024 at 60M), so a
    // bucket's dedup table stays cache-sized while few pages are written to at once.
    let eb: u32 = ((ev_bytes / (5 << 19)).max(1).ilog2()).clamp(6, 12);
    let neb = 1usize << eb;
    let npb = 1usize << PB;
    let reserve = (ev_bytes / 80 / neb / nt) * 5 / 4 + 16;
    let exp: u32 = if cfg!(feature = "profile") { std::env::var("SS_EXP").ok().and_then(|s| s.parse().ok()).unwrap_or(0) } else { 0 };
    EXP_G.store(exp, Relaxed);
    let ev_chunk_count = make_chunks(sizes, 0..ne).len();
    let ck_chunk_count = make_chunks_sized(sizes, ne..ne + nc, CK_CHUNK).len();

    // Linked IDs do not depend on event parsing, so they run at the same time: the person
    // map is ready long before the parse finishes.
    let (pmap, stage1) = rayon::join(
        || {
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
                    let cap = (n * 4).next_power_of_two().max(4);
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
                // Ask for both ends' table entries 16 links ahead (the table is far bigger than
                // the cache, and these lookups otherwise wait on memory one by one).
                for &(ka, kb) in v.iter().take(16) {
                    pmap.prefetch(ka);
                    pmap.prefetch(kb);
                }
                for (i, &(ka, kb)) in v.iter().enumerate() {
                    if let Some(&(pa, pb)) = v.get(i + 16) {
                        pmap.prefetch(pa);
                        pmap.prefetch(pb);
                    }
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
            pmap
        },
        || {
            // ------------------------------------------------------------ 2. events parse
            let ev_chunks = make_chunks(sizes, 0..ne);
            let stage1: PerThread<ERaw> = PerThread::new(nt, neb, reserve);
            let stage1p: PerThread<EPur> = PerThread::new(nt, neb, 0);
            let do_chunk = |buf: &[u8], cs: usize, ce: usize| {
                stage1.with(|g| stage1p.with(|gp| {
                    if exp == 4 {
                        std::hint::black_box(buf.len());
                        return;
                    }
                    TSP.with(|tc| {
                    let tsp = &mut *tc.borrow_mut();
                    scan_fields::<7>(buf, cs, ce, |fb| {
                        if exp == 2 {
                            std::hint::black_box(fb);
                            return;
                        }
                        let (es, ee) = fb[0];
                        if es == ee {
                            return;
                        }
                        let (us, ue) = fb[1];
                        // Both IDs in one pass when both are short (always, in practice).
                        let (ek, uk_fast) = if exp == 5 {
                            (K(std::hint::black_box(es as u64 * 0x9E37_79B9 | 1)), Some(K(std::hint::black_box(us as u64 | 1))))
                        } else if ee - es <= 12 && ue > us && ue - us <= 12 {
                            let (a, b) = compact_key2(buf, es, ee - es, us, ue - us);
                            (a.unwrap_or_else(|| key_of(buf, es, ee - es, None)), b)
                        } else {
                            (key_of(buf, es, ee - es, None), None)
                        };
                        // A purchase will look up its product: start loading that table slot now.
                        if fb[4].1 - fb[4].0 == 8 {
                            prod_tab.prefetch(buf, fb[5].0, fb[5].1 - fb[5].0);
                        }
                        // Both timestamps together (shared date when they match).
                        let (ts_opt, ingest_opt) = if exp == 3 {
                            (Some(std::hint::black_box(ws + 1000)), Some(std::hint::black_box(ws + 5000)))
                        } else {
                            tsp.parse_pair(fld(buf, fb[2]), fld(buf, fb[3]))
                        };
                        let ingest_ms = ingest_opt.unwrap_or(i64::MAX / 4);
                        // ingest_ts is whole seconds (F1), so ms / 1000 is exact; unsigned math
                        // is cheaper than div_euclid (values here are far above zero).
                        let ingest = ((ingest_ms as u64 / 1000) as i64 - base_s + (1i64 << 31)).clamp(0, u32::MAX as i64) as u32;
                        let hm = bmix(ek.0);
                        let mut r = ERaw { em: hm, uk: K::EMPTY, ingest, ts: u32::MAX };
                        let mut pc = 0u32;
                        'v: {
                            if us == ue {
                                break 'v;
                            }
                            let Some(ts) = ts_opt else { break 'v };
                            if ts < ws || ts >= we {
                                break 'v;
                            }
                            let (ty, tye) = fb[4];
                            let purchase = match tye - ty {
                                4 => {
                                    let w = u32::from_le_bytes([buf[ty], buf[ty + 1], buf[ty + 2], buf[ty + 3]]);
                                    if w == u32::from_le_bytes(*b"view") || w == u32::from_le_bytes(*b"cart") {
                                        false
                                    } else {
                                        break 'v;
                                    }
                                }
                                8 if load8(buf, ty) == u64::from_le_bytes(*b"purchase") => true,
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
                                let Some(price) = parse_price(fld(buf, fb[6])) else { break 'v };
                                let cat = prod_tab.get(buf, ps, pe - ps).unwrap_or(unknown_cat);
                                pc = price | (cat << 24);
                            }
                            r.uk = match uk_fast {
                                Some(k) => k,
                                None => key_of(buf, us, ue - us, Some(reg)),
                            };
                            r.ts = (ts - base) as u32;
                        }
                        if exp == 1 {
                            std::hint::black_box(r);
                            return;
                        }
                        let bk = (hm >> (64 - eb)) as usize;
                        if pc == 0 {
                            unsafe { g.get_unchecked_mut(bk) }.push(r);
                        } else {
                            unsafe { gp.get_unchecked_mut(bk) }.push(EPur { em: r.em, uk: r.uk, ingest: r.ingest, ts: r.ts, pc });
                        }
                    });
                    });
                }));
            };
            #[cfg(windows)]
            let ordered = uio::run_ordered(paths, sizes, &ev_chunks, nt, &do_chunk, &with_chunk);
            #[cfg(not(windows))]
            let ordered = false;
            if !ordered {
                let ml1 = std::env::var_os("SS_ML1").is_some();
                let ev_one = |c: &Chunk| {
                    let t = Instant::now();
                    with_chunk(c, &mut |buf, cs, ce| do_chunk(buf, cs, ce));
                    ts_add(&TS_EV_N, &TS_EV_NS, t);
                };
                if ml1 {
                    ev_chunks.par_iter().with_max_len(1).for_each(ev_one);
                } else {
                    ev_chunks.par_iter().for_each(ev_one);
                }
            }
            (stage1.into_inner(), stage1p.into_inner())
        },
    );
    let pmap = &pmap;
    let (stage1, stage1p) = stage1;
    lap(&format!("links + events parse ({ev_chunk_count} chunks)"));

    // ------------------------------------------------------------ 3. dedup -> person buckets
    let pev: PerThread<PEv> = PerThread::new(nt, npb, reserve);
    (0..neb).into_par_iter().for_each(|b| DSCR.with(|scr| {
        let mut scr = scr.borrow_mut();
        let (slots, kept, used) = &mut *scr;
        kept.clear();
        used.clear();
        let parts: Vec<&Stream<ERaw>> = stage1.iter().map(|t| &t[b]).collect();
        let partsp: Vec<&Stream<EPur>> = stage1p.iter().map(|t| &t[b]).collect();
        let n: usize = parts.iter().map(|p| p.len()).sum::<usize>() + partsp.iter().map(|p| p.len()).sum::<usize>();
        let cap = ((n * 3) / 2).next_power_of_two().max(16);
        let mask = cap - 1;
        // Reuse this core's table: clearing warm memory beats faulting in fresh pages.
        if slots.len() < cap {
            slots.resize(cap, DSlot::EMPTY);
        }
        // The table is always left all-empty by the previous bucket (the read pass below
        // resets every slot it reads), so there is no separate clearing pass.
        let slots = &mut slots[..cap];
        // A copy's place: bit 31 = purchase stream, bits 26..31 = thread, low 26 bits = index.
        let key_ing = |x: u32| -> (u64, u32) {
            let (t, i) = (((x >> 26) & 31) as usize, (x & ((1 << 26) - 1)) as usize);
            if x >> 31 == 0 {
                let r = unsafe { parts.get_unchecked(t) }.get(i);
                (r.em, r.ingest)
            } else {
                let r = unsafe { partsp.get_unchecked(t) }.get(i);
                (r.em, r.ingest)
            }
        };
        let get = |x: u32| -> (K, u32, u32) {
            let (t, i) = (((x >> 26) & 31) as usize, (x & ((1 << 26) - 1)) as usize);
            if x >> 31 == 0 {
                let r = unsafe { parts.get_unchecked(t) }.get(i);
                (r.uk, r.ts, 0)
            } else {
                let r = unsafe { partsp.get_unchecked(t) }.get(i);
                (r.uk, r.ts, r.pc)
            }
        };
        macro_rules! insert_all {
            ($parts:expr, $kind:expr) => {
                for (t, part) in $parts.iter().enumerate() {
                    let mut base_i = 0usize;
                    for chunk in part.chunks() {
                        for (j, r) in chunk.iter().enumerate() {
                            let i = base_i + j;
                            // Start loading the slot the record 8 ahead will probe.
                            if let Some(ahead) = chunk.get(j + 8) {
                                prefetch(unsafe { slots.as_ptr().add((ahead.em as usize) & mask) });
                            }
                            let packed = ($kind << 31) | ((t as u32) << 26) | i as u32;
                            let (rem, ring) = (r.em, r.ingest);
                            // Bits below the bucket bits (those are the same for the whole bucket).
                            let fp = ((rem >> 16) as u32) | 1;
                            let mut s = (rem as usize) & mask;
                            loop {
                                let sl = unsafe { slots.get_unchecked_mut(s) };
                                if sl.fp == 0 {
                                    *sl = DSlot { fp, idx: packed };
                                    used.push(s as u32);
                                    break;
                                }
                                if sl.fp == fp {
                                    let (oem, oing) = key_ing(sl.idx);
                                    if oem == rem {
                                        if ring < oing {
                                            sl.idx = packed;
                                        }
                                        break;
                                    }
                                }
                                s = (s + 1) & mask;
                            }
                        }
                        base_i += chunk.len();
                    }
                }
            };
        }
        insert_all!(parts, 0u32);
        insert_all!(partsp, 1u32);
        pev.with(|g| {
            // Visit only the slots that were filled (no scan over empty ones).
            for &s in used.iter() {
                let sl = unsafe { slots.get_unchecked_mut(s as usize) };
                let idx = sl.idx;
                *sl = DSlot::EMPTY; // reset while the line is in cache: next bucket starts clean
                let (uk, ts, pc) = get(idx);
                if ts == u32::MAX {
                    continue;
                }
                kept.push((uk, ts, pc));
            }
            // This event bucket is fully consumed: recycle its blocks for the person records
            // written next (they are already in memory, so no fresh page faults).
            for part in parts.iter() {
                pool_give(&part.blocks);
            }
            for part in partsp.iter() {
                pool_give(&part.blocks);
            }
            // Person lookups jump around a table far bigger than the CPU cache. Ask for the
            // entry PF events ahead so the memory waits overlap instead of queueing up.
            let pf = prefetch_distance();
            for x in kept.iter().take(pf) {
                pmap.prefetch(x.0);
            }
            for i in 0..kept.len() {
                if i + pf < kept.len() {
                    pmap.prefetch(kept[i + pf].0);
                }
                let (uk, ts, pc) = kept[i];
                let pk = if exp == 7 { uk } else { pmap.get(uk) };
                if exp == 8 {
                    std::hint::black_box(pk);
                    continue;
                }
                unsafe { g.get_unchecked_mut((pk.mix() >> (64 - PB)) as usize) }.push(PEv { pk, ts, pc });
            }
        });
    }));
    std::mem::forget(stage1); // freeing is pointless work: the OS reclaims it at exit
    std::mem::forget(stage1p);
    let pev = pev.into_inner();
    lap("dedup");
    // ------------------------------------------------------------ 4. clicks
    // Smaller chunks for clicks: there is ~4x less click data, and 4 MB chunks left threads idle
    // at the end of the stage.
    let ck_chunks = make_chunks_sized(sizes, ne..ne + nc, CK_CHUNK);
    let pck: PerThread<PCk> = PerThread::new(nt, npb, (ck_bytes / 45 / npb / nt) * 5 / 4 + 16);
    let ck_one = |c: &Chunk| {
        let tck = Instant::now();
        pck.with(|g| CSCR.with(|cs_| with_chunk(c, &mut |buf, s0, e0| {
            let mut local = cs_.borrow_mut();
            local.clear();
            let mut tsp = TsParser::new();
            // Parse first (pk holds the user key for now)...
            scan_fields::<4>(buf, s0, e0, |fb| {
                let (cs, ce) = fb[0];
                let (us, ue) = fb[1];
                if cs == ce || us == ue {
                    return;
                }
                let Some(ts) = tsp.parse(fld(buf, fb[3])) else { return };
                if ts < base || ts >= we {
                    return;
                }
                let Some(camp) = camp_tab.get(buf, fb[2].0, fb[2].1 - fb[2].0) else { return };
                let uk = key_of(buf, us, ue - us, Some(reg));
                let ck = key_of(buf, cs, ce - cs, Some(reg));
                local.push(PCk { pk: uk, ck, ts: (ts - base) as u32, camp });
            });
            // ...then map users to persons with the table entries prefetched ahead.
            let pf = prefetch_distance();
            for x in local.iter().take(pf) {
                pmap.prefetch(x.pk);
            }
            for i in 0..local.len() {
                if i + pf < local.len() {
                    pmap.prefetch(local[i + pf].pk);
                }
                let mut r = local[i];
                r.pk = pmap.get(r.pk);
                unsafe { g.get_unchecked_mut((r.pk.mix() >> (64 - PB)) as usize) }.push(r);
            }
        })));
        ts_add(&TS_CK_N, &TS_CK_NS, tck);
    };
    if std::env::var_os("SS_ML1").is_some() {
        ck_chunks.par_iter().with_max_len(1).for_each(ck_one);
    } else {
        ck_chunks.par_iter().for_each(ck_one);
    }
    let pck = pck.into_inner();
    lap(&format!("clicks ({ck_chunk_count} chunks)"));

    // ------------------------------------------------------------ 5. sessions
    let camp_acc: Vec<Mutex<Vec<[i64; 4]>>> = (0..nt).map(|_| Mutex::new(vec![[0i64; 4]; (ncamp + 1) * ndays])).collect();
    let any_long = !reg.0.lock().unwrap().is_empty();
    let task_times: Mutex<Vec<(f64, usize)>> = Mutex::new(Vec::new()); // profiling (SS_EXP=13)
    // Biggest buckets first (whales, shared devices), one task each, so the long ones start
    // immediately and the small ones fill the gaps at the end.
    let mut order: Vec<usize> = (0..npb).collect();
    let weight = |b: usize| -> usize { pev.iter().map(|t| t[b].len()).sum::<usize>() + pck.iter().map(|t| t[b].len()).sum::<usize>() };
    order.sort_by_cached_key(|&b| std::cmp::Reverse(weight(b)));
    let ud: Vec<Vec<UdRow>> = order
        .par_iter()
        .with_max_len(1)
        .map(|&b| SSCR.with(|scr| {
            let _tt = TaskTimer(exp == 13, if exp == 13 { Instant::now() } else { t0 }, b, &task_times);
            // Group this bucket's events and clicks by person AND sort them by time, with no
            // comparisons: one radix sort of the whole bucket by ts, then a stable counting
            // scatter by person id. Person order does not matter for the output. While grouping,
            // each record's pk field holds its dense person id (the real key is pidx.keys[id]).
            let mut scr = scr.borrow_mut();
            let SessScratch { ea, eb, ca, cb, est, cst, pidx, .. } = &mut *scr;
            let n_ev: usize = pev.iter().map(|t| t[b].len()).sum();
            pidx.reset(n_ev / 16 + 16);
            ea.clear();
            ea.reserve(n_ev);
            for t in &pev {
                for e in t[b].iter() {
                    ea.push(PEv { pk: K(pidx.id(e.pk) as u64), ts: e.ts, pc: e.pc });
                }
            }
            let np = pidx.keys.len();
            est.clear();
            est.resize(np + 2, 0);
            for e in ea.iter() {
                est[e.pk.0 as usize + 2] += 1;
            }
            for i in 2..np + 2 {
                est[i] += est[i - 1];
            }
            // The person scatter goes into whichever buffer the sort left free (no third buffer).
            let (sorted_e, ev) = if exp == 11 || radix31(ea, eb, |e| e.ts) { (&*ea, &mut *eb) } else { (&*eb, &mut *ea) };
            ev.clear();
            ev.reserve(n_ev);
            unsafe {
                let evp = ev.as_mut_ptr();
                for e in sorted_e.iter() {
                    let w = est.get_unchecked_mut(e.pk.0 as usize + 1);
                    *evp.add(*w as usize) = *e;
                    *w += 1;
                }
                ev.set_len(n_ev);
            }
            let ev: &[PEv] = &ev[..];
            // est[p]..est[p + 1] is person p's events, in time order. Clicks of persons with no
            // events are dropped: they can never credit or assist a session.
            ca.clear();
            for t in &pck {
                for c in t[b].iter() {
                    if let Some(g) = pidx.get(c.pk) {
                        ca.push(PCk { pk: K(g as u64), ck: c.ck, ts: c.ts, camp: c.camp });
                    }
                }
            }
            let n_ck_kept = ca.len();
            cst.clear();
            cst.resize(np + 2, 0);
            for c in ca.iter() {
                cst[c.pk.0 as usize + 2] += 1;
            }
            for i in 2..np + 2 {
                cst[i] += cst[i - 1];
            }
            let (sorted_c, ck) = if exp == 11 || radix31(ca, cb, |c| c.ts) { (&*ca, &mut *cb) } else { (&*cb, &mut *ca) };
            ck.clear();
            ck.reserve(n_ck_kept);
            unsafe {
                let ckp = ck.as_mut_ptr();
                for c in sorted_c.iter() {
                    let w = cst.get_unchecked_mut(c.pk.0 as usize + 1);
                    *ckp.add(*w as usize) = *c;
                    *w += 1;
                }
                ck.set_len(n_ck_kept);
            }
            if exp == 14 {
                std::hint::black_box((ev.len(), ck.len()));
                return Vec::new();
            }
            // Clicks with the same ts within a person must be ordered by click_id (the tie-break
            // for crediting): fix up the (rare) runs of equal ts.
            for p in 0..np {
                let cks = &mut ck[cst[p] as usize..cst[p + 1] as usize];
                let mut i = 1;
                while i < cks.len() {
                    if cks[i].ts != cks[i - 1].ts {
                        i += 1;
                        continue;
                    }
                    let st = i - 1;
                    let mut en = i + 1;
                    while en < cks.len() && cks[en].ts == cks[st].ts {
                        en += 1;
                    }
                    if any_long {
                        cks[st..en].sort_unstable_by(|a, c| cmp_keys(a.ck, c.ck, reg));
                    } else {
                        cks[st..en].sort_unstable_by_key(|c| c.ck);
                    }
                    i = en;
                }
            }
            if exp == 12 {
                std::hint::black_box((ev.len(), ck.len()));
                return Vec::new();
            }

            let mut rows: Vec<UdRow> = Vec::new();
            let mut cacc_guard = camp_acc[rayon::current_thread_index().unwrap_or(0)].lock().unwrap();
            let cacc = &mut *cacc_guard;
            // Sliding 7-day click window per person: how many window clicks each campaign has,
            // and the list of campaigns currently in the window (with each one's list position).
            let mut ccount = vec![0u32; ncamp];
            let mut cpos = vec![0u32; ncamp];
            let mut active: Vec<u32> = Vec::new();
            let mut cats = vec![0i64; ncat];
            let mut touched: Vec<u16> = Vec::new();

            for pi in 0..np {
                let pk = pidx.keys[pi];
                let i = est[pi] as usize;
                let j = est[pi + 1] as usize;
                let pc = &ck[cst[pi] as usize..cst[pi + 1] as usize];
                // Sessions come in time order, so the window [start - 7 days, start] only moves
                // forward: clicks enter at `hi` and leave at `lo`.
                let mut lo = 0usize;
                let mut hi = 0usize;

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
                    while hi < pc.len() && pc[hi].ts <= start {
                        let c = pc[hi].camp as usize;
                        if ccount[c] == 0 {
                            cpos[c] = active.len() as u32;
                            active.push(c as u32);
                        }
                        ccount[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && pc[lo].ts < lo_ts {
                        let c = pc[lo].camp as usize;
                        ccount[c] -= 1;
                        if ccount[c] == 0 {
                            let at = cpos[c] as usize;
                            let last = active.pop().unwrap();
                            if at < active.len() {
                                active[at] = last;
                                cpos[last as usize] = at as u32;
                            }
                        }
                        lo += 1;
                    }
                    // Credit: the latest click in the window; on a tie in ts, the smallest click_id
                    // (clicks are sorted by (ts, click_id), so the first of the latest-ts run).
                    let credited = if hi > lo {
                        let mx = pc[hi - 1].ts;
                        let mut k = hi - 1;
                        while k > lo && pc[k - 1].ts == mx {
                            k -= 1;
                        }
                        pc[k].camp
                    } else {
                        organic
                    };
                    let a = &mut cacc[credited as usize * ndays + di];
                    a[0] += 1;
                    a[1] += purchased as i64;
                    a[2] += rev;
                    // Every campaign with a click in the window assists once.
                    for &c in active.iter() {
                        cacc[c as usize * ndays + di][3] += 1;
                    }
                    s = e;
                }
                for &c in active.iter() {
                    ccount[c as usize] = 0;
                }
                active.clear();
                if let Some(mut r) = row {
                    r.top = top_cat(&mut cats, &mut touched);
                    rows.push(r);
                }
            }
            rows
        }))
        .collect();
    std::mem::forget(pev); // freeing is pointless work: the OS reclaims it at exit
    std::mem::forget(pck); // freeing is pointless work: the OS reclaims it at exit
    if exp == 13 {
        let mut v = task_times.lock().unwrap().clone();
        v.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        let sum: f64 = v.iter().map(|x| x.0).sum();
        eprintln!("sessions tasks: {} total {:.3}s, mean {:.4}s, slowest: {:?}", v.len(), sum, sum / v.len() as f64,
            v.iter().take(6).map(|x| format!("{:.4}s", x.0)).collect::<Vec<_>>());
    }
    lap("sessions");

    // ------------------------------------------------------------ 6. write
    let ud_dir = out.join("user_daily");
    let cd_dir = out.join("campaign_daily");
    fs::create_dir_all(&ud_dir).unwrap();
    fs::create_dir_all(&cd_dir).unwrap();

    let nfiles = nt.min(npb);
    let groups: Vec<Vec<&Vec<UdRow>>> = (0..nfiles).map(|k| ud.iter().skip(k).step_by(nfiles).collect()).collect();
    // Category names PLAIN-encoded once (u32 length + bytes).
    let cat_enc: Vec<Vec<u8>> = cat_names
        .iter()
        .map(|c| {
            let mut v = (c.len() as u32).to_le_bytes().to_vec();
            v.extend_from_slice(c);
            v
        })
        .collect();
    let cat_enc_ref = &cat_enc;
    let ud_dir_ref = &ud_dir;
    rayon::scope(|sc| {
        for (k, g) in groups.iter().enumerate() {
            sc.spawn(move |_| {
                let n: usize = g.iter().map(|v| v.len()).sum();
                // Columns are encoded directly in their Parquet PLAIN form: names decode straight
                // into the length-prefixed byte column, the top category into presence bits +
                // values. Counts are bounded by the number of events (< 2^31), so INT32 holds them;
                // revenue is INT32 when every value fits, INT64 otherwise (any width is accepted).
                let mut uid: Vec<u8> = Vec::with_capacity(n * 14);
                let mut day: Vec<i32> = Vec::with_capacity(n);
                let mut ses: Vec<i32> = Vec::with_capacity(n);
                let mut evs: Vec<i32> = Vec::with_capacity(n);
                let mut bits = vec![0u8; n.div_ceil(8)];
                let mut tops: Vec<u8> = Vec::with_capacity(n * 4);
                let mut rev_fits = true;
                let mut i = 0usize;
                for v in g {
                    for r in v.iter() {
                        let s = uid.len();
                        uid.extend_from_slice(&[0u8; 4]);
                        if r.pk.is_long() {
                            uid.extend_from_slice(&key_bytes(r.pk, reg));
                        } else {
                            decode_compact(r.pk, &mut uid);
                        }
                        let l = (uid.len() - s - 4) as u32;
                        uid[s..s + 4].copy_from_slice(&l.to_le_bytes());
                        day.push(r.day);
                        ses.push(r.sessions as i32);
                        evs.push(r.events as i32);
                        rev_fits &= r.revenue <= i32::MAX as i64;
                        if r.top != u16::MAX {
                            bits[i >> 3] |= 1 << (i & 7);
                            tops.extend_from_slice(&cat_enc_ref[r.top as usize]);
                        }
                        i += 1;
                    }
                }
                let rev = if rev_fits {
                    Col::I32(g.iter().flat_map(|v| v.iter()).map(|r| r.revenue as i32).collect())
                } else {
                    Col::I64(g.iter().flat_map(|v| v.iter()).map(|r| r.revenue).collect())
                };
                pq::write(
                    &ud_dir_ref.join(format!("part-{k}.parquet")),
                    &[
                        ("user_id", Col::StrEnc(uid, n)),
                        ("day", Col::Date(day)),
                        ("sessions", Col::I32(ses)),
                        ("events", Col::I32(evs)),
                        ("revenue_cents", rev),
                        ("top_category", Col::OptStrEnc { bits, vals: tops, rows: n }),
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
    if std::env::var_os("SS_TSTAT").is_some() {
        eprintln!("{}", ts_line("EVT", &TS_EV_N, &TS_EV_NS));
        eprintln!("{}", ts_line("CKT", &TS_CK_N, &TS_CK_NS));
    }
    #[cfg(target_os = "linux")]
    if diag_on {
        eprintln!("DIAG done: failing on purpose so the dev-set logs are shown");
        std::process::exit(3);
    }
    // Output files are fully written and closed above. Exit now instead of unmapping
    // the inputs and freeing every buffer; the OS reclaims all of it instantly.
    #[cfg(all(windows, feature = "profile"))]
    if let Some(h) = sampler_thread {
        sampler::STOP.store(true, Relaxed);
        h.join().unwrap();
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every timestamp string in the dev data, plus many mangled variants, must parse
    /// identically with the fast parser and the careful one.
    #[test]
    fn fast_timestamps_match_careful_parser() {
        let dir = std::env::var("TS_TEST_DATA").unwrap_or_else(|_| "C:/dev/shopping-sessions/data/dev".into());
        let mut samples: Vec<Vec<u8>> = Vec::new();
        for sub in ["events", "clicks"] {
            for p in csv_files(&Path::new(&dir).join(sub)) {
                let buf = fs::read(p).unwrap();
                for line in buf.split(|&c| c == b'\n').skip(1) {
                    let f: Vec<&[u8]> = line.split(|&c| c == b',').collect();
                    let cols: &[usize] = if sub == "events" { &[2, 3] } else { &[3] };
                    for &c in cols {
                        if let Some(v) = f.get(c) {
                            let v = if v.len() >= 2 && v[0] == b'"' && v[v.len() - 1] == b'"' { &v[1..v.len() - 1] } else { v };
                            samples.push(v.to_vec());
                        }
                    }
                }
            }
        }
        // Hand-written edge cases.
        for s in [
            "2028-02-29T12:00:00Z", "2027-02-29T12:00:00Z", "2026-13-01T00:00:00Z", "2026-00-10T00:00:00Z",
            "2026-10-03T24:00:00Z", "2026-10-03T23:60:00Z", "2026-10-03T23:59:60Z", "2026-10-03 10:00:00Z",
            "2026-10-03t10:00:00Z", "2026-10-03T10:00:00z", "2026-10-03T10:00:00+24:00", "2026-10-03T10:00:00+05:60",
            "2026-10-03T10:00:00+0200", "2026-10-03T10:00:00-00:00", "2026-10-03T10:00:00+23:59", "1791071890000",
            "0001791071890000", "86400000", "1791071890000.0", "1.79e12", "-5", "", "179107189000a", "a791071890000",
            "9999999999999", "0000000000000", "2026-10-03T10:00:00.000Z", "2026-1O-03T10:00:00Z", "2026-10-03T10:0:000Z",
        ] {
            samples.push(s.as_bytes().to_vec());
        }
        let base_n = samples.len();
        // Mangle each sample: replace one byte, drop one byte, duplicate one byte.
        let alphabet = b"0159:-+TZzt .\r/\x00\xff";
        let mut rng: u64 = 0x1234_5678;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for i in 0..base_n.min(400_000) {
            let s = samples[i].clone();
            if s.is_empty() {
                continue;
            }
            let pos = (next() as usize) % s.len();
            let mut a = s.clone();
            a[pos] = alphabet[(next() as usize) % alphabet.len()];
            samples.push(a);
            let mut b = s.clone();
            b.remove(pos);
            samples.push(b);
            let mut c = s.clone();
            c.insert(pos, s[pos]);
            samples.push(c);
        }
        let mut tsp = TsParser::new();
        let mut checked = 0usize;
        for (i, s) in samples.iter().enumerate() {
            if i % 5000 == 0 {
                tsp = TsParser::new(); // exercise cold caches too
            }
            // Parsers read up to 16 bytes past a field inside their buffers; mimic that slack.
            let mut padded = s.clone();
            padded.extend_from_slice(&[b','; 16]);
            let fast = tsp.parse(&padded[..s.len()]);
            let careful = parse_ts(s);
            assert_eq!(fast, careful, "mismatch on {:?}", String::from_utf8_lossy(s));
            checked += 1;
        }
        eprintln!("checked {checked} timestamp strings ({base_n} real + mangled)");
    }
}

#[cfg(test)]
mod fused_tests {
    use super::*;

    /// compact_key2 == two compact_key calls, and parse_pair == two parse calls, on real
    /// dev-data fields and mangled variants of them.
    #[test]
    fn fused_paths_match_separate_paths() {
        let dir = std::env::var("TS_TEST_DATA").unwrap_or_else(|_| "C:/dev/shopping-sessions/data/dev".into());
        let mut ids: Vec<Vec<u8>> = Vec::new();
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for p in csv_files(&Path::new(&dir).join("events")) {
            let buf = fs::read(p).unwrap();
            for line in buf.split(|&c| c == b'\n').skip(1).take(60_000) {
                let f: Vec<&[u8]> = line.split(|&c| c == b',').collect();
                if f.len() == 7 {
                    let strip = |v: &[u8]| if v.len() >= 2 && v[0] == b'"' && v[v.len() - 1] == b'"' { v[1..v.len() - 1].to_vec() } else { v.to_vec() };
                    ids.push(strip(f[0]));
                    ids.push(strip(f[1]));
                    pairs.push((strip(f[2]), strip(f[3])));
                }
            }
        }
        let mut rng: u64 = 0x9E37_79B9;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let alphabet = b"09azAZ_ \r\"-:TZ+.";
        let n0 = ids.len();
        for i in 0..n0 {
            let mut a = ids[i].clone();
            if !a.is_empty() {
                let pos = (next() as usize) % a.len();
                a[pos] = alphabet[(next() as usize) % alphabet.len()];
            }
            ids.push(a);
        }
        let p0 = pairs.len();
        for i in 0..p0 {
            let (mut t, mut g) = pairs[i].clone();
            match next() % 4 {
                0 => g = t.clone(), // identical
                1 => {
                    if t.len() > 12 {
                        let pos = 11 + (next() as usize) % (t.len() - 11);
                        t[pos] = alphabet[(next() as usize) % alphabet.len()];
                    }
                }
                2 => {
                    if !g.is_empty() {
                        let pos = (next() as usize) % g.len();
                        g[pos] = alphabet[(next() as usize) % alphabet.len()];
                    }
                }
                _ => {}
            }
            pairs.push((t, g));
        }
        // IDs, two at a time, laid out like a line (with 16 bytes of slack after each).
        let mut checked = 0;
        for w in ids.windows(2) {
            let (a, b) = (&w[0], &w[1]);
            if a.is_empty() || b.is_empty() || a.len() > 12 || b.len() > 12 {
                continue;
            }
            let mut buf = a.clone();
            buf.push(b',');
            let o2 = buf.len();
            buf.extend_from_slice(b);
            buf.extend_from_slice(&[b','; 32]);
            let fused = compact_key2(&buf, 0, a.len(), o2, b.len());
            let sep = (compact_key(&buf, 0, a.len()), compact_key(&buf, o2, b.len()));
            assert_eq!(fused, sep, "ids {:?} {:?}", String::from_utf8_lossy(a), String::from_utf8_lossy(b));
            checked += 1;
        }
        let mut t1 = TsParser::new();
        let mut t2 = TsParser::new();
        for (i, (t, g)) in pairs.iter().enumerate() {
            if i % 5000 == 0 {
                t1 = TsParser::new();
                t2 = TsParser::new();
            }
            let fused = t1.parse_pair(t, g);
            let sep = (t2.parse(t), t2.parse(g));
            assert_eq!(fused, sep, "pair {:?} {:?}", String::from_utf8_lossy(t), String::from_utf8_lossy(g));
            checked += 1;
        }
        eprintln!("checked {checked} id pairs + timestamp pairs");
    }
}
