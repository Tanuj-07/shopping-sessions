//! Shopping Sessions: parallel version.
//!
//! Stages:
//!  1. links  -> union-find -> map id -> (person hash, name ref)
//!  2. events -> parse in parallel chunks -> hash-partition by event_id
//!  3. dedup each event_id bucket in parallel -> valid events -> partition by person
//!  4. clicks -> parse in parallel chunks -> partition by person
//!  5. each person bucket in parallel: sort, sessionize, attribute, aggregate
//!  6. write Parquet files in parallel

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use memchr::memchr;
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use shopping_sessions::pq::{self, Col};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const B_BITS: u32 = 8;
const NB: usize = 1 << B_BITS;
const CHUNK: usize = 4 << 20;

#[inline]
fn bucket(h: u64) -> usize {
    (h >> (64 - B_BITS)) as usize
}

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
#[inline]
fn hash(b: &[u8]) -> u64 {
    const S1: u64 = 0xe703_7ed1_a0b4_28db;
    const S2: u64 = 0x8ebc_6af0_9c88_c6e3;
    let n = b.len();
    let mut seed = S2;
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

// ---------------------------------------------------------------- refs into mapped files

#[inline]
fn mkref(f: usize, off: usize, len: usize) -> u64 {
    debug_assert!(len < (1 << 16) && off < (1usize << 32) && f < (1 << 16));
    ((f as u64) << 48) | ((off as u64) << 16) | (len.min(0xffff) as u64)
}
#[inline]
fn deref<'a>(bufs: &[&'a [u8]], r: u64) -> &'a [u8] {
    let f = (r >> 48) as usize;
    let off = ((r >> 16) & 0xffff_ffff) as usize;
    let len = (r & 0xffff) as usize;
    &bufs[f][off..off + len]
}

// ---------------------------------------------------------------- CSV dialect

/// Field ranges (relative to the line), quotes stripped. Empty range = null.
#[inline]
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
    if memchr(b'"', line).is_some() {
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
    }
    true
}

struct Chunk {
    file: usize,
    start: usize,
    end: usize,
}

fn make_chunks(bufs: &[&[u8]], files: std::ops::Range<usize>) -> Vec<Chunk> {
    let mut out = Vec::new();
    for f in files {
        let m = bufs[f];
        let n = m.len();
        let Some(h) = memchr(b'\n', m) else { continue };
        let mut s = h + 1;
        while s < n {
            let mut e = (s + CHUNK).min(n);
            if e < n {
                e = match memchr(b'\n', &m[e..]) {
                    Some(p) => e + p + 1,
                    None => n,
                };
            }
            out.push(Chunk { file: f, start: s, end: e });
            s = e;
        }
    }
    out
}

#[inline]
fn for_lines(m: &[u8], s: usize, e: usize, mut f: impl FnMut(usize, &[u8])) {
    let mut p = s;
    while p < e {
        let q = memchr(b'\n', &m[p..e]).map(|x| p + x).unwrap_or(e);
        f(p, &m[p..q]);
        p = q + 1;
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
    if b.is_empty() {
        return None;
    }
    if b[0].is_ascii_digit() && b.iter().all(|c| c.is_ascii_digit()) {
        let mut v: u64 = 0;
        for &c in b {
            v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
        }
        if v > i64::MAX as u64 {
            return None;
        }
        return Some(v as i64);
    }
    if b.len() != 20 && b.len() != 25 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
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
    let off_ms = if b.len() == 20 {
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
    Some((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s) * 1000 - off_ms)
}

#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() || !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let start = b.iter().position(|&c| c != b'0').unwrap_or(b.len());
    let b = &b[start..];
    if b.len() > 8 {
        return None;
    }
    let mut v = 0u32;
    for &c in b {
        v = v * 10 + (c - b'0') as u32;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct ERaw {
    eh: u64,
    eref: u64,
    ingest: i64,
    uh: u64,
    uref: u64,
    ts: i64,
    price: u32,
    cat: u16,
    valid: bool,
}

#[derive(Clone, Copy)]
struct PEv {
    ph: u64,
    ts: i64,
    nref: u64,
    price: u32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct PCk {
    ph: u64,
    ts: i64,
    cref: u64,
    camp: u32,
}

struct UdRow {
    nref: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: u16,
}

/// Per-thread bucketed output buffers.
struct PerThread<T> {
    slots: Vec<Mutex<Vec<Vec<T>>>>,
}

impl<T: Send> PerThread<T> {
    fn new(nt: usize) -> Self {
        PerThread { slots: (0..nt).map(|_| Mutex::new((0..NB).map(|_| Vec::new()).collect())).collect() }
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

struct PersonMap {
    map: FxHashMap<u64, (u64, u64, u64)>, // id hash -> (id ref, person hash, name ref)
}

impl PersonMap {
    #[inline]
    fn get(&self, bufs: &[&[u8]], uh: u64, uref: u64) -> (u64, u64) {
        if let Some(&(idr, ph, nr)) = self.map.get(&uh) {
            if deref(bufs, idr) == deref(bufs, uref) {
                return (ph, nr);
            }
        }
        (uh, uref)
    }
}

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let gp = p[p[x as usize] as usize];
        p[x as usize] = gp;
        x = gp;
    }
    x
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
    let nt: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or_else(|| {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
    });
    rayon::ThreadPoolBuilder::new().num_threads(nt).build_global().unwrap();

    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    let day_lo = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - day_lo + 1).max(1) as usize;

    // Map every CSV file.
    let ev_files = csv_files(&data.join("events"));
    let ck_files = csv_files(&data.join("clicks"));
    let ln_files = csv_files(&data.join("links"));
    let all: Vec<&PathBuf> = ev_files.iter().chain(&ck_files).chain(&ln_files).collect();
    let maps: Vec<Option<Mmap>> = all
        .par_iter()
        .map(|p| {
            let f = fs::File::open(p).unwrap();
            if f.metadata().unwrap().len() == 0 {
                None
            } else {
                Some(unsafe { Mmap::map(&f).unwrap() })
            }
        })
        .collect();
    let bufs: Vec<&[u8]> = maps.iter().map(|m| m.as_deref().unwrap_or(&[])).collect();
    let (ne, nc) = (ev_files.len(), ck_files.len());
    let ev_range = 0..ne;
    let ck_range = ne..ne + nc;
    let ln_range = ne + nc..bufs.len();

    // Products and campaigns (small).
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
    let ncat = cat_names.len();
    let cat_index = |c: &[u8]| cat_names.binary_search(&c).unwrap() as u16;
    let unknown_cat = cat_index(b"unknown");
    let mut prod_cat: FxHashMap<&[u8], u16> = FxHashMap::default();
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
    lap("setup");

    // ------------------------------------------------------------ 1. links
    let ln_chunks = make_chunks(&bufs, ln_range);
    let pairs: Vec<Vec<(u64, u64, u64, u64)>> = ln_chunks
        .par_iter()
        .map(|c| {
            let m = bufs[c.file];
            let mut v = Vec::new();
            let mut f = [(0usize, 0usize); 2];
            for_lines(m, c.start, c.end, |ls, line| {
                if !split(line, &mut f) || f[0].0 == f[0].1 || f[1].0 == f[1].1 {
                    return;
                }
                let a = &line[f[0].0..f[0].1];
                let b = &line[f[1].0..f[1].1];
                v.push((hash(a), mkref(c.file, ls + f[0].0, a.len()), hash(b), mkref(c.file, ls + f[1].0, b.len())));
            });
            v
        })
        .collect();
    let mut ids: FxHashMap<u64, u32> = FxHashMap::default();
    let mut id_ref: Vec<u64> = Vec::new();
    let mut id_hash: Vec<u64> = Vec::new();
    let mut intern = |h: u64, r: u64| -> u32 {
        if let Some(&i) = ids.get(&h) {
            if deref(&bufs, id_ref[i as usize]) != deref(&bufs, r) {
                panic!("64-bit hash collision between link ids");
            }
            return i;
        }
        let i = id_ref.len() as u32;
        ids.insert(h, i);
        id_ref.push(r);
        id_hash.push(h);
        i
    };
    let mut edges: Vec<(u32, u32)> = Vec::new();
    for v in &pairs {
        for &(ha, ra, hb, rb) in v {
            let a = intern(ha, ra);
            let b = intern(hb, rb);
            edges.push((a, b));
        }
    }
    drop(pairs);
    let nid = id_ref.len();
    let mut parent: Vec<u32> = (0..nid as u32).collect();
    for &(a, b) in &edges {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[ra as usize] = rb;
        }
    }
    let mut name_of_root: Vec<u32> = (0..nid as u32).collect();
    let mut roots = vec![0u32; nid];
    for i in 0..nid as u32 {
        let r = find(&mut parent, i);
        roots[i as usize] = r;
        let cur = name_of_root[r as usize];
        if deref(&bufs, id_ref[i as usize]) < deref(&bufs, id_ref[cur as usize]) {
            name_of_root[r as usize] = i;
        }
    }
    let mut pmap = PersonMap { map: FxHashMap::with_capacity_and_hasher(nid, Default::default()) };
    for i in 0..nid {
        let nm = name_of_root[roots[i] as usize] as usize;
        pmap.map.insert(id_hash[i], (id_ref[i], id_hash[nm], id_ref[nm]));
    }
    drop(ids);
    lap(&format!("links ({} ids)", nid));

    // ------------------------------------------------------------ 2. events parse
    let ev_chunks = make_chunks(&bufs, ev_range);
    let stage1: PerThread<ERaw> = PerThread::new(nt);
    ev_chunks.par_iter().for_each(|c| {
        let m = bufs[c.file];
        let mut local: Vec<Vec<ERaw>> = (0..NB).map(|_| Vec::new()).collect();
        let mut f = [(0usize, 0usize); 7];
        for_lines(m, c.start, c.end, |ls, line| {
            if !split(line, &mut f) {
                return;
            }
            let fld = |i: usize| &line[f[i].0..f[i].1];
            let eid = fld(0);
            if eid.is_empty() {
                return;
            }
            let eh = hash(eid);
            let mut r = ERaw {
                eh,
                eref: mkref(c.file, ls + f[0].0, eid.len()),
                ingest: parse_ts(fld(3)).unwrap_or(i64::MAX),
                uh: 0,
                uref: 0,
                ts: 0,
                price: 0,
                cat: 0,
                valid: false,
            };
            'v: {
                let user = fld(1);
                if user.is_empty() {
                    break 'v;
                }
                let Some(ts) = parse_ts(fld(2)) else { break 'v };
                if ts < ws || ts >= we {
                    break 'v;
                }
                let purchase = match fld(4) {
                    b"view" | b"cart" => false,
                    b"purchase" => true,
                    _ => break 'v,
                };
                let prod = fld(5);
                if prod.is_empty() {
                    break 'v;
                }
                let price = if purchase {
                    match parse_price(fld(6)) {
                        Some(p) => p,
                        None => break 'v,
                    }
                } else {
                    0
                };
                if r.ingest - ts > DAY_MS {
                    break 'v;
                }
                r.uh = hash(user);
                r.uref = mkref(c.file, ls + f[1].0, user.len());
                r.ts = ts;
                r.price = price;
                r.cat = if purchase { prod_cat.get(prod).copied().unwrap_or(unknown_cat) } else { 0 };
                r.valid = true;
            }
            local[bucket(eh)].push(r);
        });
        stage1.with(|g| {
            for (b, v) in local.into_iter().enumerate() {
                if g[b].is_empty() {
                    g[b] = v;
                } else {
                    g[b].extend_from_slice(&v);
                }
            }
        });
    });
    let stage1 = stage1.into_inner();
    lap(&format!("events parse ({} chunks)", ev_chunks.len()));

    // ------------------------------------------------------------ 3. dedup -> person buckets
    let pev: PerThread<PEv> = PerThread::new(nt);
    (0..NB).into_par_iter().for_each(|b| {
        let parts: Vec<&[ERaw]> = stage1.iter().map(|t| &t[b][..]).collect();
        let n: usize = parts.iter().map(|p| p.len()).sum();
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        let mut table = vec![u32::MAX; cap];
        let get = |x: u32| &parts[(x >> 27) as usize][(x & ((1 << 27) - 1)) as usize];
        for (t, part) in parts.iter().enumerate() {
            for (i, r) in part.iter().enumerate() {
                let packed = ((t as u32) << 27) | i as u32;
                let mut s = (r.eh as usize) & mask;
                loop {
                    let x = table[s];
                    if x == u32::MAX {
                        table[s] = packed;
                        break;
                    }
                    let o = get(x);
                    if o.eh == r.eh && deref(&bufs, o.eref) == deref(&bufs, r.eref) {
                        if r.ingest < o.ingest {
                            table[s] = packed;
                        }
                        break;
                    }
                    s = (s + 1) & mask;
                }
            }
        }
        let mut local: Vec<Vec<PEv>> = (0..NB).map(|_| Vec::new()).collect();
        for &x in &table {
            if x == u32::MAX {
                continue;
            }
            let r = get(x);
            if !r.valid {
                continue;
            }
            let (ph, nref) = pmap.get(&bufs, r.uh, r.uref);
            local[bucket(ph)].push(PEv { ph, ts: r.ts, nref, price: r.price, cat: r.cat, purchase: r.price > 0 });
        }
        pev.with(|g| {
            for (b, v) in local.into_iter().enumerate() {
                g[b].extend_from_slice(&v);
            }
        });
    });
    drop(stage1);
    let pev = pev.into_inner();
    lap("dedup");

    // ------------------------------------------------------------ 4. clicks
    let ck_chunks = make_chunks(&bufs, ck_range);
    let pck: PerThread<PCk> = PerThread::new(nt);
    ck_chunks.par_iter().for_each(|c| {
        let m = bufs[c.file];
        let mut local: Vec<Vec<PCk>> = (0..NB).map(|_| Vec::new()).collect();
        let mut f = [(0usize, 0usize); 4];
        for_lines(m, c.start, c.end, |ls, line| {
            if !split(line, &mut f) {
                return;
            }
            let fld = |i: usize| &line[f[i].0..f[i].1];
            let cid = fld(0);
            let user = fld(1);
            if cid.is_empty() || user.is_empty() {
                return;
            }
            let Some(ts) = parse_ts(fld(3)) else { return };
            if ts < ws - WEEK_MS || ts >= we {
                return;
            }
            let Some(&camp) = camp_idx.get(fld(2)) else { return };
            let (ph, _) = pmap.get(&bufs, hash(user), mkref(c.file, ls + f[1].0, user.len()));
            local[bucket(ph)].push(PCk { ph, ts, cref: mkref(c.file, ls + f[0].0, cid.len()), camp });
        });
        pck.with(|g| {
            for (b, v) in local.into_iter().enumerate() {
                g[b].extend_from_slice(&v);
            }
        });
    });
    let pck = pck.into_inner();
    lap(&format!("clicks ({} chunks)", ck_chunks.len()));

    // ------------------------------------------------------------ 5. sessions
    let camp_acc: Vec<Mutex<Vec<[i64; 4]>>> = (0..nt).map(|_| Mutex::new(vec![[0i64; 4]; (ncamp + 1) * ndays])).collect();
    let ud: Vec<Vec<UdRow>> = (0..NB)
        .into_par_iter()
        .map(|b| {
            let mut ev: Vec<PEv> = Vec::with_capacity(pev.iter().map(|t| t[b].len()).sum());
            for t in &pev {
                ev.extend_from_slice(&t[b]);
            }
            ev.sort_unstable_by_key(|e| (e.ph, e.ts));
            let mut ck: Vec<PCk> = Vec::with_capacity(pck.iter().map(|t| t[b].len()).sum());
            for t in &pck {
                ck.extend_from_slice(&t[b]);
            }
            ck.sort_unstable_by(|a, c| {
                a.ph.cmp(&c.ph).then(a.ts.cmp(&c.ts)).then_with(|| deref(&bufs, a.cref).cmp(deref(&bufs, c.cref)))
            });

            let mut rows: Vec<UdRow> = Vec::new();
            let mut cacc = vec![[0i64; 4]; (ncamp + 1) * ndays];
            let mut stamp = vec![u32::MAX; ncamp];
            let mut sess_no: u32 = 0;
            let mut cats = vec![0i64; ncat];
            let mut touched: Vec<u16> = Vec::new();

            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let ph = ev[i].ph;
                let mut j = i + 1;
                while j < ev.len() && ev[j].ph == ph {
                    j += 1;
                }
                while ci < ck.len() && ck[ci].ph < ph {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < ck.len() && ck[cj].ph == ph {
                    cj += 1;
                }
                let pc = &ck[ci..cj];
                let nref = ev[i].nref;

                let mut row: Option<UdRow> = None;
                let mut s = i;
                while s < j {
                    let mut e = s + 1;
                    while e < j && ev[e].ts - ev[e - 1].ts <= GAP_MS {
                        e += 1;
                    }
                    let start = ev[s].ts;
                    let day = start.div_euclid(DAY_MS);
                    if row.as_ref().map(|r| r.day as i64 != day).unwrap_or(false) {
                        let mut r = row.take().unwrap();
                        r.top = top_cat(&mut cats, &mut touched);
                        rows.push(r);
                    }
                    let r = row.get_or_insert(UdRow { nref, day: day as i32, sessions: 0, events: 0, revenue: 0, top: u16::MAX });
                    let mut rev = 0i64;
                    let mut purchased = false;
                    for x in &ev[s..e] {
                        if x.purchase {
                            purchased = true;
                            rev += x.price as i64;
                            if cats[x.cat as usize] == 0 {
                                touched.push(x.cat);
                            }
                            cats[x.cat as usize] += x.price as i64;
                        }
                    }
                    r.sessions += 1;
                    r.events += (e - s) as i64;
                    r.revenue += rev;

                    let di = (day - day_lo) as usize;
                    let hi = pc.partition_point(|c| c.ts <= start);
                    let lo = pc.partition_point(|c| c.ts < start - WEEK_MS);
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
            let t = rayon::current_thread_index().unwrap_or(0);
            let mut g = camp_acc[t].lock().unwrap();
            for (x, y) in g.iter_mut().zip(&cacc) {
                for k in 0..4 {
                    x[k] += y[k];
                }
            }
            rows
        })
        .collect();
    drop(pev);
    drop(pck);
    lap("sessions");

    // ------------------------------------------------------------ 6. write
    let ud_dir = out.join("user_daily");
    let cd_dir = out.join("campaign_daily");
    fs::create_dir_all(&ud_dir).unwrap();
    fs::create_dir_all(&cd_dir).unwrap();

    let nfiles = nt.min(NB);
    let groups: Vec<Vec<&Vec<UdRow>>> = (0..nfiles).map(|k| ud.iter().skip(k).step_by(nfiles).collect()).collect();
    let cat_names_ref = &cat_names;
    let bufs_ref = &bufs;
    let ud_dir_ref = &ud_dir;
    rayon::scope(|sc| {
        for (k, g) in groups.iter().enumerate() {
            sc.spawn(move |_| {
                let n: usize = g.iter().map(|v| v.len()).sum();
                let mut uid = Vec::with_capacity(n);
                let mut day = Vec::with_capacity(n);
                let mut ses = Vec::with_capacity(n);
                let mut evs = Vec::with_capacity(n);
                let mut rev = Vec::with_capacity(n);
                let mut top: Vec<Option<&[u8]>> = Vec::with_capacity(n);
                for v in g {
                    for r in v.iter() {
                        uid.push(deref(bufs_ref, r.nref));
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
