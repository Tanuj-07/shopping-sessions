//! Shopping Sessions: straightforward reference implementation.
//! Written for clarity and exact adherence to rules.md, not speed.
//! Every faster version is checked against this one on many seeds.

use std::fs;
use std::path::{Path, PathBuf};

use rustc_hash::{FxHashMap, FxHashSet};
use shopping_sessions::pq::{self, Col};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const ORGANIC: u32 = u32::MAX;

// ---------------------------------------------------------------- CSV dialect

/// Split one line into exactly `n` fields per the rules. Returns false if malformed.
/// Empty values (including `""`) become None.
fn fields<'a>(line: &'a [u8], n: usize, out: &mut Vec<Option<&'a [u8]>>) -> bool {
    out.clear();
    for f in line.split(|&c| c == b',') {
        if out.len() == n {
            return false;
        }
        let v = if f.contains(&b'"') {
            if f.len() < 2 || f[0] != b'"' || f[f.len() - 1] != b'"' || f[1..f.len() - 1].contains(&b'"') {
                return false;
            }
            &f[1..f.len() - 1]
        } else {
            f
        };
        out.push(if v.is_empty() { None } else { Some(v) });
    }
    out.len() == n
}

/// Data lines of a CSV file (header skipped). Empty lines are yielded and rejected by `fields`.
fn data_lines(buf: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut it = buf.split(|&c| c == b'\n');
    it.next(); // header
    let ends_nl = buf.last() == Some(&b'\n');
    let mut lines: Vec<&[u8]> = it.collect();
    if ends_nl {
        lines.pop(); // the empty piece after the final newline is not a line
    }
    lines.into_iter()
}

// ---------------------------------------------------------------- timestamps

fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (a, c) = (b[i], b[i + 1]);
    if a.is_ascii_digit() && c.is_ascii_digit() {
        Some(((a - b'0') as i64) * 10 + (c - b'0') as i64)
    } else {
        None
    }
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Parse F1/F2/F3 to epoch ms. None = bad timestamp (window not checked here).
fn parse_ts(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    if b.iter().all(|c| c.is_ascii_digit()) {
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
    let secs = days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s;
    Some(secs * 1000 - off_ms)
}

fn parse_price(b: Option<&[u8]>) -> Option<i64> {
    let b = b?;
    if !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let start = b.iter().position(|&c| c != b'0').unwrap_or(b.len());
    let b = &b[start..];
    if b.len() > 8 {
        return None;
    }
    let mut v = 0i64;
    for &c in b {
        v = v * 10 + (c - b'0') as i64;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

// ---------------------------------------------------------------- files

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

// ---------------------------------------------------------------- ids

struct Interner<'a> {
    map: FxHashMap<&'a [u8], u32>,
    names: Vec<&'a [u8]>,
}

impl<'a> Interner<'a> {
    fn id(&mut self, s: &'a [u8]) -> u32 {
        if let Some(&i) = self.map.get(s) {
            return i;
        }
        let i = self.names.len() as u32;
        self.map.insert(s, i);
        self.names.push(s);
        i
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

// ---------------------------------------------------------------- main

struct Ev {
    person: u32,
    ts: i64,
    purchase: bool,
    price: i64,
    cat: u32,
}

struct Ck<'a> {
    person: u32,
    ts: i64,
    id: &'a [u8],
    camp: u32,
}

#[derive(Default)]
struct UAgg {
    sessions: i64,
    events: i64,
    revenue: i64,
    cats: Vec<(u32, i64)>,
}

#[derive(Default)]
struct CAgg {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

fn main() {
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));

    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");

    // Load every file up front so parsed fields can borrow from them.
    let products_buf = fs::read(data.join("products.csv")).expect("products");
    let campaigns_buf = fs::read(data.join("campaigns.csv")).expect("campaigns");
    let ev_bufs: Vec<Vec<u8>> = csv_files(&data.join("events")).iter().map(|p| fs::read(p).unwrap()).collect();
    let ck_bufs: Vec<Vec<u8>> = csv_files(&data.join("clicks")).iter().map(|p| fs::read(p).unwrap()).collect();
    let ln_bufs: Vec<Vec<u8>> = csv_files(&data.join("links")).iter().map(|p| fs::read(p).unwrap()).collect();

    let mut f: Vec<Option<&[u8]>> = Vec::with_capacity(8);

    // Products -> category index. Categories indexed in byte-wise order, plus `unknown`.
    let mut prod_cat: FxHashMap<&[u8], &[u8]> = FxHashMap::default();
    for line in data_lines(&products_buf) {
        if fields(line, 2, &mut f) {
            if let (Some(p), Some(c)) = (f[0], f[1]) {
                prod_cat.insert(p, c);
            }
        }
    }
    let mut cat_names: Vec<&[u8]> = prod_cat.values().copied().collect();
    cat_names.push(b"unknown");
    cat_names.sort();
    cat_names.dedup();
    let cat_idx: FxHashMap<&[u8], u32> = cat_names.iter().enumerate().map(|(i, c)| (*c, i as u32)).collect();
    let unknown_cat = cat_idx[&b"unknown"[..]];

    // Campaigns.
    let mut camp_ids: Vec<&[u8]> = Vec::new();
    let mut camp_chan: Vec<&[u8]> = Vec::new();
    let mut camp_idx: FxHashMap<&[u8], u32> = FxHashMap::default();
    for line in data_lines(&campaigns_buf) {
        if fields(line, 2, &mut f) {
            if let (Some(c), Some(ch)) = (f[0], f[1]) {
                if !camp_idx.contains_key(c) {
                    camp_idx.insert(c, camp_ids.len() as u32);
                    camp_ids.push(c);
                    camp_chan.push(ch);
                }
            }
        }
    }

    let mut users = Interner { map: FxHashMap::default(), names: Vec::new() };

    // Events: parse -> dedup (smallest ingest_ts) -> validate -> late filter.
    let mut best: FxHashMap<&[u8], (i64, [Option<&[u8]>; 7])> = FxHashMap::default();
    for buf in &ev_bufs {
        for line in data_lines(buf) {
            if !fields(line, 7, &mut f) {
                continue;
            }
            let Some(eid) = f[0] else { continue };
            let ingest = match f[3].and_then(parse_ts) {
                Some(t) => t,
                None => i64::MAX, // rules say ingest_ts is always valid F1
            };
            let rec = [f[0], f[1], f[2], f[3], f[4], f[5], f[6]];
            match best.get_mut(eid) {
                Some(cur) => {
                    if ingest < cur.0 {
                        *cur = (ingest, rec);
                    }
                }
                None => {
                    best.insert(eid, (ingest, rec));
                }
            }
        }
    }
    let mut events_raw: Vec<(u32, i64, bool, i64, u32)> = Vec::with_capacity(best.len());
    for (_eid, (ingest, r)) in best.iter() {
        let Some(uid) = r[1] else { continue };
        let Some(ts) = r[2].and_then(parse_ts) else { continue };
        if ts < ws || ts >= we {
            continue;
        }
        let purchase = match r[4] {
            Some(b"view") | Some(b"cart") => false,
            Some(b"purchase") => true,
            _ => continue,
        };
        let Some(pid) = r[5] else { continue };
        let price = if purchase {
            match parse_price(r[6]) {
                Some(p) => p,
                None => continue,
            }
        } else {
            0
        };
        if ingest - ts > DAY_MS {
            continue;
        }
        let cat = match prod_cat.get(pid) {
            Some(c) => cat_idx[c],
            None => unknown_cat,
        };
        events_raw.push((users.id(uid), ts, purchase, price, cat));
    }
    drop(best);

    // Clicks: parse -> dedup on click_id -> validate.
    let mut seen: FxHashSet<&[u8]> = FxHashSet::default();
    let mut clicks_raw: Vec<(u32, i64, &[u8], u32)> = Vec::new();
    for buf in &ck_bufs {
        for line in data_lines(buf) {
            if !fields(line, 4, &mut f) {
                continue;
            }
            let Some(cid) = f[0] else { continue };
            if !seen.insert(cid) {
                continue;
            }
            let Some(uid) = f[1] else { continue };
            let Some(ts) = f[3].and_then(parse_ts) else { continue };
            if ts < ws - WEEK_MS || ts >= we {
                continue;
            }
            let Some(camp) = f[2].and_then(|c| camp_idx.get(c).copied()) else { continue };
            clicks_raw.push((users.id(uid), ts, cid, camp));
        }
    }
    drop(seen);

    // Links: union-find over all IDs.
    let mut link_pairs: Vec<(u32, u32)> = Vec::new();
    for buf in &ln_bufs {
        for line in data_lines(buf) {
            if !fields(line, 2, &mut f) {
                continue;
            }
            if let (Some(a), Some(b)) = (f[0], f[1]) {
                link_pairs.push((users.id(a), users.id(b)));
            }
        }
    }
    let n = users.names.len();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    for &(a, b) in &link_pairs {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[ra as usize] = rb;
        }
    }
    // Person name = byte-wise smallest member ID; person key = root.
    let mut root_of: Vec<u32> = vec![0; n];
    let mut name_of_root: Vec<u32> = (0..n as u32).collect();
    for i in 0..n as u32 {
        let r = find(&mut parent, i);
        root_of[i as usize] = r;
        if users.names[i as usize] < users.names[name_of_root[r as usize] as usize] {
            name_of_root[r as usize] = i;
        }
    }

    let mut events: Vec<Ev> = events_raw
        .into_iter()
        .map(|(u, ts, purchase, price, cat)| Ev { person: root_of[u as usize], ts, purchase, price, cat })
        .collect();
    events.sort_unstable_by(|a, b| (a.person, a.ts).cmp(&(b.person, b.ts)));

    let mut clicks: Vec<Ck> = clicks_raw
        .into_iter()
        .map(|(u, ts, id, camp)| Ck { person: root_of[u as usize], ts, id, camp })
        .collect();
    clicks.sort_unstable_by(|a, b| (a.person, a.ts, a.id).cmp(&(b.person, b.ts, b.id)));

    let mut uagg: FxHashMap<(u32, i64), UAgg> = FxHashMap::default();
    let mut cagg: FxHashMap<(u32, i64), CAgg> = FxHashMap::default();

    let mut ci = 0usize; // start of this person's clicks
    let mut i = 0usize;
    let mut assist: Vec<u32> = Vec::new();
    while i < events.len() {
        let person = events[i].person;
        let mut j = i;
        while j < events.len() && events[j].person == person {
            j += 1;
        }
        while ci < clicks.len() && clicks[ci].person < person {
            ci += 1;
        }
        let mut cj = ci;
        while cj < clicks.len() && clicks[cj].person == person {
            cj += 1;
        }
        let pclicks = &clicks[ci..cj];

        // Sessionize events[i..j].
        let mut s = i;
        while s < j {
            let mut e = s + 1;
            while e < j && events[e].ts - events[e - 1].ts <= GAP_MS {
                e += 1;
            }
            let start = events[s].ts;
            let day = start.div_euclid(DAY_MS);
            let mut n_ev = 0i64;
            let mut rev = 0i64;
            let mut any_purchase = false;
            let ua = uagg.entry((person, day)).or_default();
            ua.sessions += 1;
            for ev in &events[s..e] {
                n_ev += 1;
                if ev.purchase {
                    any_purchase = true;
                    rev += ev.price;
                    match ua.cats.iter_mut().find(|(c, _)| *c == ev.cat) {
                        Some(x) => x.1 += ev.price,
                        None => ua.cats.push((ev.cat, ev.price)),
                    }
                }
            }
            ua.events += n_ev;
            ua.revenue += rev;

            // Attribution: clicks with start - 7d <= ts <= start.
            let hi = pclicks.partition_point(|c| c.ts <= start);
            let lo = pclicks.partition_point(|c| c.ts < start - WEEK_MS);
            let credited = if hi > lo {
                let max_ts = pclicks[hi - 1].ts;
                let first = pclicks.partition_point(|c| c.ts < max_ts);
                pclicks[first].camp
            } else {
                ORGANIC
            };
            let ca = cagg.entry((credited, day)).or_default();
            ca.sessions += 1;
            ca.revenue += rev;
            if any_purchase {
                ca.purchasing += 1;
            }
            assist.clear();
            for c in &pclicks[lo..hi] {
                if !assist.contains(&c.camp) {
                    assist.push(c.camp);
                }
            }
            for &c in &assist {
                cagg.entry((c, day)).or_default().assisted += 1;
            }
            s = e;
        }
        i = j;
    }

    // ------------------------------------------------------------ write
    let ud_dir = out.join("user_daily");
    let cd_dir = out.join("campaign_daily");
    fs::create_dir_all(&ud_dir).unwrap();
    fs::create_dir_all(&cd_dir).unwrap();

    {
        let mut user_id = Vec::with_capacity(uagg.len());
        let mut day = Vec::with_capacity(uagg.len());
        let mut sessions = Vec::with_capacity(uagg.len());
        let mut evs = Vec::with_capacity(uagg.len());
        let mut revenue = Vec::with_capacity(uagg.len());
        let mut top: Vec<Option<&[u8]>> = Vec::with_capacity(uagg.len());
        for ((person, d), a) in &uagg {
            user_id.push(users.names[name_of_root[*person as usize] as usize]);
            day.push(*d as i32);
            sessions.push(a.sessions);
            evs.push(a.events);
            revenue.push(a.revenue);
            // Highest sum; tie -> smallest category name (= smallest index).
            let t = a.cats.iter().max_by(|x, y| x.1.cmp(&y.1).then(y.0.cmp(&x.0))).map(|(c, _)| cat_names[*c as usize]);
            top.push(t);
        }
        pq::write(
            &ud_dir.join("part-0.parquet"),
            &[
                ("user_id", Col::Str(user_id)),
                ("day", Col::Date(day)),
                ("sessions", Col::I64(sessions)),
                ("events", Col::I64(evs)),
                ("revenue_cents", Col::I64(revenue)),
                ("top_category", Col::OptStr(top)),
            ],
        )
        .unwrap();
    }
    {
        let mut cid: Vec<&[u8]> = Vec::with_capacity(cagg.len());
        let mut chan: Vec<&[u8]> = Vec::with_capacity(cagg.len());
        let mut day = Vec::with_capacity(cagg.len());
        let mut sessions = Vec::with_capacity(cagg.len());
        let mut purch = Vec::with_capacity(cagg.len());
        let mut revenue = Vec::with_capacity(cagg.len());
        let mut assisted = Vec::with_capacity(cagg.len());
        for ((c, d), a) in &cagg {
            if *c == ORGANIC {
                cid.push(b"organic");
                chan.push(b"organic");
            } else {
                cid.push(camp_ids[*c as usize]);
                chan.push(camp_chan[*c as usize]);
            }
            day.push(*d as i32);
            sessions.push(a.sessions);
            purch.push(a.purchasing);
            revenue.push(a.revenue);
            assisted.push(a.assisted);
        }
        pq::write(
            &cd_dir.join("part-0.parquet"),
            &[
                ("campaign_id", Col::Str(cid)),
                ("channel", Col::Str(chan)),
                ("day", Col::Date(day)),
                ("sessions", Col::I64(sessions)),
                ("purchasing_sessions", Col::I64(purch)),
                ("revenue_cents", Col::I64(revenue)),
                ("assisted_sessions", Col::I64(assisted)),
            ],
        )
        .unwrap();
    }
}
