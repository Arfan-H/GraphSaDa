#!/usr/bin/env python3
"""
harvest_openalex.py - Harvest publikasi bidang data science dari OpenAlex.

Alur kerja (jalankan berurutan):
  1. topics   : cari kandidat topik OpenAlex dari daftar kata kunci
                -> <out>/topics_candidates.csv  (periksa & edit kolom `include`)
  2. count    : cek jumlah hasil + estimasi biaya SEBELUM harvest
  3. harvest  : ambil semua works (cursor pagination, otomatis bisa di-resume)
                -> <out>/raw/openalex_works.jsonl
  4. flatten  : ubah JSONL menjadi CSV siap impor ke Neo4j

Contoh:
  export OPENALEX_API_KEY=xxxxx          # Windows: set OPENALEX_API_KEY=xxxxx
  python harvest_openalex.py topics  --out data
  python harvest_openalex.py count   --out data --country ID --from-year 2018
  python harvest_openalex.py harvest --out data --country ID --from-year 2018
  python harvest_openalex.py flatten --out data

Catatan:
  - Sejak Feb 2026 API OpenAlex butuh API key (gratis) di openalex.org/settings/api
  - Hanya butuh library `requests`.
"""
import argparse
import csv
import hashlib
import json
import math
import os
import sys
import time
from pathlib import Path

import requests

BASE_URL = "https://api.openalex.org"
PER_PAGE = 100          # maksimum yang diizinkan API
MAX_OR_VALUES = 100     # maksimum nilai OR dalam satu filter
COST_LIST_USD = 0.0001  # estimasi biaya per list request (cek docs jika berubah)

DEFAULT_KEYWORDS = [
    "data science", "machine learning", "deep learning", "data mining",
    "big data", "text mining", "sentiment analysis", "natural language processing",
    "computer vision", "image classification", "recommender system",
    "time series forecasting", "clustering", "neural network",
    "statistical learning", "predictive analytics", "anomaly detection",
    "information retrieval", "knowledge graph", "explainable artificial intelligence",
    "feature selection", "classification algorithm",
]

# Field yang otomatis diberi include=1 di topics_candidates.csv (tetap WAJIB kamu review)
AUTO_INCLUDE_FIELDS = {"Computer Science", "Mathematics", "Decision Sciences"}

WORK_SELECT = ",".join([
    "id", "doi", "title", "publication_year", "publication_date", "type",
    "language", "cited_by_count", "abstract_inverted_index", "authorships",
    "primary_location", "primary_topic", "topics", "keywords",
])


# --------------------------------------------------------------------------- #
# Utilitas
# --------------------------------------------------------------------------- #
def log(msg):
    print(msg, flush=True)


def short_id(url_or_id):
    """'https://openalex.org/T10028' -> 'T10028'; 'https://openalex.org/subfields/1702' -> '1702'."""
    if not url_or_id:
        return ""
    return str(url_or_id).rstrip("/").rsplit("/", 1)[-1]


def norm_doi(doi):
    if not doi:
        return ""
    d = doi.strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if d.startswith(prefix):
            d = d[len(prefix):]
    return d


def rebuild_abstract(inverted_index):
    """OpenAlex menyimpan abstrak sebagai inverted index {kata: [posisi,...]}."""
    if not inverted_index:
        return ""
    positions = []
    for word, idxs in inverted_index.items():
        for i in idxs:
            positions.append((i, word))
    positions.sort()
    return " ".join(w for _, w in positions)


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


# --------------------------------------------------------------------------- #
# HTTP client dengan retry, backoff, dan pencatatan biaya
# --------------------------------------------------------------------------- #
class OpenAlexClient:
    def __init__(self, api_key, mailto=None, delay=0.15, max_retries=6):
        self.api_key = api_key
        self.delay = delay
        self.max_retries = max_retries
        self.total_cost = 0.0
        self.n_requests = 0
        self.session = requests.Session()
        ua = "skripsi-kg-harvester/1.0"
        if mailto:
            ua += f" (mailto:{mailto})"
        self.session.headers["User-Agent"] = ua

    def _mask(self, text):
        return text.replace(self.api_key, "***") if self.api_key else text

    def get(self, path, params):
        params = dict(params)
        if self.api_key:
            params["api_key"] = self.api_key
        last_err = ""
        for attempt in range(self.max_retries):
            try:
                r = self.session.get(BASE_URL + path, params=params, timeout=60)
            except requests.RequestException as e:
                last_err = str(e)
                wait = 2 ** attempt
                log(f"  [jaringan] {e.__class__.__name__}, coba lagi dalam {wait}s ...")
                time.sleep(wait)
                continue

            if r.status_code == 200:
                data = r.json()
                self.n_requests += 1
                cost = (data.get("meta") or {}).get("cost_usd")
                if isinstance(cost, (int, float)):
                    self.total_cost += cost
                time.sleep(self.delay)
                return data

            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After", "")
                wait = float(ra) if ra.replace(".", "", 1).isdigit() else 2 ** attempt
                last_err = f"HTTP {r.status_code}"
                log(f"  [HTTP {r.status_code}] tunggu {wait:.0f}s lalu coba lagi ...")
                time.sleep(wait)
                continue

            # 400/401/403/404/409: kesalahan yang tidak akan hilang dengan retry
            raise SystemExit(
                f"[ERROR] HTTP {r.status_code}\nURL: {self._mask(r.url)}\n"
                f"Pesan: {r.text[:800]}"
            )
        raise SystemExit(
            f"[ERROR] Gagal setelah {self.max_retries}x percobaan ({last_err}).\n"
            "Jika 429 terus-menerus, kemungkinan kuota harian habis. Jalankan lagi "
            "besok; perintah `harvest` akan melanjutkan dari posisi terakhir."
        )


# --------------------------------------------------------------------------- #
# Penyusunan filter
# --------------------------------------------------------------------------- #
def load_topic_ids(topics_file):
    """Baca CSV topik. Tahan terhadap BOM dan pemisah ';' (Excel berbahasa Indonesia)."""
    ids = []
    with open(topics_file, encoding="utf-8-sig", newline="") as f:
        header_line = f.readline()
        f.seek(0)
        delim = ";" if header_line.count(";") > header_line.count(",") else ","
        for row in csv.DictReader(f, delimiter=delim):
            row = {(k or "").strip().lower(): v for k, v in row.items()}
            if str(row.get("include", "")).strip().lower() in ("1", "y", "yes", "ya", "true"):
                ids.append(short_id((row.get("topic_id") or "").strip()))
    return sorted(set(i for i in ids if i))


def topic_chunks_from_args(args):
    """Return list of (filter_key, [values]) chunks. Satu chunk = satu rangkaian cursor."""
    if args.subfields:
        vals = [short_id(s.strip()) for s in args.subfields.split(",") if s.strip()]
        key = "primary_topic.subfield.id" if args.primary_only else "topics.subfield.id"
        return [(key, c) for c in chunks(vals, MAX_OR_VALUES)]

    topics_file = Path(args.topics_file) if args.topics_file else Path(args.out) / "topics_candidates.csv"
    if not topics_file.exists():
        raise SystemExit(
            f"[ERROR] {topics_file} tidak ditemukan. Jalankan dulu perintah `topics`, "
            "atau pakai --subfields (mis. --subfields 1702,1707)."
        )
    ids = load_topic_ids(topics_file)
    if not ids:
        raise SystemExit(f"[ERROR] Tidak ada baris include=1 di {topics_file}.")
    key = "primary_topic.id" if args.primary_only else "topics.id"
    return [(key, c) for c in chunks(ids, MAX_OR_VALUES)]


def build_filter(args, topic_key, topic_vals):
    parts = []
    if args.country:
        parts.append(f"authorships.institutions.country_code:{args.country.upper()}")
    if args.institution:
        parts.append(f"authorships.institutions.id:{short_id(args.institution)}")
    if args.from_year and args.to_year:
        parts.append(f"publication_year:{args.from_year}-{args.to_year}")
    elif args.from_year:
        parts.append(f"publication_year:>{args.from_year - 1}")
    elif args.to_year:
        parts.append(f"publication_year:<{args.to_year + 1}")
    if args.types:
        parts.append("type:" + "|".join(t.strip() for t in args.types.split(",")))
    if args.languages:
        parts.append("language:" + "|".join(l.strip() for l in args.languages.split(",")))
    if args.require_abstract:
        parts.append("has_abstract:true")
    parts.append(f"{topic_key}:" + "|".join(topic_vals))
    return ",".join(parts)


def make_client(args):
    key = args.api_key or os.environ.get("OPENALEX_API_KEY", "")
    if not key:
        log("[PERINGATAN] Tanpa API key kuota sangat kecil (hanya untuk uji). "
            "Ambil key gratis di https://openalex.org/settings/api")
    return OpenAlexClient(key, mailto=args.mailto, delay=args.delay)


# --------------------------------------------------------------------------- #
# Perintah: topics
# --------------------------------------------------------------------------- #
def cmd_topics(args):
    client = make_client(args)
    keywords = DEFAULT_KEYWORDS
    if args.keywords_file:
        with open(args.keywords_file, encoding="utf-8") as f:
            keywords = [l.strip() for l in f if l.strip() and not l.startswith("#")]

    found = {}
    for kw in keywords:
        log(f"Cari topik: {kw!r}")
        data = client.get("/topics", {
            "search": kw, "per_page": args.per_keyword,
            "select": "id,display_name,description,subfield,field,works_count",
        })
        for t in data.get("results", []):
            tid = short_id(t["id"])
            row = found.setdefault(tid, {
                "topic_id": tid,
                "display_name": t.get("display_name", ""),
                "subfield_id": short_id((t.get("subfield") or {}).get("id")),
                "subfield_name": (t.get("subfield") or {}).get("display_name", ""),
                "field_name": (t.get("field") or {}).get("display_name", ""),
                "works_count": t.get("works_count", 0),
                "description": (t.get("description") or "").replace("\n", " "),
                "matched_keywords": set(),
            })
            row["matched_keywords"].add(kw)

    rows = []
    for r in sorted(found.values(), key=lambda x: (-len(x["matched_keywords"]), x["display_name"])):
        include = 1 if r["field_name"] in AUTO_INCLUDE_FIELDS else 0
        rows.append([include, r["topic_id"], r["display_name"], r["subfield_id"],
                     r["subfield_name"], r["field_name"], r["works_count"],
                     "; ".join(sorted(r["matched_keywords"])), r["description"]])
    out = Path(args.out) / "topics_candidates.csv"
    write_csv(out, ["include", "topic_id", "display_name", "subfield_id", "subfield_name",
                    "field_name", "works_count", "matched_keywords", "description"], rows)
    n_inc = sum(1 for r in rows if r[0] == 1)
    log(f"\nSelesai: {len(rows)} kandidat topik -> {out}")
    log(f"{n_inc} diberi include=1 otomatis (field: {', '.join(sorted(AUTO_INCLUDE_FIELDS))}).")
    log("PENTING: buka file itu, review manual, ubah kolom include (1/0), lalu simpan.")
    log(f"Perkiraan biaya perintah ini: ${client.total_cost:.4f} (dari meta.cost_usd)")


# --------------------------------------------------------------------------- #
# Perintah: count
# --------------------------------------------------------------------------- #
def cmd_count(args):
    client = make_client(args)
    total = 0
    for i, (key, vals) in enumerate(topic_chunks_from_args(args)):
        flt = build_filter(args, key, vals)
        data = client.get("/works", {"filter": flt, "per_page": 1, "select": "id"})
        n = data["meta"]["count"]
        total += n
        log(f"Chunk {i}: {len(vals)} nilai {key} -> {n:,} works")
    pages = math.ceil(total / PER_PAGE)
    log(f"\nTotal (batas atas, bisa ada irisan antar-chunk): {total:,} works")
    log(f"Perkiraan request: {pages:,}  |  perkiraan biaya: ${pages * COST_LIST_USD:.2f}")
    log("Kuota gratis ~US$1/hari per key. Harvest bisa dicicil beberapa hari (resume otomatis).")


# --------------------------------------------------------------------------- #
# Perintah: harvest (resumable)
# --------------------------------------------------------------------------- #
def cmd_harvest(args):
    client = make_client(args)
    raw_dir = Path(args.out) / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = raw_dir / "openalex_works.jsonl"
    state_path = raw_dir / "openalex_state.json"

    plan = topic_chunks_from_args(args)
    filters = [build_filter(args, k, v) for k, v in plan]
    plan_hash = hashlib.sha1("\n".join(filters).encode()).hexdigest()[:12]

    state = {}
    if state_path.exists() and not args.reset:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("plan_hash") != plan_hash:
            raise SystemExit(
                "[ERROR] Filter/topik berbeda dari harvest sebelumnya. Pakai --reset untuk "
                "mulai ulang (file JSONL lama akan ditambahi; duplikat dibuang saat flatten)."
            )
    if not state or args.reset:
        state = {"plan_hash": plan_hash, "chunks": {}}

    def save_state():
        tmp = state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        tmp.replace(state_path)

    fetched_this_run = 0
    with open(jsonl_path, "a", encoding="utf-8") as out:
        for i, flt in enumerate(filters):
            cs = state["chunks"].setdefault(str(i), {"cursor": "*", "done": False, "fetched": 0})
            if cs["done"]:
                log(f"Chunk {i}: sudah selesai ({cs['fetched']:,} works), dilewati.")
                continue
            log(f"Chunk {i}: mulai/lanjut (sudah {cs['fetched']:,} works)")
            while True:
                data = client.get("/works", {
                    "filter": flt, "per_page": PER_PAGE,
                    "cursor": cs["cursor"], "select": WORK_SELECT,
                })
                results = data.get("results", [])
                for w in results:
                    out.write(json.dumps(w, ensure_ascii=False) + "\n")
                out.flush()
                cs["fetched"] += len(results)
                fetched_this_run += len(results)
                nxt = (data.get("meta") or {}).get("next_cursor")
                if not results or not nxt:
                    cs["done"] = True
                    cs["cursor"] = None
                    save_state()
                    log(f"Chunk {i}: SELESAI, total {cs['fetched']:,} works")
                    break
                cs["cursor"] = nxt
                save_state()
                if cs["fetched"] % 1000 < PER_PAGE:
                    log(f"  ... {cs['fetched']:,} works (biaya sejauh ini ${client.total_cost:.4f})")
                if args.max_records and fetched_this_run >= args.max_records:
                    log(f"Berhenti karena --max-records={args.max_records}. Jalankan lagi untuk lanjut.")
                    log(f"Biaya run ini: ${client.total_cost:.4f}")
                    return
    log(f"\nHarvest selesai. Run ini: {fetched_this_run:,} works, {client.n_requests} request, "
        f"biaya ${client.total_cost:.4f}")
    log(f"Lanjutkan dengan: python harvest_openalex.py flatten --out {args.out}")


# --------------------------------------------------------------------------- #
# Perintah: flatten -> CSV
# --------------------------------------------------------------------------- #
def cmd_flatten(args):
    out_dir = Path(args.out)
    jsonl_path = out_dir / "raw" / "openalex_works.jsonl"
    if not jsonl_path.exists():
        raise SystemExit(f"[ERROR] {jsonl_path} tidak ada. Jalankan `harvest` dulu.")

    works, authors, institutions, topics = {}, {}, {}, {}
    authorships, work_topics, work_keywords = [], [], []
    n_lines = 0

    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            n_lines += 1
            w = json.loads(line)
            wid = short_id(w.get("id"))
            if not wid or wid in works:
                continue  # buang duplikat (akibat resume / irisan chunk)

            src = ((w.get("primary_location") or {}).get("source") or {})
            pt = w.get("primary_topic") or {}
            abstract = rebuild_abstract(w.get("abstract_inverted_index"))
            works[wid] = [
                wid, norm_doi(w.get("doi")), (w.get("title") or "").strip(), abstract,
                1 if abstract else 0, w.get("publication_year") or "",
                w.get("publication_date") or "", w.get("type") or "",
                w.get("language") or "", short_id(src.get("id")), src.get("display_name") or "",
                w.get("cited_by_count") or 0, short_id(pt.get("id")),
            ]

            for a in w.get("authorships") or []:
                au = a.get("author") or {}
                aid = short_id(au.get("id"))
                if not aid:
                    continue
                authors.setdefault(aid, [aid, au.get("display_name") or "",
                                         (au.get("orcid") or "").replace("https://orcid.org/", "")])
                inst_ids = []
                for inst in a.get("institutions") or []:
                    iid = short_id(inst.get("id"))
                    if not iid:
                        continue
                    inst_ids.append(iid)
                    institutions.setdefault(iid, [
                        iid, inst.get("display_name") or "", inst.get("country_code") or "",
                        inst.get("type") or "", (inst.get("ror") or "").replace("https://ror.org/", ""),
                    ])
                authorships.append([
                    wid, aid, a.get("author_position") or "",
                    1 if a.get("is_corresponding") else 0,
                    ";".join(inst_ids), ";".join(a.get("countries") or []),
                    a.get("raw_author_name") or "",
                ])

            for t in w.get("topics") or []:
                tid = short_id(t.get("id"))
                if not tid:
                    continue
                sf, fd, dm = t.get("subfield") or {}, t.get("field") or {}, t.get("domain") or {}
                topics.setdefault(tid, [
                    tid, t.get("display_name") or "", short_id(sf.get("id")),
                    sf.get("display_name") or "", short_id(fd.get("id")),
                    fd.get("display_name") or "", dm.get("display_name") or "",
                ])
                work_topics.append([wid, tid, t.get("score") or "",
                                    1 if tid == short_id(pt.get("id")) else 0])

            for k in w.get("keywords") or []:
                work_keywords.append([wid, short_id(k.get("id")), k.get("display_name") or "",
                                      k.get("score") or ""])

    csv_dir = out_dir / "openalex"
    write_csv(csv_dir / "works.csv",
              ["work_id", "doi", "title", "abstract", "has_abstract", "year", "publication_date",
               "type", "language", "source_id", "source_name", "cited_by_count", "primary_topic_id"],
              works.values())
    write_csv(csv_dir / "authors.csv", ["author_id", "name", "orcid"], authors.values())
    write_csv(csv_dir / "institutions.csv",
              ["institution_id", "name", "country_code", "type", "ror"], institutions.values())
    write_csv(csv_dir / "authorships.csv",
              ["work_id", "author_id", "author_position", "is_corresponding",
               "institution_ids", "countries", "raw_author_name"], authorships)
    write_csv(csv_dir / "topics.csv",
              ["topic_id", "name", "subfield_id", "subfield_name", "field_id",
               "field_name", "domain_name"], topics.values())
    write_csv(csv_dir / "work_topics.csv", ["work_id", "topic_id", "score", "is_primary"], work_topics)
    write_csv(csv_dir / "work_keywords.csv", ["work_id", "keyword_id", "keyword", "score"], work_keywords)

    n_abs = sum(1 for r in works.values() if r[4] == 1)
    years = [r[5] for r in works.values() if r[5]]
    log(f"Baris JSONL dibaca : {n_lines:,}")
    log(f"Works unik         : {len(works):,}")
    if works:
        log(f"  punya abstrak    : {n_abs:,} ({100 * n_abs / len(works):.1f}%)")
    if years:
        log(f"  rentang tahun    : {min(years)}-{max(years)}")
    log(f"Authors unik       : {len(authors):,}")
    log(f"Institutions unik  : {len(institutions):,}")
    log(f"Topics unik        : {len(topics):,}")
    log(f"CSV tersimpan di   : {csv_dir}")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def add_common(p):
    p.add_argument("--out", default="data", help="folder output (default: data)")
    p.add_argument("--api-key", help="API key OpenAlex (atau env OPENALEX_API_KEY)")
    p.add_argument("--mailto", help="email kamu, dicantumkan di User-Agent (sopan)")
    p.add_argument("--delay", type=float, default=0.15, help="jeda antar request (detik)")


def add_filters(p):
    p.add_argument("--topics-file", help="CSV topik (default: <out>/topics_candidates.csv)")
    p.add_argument("--subfields", help="alternatif topik: ID subfield dipisah koma, mis. 1702,1707")
    p.add_argument("--primary-only", action="store_true",
                   help="hanya cocokkan primary topic (presisi naik, recall turun)")
    p.add_argument("--country", default="ID", help="kode negara afiliasi (default ID). Kosongkan: --country ''")
    p.add_argument("--institution", help="batasi ke satu institusi (ID OpenAlex, mis. I123456)")
    p.add_argument("--from-year", type=int, default=2018)
    p.add_argument("--to-year", type=int)
    p.add_argument("--types", default="article", help="jenis karya, pisah koma (default: article)")
    p.add_argument("--languages", help="bahasa ISO 639-1, pisah koma, mis. en,id")
    p.add_argument("--require-abstract", action="store_true",
                   help="hanya ambil works yang punya abstrak di OpenAlex")


def main():
    ap = argparse.ArgumentParser(description="Harvester OpenAlex untuk KG peneliti data science")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("topics", help="cari kandidat topik dari kata kunci")
    add_common(p)
    p.add_argument("--keywords-file", help="file teks, satu kata kunci per baris (bahasa Inggris)")
    p.add_argument("--per-keyword", type=int, default=25)
    p.set_defaults(func=cmd_topics)

    p = sub.add_parser("count", help="hitung jumlah hasil + estimasi biaya")
    add_common(p)
    add_filters(p)
    p.set_defaults(func=cmd_count)

    p = sub.add_parser("harvest", help="ambil works (resumable)")
    add_common(p)
    add_filters(p)
    p.add_argument("--max-records", type=int, help="berhenti setelah N works (untuk uji coba)")
    p.add_argument("--reset", action="store_true", help="abaikan state lama, mulai dari awal")
    p.set_defaults(func=cmd_harvest)

    p = sub.add_parser("flatten", help="JSONL -> CSV siap Neo4j")
    add_common(p)
    p.set_defaults(func=cmd_flatten)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
