#!/usr/bin/env python3
"""
make_subset.py - Potong CSV hasil harvest menjadi subset kecil yang muat di AuraDB Free.

Kenapa perlu?
  AuraDB Free membatasi jumlah node dan relasi (50rb-200rb node tergantung sumber
  dokumentasi; skrip ini menargetkan jauh di bawah batas terendah agar aman).
  Data penuh kamu ~332rb node, jadi harus dipotong.

Strategi (menjaga graf tetap terhubung):
  1. Pilih publikasi sesuai filter (tahun, punya abstrak, topik tertentu)
  2. Urutkan berdasarkan sitasi, ambil N teratas
  3. Ambil SEMUA penulis dari publikasi terpilih
  4. Ambil institusi & topik yang benar-benar dirujuk
  5. (opsional) Buang penulis yang cuma punya 1 paper di subset -> graf lebih padat

Cara pakai:
  python make_subset.py --data data --max-works 4000
  python make_subset.py --data data --max-works 4000 --from-year 2023 --min-author-works 2
  python make_subset.py --data data --subfield 1702        # hanya Artificial Intelligence

Output: data/subset/*.csv  (siap diunggah dengan upload_aura.py)
"""
import argparse
import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

# Batas aman AuraDB Free (dokumentasi Neo4j menyebut 50rb-200rb node;
# kita target di bawah angka terendah supaya pasti muat).
SAFE_NODES = 45_000
SAFE_RELS = 140_000


def log(m):
    print(m, flush=True)


def read_csv(path):
    if not Path(path).exists():
        raise SystemExit(f"[ERROR] Tidak ketemu: {path}\nJalankan `flatten` dulu.")
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return len(rows)


def to_int(x, default=0):
    try:
        return int(str(x).strip())
    except (TypeError, ValueError):
        return default


def main():
    ap = argparse.ArgumentParser(description="Buat subset kecil untuk AuraDB Free")
    ap.add_argument("--data", default="data")
    ap.add_argument("--max-works", type=int, default=4000,
                    help="jumlah publikasi maksimum (default 4000)")
    ap.add_argument("--from-year", type=int, default=2022,
                    help="hanya publikasi tahun >= ini (default 2022)")
    ap.add_argument("--subfield", help="batasi ke satu subfield, mis. 1702 (Artificial Intelligence)")
    ap.add_argument("--min-author-works", type=int, default=1,
                    help="buang penulis dengan paper < N di subset (default 1 = tidak dibuang)")
    ap.add_argument("--require-abstract", action="store_true", default=True)
    ap.add_argument("--out", help="folder output (default <data>/subset)")
    args = ap.parse_args()

    data = Path(args.data)
    oa = data / "openalex"
    out = Path(args.out) if args.out else data / "subset"

    log("Membaca CSV ...")
    works = read_csv(oa / "works.csv")
    authorships = read_csv(oa / "authorships.csv")
    authors = {r["author_id"]: r for r in read_csv(oa / "authors.csv")}
    institutions = {r["institution_id"]: r for r in read_csv(oa / "institutions.csv")}
    topics = {r["topic_id"]: r for r in read_csv(oa / "topics.csv")}
    work_topics = read_csv(oa / "work_topics.csv")
    log(f"  works={len(works):,} authorships={len(authorships):,} authors={len(authors):,}")

    # ── 1. Filter publikasi ────────────────────────────────────────────────
    topic_subfield = {t["topic_id"]: t.get("subfield_id", "") for t in topics.values()}
    wt_by_work = defaultdict(list)
    for r in work_topics:
        wt_by_work[r["work_id"]].append(r)

    cand = []
    for w in works:
        if to_int(w.get("year")) < args.from_year:
            continue
        if args.require_abstract and w.get("has_abstract") != "1":
            continue
        if args.subfield:
            sfs = {topic_subfield.get(r["topic_id"], "") for r in wt_by_work.get(w["work_id"], [])}
            if args.subfield not in sfs:
                continue
        cand.append(w)

    log(f"\nLolos filter: {len(cand):,} publikasi")
    if not cand:
        raise SystemExit("[ERROR] Tidak ada publikasi yang lolos. Longgarkan filternya.")

    # Urutkan berdasarkan sitasi (paper berpengaruh -> graf lebih bermakna)
    cand.sort(key=lambda w: (-to_int(w.get("cited_by_count")), -to_int(w.get("year"))))
    sel_works = cand[:args.max_works]
    work_ids = {w["work_id"] for w in sel_works}
    log(f"Diambil     : {len(sel_works):,} publikasi teratas berdasarkan sitasi")

    # ── 2. Authorship pada publikasi terpilih ──────────────────────────────
    sel_as = [r for r in authorships if r["work_id"] in work_ids]

    # ── 3. Opsional: buang penulis dengan sedikit paper ────────────────────
    if args.min_author_works > 1:
        cnt = Counter(r["author_id"] for r in sel_as)
        keep = {a for a, n in cnt.items() if n >= args.min_author_works}
        before = len(sel_as)
        sel_as = [r for r in sel_as if r["author_id"] in keep]
        # buang publikasi yang jadi tanpa penulis sama sekali
        still = {r["work_id"] for r in sel_as}
        dropped_w = work_ids - still
        if dropped_w:
            work_ids = still
            sel_works = [w for w in sel_works if w["work_id"] in work_ids]
        log(f"Filter penulis (min {args.min_author_works} paper): "
            f"authorship {before:,} -> {len(sel_as):,}, publikasi -> {len(sel_works):,}")

    author_ids = {r["author_id"] for r in sel_as}

    # ── 4. Institusi & topik yang benar-benar dirujuk ──────────────────────
    inst_ids = set()
    for r in sel_as:
        for i in (r.get("institution_ids") or "").split(";"):
            i = i.strip()
            if i:
                inst_ids.add(i)
    inst_ids &= set(institutions)

    sel_wt = [r for r in work_topics if r["work_id"] in work_ids and r["topic_id"] in topics]
    topic_ids = {r["topic_id"] for r in sel_wt}

    # ── 5. Hitung ukuran ───────────────────────────────────────────────────
    n_affil = sum(1 for r in sel_as for i in (r.get("institution_ids") or "").split(";") if i.strip() in inst_ids)
    n_nodes = len(sel_works) + len(author_ids) + len(inst_ids) + len(topic_ids)
    n_rels = len(sel_as) + len(sel_wt) + n_affil

    log("\n=== Ukuran subset ===")
    log(f"  Publikasi   : {len(sel_works):>7,}")
    log(f"  Peneliti    : {len(author_ids):>7,}")
    log(f"  Institusi   : {len(inst_ids):>7,}")
    log(f"  Topik       : {len(topic_ids):>7,}")
    log(f"  TOTAL NODE  : {n_nodes:>7,}   (batas aman {SAFE_NODES:,})")
    log(f"  MENULIS     : {len(sel_as):>7,}")
    log(f"  MEMBAHAS    : {len(sel_wt):>7,}")
    log(f"  BERAFILIASI : {n_affil:>7,}  (maks, sebelum dedup)")
    log(f"  TOTAL RELASI: {n_rels:>7,}   (batas aman {SAFE_RELS:,})")

    ok = True
    if n_nodes > SAFE_NODES:
        log(f"\n  [!] Node melebihi batas aman. Turunkan --max-works "
            f"(coba {int(args.max_works * SAFE_NODES / n_nodes)}).")
        ok = False
    if n_rels > SAFE_RELS:
        log(f"\n  [!] Relasi melebihi batas aman. Turunkan --max-works "
            f"(coba {int(args.max_works * SAFE_RELS / n_rels)}).")
        ok = False
    if ok:
        log("\n  [OK] Muat di AuraDB Free.")

    # ── 6. Tulis file ──────────────────────────────────────────────────────
    log(f"\n=== Menulis ke {out} ===")
    # abstrak sengaja tidak dibawa: memperbesar database tanpa dipakai query graf
    wfields = ["work_id", "doi", "title", "year", "publication_date", "type",
               "language", "source_id", "source_name", "cited_by_count", "primary_topic_id"]
    n = write_csv(out / "works.csv", sel_works, wfields)
    log(f"  works.csv        {n:>7,}")
    n = write_csv(out / "authors.csv", [authors[a] for a in sorted(author_ids) if a in authors],
                  ["author_id", "name", "orcid"])
    log(f"  authors.csv      {n:>7,}")
    n = write_csv(out / "institutions.csv", [institutions[i] for i in sorted(inst_ids)],
                  ["institution_id", "name", "country_code", "type", "ror"])
    log(f"  institutions.csv {n:>7,}")
    n = write_csv(out / "topics.csv", [topics[t] for t in sorted(topic_ids)],
                  ["topic_id", "name", "subfield_id", "subfield_name",
                   "field_id", "field_name", "domain_name"])
    log(f"  topics.csv       {n:>7,}")
    n = write_csv(out / "authorships.csv", sel_as,
                  ["work_id", "author_id", "author_position", "is_corresponding",
                   "institution_ids", "countries", "raw_author_name"])
    log(f"  authorships.csv  {n:>7,}")
    n = write_csv(out / "work_topics.csv", sel_wt,
                  ["work_id", "topic_id", "score", "is_primary"])
    log(f"  work_topics.csv  {n:>7,}")

    log(f"\nLanjutkan dengan:\n  python upload_aura.py --subset {out}")


if __name__ == "__main__":
    main()
