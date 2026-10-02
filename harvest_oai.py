#!/usr/bin/env python3
"""
harvest_oai.py - Harvest artikel dari jurnal OJS (atau repository EPrints/DSpace)
lewat protokol OAI-PMH, lalu tandai artikel yang berkaitan dengan data science.

Kenapa perlu selain OpenAlex?
  Banyak jurnal nasional (terutama yang tidak ber-DOI) tidak lengkap di OpenAlex,
  dan abstraknya sering kosong. OAI-PMH OJS biasanya menyertakan abstrak
  (dc:description), sering dalam dua bahasa.

Alur:
  1. isi journals.txt  (satu base URL OAI per baris)
  2. python harvest_oai.py check   --journals journals.txt
  3. python harvest_oai.py harvest --journals journals.txt --out data --mailto email@kamu
  4. python harvest_oai.py flatten --out data            (otomatis dijalankan setelah harvest)

Semua record disimpan; artikel data science ditandai kolom ds_match=1 sehingga
daftar kata kunci bisa diperbaiki lalu `flatten` diulang TANPA harvest ulang.
"""
import argparse
import csv
import json
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc": "http://purl.org/dc/elements/1.1/",
}
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"
DOI_RE = re.compile(r"10\.\d{4,9}/[^\s\"<>]+", re.I)
YEAR_RE = re.compile(r"\b(19[5-9]\d|20\d{2})\b")

ID_STOP = {"yang", "dan", "dengan", "untuk", "pada", "ini", "dari", "dalam", "adalah", "tersebut", "sebagai"}
EN_STOP = {"the", "and", "of", "to", "in", "is", "with", "for", "this", "that", "are", "was"}


def log(msg):
    print(msg, flush=True)


def slugify(text):
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")[:80] or "journal"


def guess_lang(text):
    """Heuristik sederhana ID vs EN berdasarkan stopword."""
    words = re.findall(r"[a-zA-Z]+", text.lower())
    if not words:
        return ""
    id_score = sum(w in ID_STOP for w in words)
    en_score = sum(w in EN_STOP for w in words)
    if id_score == en_score == 0:
        return ""
    return "id" if id_score > en_score else "en"


def lang_code(raw, text):
    raw = (raw or "").lower().replace("_", "-")
    if raw.startswith("en"):
        return "en"
    if raw.startswith("id") or raw.startswith("in"):
        return "id"
    return guess_lang(text) if not raw else raw[:2]


def read_journals(path):
    items = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|")]
            base = parts[0]
            name = parts[1] if len(parts) > 1 and parts[1] else base
            items.append((base, name))
    return items


def load_keywords(path):
    if path and Path(path).exists():
        with open(path, encoding="utf-8") as f:
            return [l.strip().lower() for l in f if l.strip() and not l.strip().startswith("#")]
    return []


def compile_keywords(keywords):
    return [(kw, re.compile(r"(?<![\w-])" + re.escape(kw) + r"(?![\w-])", re.I)) for kw in keywords]


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
class OAIClient:
    def __init__(self, mailto=None, delay=1.0, max_retries=5, timeout=60):
        self.delay = delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        ua = "skripsi-kg-harvester/1.0 (OAI-PMH harvester untuk penelitian skripsi"
        ua += f"; mailto:{mailto})" if mailto else ")"
        self.session.headers["User-Agent"] = ua

    def get(self, base, params):
        last = ""
        for attempt in range(self.max_retries):
            try:
                r = self.session.get(base, params=params, timeout=self.timeout)
            except requests.RequestException as e:
                last = str(e)
                wait = 5 * (attempt + 1)
                log(f"    [jaringan] {e.__class__.__name__}, tunggu {wait}s ...")
                time.sleep(wait)
                continue
            if r.status_code == 200:
                time.sleep(self.delay)
                return r.content
            if r.status_code in (429, 500, 502, 503, 504):
                # OAI-PMH memakai 503 + Retry-After untuk flow control
                ra = r.headers.get("Retry-After", "")
                wait = int(ra) if ra.isdigit() else 10 * (attempt + 1)
                last = f"HTTP {r.status_code}"
                log(f"    [HTTP {r.status_code}] tunggu {wait}s ...")
                time.sleep(min(wait, 300))
                continue
            raise RuntimeError(f"HTTP {r.status_code} dari {r.url}")
        raise RuntimeError(f"Gagal setelah {self.max_retries}x ({last})")


def oai_error(root):
    err = root.find("oai:error", NS)
    if err is None:
        return None, None
    return err.get("code"), (err.text or "").strip()


# --------------------------------------------------------------------------- #
# Parsing record Dublin Core
# --------------------------------------------------------------------------- #
def parse_record(rec, journal_name, base_url):
    header = rec.find("oai:header", NS)
    if header is None or header.get("status") == "deleted":
        return None
    md = rec.find("oai:metadata/oai_dc:dc", NS)
    if md is None:
        return None

    def values(tag):
        out = []
        for e in md.findall(f"dc:{tag}", NS):
            t = re.sub(r"\s+", " ", (e.text or "")).strip()
            if t:
                out.append((t, e.get(XML_LANG, "")))
        return out

    titles = values("title")
    descs = values("description")
    title_by_lang, abs_by_lang = {}, {}
    for t, lg in titles:
        title_by_lang.setdefault(lang_code(lg, t), t)
    for t, lg in descs:
        # buang teks HTML sisa
        clean = re.sub(r"<[^>]+>", " ", t)
        clean = re.sub(r"\s+", " ", clean).strip()
        abs_by_lang.setdefault(lang_code(lg, clean), clean)

    identifiers = [t for t, _ in values("identifier")] + [t for t, _ in values("relation")]
    doi = ""
    for ident in identifiers:
        m = DOI_RE.search(ident)
        if m:
            doi = m.group(0).rstrip(".,;").lower()
            break
    url = next((i for i in identifiers if i.startswith("http") and "doi.org" not in i), "")

    subjects = []
    for t, _ in values("subject"):
        subjects.extend(s.strip() for s in re.split(r"[;,]", t) if s.strip())

    dates = [t for t, _ in values("date")]
    year = ""
    for d in dates:
        m = YEAR_RE.search(d)
        if m:
            year = m.group(1)
            break

    return {
        "record_id": header.findtext("oai:identifier", "", NS),
        "datestamp": header.findtext("oai:datestamp", "", NS),
        "journal": journal_name,
        "oai_base": base_url,
        "doi": doi,
        "url": url,
        "title": titles[0][0] if titles else "",
        "title_en": title_by_lang.get("en", ""),
        "title_id": title_by_lang.get("id", ""),
        "abstract_en": abs_by_lang.get("en", ""),
        "abstract_id": abs_by_lang.get("id", ""),
        "abstract_other": " ||| ".join(v for k, v in abs_by_lang.items() if k not in ("en", "id")),
        "keywords": subjects,
        "authors": [t for t, _ in values("creator")],
        "year": year,
        "language": ";".join(sorted({t for t, _ in values("language")})),
        "source": "; ".join(t for t, _ in values("source")),
        "type": ";".join(t for t, _ in values("type")),
    }


# --------------------------------------------------------------------------- #
# Perintah: check
# --------------------------------------------------------------------------- #
def cmd_check(args):
    client = OAIClient(mailto=args.mailto, delay=args.delay, max_retries=2, timeout=30)
    for base, name in read_journals(args.journals):
        try:
            root = ET.fromstring(client.get(base, {"verb": "Identify"}))
            code, msg = oai_error(root)
            if code:
                log(f"[GAGAL] {name}: OAI error {code} {msg}")
                continue
            ident = root.find("oai:Identify", NS)
            repo = ident.findtext("oai:repositoryName", "", NS) if ident is not None else ""
            earliest = ident.findtext("oai:earliestDatestamp", "", NS) if ident is not None else ""
            log(f"[OK]    {name}: {repo} (data sejak {earliest})")
        except Exception as e:  # noqa: BLE001
            log(f"[GAGAL] {name}: {e}")


# --------------------------------------------------------------------------- #
# Perintah: harvest (resumable per jurnal)
# --------------------------------------------------------------------------- #
def harvest_one(client, base, name, raw_dir, args):
    slug = slugify(name)
    jsonl = raw_dir / f"{slug}.jsonl"
    state_path = raw_dir / f"{slug}.state.json"
    state = {"token": None, "done": False, "count": 0}
    if state_path.exists() and not args.reset:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    if state["done"]:
        log(f"- {name}: sudah selesai ({state['count']} record), dilewati.")
        return

    def save():
        state_path.write_text(json.dumps(state), encoding="utf-8")

    log(f"- {name}: mulai/lanjut (sudah {state['count']} record)")
    fetched = 0
    with open(jsonl, "a", encoding="utf-8") as out:
        while True:
            if state["token"]:
                params = {"verb": "ListRecords", "resumptionToken": state["token"]}
            else:
                params = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
                if args.from_date:
                    params["from"] = args.from_date
                if args.set:
                    params["set"] = args.set
            root = ET.fromstring(client.get(base, params))
            code, msg = oai_error(root)
            if code == "noRecordsMatch":
                log("    tidak ada record.")
                break
            if code == "badResumptionToken":
                log("    resumptionToken kedaluwarsa, mulai ulang jurnal ini (duplikat dibuang saat flatten).")
                state["token"] = None
                save()
                continue
            if code:
                raise RuntimeError(f"OAI error {code}: {msg}")

            lr = root.find("oai:ListRecords", NS)
            if lr is None:
                break
            for rec in lr.findall("oai:record", NS):
                item = parse_record(rec, name, base)
                if item:
                    out.write(json.dumps(item, ensure_ascii=False) + "\n")
                    fetched += 1
                    state["count"] += 1
            out.flush()

            tok_el = lr.find("oai:resumptionToken", NS)
            token = (tok_el.text or "").strip() if tok_el is not None else ""
            if not token:
                break
            state["token"] = token
            save()
            if args.max_records and fetched >= args.max_records:
                log(f"    berhenti karena --max-records ({fetched}). Jalankan lagi untuk lanjut.")
                return
    state["done"], state["token"] = True, None
    save()
    log(f"    selesai: {state['count']} record")


def cmd_harvest(args):
    raw_dir = Path(args.out) / "raw" / "oai"
    raw_dir.mkdir(parents=True, exist_ok=True)
    client = OAIClient(mailto=args.mailto, delay=args.delay)
    journals = read_journals(args.journals)
    log(f"{len(journals)} sumber OAI di {args.journals}")
    for base, name in journals:
        try:
            harvest_one(client, base, name, raw_dir, args)
        except Exception as e:  # noqa: BLE001  - satu jurnal gagal tidak menghentikan semuanya
            log(f"    [GAGAL] {name}: {e}  (state disimpan, bisa dilanjutkan nanti)")
    cmd_flatten(args)


# --------------------------------------------------------------------------- #
# Perintah: flatten + penandaan data science
# --------------------------------------------------------------------------- #
def cmd_flatten(args):
    raw_dir = Path(args.out) / "raw" / "oai"
    out_dir = Path(args.out) / "oai"
    out_dir.mkdir(parents=True, exist_ok=True)
    patterns = compile_keywords(load_keywords(args.keywords))
    if not patterns:
        log("[PERINGATAN] file kata kunci kosong/tidak ada; semua record ds_match=0.")

    header = ["record_id", "journal", "doi", "url", "title", "title_en", "title_id",
              "abstract_en", "abstract_id", "abstract_other", "keywords", "authors",
              "year", "language", "source", "ds_match", "matched_terms"]
    seen, rows_all, n_ds, n_abs = set(), [], 0, 0
    for jsonl in sorted(raw_dir.glob("*.jsonl")):
        with open(jsonl, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                if r["record_id"] in seen:
                    continue
                seen.add(r["record_id"])
                if args.from_year and r["year"] and int(r["year"]) < args.from_year:
                    continue
                text = " ".join([r["title"], r["title_en"], r["title_id"], r["abstract_en"],
                                 r["abstract_id"], r["abstract_other"], " ".join(r["keywords"])])
                matched = sorted({kw for kw, rx in patterns if rx.search(text)})
                ds = 1 if len(matched) >= args.min_matches else 0
                n_ds += ds
                has_abs = bool(r["abstract_en"] or r["abstract_id"] or r["abstract_other"])
                n_abs += has_abs
                rows_all.append([
                    r["record_id"], r["journal"], r["doi"], r["url"], r["title"], r["title_en"],
                    r["title_id"], r["abstract_en"], r["abstract_id"], r["abstract_other"],
                    "; ".join(r["keywords"]), " ; ".join(r["authors"]), r["year"],
                    r["language"], r["source"], ds, "; ".join(matched),
                ])

    for fname, rows in (("oai_works_all.csv", rows_all),
                        ("oai_works_ds.csv", [x for x in rows_all if x[15] == 1])):
        with open(out_dir / fname, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)
    total = len(rows_all)
    log(f"\nRecord unik       : {total:,}")
    if total:
        log(f"  punya abstrak   : {n_abs:,} ({100 * n_abs / total:.1f}%)")
        log(f"  terkait DS      : {n_ds:,} ({100 * n_ds / total:.1f}%)  -> {out_dir / 'oai_works_ds.csv'}")
    log("Tips: cek 50 baris acak di oai_works_ds.csv; kalau banyak salah tandai, ubah "
        "keywords_ds.txt atau naikkan --min-matches, lalu jalankan `flatten` lagi.")


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description="Harvester OAI-PMH jurnal untuk KG peneliti data science")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, need_journals=True):
        if need_journals:
            p.add_argument("--journals", default="journals.txt", help="daftar base URL OAI")
        p.add_argument("--out", default="data")
        p.add_argument("--mailto", help="email kamu untuk User-Agent (sopan & bisa dihubungi)")
        p.add_argument("--delay", type=float, default=1.0, help="jeda antar request (detik, default 1)")

    def flatten_opts(p):
        p.add_argument("--keywords", default="keywords_ds.txt", help="kata kunci data science")
        p.add_argument("--min-matches", type=int, default=1,
                       help="minimal jumlah kata kunci berbeda yang cocok (default 1)")
        p.add_argument("--from-year", type=int, default=2018)

    p = sub.add_parser("check", help="uji endpoint OAI (verb=Identify)")
    common(p)
    p.set_defaults(func=cmd_check)

    p = sub.add_parser("harvest", help="harvest semua jurnal (resumable)")
    common(p)
    flatten_opts(p)
    p.add_argument("--from-date", help="hanya record diubah sejak YYYY-MM-DD (harvest bertahap)")
    p.add_argument("--set", help="setSpec OAI tertentu (opsional)")
    p.add_argument("--max-records", type=int, help="batas per jurnal per run (uji coba)")
    p.add_argument("--reset", action="store_true")
    p.set_defaults(func=cmd_harvest)

    p = sub.add_parser("flatten", help="JSONL -> CSV + tandai data science")
    common(p, need_journals=False)
    flatten_opts(p)
    p.set_defaults(func=cmd_flatten)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
