#!/usr/bin/env python3
"""
harvest_pdf.py - Unduh PDF open access + metadata "halaman depan" (judul, penulis,
abstrak, kata kunci) untuk works hasil harvest_openalex.py / harvest_oai.py.

Kenapa perlu langkah resolve?
  works.csv dari harvest_openalex.py TIDAK menyimpan status open access maupun URL PDF
  (field open_access, best_oa_location, locations tidak ikut di-select). Jadi URL PDF
  dicari ulang ke OpenAlex per 100 ID -> murah (~1 request per 100 works), tanpa
  harus harvest ulang.

Alur kerja (jalankan berurutan):
  1. resolve  : cari kandidat URL PDF & halaman artikel untuk tiap work
                - OpenAlex : open_access, best_oa_location, locations
                - OAI      : kolom url (halaman artikel OJS/EPrints), digabung via DOI
                -> <out>/pdf/candidates.csv
  2. download : unduh PDF (hanya open access), patuhi robots.txt, jeda per-host,
                bisa dihentikan (Ctrl+C) dan dilanjutkan kapan saja
                fallback: buka halaman artikel -> meta citation_pdf_url / link galeri PDF
                -> <out>/pdf/files/<work_id>.pdf
                -> <out>/pdf/download_log.csv     (status per work)
                -> <out>/pdf/landing_meta.jsonl   (meta tag halaman artikel)
  3. extract  : ambil teks PDF -> abstrak & kata kunci (ID/EN) dari halaman depan
                + teks lengkap, digabung dengan meta halaman artikel
                -> <out>/pdf/pdf_front.csv, <out>/pdf/text/<work_id>.txt
  4. stats    : ringkasan keberhasilan (angka untuk BAB III/IV)
                -> <out>/pdf/pdf_report.txt

Contoh:
  set OPENALEX_API_KEY=xxxxx
  python harvest_pdf.py resolve  --out data --mailto email@kamu.ac.id
  python harvest_pdf.py download --out data --mailto email@kamu.ac.id --limit 50   # uji coba
  python harvest_pdf.py download --out data --mailto email@kamu.ac.id
  python harvest_pdf.py extract  --out data
  python harvest_pdf.py stats    --out data

Batasan yang disengaja (etika):
  - Hanya mengunduh karya open access. Karya tertutup dicatat status=closed dan dilewati.
  - Tidak menembus login, CAPTCHA, atau proteksi bot (Cloudflare dsb.). Host yang terus
    menolak (403/429) otomatis berhenti dicoba pada run tersebut.
  - Email di teks PDF diganti [email] (UU PDP).

Butuh: requests, pypdf  (opsional PyMuPDF -> ekstraksi teks lebih cepat & rapi)
"""
import argparse
import csv
import hashlib
import json
import os
import re
import sys
import threading
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests

from harvest_openalex import MAX_OR_VALUES, add_common, chunks, log, make_client, norm_doi, short_id

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

UA_NAME = "skripsi-kg-harvester"
OA_SELECT = "id,doi,open_access,best_oa_location,locations"
AVG_PDF_MB = 0.8                  # rata-rata kasar ukuran PDF artikel, untuk estimasi disk
MAX_HOPS = 3                      # kedalaman maksimum landing page -> viewer -> PDF
MAX_REQUESTS_PER_WORK = 10
HOST_BLOCK_AFTER = 5              # 403/429 berturut-turut -> host dihentikan untuk run ini
LOG_FIELDS = ["work_id", "status", "pdf_url", "http_status", "bytes", "sha1",
              "n_requests", "error", "ts"]
# status yang tidak dicoba lagi walau --retry-failed (kecuali --retry-all)
FINAL_STATUSES = {"ok", "closed", "no_url"}

META_KEYS = {
    "citation_title", "citation_author", "citation_author_institution", "citation_abstract",
    "citation_keywords", "citation_doi", "citation_date", "citation_publication_date",
    "citation_online_date", "citation_journal_title", "citation_conference_title",
    "citation_pdf_url", "citation_language", "citation_publisher",
    "dc.title", "dc.creator.personalname", "dc.creator", "dc.description",
    "dc.description.abstract", "dc.subject", "dc.date.issued", "dc.identifier.doi",
    "description", "og:description", "keywords",
}

EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
WS_RE = re.compile(r"\s+")
# Penanda bagian di halaman depan PDF. Introduction/Pendahuluan wajib di awal baris
# supaya kata "introduction" di dalam kalimat abstrak tidak ikut terpotong.
SECTION_RE = re.compile(
    r"(?P<abs_en>\babstract\b)|(?P<abs_id>\babstrak\b)|"
    r"(?P<kw_en>\b(?:key\s*-?\s*words?|index\s+terms)\b)|(?P<kw_id>\bkata[\s-]*kunci\b)|"
    r"(?P<intro>^[ \t]*(?:(?:1|I)\s*\.?\s*)?(?:introduction|pendahuluan|latar\s+belakang)\b)",
    re.I | re.M,
)


# --------------------------------------------------------------------------- #
# Utilitas
# --------------------------------------------------------------------------- #
def safe_name(work_id):
    """ID jadi nama file aman (Windows juga). W123 tetap W123; oai:x:y/1 -> oai_x_y_1."""
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", work_id).strip("._")
    if len(s) > 120:
        s = s[:100] + "_" + hashlib.sha1(work_id.encode()).hexdigest()[:12]
    return s or hashlib.sha1(work_id.encode()).hexdigest()[:16]


def uniq(items):
    seen, out = set(), []
    for x in items:
        x = (x or "").strip()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def host_of(url):
    return (urlparse(url).hostname or "").lower()


def read_csv(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def to_int(x, default=0):
    try:
        return int(str(x).strip())
    except (TypeError, ValueError):
        return default


def pdf_dir(args):
    return Path(args.out) / "pdf"


def read_jsonl(path):
    """Baca JSONL; baris terakhir yang terpotong (proses dihentikan paksa) dilewati."""
    if not path.exists():
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def append_jsonl(path):
    """Buka JSONL untuk ditambah; jika baris terakhir terpotong, mulai di baris baru."""
    needs_nl = False
    if path.exists() and path.stat().st_size:
        with open(path, "rb") as f:
            f.seek(-1, os.SEEK_END)
            needs_nl = f.read(1) != b"\n"
    f = open(path, "a", encoding="utf-8")
    if needs_nl:
        f.write("\n")
    return f


def read_log(path):
    """download_log.csv append-only; baris terakhir per work yang berlaku."""
    last = {}
    if path.exists():
        for r in read_csv(path):
            last[r["work_id"]] = r
    return last


# --------------------------------------------------------------------------- #
# Perintah: resolve
# --------------------------------------------------------------------------- #
def oa_urls(rec):
    """Urutan kandidat dari record OpenAlex: PDF langsung dulu, lalu halaman artikel."""
    oa = rec.get("open_access") or {}
    best = rec.get("best_oa_location") or {}
    locs = [l for l in (rec.get("locations") or []) if l and l.get("is_oa")]
    pdfs = [best.get("pdf_url")] + [l.get("pdf_url") for l in locs]
    landings = [best.get("landing_page_url")] + [l.get("landing_page_url") for l in locs]
    oa_url = oa.get("oa_url") or ""
    (pdfs if oa_url.lower().split("?")[0].endswith(".pdf") else landings).append(oa_url)
    return uniq(pdfs), uniq(landings), best.get("license") or "", best.get("version") or ""


def resolve_openalex(args, work_ids, raw_path):
    """Ambil info OA untuk work_ids yang belum ada di raw_path (resumable)."""
    done = {short_id(rec.get("id")) for rec in read_jsonl(raw_path)}
    todo = [w for w in work_ids if w not in done]
    log(f"OpenAlex: {len(work_ids):,} works, {len(done & set(work_ids)):,} sudah di-resolve, "
        f"{len(todo):,} perlu query (~{-(-len(todo) // MAX_OR_VALUES):,} request)")
    if not todo:
        return
    client = make_client(args)
    with append_jsonl(raw_path) as out:
        for i, batch in enumerate(chunks(todo, MAX_OR_VALUES), 1):
            data = client.get("/works", {
                "filter": "openalex:" + "|".join(batch),
                "per_page": MAX_OR_VALUES, "select": OA_SELECT,
            })
            got = set()
            for rec in data.get("results", []):
                got.add(short_id(rec.get("id")))
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            for wid in set(batch) - got:  # ID dihapus/digabung di OpenAlex: catat agar tidak diulang
                out.write(json.dumps({"id": wid, "missing": True}) + "\n")
            out.flush()
            if i % 50 == 0:
                log(f"  ... {i * MAX_OR_VALUES:,} works (biaya ${client.total_cost:.4f})")
    log(f"  selesai, {client.n_requests} request, biaya ${client.total_cost:.4f}")


def cmd_resolve(args):
    out_dir = pdf_dir(args)
    (out_dir / "raw").mkdir(parents=True, exist_ok=True)
    data = Path(args.out)
    only = None
    if args.only_ids:
        only = {r["work_id"] for r in read_csv(args.only_ids) if r.get("work_id")}
        log(f"Dibatasi ke {len(only):,} work_id dari {args.only_ids}")

    def keep(year):
        y = to_int(year)
        if args.from_year and y and y < args.from_year:
            return False
        return not (args.to_year and y and y > args.to_year)

    rows = {}  # work_id -> dict kandidat
    by_doi = {}
    oa_works = data / "openalex" / "works.csv"
    if "openalex" in args.sources and oa_works.exists():
        works = [w for w in read_csv(oa_works)
                 if (only is None or w["work_id"] in only) and keep(w.get("year"))]
        raw_path = out_dir / "raw" / "oa_locations.jsonl"
        resolve_openalex(args, [w["work_id"] for w in works], raw_path)
        oa_info = {short_id(rec.get("id")): rec for rec in read_jsonl(raw_path)}
        for w in works:
            rec = oa_info.get(w["work_id"]) or {}
            oa = rec.get("open_access") or {}
            pdfs, landings, lic, ver = oa_urls(rec)
            is_oa = bool(oa.get("is_oa"))
            if is_oa and w.get("doi"):
                landings.append("https://doi.org/" + w["doi"])
            rows[w["work_id"]] = {
                "work_id": w["work_id"], "origin": "openalex", "doi": w.get("doi", ""),
                "title": w.get("title", ""), "year": w.get("year", ""),
                "is_oa": 1 if is_oa else 0, "oa_status": oa.get("oa_status") or "",
                "license": lic, "version": ver,
                "pdf_urls": uniq(pdfs) if is_oa else [],
                "landing_urls": uniq(landings) if is_oa else [],
            }
            if w.get("doi"):
                by_doi[w["doi"]] = w["work_id"]
    elif "openalex" in args.sources:
        log(f"[PERINGATAN] {oa_works} tidak ada, sumber OpenAlex dilewati.")

    oai_file = data / "oai" / args.oai_file
    if "oai" in args.sources and oai_file.exists():
        n_merged = n_new = 0
        for r in read_csv(oai_file):
            if not keep(r.get("year")):
                continue
            doi = norm_doi(r.get("doi"))
            links = uniq([r.get("url"), ("https://doi.org/" + doi) if doi else ""])
            if doi in by_doi:
                # sama dengan work OpenAlex: halaman OJS sering punya PDF walau OpenAlex tidak
                tgt = rows[by_doi[doi]]
                tgt["landing_urls"] = uniq(tgt["landing_urls"] + links)
                if not tgt["is_oa"]:
                    tgt["is_oa"], tgt["oa_status"] = 1, "oai"
                n_merged += 1
                continue
            wid = r["record_id"]
            if only is not None and wid not in only:
                continue
            rows[wid] = {
                "work_id": wid, "origin": "oai", "doi": doi, "title": r.get("title", ""),
                "year": r.get("year", ""), "is_oa": 1, "oa_status": "oai", "license": "",
                "version": "", "pdf_urls": [], "landing_urls": links,
            }
            n_new += 1
        log(f"OAI ({oai_file.name}): {n_new:,} works baru, {n_merged:,} digabung ke work OpenAlex via DOI")
    elif "oai" in args.sources:
        log(f"[INFO] {oai_file} tidak ada, sumber OAI dilewati.")

    if not rows:
        raise SystemExit("[ERROR] Tidak ada work. Jalankan harvest_openalex.py/harvest_oai.py flatten dulu.")

    header = ["work_id", "origin", "doi", "title", "year", "is_oa", "oa_status", "license",
              "version", "pdf_urls", "landing_urls"]
    with open(out_dir / "candidates.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows.values():
            w.writerow([r[k] if k not in ("pdf_urls", "landing_urls") else " ; ".join(r[k])
                        for k in header])

    total = len(rows)
    n_oa = sum(r["is_oa"] for r in rows.values())
    n_pdf = sum(1 for r in rows.values() if r["pdf_urls"])
    n_any = sum(1 for r in rows.values() if r["pdf_urls"] or r["landing_urls"])
    log(f"\nTotal works          : {total:,}")
    log(f"  open access        : {n_oa:,} ({100 * n_oa / total:.1f}%)")
    log(f"  ada URL PDF langsung: {n_pdf:,} ({100 * n_pdf / total:.1f}%)")
    log(f"  ada URL apa pun    : {n_any:,} ({100 * n_any / total:.1f}%)  <- yang akan dicoba")
    log("  oa_status          : " + ", ".join(
        f"{k or '-'}={v:,}" for k, v in Counter(r["oa_status"] for r in rows.values()).most_common()))
    log(f"Perkiraan disk jika semua berhasil: ~{n_any * AVG_PDF_MB / 1024:.1f} GB "
        f"(asumsi {AVG_PDF_MB} MB/PDF)")
    log(f"Tersimpan: {out_dir / 'candidates.csv'}")
    log(f"Lanjutkan: python harvest_pdf.py download --out {args.out} --limit 50   (uji coba dulu)")


# --------------------------------------------------------------------------- #
# HTTP: robots.txt, jeda per-host, blokir host bermasalah
# --------------------------------------------------------------------------- #
class LinkMetaParser(HTMLParser):
    """Kumpulkan <meta> penting dan kandidat link PDF dari halaman artikel."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.meta = defaultdict(list)
        self.links = []  # (href, petunjuk teks/kelas)
        self._a = None

    def handle_starttag(self, tag, attrs):
        a = {k.lower(): (v or "") for k, v in attrs}
        if tag == "meta":
            key = (a.get("name") or a.get("property") or "").strip().lower()
            if key in META_KEYS and a.get("content", "").strip():
                self.meta[key].append(unescape(a["content"]).strip())
        elif tag == "a" and a.get("href"):
            self._a = [a["href"], (a.get("class", "") + " " + a.get("title", "")).lower()]
        elif tag in ("iframe", "embed") and a.get("src"):
            self.links.append((a["src"], "frame"))
        elif tag == "object" and a.get("data"):
            self.links.append((a["data"], "frame"))
        elif tag == "link" and "pdf" in a.get("type", "").lower() and a.get("href"):
            self.links.append((a["href"], "pdf"))

    def handle_data(self, data):
        if self._a is not None:
            self._a[1] += " " + data.strip().lower()

    def handle_endtag(self, tag):
        if tag == "a" and self._a is not None:
            self.links.append(tuple(self._a))
            self._a = None


def pdf_links_from_html(html, base_url):
    p = LinkMetaParser()
    try:
        p.feed(html)
    except Exception:  # noqa: BLE001 - HTML rusak: pakai apa yang sudah terbaca
        pass
    cands = list(p.meta.get("citation_pdf_url", []))
    for href, hint in p.links:
        h = href.lower()
        if (h.split("?")[0].endswith(".pdf") or "/article/download/" in h
                or "/bitstream/" in h or hint == "frame"
                or ("pdf" in hint and ("/article/view/" in h or "download" in h))):
            cands.append(href)
    out = []
    for c in cands:
        u = urljoin(base_url, unescape(c).strip())
        if not u.startswith("http"):
            continue
        # OJS: /article/view/<id>/<galley> adalah viewer HTML; /article/download/ = file PDF
        if "/article/view/" in u and re.search(r"/article/view/[^/]+/[^/?#]+", u):
            out.append(u.replace("/article/view/", "/article/download/", 1))
        out.append(u)
    return uniq(out), dict(p.meta)


class Fetcher:
    def __init__(self, mailto=None, delay=1.0, timeout=45, max_mb=40, respect_robots=True):
        self.delay = delay
        self.timeout = timeout
        self.max_bytes = int(max_mb * 1024 * 1024)
        self.respect_robots = respect_robots
        ua = f"{UA_NAME}/1.0 (unduh PDF open access untuk penelitian skripsi"
        ua += f"; mailto:{mailto})" if mailto else ")"
        self.ua = ua
        self._local = threading.local()
        self._lock = threading.Lock()
        self._host_locks = {}
        self._next_ok = defaultdict(float)
        self._host_delay = {}
        self._robots = {}
        self._denials = Counter()
        self.blocked_hosts = set()

    @property
    def session(self):
        s = getattr(self._local, "s", None)
        if s is None:
            s = requests.Session()
            s.headers["User-Agent"] = self.ua
            s.headers["Accept"] = "application/pdf,text/html;q=0.9,*/*;q=0.5"
            self._local.s = s
        return s

    def _hlock(self, name):
        with self._lock:
            return self._host_locks.setdefault(name, threading.Lock())

    def _robots_for(self, url):
        p = urlparse(url)
        key = f"{p.scheme}://{p.netloc}"
        with self._hlock("robots:" + key):
            if key in self._robots:
                return self._robots[key]
            rp = RobotFileParser()
            try:
                r = self.session.get(key + "/robots.txt", timeout=20)
                if r.status_code == 200:
                    rp.parse(r.text.splitlines())
                elif 400 <= r.status_code < 500:
                    rp.allow_all = True       # RFC 9309: robots.txt tidak ada -> boleh
                else:
                    rp.disallow_all = True    # 5xx -> anggap dilarang sementara
            except requests.RequestException:
                rp.disallow_all = True
            cd = None
            try:
                cd = rp.crawl_delay(UA_NAME)
            except Exception:  # noqa: BLE001
                pass
            self._host_delay[p.hostname] = min(max(self.delay, float(cd or 0)), 30.0)
            self._robots[key] = rp
            return rp

    def _throttle(self, host):
        with self._hlock(host):
            d = self._host_delay.get(host, self.delay)
            wait_s = self._next_ok[host] - time.monotonic()
            if wait_s > 0:
                time.sleep(wait_s)
            self._next_ok[host] = time.monotonic() + d

    def _note(self, host, status):
        with self._lock:
            if status in (401, 403, 429):
                self._denials[host] += 1
                if self._denials[host] >= HOST_BLOCK_AFTER and host not in self.blocked_hosts:
                    self.blocked_hosts.add(host)
                    log(f"  [host diblokir untuk run ini] {host}: {self._denials[host]}x HTTP {status} "
                        "(kemungkinan proteksi bot; tidak ditembus)")
            elif status == 200:
                self._denials[host] = 0

    def get(self, url):
        """Return (kind, payload, final_url, http_status). kind: pdf|html|other|error.
        Untuk pdf, payload = bytes; untuk html = str; selain itu = pesan error.
        Redirect diikuti manual supaya robots.txt & jeda berlaku di tiap host tujuan."""
        cur, hops, attempt = url, 0, 0
        while True:
            host = host_of(cur)
            if host in self.blocked_hosts:
                return "error", "host_blocked", cur, ""
            if self.respect_robots and not self._robots_for(cur).can_fetch(UA_NAME, cur):
                return "error", "robots_blocked", cur, ""
            self._throttle(host)
            try:
                r = self.session.get(cur, timeout=self.timeout, stream=True, allow_redirects=False)
            except requests.RequestException as e:
                attempt += 1
                if attempt < 3:
                    time.sleep(5 * attempt)
                    continue
                return "error", f"net_{e.__class__.__name__}", cur, ""
            with r:
                if r.is_redirect:
                    hops += 1
                    if hops > 8:
                        return "error", "too_many_redirects", cur, r.status_code
                    cur = urljoin(cur, r.headers["Location"])
                    continue
                self._note(host, r.status_code)
                if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                    attempt += 1
                    ra = r.headers.get("Retry-After", "")
                    time.sleep(min(int(ra) if ra.isdigit() else 5 * attempt, 60))
                    continue
                if r.status_code != 200:
                    return "error", f"http_{r.status_code}", cur, r.status_code
                if to_int(r.headers.get("Content-Length"), 0) > self.max_bytes:
                    return "error", "too_large", cur, 200
                buf = bytearray()
                try:
                    for chunk in r.iter_content(64 * 1024):
                        buf.extend(chunk)
                        if len(buf) > self.max_bytes:
                            return "error", "too_large", cur, 200
                except requests.RequestException as e:
                    return "error", f"net_{e.__class__.__name__}", cur, 200
                body = bytes(buf)
                if body.lstrip()[:5] == b"%PDF-":
                    return "pdf", body, cur, 200
                ctype = r.headers.get("Content-Type", "").lower()
                if "html" in ctype or b"<html" in body[:2000].lower():
                    m = re.search(r"charset=([\w-]+)", ctype)
                    return "html", body.decode(m.group(1) if m else "utf-8", errors="replace"), cur, 200
                return "other", ctype or "unknown_type", cur, 200


# --------------------------------------------------------------------------- #
# Perintah: download
# --------------------------------------------------------------------------- #
def fetch_work(fetcher, cand, files_dir):
    """Coba semua URL kandidat sampai dapat PDF. Dipanggil di thread pekerja."""
    wid = cand["work_id"]
    res = {"work_id": wid, "status": "no_url", "pdf_url": "", "http_status": "", "bytes": 0,
           "sha1": "", "n_requests": 0, "error": "", "landing_meta": None}
    queue = deque((u, 0) for u in cand["pdf_urls"] + cand["landing_urls"])
    seen = set()
    errors = []
    while queue and res["n_requests"] < MAX_REQUESTS_PER_WORK:
        url, depth = queue.popleft()
        if url in seen:
            continue
        seen.add(url)
        res["n_requests"] += 1
        kind, payload, final_url, http = fetcher.get(url)
        if kind == "pdf":
            path = files_dir / f"{safe_name(wid)}.pdf"
            tmp = path.with_suffix(".pdf.part")
            tmp.write_bytes(payload)
            os.replace(tmp, path)
            res.update(status="ok", pdf_url=final_url, http_status=200, bytes=len(payload),
                       sha1=hashlib.sha1(payload).hexdigest(), error="")
            return res
        if kind == "html":
            links, meta = pdf_links_from_html(payload, final_url)
            if meta and res["landing_meta"] is None and any(k.startswith(("citation_", "dc."))
                                                            for k in meta):
                res["landing_meta"] = {"url": final_url, "meta": meta}
            if depth < MAX_HOPS:
                queue.extendleft(reversed([(l, depth + 1) for l in links if l not in seen]))
            if not links:
                errors.append("html_no_pdf_link")
            continue
        errors.append(payload if kind == "error" else f"not_pdf:{payload}")
        res["http_status"] = http or res["http_status"]
    if errors:
        # status = error yang paling informatif (urutan prioritas)
        for key in ("robots_blocked", "host_blocked", "http_403", "http_401", "too_large",
                    "http_404", "html_no_pdf_link"):
            if key in errors:
                res["status"] = key
                break
        else:
            res["status"] = errors[-1].split(":")[0]
        res["error"] = " | ".join(uniq(errors))[:500]
    return res


def interleave_by_host(cands):
    """Susun antrean bergiliran per host, supaya worker tidak antre di satu server."""
    groups = defaultdict(deque)
    for c in cands:
        first = (c["pdf_urls"] + c["landing_urls"] + [""])[0]
        groups[host_of(first)].append(c)
    order = sorted(groups, key=lambda h: -len(groups[h]))
    out = []
    while order:
        nxt = []
        for h in order:
            out.append(groups[h].popleft())
            if groups[h]:
                nxt.append(h)
        order = nxt
    return out


def cmd_download(args):
    out_dir = pdf_dir(args)
    cand_path = out_dir / "candidates.csv"
    if not cand_path.exists():
        raise SystemExit(f"[ERROR] {cand_path} tidak ada. Jalankan `resolve` dulu.")
    files_dir = out_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "download_log.csv"
    meta_path = out_dir / "landing_meta.jsonl"
    prev = read_log(log_path)

    def split(s):
        return [u.strip() for u in (s or "").split(" ; ") if u.strip()]

    todo, n_skip, closed_rows = [], 0, []
    for r in read_csv(cand_path):
        if args.origin and r["origin"] != args.origin:
            continue
        p = prev.get(r["work_id"])
        if p and not (args.retry_all or (args.retry_failed and p["status"] not in FINAL_STATUSES)):
            n_skip += 1
            continue
        c = {"work_id": r["work_id"], "pdf_urls": split(r["pdf_urls"]),
             "landing_urls": split(r["landing_urls"])}
        if r["is_oa"] != "1" and not c["pdf_urls"] and not c["landing_urls"]:
            if not p:
                closed_rows.append(r["work_id"])
            continue
        todo.append(c)

    new_log = not log_path.exists()
    logf = open(log_path, "a", newline="", encoding="utf-8")
    logw = csv.DictWriter(logf, fieldnames=LOG_FIELDS, extrasaction="ignore")
    if new_log:
        logw.writeheader()
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    for wid in closed_rows:  # karya tertutup dicatat sekali, tanpa request
        logw.writerow({"work_id": wid, "status": "closed", "ts": now})
    logf.flush()

    todo = interleave_by_host(todo)
    if args.limit:
        todo = todo[:args.limit]
    log(f"Kandidat: {len(todo):,} works akan dicoba, {n_skip:,} sudah diproses sebelumnya "
        f"(pakai --retry-failed untuk mengulang yang gagal), {len(closed_rows):,} tertutup dicatat.")
    if not todo:
        logf.close()
        return

    fetcher = Fetcher(mailto=args.mailto, delay=args.delay, timeout=args.timeout,
                      max_mb=args.max_mb, respect_robots=not args.ignore_robots)
    if args.ignore_robots:
        log("[PERINGATAN] robots.txt diabaikan. Gunakan hanya dengan izin pemilik situs.")
    stats = Counter()
    total_bytes = 0
    t0 = time.time()
    metaf = append_jsonl(meta_path)
    pool = ThreadPoolExecutor(max_workers=args.workers)
    pending = set()
    it = iter(todo)
    done_n = 0
    try:
        while True:
            while len(pending) < args.workers * 4:
                c = next(it, None)
                if c is None:
                    break
                pending.add(pool.submit(fetch_work, fetcher, c, files_dir))
            if not pending:
                break
            finished, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
            for fut in finished:
                res = fut.result()
                res["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                logw.writerow(res)
                if res["landing_meta"]:
                    metaf.write(json.dumps({"work_id": res["work_id"], **res["landing_meta"]},
                                           ensure_ascii=False) + "\n")
                stats[res["status"]] += 1
                total_bytes += res["bytes"] or 0
                done_n += 1
                if done_n % 25 == 0 or done_n == len(todo):
                    logf.flush()
                    metaf.flush()
                    rate = done_n / max(time.time() - t0, 1e-6)
                    eta_h = (len(todo) - done_n) / max(rate, 1e-6) / 3600
                    log(f"  {done_n:,}/{len(todo):,}  ok={stats['ok']:,}  "
                        f"{total_bytes / 1e9:.2f} GB  {rate * 60:.0f} works/menit  sisa ~{eta_h:.1f} jam")
    except KeyboardInterrupt:
        log("\nDihentikan (Ctrl+C). Menunggu request yang sedang berjalan selesai ...")
        for fut in pending:
            fut.cancel()
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
        logf.close()
        metaf.close()
        for part in files_dir.glob("*.part"):
            part.unlink(missing_ok=True)

    log("\nHasil run ini:")
    for k, v in stats.most_common():
        log(f"  {k:<18} {v:>7,}")
    log(f"  total unduhan      {total_bytes / 1e9:.2f} GB")
    if fetcher.blocked_hosts:
        log("  host diblokir      " + ", ".join(sorted(fetcher.blocked_hosts)))
    log(f"Lanjutkan: python harvest_pdf.py extract --out {args.out}")


# --------------------------------------------------------------------------- #
# Perintah: extract
# --------------------------------------------------------------------------- #
def pdf_pages(path, max_pages=None):
    """Return (list teks per halaman, jumlah halaman, judul metadata PDF)."""
    try:
        import fitz  # PyMuPDF
        with fitz.open(path) as doc:
            n = doc.page_count
            pages = [doc[i].get_text() for i in range(n if max_pages is None else min(n, max_pages))]
            return pages, n, (doc.metadata or {}).get("title") or ""
    except ImportError:
        pass
    from pypdf import PdfReader
    reader = PdfReader(str(path))
    n = len(reader.pages)
    pages = []
    for i in range(n if max_pages is None else min(n, max_pages)):
        try:
            pages.append(reader.pages[i].extract_text() or "")
        except Exception:  # noqa: BLE001 - satu halaman rusak tidak menggagalkan semuanya
            pages.append("")
    title = ""
    try:
        title = (reader.metadata or {}).get("/Title") or ""
    except Exception:  # noqa: BLE001
        pass
    return pages, n, str(title)


def clean_text(t):
    t = t.replace("­", "")
    t = re.sub(r"(\w)-\n(\w)", r"\1\2", t)      # sambung kata terpotong tanda hubung
    return EMAIL_RE.sub("[email]", t)


def front_sections(text):
    """Cari abstrak & kata kunci (EN/ID) di teks halaman depan. Heuristik; laporkan akurasinya."""
    marks = [(m.start(), m.end(), m.lastgroup) for m in SECTION_RE.finditer(text)]
    out = {"abstract_en": "", "abstract_id": "", "keywords_en": "", "keywords_id": ""}
    for i, (s, e, kind) in enumerate(marks):
        if kind == "intro":
            continue
        field = {"abs_en": "abstract_en", "abs_id": "abstract_id",
                 "kw_en": "keywords_en", "kw_id": "keywords_id"}[kind]
        if out[field]:
            continue
        nxt = marks[i + 1][0] if i + 1 < len(marks) else len(text)
        body = text[e:nxt]
        if field.startswith("keywords"):
            body = re.split(r"\n\s*\n", body.lstrip(" :.-—–\t\n"), maxsplit=1)[0][:400]
        else:
            body = body[:4000]
        body = WS_RE.sub(" ", body).strip(" :.-—–")
        min_len = 3 if field.startswith("keywords") else 80
        if len(body) >= min_len:
            out[field] = body
    return out


def landing_fields(meta):
    m = meta or {}

    def first(*keys):
        for k in keys:
            if m.get(k):
                return m[k][0]
        return ""

    abstracts = [a for k in ("citation_abstract", "dc.description.abstract", "dc.description",
                             "og:description", "description") for a in m.get(k, [])]
    authors = m.get("citation_author") or m.get("dc.creator.personalname") or m.get("dc.creator") or []
    kws = m.get("citation_keywords") or m.get("dc.subject") or m.get("keywords") or []
    return {
        "landing_title": first("citation_title", "dc.title"),
        "landing_authors": " ; ".join(uniq(authors)),
        "landing_affiliations": " ; ".join(uniq(m.get("citation_author_institution", []))),
        "landing_abstract": WS_RE.sub(" ", max(abstracts, key=len)).strip() if abstracts else "",
        "landing_keywords": " ; ".join(uniq(k for s in kws for k in re.split(r"[;,]", s))),
        "landing_date": first("citation_publication_date", "citation_date",
                              "citation_online_date", "dc.date.issued"),
        "landing_journal": first("citation_journal_title", "citation_conference_title"),
    }


def cmd_extract(args):
    out_dir = pdf_dir(args)
    files_dir = out_dir / "files"
    text_dir = out_dir / "text"
    if not args.no_fulltext:
        text_dir.mkdir(parents=True, exist_ok=True)
    log_rows = read_log(out_dir / "download_log.csv")
    landing = {rec["work_id"]: rec.get("meta") or {}
               for rec in read_jsonl(out_dir / "landing_meta.jsonl")}

    ok_ids = [w for w, r in log_rows.items() if r["status"] == "ok"]
    ids = uniq(ok_ids + list(landing))
    log(f"Ekstraksi: {len(ok_ids):,} PDF, {len(landing):,} halaman artikel dengan meta tag")
    header = ["work_id", "has_pdf", "n_pages", "n_chars", "text_layer", "pdf_meta_title",
              "abstract_en", "abstract_id", "keywords_en", "keywords_id",
              "landing_title", "landing_authors", "landing_affiliations", "landing_abstract",
              "landing_keywords", "landing_date", "landing_journal", "front_text"]
    stats = Counter()
    with open(out_dir / "pdf_front.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for i, wid in enumerate(ids, 1):
            row = {"work_id": wid, "has_pdf": 0, **landing_fields(landing.get(wid))}
            path = files_dir / f"{safe_name(wid)}.pdf"
            if wid in log_rows and log_rows[wid]["status"] == "ok" and path.exists():
                try:
                    pages, n, title = pdf_pages(path, None if not args.no_fulltext else args.front_pages)
                    pages = [clean_text(p) for p in pages]
                    front = "\n".join(pages[:args.front_pages])
                    n_chars = sum(len(p.strip()) for p in pages)
                    text_layer = 1 if n_chars / max(len(pages), 1) >= 50 else 0
                    row.update(has_pdf=1, n_pages=n, n_chars=n_chars, text_layer=text_layer,
                               pdf_meta_title=WS_RE.sub(" ", title).strip(),
                               front_text=WS_RE.sub(" ", front).strip()[:args.front_chars],
                               **front_sections(front))
                    if not args.no_fulltext:
                        (text_dir / f"{safe_name(wid)}.txt").write_text(
                            "\n\f\n".join(pages), encoding="utf-8")
                    stats["pdf_ok"] += 1
                    stats["text_layer"] += text_layer
                    stats["abstract_pdf"] += bool(row["abstract_en"] or row["abstract_id"])
                    if args.delete_pdf:
                        path.unlink()
                except Exception as e:  # noqa: BLE001 - PDF rusak/terenkripsi
                    stats["pdf_error"] += 1
                    row["front_text"] = f"[ERROR ekstraksi: {e.__class__.__name__}]"
            stats["abstract_landing"] += bool(row["landing_abstract"])
            w.writerow(row)
            if i % 500 == 0:
                log(f"  ... {i:,}/{len(ids):,}")

    log(f"\nPDF terbaca          : {stats['pdf_ok']:,} (gagal {stats['pdf_error']:,})")
    log(f"  punya lapisan teks : {stats['text_layer']:,}  (sisanya kemungkinan hasil scan -> perlu OCR)")
    log(f"  abstrak dari PDF   : {stats['abstract_pdf']:,}")
    log(f"Abstrak dari halaman : {stats['abstract_landing']:,}")
    log(f"Tersimpan: {out_dir / 'pdf_front.csv'}" + ("" if args.no_fulltext else f", {text_dir}"))
    log("Tips: cek 50 baris acak; heuristik abstrak bisa salah potong, laporkan akurasinya di BAB IV.")


# --------------------------------------------------------------------------- #
# Perintah: stats
# --------------------------------------------------------------------------- #
def cmd_stats(args):
    out_dir = pdf_dir(args)
    cands = {r["work_id"]: r for r in read_csv(out_dir / "candidates.csv")}
    logs = read_log(out_dir / "download_log.csv")
    lines = []

    def p(s=""):
        lines.append(s)
        log(s)

    total = len(cands)
    st = Counter(r["status"] for r in logs.values())
    n_ok = st.get("ok", 0)
    p(f"Total works di candidates.csv : {total:,}")
    p(f"Sudah diproses                : {len(logs):,}")
    p(f"PDF berhasil                  : {n_ok:,} ({100 * n_ok / max(total, 1):.1f}% dari semua works)")
    p(f"Ukuran total                  : {sum(to_int(r['bytes']) for r in logs.values()) / 1e9:.2f} GB")
    p("\nStatus:")
    for k, v in st.most_common():
        p(f"  {k:<20} {v:>8,}")

    p("\nTingkat keberhasilan per oa_status:")
    by = defaultdict(Counter)
    for wid, r in logs.items():
        c = cands.get(wid)
        if c:
            by[c["oa_status"] or "-"][r["status"] == "ok"] += 1
    for k, c in sorted(by.items(), key=lambda x: -sum(x[1].values())):
        n = sum(c.values())
        p(f"  {k:<10} {c[True]:>7,}/{n:<7,} ({100 * c[True] / n:.1f}%)")

    p("\nTahun (berhasil/total diproses):")
    by_year = defaultdict(Counter)
    for wid, r in logs.items():
        if wid in cands:
            by_year[cands[wid]["year"] or "-"][r["status"] == "ok"] += 1
    for y in sorted(by_year):
        c = by_year[y]
        p(f"  {y:<6} {c[True]:>7,}/{sum(c.values()):<7,}")

    p("\nHost dengan kegagalan terbanyak (status selain ok/closed):")
    fails = Counter()
    for wid, r in logs.items():
        if r["status"] not in ("ok", "closed") and wid in cands:
            urls = (cands[wid]["pdf_urls"] + " ; " + cands[wid]["landing_urls"]).split(" ; ")
            fails[host_of(next((u for u in urls if u.strip()), ""))] += 1
    for h, n in fails.most_common(15):
        p(f"  {h or '-':<45} {n:>6,}")

    (out_dir / "pdf_report.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"\nTersimpan: {out_dir / 'pdf_report.txt'}")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description="Unduh PDF open access + metadata halaman depan")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("resolve", help="cari URL PDF tiap work (OpenAlex + OAI)")
    add_common(p)  # --out, --api-key, --mailto, --delay (jeda request OpenAlex)
    p.add_argument("--sources", default="openalex,oai", help="openalex,oai (default keduanya)")
    p.add_argument("--oai-file", default="oai_works_ds.csv",
                   help="file di <out>/oai/ (default oai_works_ds.csv; atau oai_works_all.csv)")
    p.add_argument("--only-ids", help="CSV berkolom work_id untuk membatasi, mis. data/subset/works.csv")
    p.add_argument("--from-year", type=int)
    p.add_argument("--to-year", type=int)
    p.set_defaults(func=cmd_resolve)

    p = sub.add_parser("download", help="unduh PDF (resumable, Ctrl+C aman)")
    p.add_argument("--out", default="data")
    p.add_argument("--mailto", help="email kamu, dicantumkan di User-Agent (sangat disarankan)")
    p.add_argument("--workers", type=int, default=4, help="jumlah thread (default 4)")
    p.add_argument("--delay", type=float, default=1.5, help="jeda minimum per host, detik (default 1.5)")
    p.add_argument("--timeout", type=float, default=45)
    p.add_argument("--max-mb", type=float, default=40, help="lewati file lebih besar dari ini")
    p.add_argument("--limit", type=int, help="hanya coba N works (uji coba)")
    p.add_argument("--origin", choices=["openalex", "oai"], help="hanya works dari sumber ini")
    p.add_argument("--retry-failed", action="store_true", help="ulangi work yang gagal sebelumnya")
    p.add_argument("--retry-all", action="store_true", help="ulangi semua, termasuk yang sudah ok")
    p.add_argument("--ignore-robots", action="store_true",
                   help="JANGAN dipakai kecuali punya izin tertulis pemilik situs")
    p.set_defaults(func=cmd_download)

    p = sub.add_parser("extract", help="PDF -> teks, abstrak, kata kunci (+ meta halaman artikel)")
    p.add_argument("--out", default="data")
    p.add_argument("--front-pages", type=int, default=2, help="halaman depan yang dipindai (default 2)")
    p.add_argument("--front-chars", type=int, default=4000, help="panjang front_text di CSV")
    p.add_argument("--no-fulltext", action="store_true", help="jangan simpan teks lengkap ke text/")
    p.add_argument("--delete-pdf", action="store_true", help="hapus PDF setelah diekstrak (hemat disk)")
    p.set_defaults(func=cmd_extract)

    p = sub.add_parser("stats", help="ringkasan hasil unduhan")
    p.add_argument("--out", default="data")
    p.set_defaults(func=cmd_stats)

    args = ap.parse_args()
    if getattr(args, "sources", None):
        args.sources = {s.strip() for s in args.sources.split(",") if s.strip()}
    args.func(args)


if __name__ == "__main__":
    main()
