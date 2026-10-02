# DS Harvester — Pengumpul Data Publikasi Data Science untuk Knowledge Graph

Tiga skrip Python untuk membangun korpus publikasi data science (fokus afiliasi Indonesia):

| Skrip | Sumber | Kekuatan |
|---|---|---|
| `harvest_openalex.py` | OpenAlex API | ID penulis & institusi sudah didisambiguasi, topik terklasifikasi |
| `harvest_oai.py` | OAI-PMH jurnal OJS / repository | Menjangkau jurnal nasional non-DOI, abstrak (sering dwibahasa) |
| `merge_dedup.py` | Gabungan keduanya | Buang duplikat, isi abstrak OpenAlex yang kosong dari OAI |
| `harvest_pdf.py` | URL open access (OpenAlex + halaman OJS/EPrints) | Unduh PDF, ambil abstrak/kata kunci/penulis dari halaman depan & meta tag |

## 1. Persiapan

```bash
pip install -r requirements.txt
```

Buat API key OpenAlex gratis di https://openalex.org/settings/api (wajib sejak Feb 2026), lalu:

```bash
export OPENALEX_API_KEY=isi_key_kamu        # Linux/Mac
set OPENALEX_API_KEY=isi_key_kamu           # Windows CMD
```

## 2. Jalur A — OpenAlex

```bash
# (a) Cari kandidat topik dari kata kunci -> data/topics_candidates.csv
python harvest_openalex.py topics --out data --mailto email@kamu.ac.id

# (b) BUKA data/topics_candidates.csv, review kolom `include` (1 = ambil, 0 = buang).
#     Topik dari field Computer Science/Mathematics/Decision Sciences sudah diberi 1
#     otomatis, tapi tetap periksa satu per satu. Simpan daftar final ini:
#     ini akan jadi lampiran metodologi skripsimu.

# (c) Cek jumlah & estimasi biaya dulu
python harvest_openalex.py count --out data --country ID --from-year 2018

# (d) Harvest (bisa dihentikan & dilanjutkan kapan saja)
python harvest_openalex.py harvest --out data --country ID --from-year 2018 --mailto email@kamu.ac.id

# (e) Ubah ke CSV
python harvest_openalex.py flatten --out data
```

Opsi berguna:
- `--subfields 1702,1707` : pakai subfield langsung, tanpa file topik (1702 = Artificial Intelligence).
- `--primary-only` : hanya cocokkan primary topic (lebih presisi, lebih sedikit hasil).
- `--institution I123456789` : batasi ke satu kampus (cari ID-nya di https://api.openalex.org/institutions?search=nama+kampus).
- `--require-abstract` : hanya works yang punya abstrak di OpenAlex.
- `--max-records 500` : uji coba kecil dulu.
- `--country ''` : tanpa batasan negara (hasil bisa sangat besar, cek `count` dulu).

Biaya: sekitar US$0,0001 per halaman (100 works). Kuota gratis ≈ US$1/hari per key,
setara kira-kira sampai ratusan ribu works per hari. Jika kuota habis, jalankan lagi
besok; skrip melanjutkan dari posisi terakhir.

## 3. Jalur B — OAI-PMH jurnal nasional

```bash
# (a) Isi journals.txt (lihat petunjuk di dalam file). Pola OJS 3:
#     https://<domain>/index.php/<kode-jurnal>/oai | Nama Jurnal
# (b) Uji semua endpoint
python harvest_oai.py check --journals journals.txt
# (c) Harvest + tandai artikel data science
python harvest_oai.py harvest --journals journals.txt --out data --mailto email@kamu.ac.id
```

Semua artikel disimpan; artikel data science ditandai `ds_match=1` berdasarkan
`keywords_ds.txt`. Kalau penandaan kurang tepat, ubah daftar kata kunci lalu:

```bash
python harvest_oai.py flatten --out data --min-matches 2
```

Tips memilih jurnal: ambil jurnal informatika/ilmu komputer/statistika dari daftar
SINTA (misalnya S1–S4), sehingga filter kata kunci bekerja pada korpus yang sudah relevan.

## 4. Gabungkan

```bash
python merge_dedup.py --data data
```

## 4b. Jalur C — PDF lengkap + halaman depan (`harvest_pdf.py`)

`works.csv` dari OpenAlex belum menyimpan URL PDF, jadi langkah `resolve` menanyakan ulang
status open access ke OpenAlex per 100 ID (~1.200 request untuk 116 ribu works, sangat murah).
Works OAI digabung lewat DOI, karena halaman OJS sering punya PDF walau OpenAlex tidak.

```bash
python harvest_pdf.py resolve  --out data --mailto email@kamu.ac.id
python harvest_pdf.py download --out data --mailto email@kamu.ac.id --limit 50   # uji coba
python harvest_pdf.py download --out data --mailto email@kamu.ac.id              # semua (resumable)
python harvest_pdf.py extract  --out data
python harvest_pdf.py stats    --out data
```

Opsi berguna:
- `resolve --only-ids data/subset/works.csv` : hanya works di subset (hemat waktu & disk).
- `resolve --from-year 2020` : batasi tahun.
- `download --workers 4 --delay 1.5` : thread & jeda minimum per host (jangan terlalu agresif).
- `download --retry-failed` : ulangi yang gagal (mis. jaringan putus).
- `extract --delete-pdf` : simpan teksnya saja, hapus PDF (hemat disk).

Output di `data/pdf/`:

| File | Isi |
|---|---|
| `candidates.csv` | work_id, status OA, lisensi, kandidat URL PDF & halaman artikel |
| `files/<work_id>.pdf` | PDF hasil unduhan (hanya open access) |
| `download_log.csv` | status per work: ok, closed, robots_blocked, http_403, html_no_pdf_link, ... |
| `landing_meta.jsonl` | meta tag halaman artikel (citation_title/author/abstract/keywords, DC.*) |
| `pdf_front.csv` | abstrak & kata kunci EN/ID dari halaman depan PDF + judul/penulis/afiliasi/abstrak dari halaman artikel |
| `text/<work_id>.txt` | teks lengkap PDF (halaman dipisah `\f`) |
| `pdf_report.txt` | statistik keberhasilan per oa_status, tahun, dan host (untuk BAB IV) |

Catatan:
- Karya tertutup (`is_oa=0`) tidak diunduh. Proteksi bot, login, dan CAPTCHA tidak ditembus;
  host yang terus membalas 403/429 dihentikan otomatis pada run itu.
- PDF hasil scan tidak punya lapisan teks (`text_layer=0`) dan butuh OCR terpisah.
- Pemotongan abstrak dari PDF bersifat heuristik; validasi dengan sampel manual.
- PDF tetap milik penerbit/penulisnya: pakai untuk analisis teks, jangan didistribusikan ulang.

## 5. Struktur output

```
data/
├── topics_candidates.csv       # daftar topik (hasil review manual)
├── raw/                        # data mentah JSONL + state resume (JANGAN dihapus saat harvest)
├── openalex/
│   ├── works.csv               # work_id, doi, title, abstract, year, source_name, ...
│   ├── authors.csv             # author_id, name, orcid
│   ├── institutions.csv        # institution_id, name, country_code, type, ror
│   ├── authorships.csv         # work_id, author_id, position, institution_ids, ...
│   ├── topics.csv              # topic_id, name, subfield, field, domain
│   ├── work_topics.csv         # work_id, topic_id, score, is_primary
│   └── work_keywords.csv
├── oai/
│   ├── oai_works_all.csv
│   └── oai_works_ds.csv
└── merged/
    ├── works.csv               # korpus gabungan (sumber utama untuk ekstraksi LLM)
    ├── authorships.csv
    └── merge_report.txt        # angka-angka untuk BAB IV (statistik data)
```

## 6. Contoh impor ke Neo4j

Salin CSV ke folder `import` Neo4j, lalu:

```cypher
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Publikasi) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (r:Peneliti)  REQUIRE r.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (t:Topik)     REQUIRE t.id IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///merged/works.csv' AS row
MERGE (p:Publikasi {id: row.work_id})
SET p.judul = row.title, p.abstrak = row.abstract, p.tahun = toInteger(row.year),
    p.doi = row.doi, p.venue = row.venue, p.sumber = row.source;

LOAD CSV WITH HEADERS FROM 'file:///merged/authorships.csv' AS row
MERGE (r:Peneliti {id: row.author_id}) ON CREATE SET r.nama = row.author_name
WITH r, row MATCH (p:Publikasi {id: row.work_id})
MERGE (r)-[:MENULIS {posisi: row.author_position}]->(p);

LOAD CSV WITH HEADERS FROM 'file:///openalex/topics.csv' AS row
MERGE (t:Topik {id: row.topic_id})
SET t.nama = row.name, t.subfield = row.subfield_name;

LOAD CSV WITH HEADERS FROM 'file:///openalex/work_topics.csv' AS row
MATCH (p:Publikasi {id: row.work_id}), (t:Topik {id: row.topic_id})
MERGE (p)-[:MEMBAHAS {skor: toFloat(row.score), utama: row.is_primary = '1'}]->(t);
```

Node Metode dan Dataset belum ada di sini. Keduanya berasal dari tahap ekstraksi
LLM atas kolom `abstract` di `merged/works.csv` (tahap berikutnya di BAB III).

## 7. Etika & batasan

- OpenAlex (CC0) dan OAI-PMH memang disediakan untuk diambil mesin; tetap gunakan jeda
  (`--delay`) dan cantumkan email di `--mailto`.
- Jangan menambahkan scraping Google Scholar ke pipeline ini (dilarang ToS Google).
- Penulis dari OAI hanya berupa nama (ID `NAME:...`), sehingga nama yang sama belum tentu
  orang yang sama, dan satu orang bisa muncul dengan beberapa ejaan. Laporkan ini sebagai
  keterbatasan, atau cocokkan manual ke ID OpenAlex/SINTA untuk peneliti di kampusmu.
- Penandaan data science di jalur OAI berbasis kata kunci; validasi dengan sampel manual
  dan laporkan presisinya.
