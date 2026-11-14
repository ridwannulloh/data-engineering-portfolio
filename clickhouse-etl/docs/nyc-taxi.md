# Analitik perjalanan nyata dengan ClickHouse

Dataset: **NYC TLC Yellow Taxi**, default Januari 2024. Sumber resmi menyediakan waktu pickup
dan dropoff, location ID, jarak, penumpang, jenis pembayaran, tarif, dan tip. File bulanan tersedia
sebagai Parquet tanpa akun Kaggle. Mulai dari satu bulan; jumlah baris dan ukuran aktual ditampilkan
oleh loader dan demo, bukan angka benchmark yang diasumsikan.

- [Halaman dataset resmi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [File Januari 2024](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet)
- [Kamus kolom](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [Nama lokasi untuk location ID](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)

TLC menyatakan data berasal dari penyedia teknologi dan tidak menjamin akurasinya. Sumber dapat
direvisi; satu import merupakan snapshot file pada saat itu, bukan salinan dengan checksum yang
dipatok. Loader mempertahankan seluruh baris dan kolom yang dipilih, termasuk baris yang dikecualikan
dari analitik. Dua baris identik yang memang ada di file tetap dipertahankan: dataset ini tidak
menyediakan primary key perjalanan untuk membuktikan keduanya merupakan perjalanan yang sama.

## ClickHouse cocok untuk apa?

ClickHouse adalah database analitik kolumnar. Gunakan untuk dashboard produk, analitik transaksi,
log/observability, time series, dan laporan yang menghitung banyak baris sekaligus. Contohnya:
berapa perjalanan per jam, area mana paling ramai, bagaimana distribusi jarak, dan berapa total
tagihan per hari. Penyimpanan per kolom, kompresi, pruning indeks, serta agregasi paralel membantu
jenis pekerjaan ini.

Database aplikasi seperti PostgreSQL/MySQL tetap lebih sesuai untuk workflow transaksi dengan
banyak perubahan satu record, constraint relasional, dan transaksi lintas tabel. Pola yang umum:
database aplikasi menyimpan transaksi operasional; ClickHouse menerima salinan untuk analitik.
Demo ini mengukur dua jalur query di ClickHouse, bukan membuktikan ClickHouse lebih cepat dari
PostgreSQL atau database lain.

Referensi: [ClickHouse overview](https://clickhouse.com/docs/intro),
[materialized views](https://clickhouse.com/docs/materialized-view/refreshable-materialized-view).

## Menjalankan di Cloud

Gunakan Python 3.11+ dan `uv`. Dari direktori repository:

```bash
cp .env.example .env
```

Isi host tanpa `https://`, user database, dan password dari menu Connect layanan Cloud:

```dotenv
CLICKHOUSE_HOST=your-service.clickhouse.cloud
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-database-password
CLICKHOUSE_SECURE=true
```

Izinkan IP komputer/server yang menjalankan CLI di pengaturan akses layanan. User membutuhkan izin
membuat database/tabel/view, SELECT, INSERT, ALTER untuk mengganti partition, DROP untuk membersihkan
tabel import milik proses, serta menjalankan refresh/wait materialized view. Gunakan layanan
development untuk percobaan awal. Migration menambahkan tabel demo; source file satu bulan dibaca
oleh server ClickHouse, sehingga server perlu akses HTTPS ke `d37ci6vzurychx.cloudfront.net`.

```bash
uv sync
uv run ch-etl ping
uv run ch-etl migrate
uv run ch-etl load-taxi --month 2024-01
uv run ch-etl taxi-demo --month 2024-01 --repeats 3 --output reports/taxi-2024-01.json
```

Tidak perlu menjalankan Docker jika menggunakan Cloud. Untuk server lokal:

```bash
make up
# Ubah .env ke host localhost, port 8123, password localdev, secure=false.
make migrate
make taxi
make taxi-demo
```

Shortcut bulan lain: `make taxi MONTH=2024-02`, lalu `make taxi-demo MONTH=2024-02`.
Menambah bulan lain mempertahankan bulan sebelumnya; mengimpor bulan yang sama mengganti snapshot
bulan itu. `transform`, `stats`, dan `load-sample` tetap khusus contoh event sintetis lama.

Januari 2024 hanya default demo. TLC menyediakan file bulanan sejak 2009; loader ini menerima
bulan mulai 2017 karena mengharapkan kolom location ID. Pilih bulan yang sudah diterbitkan di
halaman sumber. Schema tahun lama atau perubahan schema sumber tetap perlu diverifikasi.
Contoh memuat beberapa bulan secara berurutan (berhenti jika salah satu import gagal):

```bash
for month in 2024-01 2024-02 2024-03; do
  uv run ch-etl load-taxi --month "$month" || break
done
uv run ch-etl taxi-demo --month 2024-03 --output reports/taxi-2024-03.json
```

Jika versi sebelumnya gagal dengan `Expected identifier` pada `REPLACE PARTITION ... FROM`,
gunakan kode loader terbaru lalu ulangi `load-taxi`. Perbaikannya ada di Python, sehingga tidak
perlu migration ulang. Import yang gagal pada tahap sintaks belum mengganti partition tujuan.

## Alur dan perilaku saat gagal

```text
NYC TLC Parquet di HTTPS
  → tabel import sementara milik satu proses (parsing + penandaan kualitas)
  → validasi jumlah baris
  → REPLACE PARTITION bulanan di raw.taxi_trips
  → staging.taxi_trips (view yang memilih quality_issue = 'valid')
  → mart.taxi_daily_mv (refreshable materialized view)
  → mart.taxi_daily (rekap hari × lokasi pickup × jenis pembayaran)
```

- Import gagal sebelum publikasi: snapshot lama tetap tersedia; baris parsial tidak dipublikasikan.
- Publikasi menggunakan [REPLACE PARTITION yang atomik](https://clickhouse.com/docs/sql-reference/statements/alter/partition#replace-partition).
  Jika koneksi putus saat publikasi, ulangi `load-taxi`; penggantian satu snapshot tidak menambah data.
- Tidak ada retry otomatis pada query import besar. Satu kali retry dilakukan dengan menjalankan
  ulang perintah setelah penyebab kegagalan ditangani.
- Jika proses mati paksa, tabel bernama `raw._taxi_import_<uuid>` dapat tertinggal. Hapus hanya tabel
  import dari proses yang sudah selesai/mati. Kegagalan cleanup dicatat di log.
- Refresh mengganti seluruh hasil mart secara atomik, sehingga grup yang hilang dari file koreksi
  ikut hilang. Ini menghindari masalah agregat lama tertinggal pada contoh event sebelumnya.
- Publikasi raw dan refresh mart adalah dua operasi terpisah. Selama jeda, mart bisa berisi snapshot
  sebelumnya. Jika refresh gagal, `taxi-demo` meminta refresh baru. Tidak ada janji transaksi lintas
  kedua tabel. Jangan menjalankan loader bersamaan dengan benchmark.
- Jalankan satu loader pada satu waktu. Belum ada lock terdistribusi untuk publisher paralel.

Materialized view ini **refreshable**, bukan incremental. Refresh otomatis dijadwalkan sekali sehari;
loader dan demo juga melakukan refresh lalu menunggu selesai. Setiap refresh membaca seluruh data
valid yang sudah dimuat, termasuk bulan lain. Ini sesuai untuk demonstrasi snapshot bulanan, tetapi
biayanya bertambah seiring data. Pipeline streaming berskala besar dapat memakai incremental MV
dengan strategi dedup yang dirancang untuk sumbernya. `REPLACE PARTITION` tidak memicu incremental MV.

## Kebijakan kualitas dan arti angka

Waktu diperlakukan sebagai waktu lokal `America/New_York`, bukan WIB. File TLC tidak menyediakan
offset per record; waktu ambigu saat pergantian daylight saving mengikuti parsing ClickHouse.
Uang dinormalisasi ke dua desimal USD. Dataset menggunakan jarak dalam mil.

`quality_issue` mengambil **alasan pertama** yang cocok, agar jumlah semua kategori sama dengan
jumlah seluruh baris sumber:

1. `missing_time`: waktu pickup/dropoff hilang atau tidak dapat diparse.
2. `outside_source_month`: pickup berada di luar bulan file.
3. `negative_duration`: dropoff lebih awal daripada pickup.
4. `invalid_distance`: jarak hilang, non-finite, atau negatif.
5. `missing_or_invalid_amount`: tarif/tip/total hilang atau tidak dapat diparse.
6. `negative_amount`: salah satu nilai uang negatif; dapat mencerminkan refund/koreksi.
7. `invalid_location`: location ID di luar rentang 1–265.
8. `valid`: masuk agregasi demo.

Ini filter analitik eksplisit, bukan klaim semua baris yang dikecualikan salah. Penumpang yang tidak
diisi tetap `NULL`; jumlah penumpang tidak digunakan untuk memutuskan validitas. Location ID 264/265
dipertahankan sebagai kode lokasi tidak diketahui sebagaimana sumber. `total_amount` adalah total
tagihan yang direkam, **bukan laba atau pendapatan bersih sopir**. Tip tunai tidak lengkap dalam data
TLC, sehingga total tip yang dilaporkan tidak mewakili seluruh tip.

## Membaca bukti dari taxi-demo

| Fitur | Yang dijalankan | Yang diperiksa |
|---|---|---|
| Pembacaan Parquet native | `url(..., 'Parquet')` saat load | jumlah seluruh baris dan baris valid |
| Agregasi SQL | query harian dari detail | count, total tagihan, dan tip per grup |
| Materialized view | query hasil rekap tersimpan | **setiap grup** harus identik dengan detail |
| Columnar compression | `system.parts` untuk bulan tersebut | byte kolom sebelum/sesudah kompresi |
| Partition dan primary index pruning | `EXPLAIN indexes = 1` untuk hari pertama | parts/granules yang dipilih |
| Analitik eksplorasi | busiest hours, top pickup IDs, p50/p95 jarak | hasil query dari data valid |
| Import ulang | integration test menjalankan load dua kali | jumlah dan hasil agregasi tetap sama |

Benchmark melakukan warm-up kedua query, menonaktifkan query-result cache, mengganti urutan query
pada tiap pengulangan, lalu melaporkan median waktu dari sisi Python. Waktu ini termasuk jaringan
dan transfer hasil. Cache filesystem/OS/server tetap dapat aktif; ini **bukan cold-cache benchmark**.
`read_rows`, `read_bytes`, dan `elapsed_ns` berasal dari summary HTTP server bila tersedia; nilai
yang tidak dikirim ditulis `null`. Tidak perlu mengakses query log seluruh replica.

Rasio waktu = waktu detail / waktu mart. Lebih dari 1 berarti mart lebih cepat pada pengukuran itu;
kurang dari 1 berarti mart lebih lambat. Hasil tidak dipaksa lolos berdasarkan speedup. Dataset kecil,
latensi jaringan, ukuran layanan, merge, dan beban lain dapat mengubah hasil.

Kompresi membandingkan byte kolom logis dan byte kolom terkompresi pada parts aktif, bukan ukuran
download Parquet, tagihan Cloud, atau seluruh pemakaian disk. Stats dibaca dari satu replica untuk
menghindari menghitung penyimpanan SharedMergeTree berulang kali.

JSON di `reports/` menyimpan versi server, sumber, bulan, hasil kualitas, hasil analitik, query plan,
dan semua sampel benchmark. File dibuat setelah pemeriksaan kesetaraan berhasil; tidak ada laporan
angka performa yang dibuat tanpa menjalankan server.

## Verifikasi

```bash
make test
make lint
```

Unit test mencakup validasi bulan, publikasi hanya setelah validasi, kegagalan import, cleanup,
dan benchmark yang menolak hasil grup berbeda walaupun grand total sama. Tes itu tidak membuktikan
bahwa SQL berjalan di server; live test di bawah menguji jalur tersebut.

```bash
# Memakai .env; mengunduh data resmi pada server dan mengganti Januari 2024 dua kali.
make test-integration
```

Live test menjalankan migration, membaca Parquet nyata, mengganti partition, menguji import ulang,
menunggu refresh MV, memeriksa semua grup, serta mengambil statistik dan query plan. Tidak ada
penghapusan hasil akhirnya agar bisa dieksplorasi. Tanpa `CH_ETL_INTEGRATION=1`, test ini di-skip.

Jika mart tidak cocok dengan detail, hentikan publisher lain lalu ulangi `taxi-demo`. Jika import
gagal karena HTTP/schema, cek ketersediaan file bulan tersebut dan perubahan schema di sumber TLC.
Jika ada timeout, periksa status query di server sebelum mengulang. Hasil benchmark hanya dapat
diklaim setelah live run berhasil; tes Python yang lolos bukan pengganti pengukuran tersebut.
