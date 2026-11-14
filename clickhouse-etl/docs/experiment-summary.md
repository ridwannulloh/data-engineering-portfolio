# Menjalankan dan merangkum percobaan ClickHouse

## Memuat Januari 2024 sampai bulan berjalan

```bash
uv run ch-etl load-taxi-range --start 2024-01 --end current
```

`current` mengikuti bulan pada mesin yang menjalankan CLI. Untuk rentang percobaan tetap:

```bash
uv run ch-etl load-taxi-range --start 2024-01 --end 2026-11
```

Kedua batas inklusif; rentang Januari 2024–November 2026 meminta 35 bulan. Ini permintaan rentang,
bukan janji 35 file sudah tersedia. TLC menerbitkan file bulanan biasanya dengan jeda sekitar dua
bulan. Command membaca [katalog resmi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
dan hanya memuat Yellow Taxi yang tercantum di sana. Bulan yang belum tercantum dicetak sebagai
peringatan dan dicatat di `unavailable` pada `reports/taxi-range.json`.

Perilaku default melanjutkan snapshot yang sudah ada: bulan dengan data di `raw.taxi_trips`
dilewati. Satu bulan dimuat utuh dan dipublikasikan secara atomik. Jika bulan berikutnya gagal,
bulan yang selesai tetap ada. Jalankan ulang command yang sama untuk melanjutkan. Error jaringan,
schema, parsing, dan database tetap menghentikan proses; error tersebut tidak dianggap bulan yang
belum terbit. Laporan JSON hanya ditulis saat command selesai normal; log dan snapshot di database
menunjukkan progres jika proses terhenti.

`--reload` memuat ulang bulan yang sudah ada (termasuk mengambil revisi sumber). `--strict` menolak
rentang yang memiliki bulan belum tercantum sebelum mulai memuat. Tanpa reload, snapshot yang sudah
ada tetap dapat dipakai walaupun linknya kemudian hilang dari katalog. Jalankan satu loader pada
satu waktu. Refresh mart membaca semua bulan valid dan hanya dijalankan sekali setelah rangkaian
load, termasuk saat tidak ada bulan baru agar refresh yang sebelumnya gagal dapat dipulihkan.
Jika proses mati paksa, jalankan command lagi untuk menyelesaikan refresh.

Selama bulk load, dashboard mart menampilkan hasil refresh sebelumnya. Snapshot raw per bulan dan
refresh mart tidak merupakan satu transaksi lintas tabel. Pemakaian compute, jaringan, dan storage
bertambah saat memuat banyak bulan. Laporan `loaded_rows` menghitung baris yang dimuat pada eksekusi
itu saja; total keseluruhan dapat diperoleh dari query coverage/dashboard.

## Fungsi taxi-demo

```bash
uv run ch-etl taxi-demo --month 2024-01 --repeats 5 --output reports/taxi-2024-01.json
```

Ini alat verifikasi dan pengukuran untuk **satu bulan** yang sudah dimuat:

- Membandingkan setiap grup agregat detail dengan mart: tanggal, lokasi, pembayaran, count, tagihan,
  dan tip harus cocok. Selisih menghentikan demo.
- Mengukur query detail vs materialized view, menampilkan median waktu dan baris/byte yang dibaca.
- Menampilkan kompresi kolom, query plan pruning indeks, kualitas data, jam sibuk, dan lokasi ramai.
- Menyimpan hasil aktual beserta versi server dalam JSON untuk bahan laporan.

Demo bukan syarat menjalankan dashboard. Ia meminta refresh semua bulan sebelum mengukur satu bulan.
Benchmark memakai warm-up, mematikan query-result cache, dan mengubah urutan pengukuran. Waktu dari
Python mencakup jaringan. Cache filesystem tetap bisa berpengaruh. Rasio di atas 1 berarti query
mart lebih cepat pada percobaan itu; hasil lain tetap sah. Belum ada perbandingan dengan PostgreSQL.

## Dashboard dari data ClickHouse

Cara utama: service dashboard bawaan repository.

```bash
make dashboard            # atau: uv run ch-etl dashboard --port 8050
```

Buka <http://127.0.0.1:8050>. Halaman menampilkan KPI dengan sparkline dan perubahan bulanan,
tren bulanan/harian, perbandingan antar tahun, pola hari dalam seminggu, 10 zona pickup teramai
(dengan nama zona dari file lookup TLC), komposisi jenis pembayaran, baris yang dikecualikan per
alasan, dan tabel coverage per bulan. Periode dipilih lewat preset atau From/To dan tersimpan di
URL. Service tidak punya login dan default hanya mendengarkan `127.0.0.1`; lihat README sebelum
memakai `--host 0.0.0.0` atau Docker (`make dashboard-docker`).

Alternatif tanpa menjalankan service: dashboard bawaan Cloud
([panduan resmi](https://clickhouse.com/docs/cloud/manage/dashboards)).

1. Buka SQL Console service yang sudah berisi data.
2. Jalankan setiap file di `sql/dashboard/` sebagai query tersendiri. Console meminta nilai
   `start` dan `end`: tanggal awal bulan, misalnya `2024-01-01` dan `2026-11-01`. Simpan query.
3. Pilih visualisasi dari hasil query.
4. Buka Dashboards → New Dashboard, lalu tambahkan visualisasi dari saved query.

| Query | Visualisasi | Arti |
|---|---|---|
| `00_available_months.sql` | (filter periode) | bulan yang sudah ada di mart |
| `01_kpis.sql` | KPI | jumlah perjalanan valid, total tagihan USD, rata-rata, tip tercatat |
| `02_monthly_trend.sql` | line chart | tren bulanan perjalanan dan tagihan |
| `03_daily_trend.sql` | line chart | perubahan harian |
| `04_top_pickup_locations.sql` | bar chart | 10 location ID pickup teramai |
| `05_quality_and_coverage.sql` | tabel/stacked bar | bulan yang termuat dan alasan eksklusi |
| `06_payment_types.sql` | donut/bar | perjalanan per kode jenis pembayaran TLC |

Bulan tanpa file belum punya baris; jangan mengartikannya sebagai nol perjalanan. Periksa
`unavailable` pada laporan range. Di SQL Console, label lokasi masih ID TLC; service dashboard
menambahkan nama zona. Tarif memakai USD dan total tagihan tercatat bukan laba sopir; tip tunai tidak
tercakup. Empat query mart memakai hasil rekap; query kualitas membaca raw.

## ClickHouse cocok untuk apa?

ClickHouse menyimpan data per kolom dan dirancang untuk OLAP: pemindaian serta agregasi atas banyak
baris. Contoh penggunaan: analitik produk/clickstream, dashboard transaksi, log/observability, dan
time series. Untuk aplikasi operasional dengan transaksi lintas tabel, constraint relasional,
serta perubahan individual yang intensif, PostgreSQL/MySQL sering tetap menjadi database sumber;
ClickHouse menerima salinannya untuk analitik.

Percobaan taxi ini menunjukkan alur ingest Parquet, penyimpanan kolumnar, pengelompokan SQL,
partition bulanan, pruning indeks, dan refreshable materialized view. Ini snapshot batch bulanan;
belum menguji ingest streaming, latensi real-time, high availability, atau banyak pengguna bersamaan.
Referensi: [ClickHouse overview](https://clickhouse.com/docs/intro).

## Template summary berdasarkan hasil nyata

Isi placeholder setelah load dan demo selesai. Jangan mengisi angka dari unit test atau asumsi.

> Kami mencoba ClickHouse Cloud untuk analitik NYC Yellow Taxi. Rentang permintaan adalah Januari
> 2024–November 2026; data yang benar-benar tersedia mencakup **[N bulan, periode aktual]**, dengan
> **[N seluruh baris]**, **[N baris valid]**, dan **[N baris dikecualikan]**. Data Parquet dibaca
> langsung dari sumber resmi, kemudian dipublikasikan sebagai snapshot bulanan yang dapat diulang.
>
> Pada benchmark bulan **[bulan]** di ClickHouse **[versi/ukuran layanan]**, seluruh grup agregasi
> detail dan materialized view cocok. Median query detail adalah **[X ms]** dan query mart **[Y ms]**
> dalam **[N pengulangan]**. Rasio waktu detail/mart adalah **[X/Y]**, dan rasio kompresi kolom raw
> **[Z]**. Pengukuran memakai warm-up dan mencakup latensi jaringan.
>
> Dashboard menampilkan tren perjalanan, tagihan tercatat, lokasi pickup, dan kualitas data.
> Hasil ini mendukung penggunaan ClickHouse untuk **[kesimpulan sesuai angka]**. Batas percobaan:
> file sumber terbit terlambat, ada filter kualitas, materialized view memakai refresh batch,
> dan pengujian belum mencakup streaming maupun concurrency.

Sumber angka: laporan range untuk bulan yang baru dimuat/terlewat; query dashboard kualitas untuk
coverage dan seluruh baris per kategori; laporan `taxi-demo` untuk benchmark dan kompresi satu bulan.
Jika sumber diperbarui di antara pengukuran, catat waktu percobaan dan jalankan ulang verifikasi.
