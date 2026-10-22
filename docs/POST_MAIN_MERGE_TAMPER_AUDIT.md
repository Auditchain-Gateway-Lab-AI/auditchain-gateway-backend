# Audit integrasi tamper setelah merge main

Audit kode backend dan Agent, 22 Oktober 2026. Baseline backend HEAD `15616ea`,
Agent HEAD `a378f2b`, termasuk perubahan monitoring lokal yang dipulihkan dari stash.
Audit ini tidak membaca atau mengubah database deployment.

## Resolusi konflik

Konflik `internal/config/database.go` berasal dari Updated upstream versus Stashed changes.
Pertahankan ketiga migrasi berikut secara bersamaan:

- `ClientDataTamperRecord`: insiden data operasional client dari fitur main.
- `IntegrityCheck`: histori setiap attempt pemeriksaan proof/source.
- `BackgroundWorkerStatus`: heartbeat dan konfigurasi worker monitoring.

Ketiga tabel bukan pengganti satu sama lain. Resolusi telah ditambahkan ke index Git;
stash tidak dihapus dan tidak dibuat commit baru.

## Pemetaan data aktual

| Data | Penyimpanan | Writer / reader saat audit |
| --- | --- | --- |
| Insiden bukti Gateway | `tamper_incidents`, scope `GATEWAY_INTEGRITY` | TamperScanner / recovery lama |
| Insiden sumber legacy | `tamper_incidents`, scope `CLIENT_SOURCE` | Full verification dan recovery direct lama |
| Insiden data client baru | `client_data_tamper_records` | Agent audit report / `/dashboard/client-tampering` |
| Histori pemeriksaan | `integrity_checks`, dimension proof/source | Writer attempt monitoring |
| Kondisi terakhir log | `audit_logs.integrity_*` dan `source_integrity_*` | Verifier dan Agent audit report |
| Heartbeat worker | `background_worker_statuses` | Worker monitoring |

Pemisahan merupakan domain data, bukan sekadar portal internal versus portal client.
Client juga dapat melihat insiden bukti Gateway yang terkait tenant-nya.
Dokumentasi main menginginkan `tamper_incidents` khusus Gateway, tetapi writer legacy
`ensureClientSourceIncident` dan consumer recovery saat ini masih memakai tabel itu.

## Temuan prioritas

### P1 — Writer dan reader insiden client terbelah

- `ProcessAgentAuditReport` menulis `ClientDataTamperRecord`.
- `ensureClientSourceIncident` masih membuat `TamperIncident` scope `CLIENT_SOURCE`.
- `VerifyClientSourceIntegrity` menulis status/histori source, tetapi tidak menulis
  `ClientDataTamperRecord`. Guard insiden yang mensyaratkan proof terverifikasi tidak
  terpenuhi oleh nilai proof `NOT_CHECKED` pada source-only verifier.
- Client frontend `api-recovery.service.ts` masih membaca `/dashboard/recovery/incidents`,
  bukan `/dashboard/client-tampering`.

Dampak: ketidakcocokan sumber yang sama dapat tercatat berbeda bergantung jalur
pemeriksaan; record baru mentor tidak otomatis muncul di recovery UI lama.

Rekomendasi: jadikan tabel baru sumber utama insiden data client, pusatkan writer source,
dan sediakan adapter API/UI untuk kompatibilitas ID serta lifecycle legacy. Jangan
memindahkan atau menghapus insiden legacy sebelum relasi recovery requests/events
dipetakan dan diuji.

### P1 — Laporan Agent belum terhubung ke histori attempt

`ProcessAgentAuditReport` memperbarui status log dan statistik langsung dalam transaksi,
tanpa insert `IntegrityCheck` untuk hasil laporan normal. Recheck khusus blockchain
tampered memakai `VerifyLogIntegrity`, sehingga dapat tercatat sebagai manual single-log.

Dampak: statistik/status dan tabel tamper berubah, sementara Integrity Trend dapat tetap
kosong atau memiliki pemicu yang keliru.

Rekomendasi: pencatatan Agent report harus memakai attempt stabil dari run, dimensi
terpisah, pemicu `AGENT_AUDIT`, waktu pemeriksaan yang eksplisit, dan transaksi yang sama
dengan status. Jangan mengubah status lama menjadi histori buatan.

### P1 — Hasil SKIPPED masih dapat menaikkan proof menjadi VALID

Pada report ingestion, kombinasi data VALID dan blockchain SKIPPED menghasilkan
`integrity_status=VALID`. Data ERROR juga dapat menurunkan status proof walaupun kegagalan
terjadi pada pembacaan sumber. Pada data TAMPERED, variabel sourceStatus diubah setelah
update `audit_logs`, sehingga status sumber yang tersimpan dapat tetap memakai cache lama.

Dampak: dimensi sumber dan proof saling memengaruhi tanpa pemeriksaan yang sesuai.

Rekomendasi: perbarui proof hanya dari pemeriksaan proof yang benar-benar selesai;
SKIPPED tidak membuktikan proof valid. Tentukan source status sebelum update, berdasarkan
hasil sumber dan reference log tenant yang cocok.

### P1 — Validasi report, error persistence, dan konkurensi belum utuh

Writer tabel baru memakai resource, hash, action, dan log ID dari item report. Writer
tersebut belum memastikan referensi resource/log adalah log terbaru tenant yang sama.
Beberapa `Create/Updates/Count` mengabaikan `.Error`, termasuk cabang lookup yang tidak
menangani error selain record-not-found. Model baru belum memiliki unique index parsial
untuk satu insiden OPEN per tenant/resource.

Dampak: kegagalan penulisan tidak selalu dikembalikan secara eksplisit; laporan paralel
dapat menghasilkan insiden OPEN ganda; referensi yang tidak cocok dapat menjadi dasar
record recovery. Ini temuan jalur kode, bukan bukti kejadian pada deployment.

Rekomendasi: validasi referensi tenant/resource/latest dan hash; propagasikan semua error;
tambahkan lock/upsert dan index setelah audit duplikasi existing records.

### P1 — Recovery baru belum setara dengan jalur recovery command

`RecoverClientTamperRecord` membaca metadata mentah log dan mengirimnya ke POST `/recover`.
Agent lokal memang menyediakan endpoint itu, jadi ini bukan masalah endpoint hilang.
Namun jalur tersebut berbeda dari `/recover/:table/:record_id` yang memakai command ID,
expected-before hash, desired-state hash, policy dan readback evidence.

Jalur baru belum memanggil pemeriksaan trusted/latest reference sebelum write; metadata
envelope seperti `after`/`data_baru` tidak diekstrak di backend call tersebut. Client HTTP
juga masih fallback ke VerifyToken jika RecoveryToken kosong, sementara Agent configured
memakai token recovery terpisah. Backend menerima HTTP 200 tanpa mengecek status hasil
atau readback sebelum menandai record RECOVERED.

Rekomendasi: pertahankan tabel/endpoint baru mentor, tetapi gunakan trusted-reference
validator dan recovery command yang sudah ada sebagai executor. Jangan mengaktifkan
fallback token baca untuk operasi tulis.

### P2 — Scheduler Agent terpisah dari worker backend

Agent memiliki runner sendiri: `AGENT_AUDIT_ENABLED=false`, interval default 60 menit,
window 1440 menit, auto-recover default false. Runner memakai rolling event-time window;
report gagal dikirim hanya dicatat pada log, tanpa durable retry/outbox. Laporan HTTP
audit dikirim secara asynchronous. Workerstatus backend tidak merepresentasikan
heartbeat runner Agent tersebut.

Dampak: polling browser dan heartbeat Source Sweep tidak membuktikan runner outbound
Agent aktif. Event dengan waktu lama di luar window dan report gagal kirim perlu
mekanisme catch-up/retry tersendiri.

### P2 — Dokumentasi menyamakan dua jenis hash

`ClientDataTamperRecord.ExpectedHash` didokumentasikan sebagai `AuditLog.HashValue`.
Implementasi Agent `CompareState` justru membandingkan canonical operational-state hash
client dengan canonical state hash hasil ekstraksi metadata offchain. Audit leaf hash
log bukan operational-state hash tersebut.

Rekomendasi: beri nama/label expected state hash versus audit leaf hash secara eksplisit;
jangan membandingkan kedua algoritma hash sebagai nilai yang setara.

## Urutan integrasi yang disarankan

1. Pertahankan migrasi main beserta histori/worker monitoring lokal; konflik selesai.
2. Satukan validasi dan persistence Agent report + Source Sweep untuk insiden tabel baru
   dan histori per dimensi; tambahkan integration tests transaksi/tenant/konkurensi.
3. Adaptasikan recovery command yang tervalidasi untuk record tabel baru, termasuk
   envelope metadata, DELETE/missing row, readback dan token recovery terpisah.
4. Hubungkan client UI dengan `/dashboard/client-tampering` atau adapter kompatibel;
   tampilkan insiden Gateway dan client source dengan domain yang jelas.
5. Migrasikan legacy CLIENT_SOURCE melalui migrasi eksplisit setelah mapping relasi
   recovery disetujui; pertahankan insiden Gateway.
6. Uji pilot dengan DB/Fabric/Agent: source mismatch, proof failure, SKIPPED, report replay,
   persistence failure, dua instance, manual recovery dan report retry.

## Verifikasi yang dilakukan

- `go test ./...` backend: lulus.
- `go test ./...` Agent: lulus.
- `git diff --name-only --diff-filter=U`: kosong setelah resolusi.
- Database deployment, scheduler efektif, request recovery nyata dan migrasi data belum
  dijalankan. Screenshot menunjukkan tabel, bukan bukti writer/worker sedang aktif.

Tes yang ada belum membuktikan integrasi database lintas kedua jalur insiden. Lulusnya
suite tidak menghapus temuan kode di atas.
