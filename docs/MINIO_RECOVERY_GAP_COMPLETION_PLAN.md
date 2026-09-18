# Implement Plan Penyelesaian Tamper Recovery AuditChain

Dokumen ini adalah addendum implementasi untuk menutup gap recovery AuditChain.
Scope hanya backend Gateway, PostgreSQL AuditChain, MinIO, Hyperledger Fabric,
dan deployment server. Database operasional milik client tidak pernah dibaca
atau diubah oleh recovery.

## Status implementasi

| Komponen | Status | Catatan |
|---|---|---|
| Snapshot terenkripsi + version ID | Implemented | Ditulis melalui transactional outbox dan snapshot worker. |
| Checksum exact object saat recovery | Implemented | SHA-256 ciphertext dibandingkan dengan referensi request. |
| Hash snapshot + Merkle/Fabric preflight | Implemented | Dipakai oleh preflight dan execute; execute selalu validasi ulang. |
| Frozen recovery references | Implemented | Request menyimpan checksum, plaintext hash, anchor ID, dan expected root. |
| Tamper scanner | Implemented | Batch scanner memeriksa log ANCHORED secara berkala dan meng-cache pembacaan anchor Fabric per anchor dalam satu siklus. |
| API candidates/preflight | Implemented | Kontrak siap digunakan dashboard. |
| PostgreSQL anchoring gate | Configurable | Aktif setelah SNAPSHOT_REQUIRED_FOR_ANCHOR=true. |
| Object Lock COMPLIANCE | Configuration ready | Migrasi object lama harus dilakukan di server secara terkontrol. |
| Dashboard Recovery Center | Out of scope | Dikerjakan pada repository frontend terpisah. |

## Urutan validasi recovery

Sebelum row audit_logs diubah, service wajib:

1. Membaca object menggunakan snapshot_object_key dan snapshot_version_id.
2. Memastikan Version ID yang dikembalikan sama persis.
3. Membandingkan SHA-256 ciphertext dengan snapshot_checksum.
4. Mendekripsi envelope AES-256-GCM.
5. Memvalidasi schema, log_id, dan client_id.
6. Menghitung ulang SHA3-256 canonical audit log.
7. Membandingkan hasilnya dengan snapshot_plaintext_hash.
8. Mengambil Merkle Proof dari PostgreSQL.
9. Mengambil anchor menggunakan anchor_id dari request.
10. Membandingkan Merkle Root hasil rekonstruksi dengan Fabric.
11. Memulihkan row dalam transaksi PostgreSQL.
12. Membaca ulang row dan memastikan hash identik.

Jika satu langkah gagal, row tidak boleh diubah. Nilai tampered hanya disimpan
sebagai evidence terenkripsi setelah seluruh validasi snapshot berhasil.

Recovery hanya boleh memakai snapshot dengan log_id yang sama dengan incident.
Histori resource dapat ditampilkan sebagai informasi, tetapi tidak dapat dipakai
untuk menimpa event audit lain.

> Kolom referensi beku pada `recovery_requests` sengaja ditambahkan nullable pada
> level PostgreSQL agar `AutoMigrate` tidak gagal pada instalasi yang sudah
> memiliki request legacy. Request baru tetap fail-closed dan wajib mengisi
> seluruh referensi sebelum dapat dibuat.

## Scanner tamper

Feature flag dan default konfigurasi:

~~~text
TAMPER_SCANNER_ENABLED=false
TAMPER_SCAN_INTERVAL_SECONDS=300
TAMPER_SCAN_BATCH_SIZE=100
TAMPER_SCAN_CONCURRENCY=2
~~~

Scanner mengambil log berstatus ANCHORED yang paling lama diperiksa. Status
operasional disimpan pada audit_logs.integrity_status:

~~~text
NOT_CHECKED | VALID | TAMPERED | PENDING | UNREACHABLE
~~~

Scanner hanya memeriksa database Gateway dan Fabric. Scanner tidak memanggil
Agent atau database client. Saat hash/Merkle mismatch, scanner membuat satu
tamper_incident aktif secara idempoten. Saat Fabric tidak tersedia, status
menjadi UNREACHABLE dan tidak dibuat incident palsu.

Scanner tidak menjalankan recovery otomatis. Recovery selalu memerlukan request
dan approval admin.

## Kontrak API untuk dashboard

Semua endpoint berada di bawah /api/dashboard/recovery dan memerlukan JWT.
Endpoint approve, reject, dan execute memerlukan role admin.

Admin dapat memilih tenant secara eksplisit dengan query `client_id`, misalnya
`GET /api/dashboard/recovery/incidents?client_id=<client-id>`. Query ini hanya
dihormati untuk token ber-role `admin`; user biasa selalu dibatasi pada
`client_id` yang terdapat di tokennya.

### Kandidat recovery

~~~text
GET /api/dashboard/recovery/incidents/{incident_id}/candidates
~~~

Response sukses:

~~~json
{
  "data": [
    {
      "log_id": "1789651685834089911",
      "snapshot_object_key": "production/.../snapshot.snapshot",
      "snapshot_version_id": "version-id",
      "snapshot_checksum": "sha256-ciphertext",
      "snapshot_plaintext_hash": "sha3-audit-hash",
      "snapshot_verified_at": "2026-09-17T13:29:49Z",
      "eligible": true,
      "reason": ""
    }
  ]
}
~~~

### Preflight MinIO/Fabric

~~~text
POST /api/dashboard/recovery/incidents/{incident_id}/preflight
~~~

Preflight tidak mengubah PostgreSQL. Response valid:

~~~json
{
  "status": "VALID",
  "recoverable": true,
  "log_id": "1789651685834089911",
  "snapshot_hash": "sha3-audit-hash",
  "merkle_root": "merkle-root",
  "anchor_id": "anchor-id",
  "object_version_id": "version-id",
  "snapshot_preview": {
    "actor": "user-indah",
    "action": "UPDATE",
    "resource": "DATA_RM:123",
    "timestamp": "2026-09-17T13:00:00Z",
    "metadata": {
      "diagnosis": "sakit demam"
    }
  }
}
~~~

Execute tetap melakukan seluruh validasi ulang. Hasil preflight tidak menjadi
izin permanen apabila object, database, atau anchor berubah setelahnya.

Endpoint existing tetap dipakai:

~~~text
GET  /incidents
GET  /incidents/{id}
GET  /resources/{resource}/versions
GET  /requests
GET  /requests/{id}
POST /requests
POST /requests/{id}/approve
POST /requests/{id}/reject
POST /requests/{id}/execute
~~~

## Feature flag dan startup gate

Konfigurasi produksi setelah backlog snapshot sehat:

~~~text
SNAPSHOT_WRITER_ENABLED=true
SNAPSHOT_REQUIRED_FOR_ANCHOR=true
TAMPER_SCANNER_ENABLED=true
RECOVERY_ENABLED=true
MINIO_RETENTION_MODE=COMPLIANCE
MINIO_RETENTION_DAYS=30
~~~

Gateway gagal startup jika:

- anchoring gate aktif tanpa snapshot writer atau konfigurasi MinIO;
- anchoring gate aktif tanpa Fabric;
- recovery aktif tanpa MinIO, encryption key, snapshot builder, atau Fabric;
- scanner aktif tanpa Fabric.

Pada server, tambahkan flag tersebut ke `.env` yang sudah ada; jangan menimpa
file `.env` produksi dengan `.env.example` karena file itu berisi secret dan
endpoint lokal. Setelah baseline serta staging lulus, validasi konfigurasi dan
restart bertahap dengan `docker compose config --quiet` lalu
`docker compose up -d --build api-gateway`. Pastikan log startup menampilkan
tamper scanner aktif dan tidak ada error snapshot worker sebelum mengaktifkan
gate anchoring.

## Runbook MinIO COMPLIANCE

Jalankan pada maintenance window server:

1. Backup PostgreSQL dan inventarisasi semua object/version ID.
2. Pastikan bucket versioning dan Object Lock aktif.
3. Verifikasi waktu server/NTP.
4. Uji bucket staging dengan COMPLIANCE.
5. Uji update/delete menggunakan writer dan root; operasi harus ditolak selama retention.
6. Set default bucket production ke COMPLIANCE 30 hari.
7. Terapkan COMPLIANCE ke seluruh versi object lama.
8. Verifikasi mode COMPLIANCE dan retain-until dengan mc stat atau mc retention info.
9. Jangan menghapus atau mengganti bucket sebelum seluruh bukti inventaris dan validasi tersimpan.

COMPLIANCE bersifat irreversible sampai masa retention berakhir. Gunakan
staging/ephemeral bucket untuk smoke test agar object test tidak bercampur
dengan bucket recovery produksi.

## Timeline implementasi backend/server

Estimasi satu developer, satu hari kerja intensif, dengan rollout bertahap:

| Waktu | Aktivitas | Output |
|---|---|---|
| H+0–0:45 | Backup, baseline flags, inventaris MinIO/Fabric | Baseline tervalidasi |
| H+0:45–3:00 | Frozen references, checksum, shared recovery validation | Recovery fail-closed |
| H+3:00–4:30 | Candidates/preflight API | Kontrak dashboard siap |
| H+4:30–6:00 | Tamper scanner dan startup validation | Scanner siap dijalankan |
| H+6:00–7:30 | Unit/integration test dan build image | Backend lulus verifikasi |
| H+7:30–9:15 | Deploy flag nonaktif, smoke-test Compliance, migrasi retention | Storage terlindungi |
| H+9:15–10:00 | Rekonsiliasi snapshot dan anchoring gate | Tidak ada anchor tanpa snapshot |
| H+10:00–11:00 | E2E tamper → preflight → approval → recovery | Recovery server terbukti |
| H+11:00–12:00 | Monitoring, API handoff, dan runbook | Handoff frontend/server |

## Acceptance test

1. Buat audit event dan pastikan snapshot VERIFIED.
2. Pastikan event menjadi ANCHORED.
3. Ubah metadata langsung di PostgreSQL AuditChain.
4. Tunggu scanner menandai TAMPERED dan membuat incident.
5. Jalankan preflight dan pastikan snapshot MinIO cocok dengan Fabric.
6. Buat request, approve sebagai admin, lalu execute.
7. Pastikan metadata kembali ke snapshot asli.
8. Pastikan hash hasil recovery cocok dengan anchor Fabric.
9. Pastikan evidence tampered tersimpan terenkripsi.
10. Pastikan event RECOVERY baru dibuat dan masuk outbox snapshot.
11. Pastikan database client tidak pernah disentuh.

Failure test wajib meliputi checksum berbeda, Version ID salah, ciphertext rusak,
client/log ID berbeda, Merkle Proof salah, Fabric unavailable, request tanpa
approval, idempotency duplicate, akses non-admin, dan percobaan delete object
COMPLIANCE.

## Hard stop rollout

Jangan mengaktifkan RECOVERY_ENABLED, TAMPER_SCANNER_ENABLED, atau
SNAPSHOT_REQUIRED_FOR_ANCHOR jika backup gagal, Fabric tidak dapat dibaca,
exact MinIO version tidak dapat dibaca, backlog snapshot belum sehat, E2E gagal,
atau object COMPLIANCE masih dapat dihapus.

Dashboard/frontend berada di repository terpisah. Dokumen ini menjadi kontrak
backend untuk implementasi Recovery Center oleh developer frontend.
