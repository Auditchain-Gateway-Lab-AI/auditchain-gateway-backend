# Recovery API Contract

Semua endpoint menggunakan prefix `/api` dan JWT `Authorization: Bearer <token>`.
Tambahkan `?client_id=<client-id>` hanya untuk token admin global. User biasa
selalu dibatasi pada `client_id` di tokennya.

## Read dan preflight

```text
GET  /api/dashboard/recovery/incidents?status=OPEN
GET  /api/dashboard/recovery/incidents/:incident_id
GET  /api/dashboard/recovery/incidents/:incident_id/candidates
POST /api/dashboard/recovery/incidents/:incident_id/preflight
GET  /api/dashboard/recovery/resources/:resource/versions
GET  /api/dashboard/recovery/requests?status=PENDING_APPROVAL
GET  /api/dashboard/recovery/requests/:request_id
```

`candidates` hanya mengembalikan event dengan `log_id` yang sama dengan
incident. Kandidat legacy sebelum `RECOVERY_CUTOFF_AT` dikembalikan dengan:

```json
{
  "eligible": false,
  "reason": "legacy_recovery_out_of_scope"
}
```

Preflight tidak mengubah PostgreSQL. Status `VALID` berarti exact object
version, ciphertext checksum, AES-GCM, snapshot hash, Merkle proof, dan anchor
Fabric semuanya cocok.

## Request dan workflow admin

```text
POST /api/dashboard/recovery/requests
POST /api/dashboard/recovery/requests/:request_id/approve
POST /api/dashboard/recovery/requests/:request_id/reject
POST /api/dashboard/recovery/requests/:request_id/execute
```

Body request:

```json
{
  "incident_id": "incident-uuid",
  "selected_log_id": "exact-log-id",
  "reason": "alasan recovery",
  "idempotency_key": "client-generated-unique-key"
}
```

Approve, reject, dan execute hanya dapat dilakukan oleh role `admin`. Execute
selalu mengulang seluruh validasi snapshot; hasil preflight bukan izin permanen.

Recovery yang berhasil menghasilkan status `SUCCEEDED`, memulihkan target
AuditChain PostgreSQL, menyimpan bukti tampered terenkripsi, menutup incident,
dan membuat event `RECOVERY` baru. Database operasional client tidak pernah
diubah.

