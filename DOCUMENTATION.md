# Dokumentasi API Admin Panel — AuditChain Gateway Backend

Dokumentasi ini menjelaskan endpoint API **Admin Panel** pada AuditChain Gateway Backend yang digunakan untuk manajemen klien (perusahaan/tenant), konfigurasi stream Kafka, dan ringkasan dashboard.

---

## 1. Ringkasan Endpoint

| Method | Endpoint | Deskripsi |
| :--- | :--- | :--- |
| `POST` | `/api/admin/clients` | Mendaftarkan klien/perusahaan baru |
| `GET` | `/api/admin/clients` | Mengambil daftar semua klien terdaftar |
| `POST` | `/api/admin/kafka-config` | Mendaftarkan konfigurasi stream Kafka untuk klien |
| `GET` | `/api/admin/kafka-configs` | Mengambil daftar semua konfigurasi Kafka |
| `PATCH` | `/api/admin/kafka-config/:id/toggle` | Mengaktifkan/Menonaktifkan stream Kafka |
| `GET` | `/api/admin/summary` | Mengambil statistik ringkasan dashboard |

---

## 2. Skema Database

### Tabel `clients`

| Kolom | Tipe Data | Keterangan |
| :--- | :--- | :--- |
| `id` | `varchar(36)` | Primary Key (UUID, auto-generate) |
| `company_name` | `varchar(100)` | Nama perusahaan/klien (wajib diisi) |
| `api_key_prefix` | `varchar(20)` | Prefix API Key yang disamarkan (contoh: `ak_live_38`) |
| `api_key_hash` | `varchar(255)` | Hash SHA-256 dari API Key (tidak ditampilkan di respon) |
| `status` | `varchar(20)` | Status klien (`active` / `inactive`), default: `active` |
| `actor_field` | `varchar(100)` | Mapping field kustom untuk aktor/pengguna |
| `fallback_actor_field` | `varchar(100)` | Mapping fallback jika actor_field bernilai null |
| `action_field` | `varchar(100)` | Mapping field kustom untuk aksi/operasi |
| `resource_field` | `varchar(100)` | Mapping field kustom untuk resource/tabel |
| `created_at` | `timestamptz` | Tanggal pembuatan |
| `updated_at` | `timestamptz` | Tanggal pembaruan terakhir |
| `deleted_at` | `timestamptz` | Soft delete (tidak ditampilkan di respon) |

### Tabel `client_kafka_configs`

| Kolom | Tipe Data | Keterangan |
| :--- | :--- | :--- |
| `id` | `varchar(36)` | Primary Key (UUID, auto-generate) |
| `client_id` | `varchar(36)` | Foreign Key ke tabel `clients` (unique, 1 klien = 1 config) |
| `topic_prefix` | `varchar(100)` | Prefix topic Kafka (contoh: `morbis_simrs.`) |
| `kafka_brokers` | `varchar(255)` | Alamat broker Kafka (contoh: `100.87.214.4:29092`) |
| `source_system` | `varchar(100)` | Nama sistem sumber data (contoh: `SIMRS-MORBIS`) |
| `actor_field` | `varchar(100)` | Field aktor di payload Kafka, default: `__user_name` |
| `pk_field` | `varchar(100)` | Field primary key di payload Kafka, default: `ID` |
| `is_active` | `boolean` | Status keaktifan stream, default: `true` |
| `created_at` | `timestamptz` | Tanggal pembuatan |
| `updated_at` | `timestamptz` | Tanggal pembaruan terakhir |
| `deleted_at` | `timestamptz` | Soft delete (tidak ditampilkan di respon) |

---

## 3. Detail Endpoint

### A. Mendaftarkan Klien Baru (`POST /api/admin/clients`)

Endpoint ini digunakan oleh admin untuk mendaftarkan klien/perusahaan baru ke dalam sistem AuditChain Gateway. Sistem akan otomatis men-generate API Key unik untuk klien tersebut.

* **Method**: `POST`
* **URL**: `http://localhost:8080/api/admin/clients`
* **Headers**:
  ```
  Content-Type: application/json
  ```

* **Request Body**:
  ```json
  {
    "company_name": "PT Karya Bangsa",
    "status": "active",
    "actor_field": "app_user",
    "fallback_actor_field": "db_user",
    "action_field": "operasi",
    "resource_field": "tabel"
  }
  ```

  | Field | Tipe | Wajib | Keterangan |
  | :--- | :--- | :--- | :--- |
  | `company_name` | string | ✅ Ya | Nama perusahaan klien |
  | `status` | string | Tidak | Status klien, default: `active` |
  | `actor_field` | string | Tidak | Mapping field aktor |
  | `fallback_actor_field` | string | Tidak | Fallback jika actor_field null |
  | `action_field` | string | Tidak | Mapping field aksi |
  | `resource_field` | string | Tidak | Mapping field resource |

* **Response Sukses (`201 Created`)**:
  ```json
  {
    "message": "Klien / Perusahaan SaaS berhasil didaftarkan",
    "client_id": "a1b2c3d4-e5f6-7890-1234-56789abcdef0",
    "api_key": "ak_live_abcdef1234567890abcdef1234567890abcdef1234567890abcdef12345678",
    "field_mapping": {
      "actor_field": "app_user",
      "fallback_actor_field": "db_user",
      "action_field": "operasi",
      "resource_field": "tabel"
    }
  }
  ```

  > ⚠️ **Penting**: Nilai `api_key` hanya ditampilkan **satu kali** saat pendaftaran berhasil. Pastikan untuk menyimpannya dengan aman.

---

### B. Mengambil Daftar Klien (`GET /api/admin/clients`)

Endpoint ini digunakan untuk menampilkan semua klien yang terdaftar di sistem.

* **Method**: `GET`
* **URL**: `http://localhost:8080/api/admin/clients`

* **Response Sukses (`200 OK`)**:
  ```json
  [
    {
      "id": "6ab9ce23-f8c7-423a-9d39-ca08fdc7989f",
      "company_name": "PT Karya Bangsa 2",
      "api_key_prefix": "ak_live_38",
      "status": "active",
      "actor_field": "",
      "fallback_actor_field": "",
      "action_field": "",
      "resource_field": "",
      "created_at": "2026-04-22T09:50:42.448783+07:00",
      "updated_at": "2026-04-22T09:50:42.448783+07:00"
    },
    {
      "id": "182826b6-fa72-4085-98d3-d2d607ac37b8",
      "company_name": "SIMRS Dummy 2",
      "api_key_prefix": "ak_live_ff3bf111",
      "status": "active",
      "actor_field": "app_user",
      "fallback_actor_field": "db_user",
      "action_field": "operasi",
      "resource_field": "tabel",
      "created_at": "2026-05-25T16:52:01.365794+07:00",
      "updated_at": "2026-05-25T16:52:01.365794+07:00"
    }
  ]
  ```

---

### C. Mendaftarkan Konfigurasi Kafka (`POST /api/admin/kafka-config`)

Endpoint ini digunakan untuk mendaftarkan konfigurasi stream Kafka baru untuk klien tertentu. Setiap klien hanya boleh memiliki **satu** konfigurasi Kafka.

* **Method**: `POST`
* **URL**: `http://localhost:8080/api/admin/kafka-config`
* **Headers**:
  ```
  Content-Type: application/json
  ```

* **Request Body**:
  ```json
  {
    "client_id": "6ab9ce23-f8c7-423a-9d39-ca08fdc7989f",
    "topic_prefix": "morbis_simrs.",
    "kafka_brokers": "100.87.214.4:29092",
    "source_system": "SIMRS-MORBIS",
    "actor_field": "__user_name",
    "pk_field": "ID"
  }
  ```

  | Field | Tipe | Wajib | Keterangan |
  | :--- | :--- | :--- | :--- |
  | `client_id` | string | ✅ Ya | UUID klien dari tabel `clients` |
  | `topic_prefix` | string | ✅ Ya | Prefix topic Kafka |
  | `kafka_brokers` | string | ✅ Ya | Alamat broker Kafka |
  | `source_system` | string | ✅ Ya | Nama sistem sumber |
  | `actor_field` | string | Tidak | Field aktor, default: `__user_name` |
  | `pk_field` | string | Tidak | Field primary key, default: `ID` |

* **Response Sukses (`201 Created`)**:
  ```json
  {
    "message": "Kafka config berhasil didaftarkan",
    "id": "e164415b-d1bf-45e3-a9ce-9efe52118855",
    "topic_prefix": "morbis_simrs.",
    "kafka_brokers": "100.87.214.4:29092"
  }
  ```

* **Response Error — Klien Sudah Memiliki Config (`500`)**:
  ```json
  {
    "error": "Gagal simpan kafka config"
  }
  ```
  > Ini terjadi jika `client_id` yang dikirimkan sudah memiliki konfigurasi Kafka di database (constraint unique).

---

### D. Mengambil Daftar Konfigurasi Kafka (`GET /api/admin/kafka-configs`)

Endpoint ini digunakan untuk menampilkan semua konfigurasi stream Kafka beserta nama perusahaan klien terkait.

* **Method**: `GET`
* **URL**: `http://localhost:8080/api/admin/kafka-configs`

* **Response Sukses (`200 OK`)**:
  ```json
  [
    {
      "id": "e164415b-d1bf-45e3-a9ce-9efe52118855",
      "client_id": "67e493e9-f7a1-441a-8229-f688cb876fd2",
      "topic_prefix": "morbis_simrs.",
      "kafka_brokers": "100.87.214.4:29092",
      "source_system": "SIMRS-MORBIS",
      "actor_field": "__user_name",
      "pk_field": "ID",
      "is_active": true,
      "created_at": "2026-06-26T10:33:02.72822+07:00",
      "updated_at": "2026-06-26T10:33:02.72822+07:00",
      "company_name": "SIMRS Morbis 1"
    },
    {
      "id": "4d863dc3-fa46-4f40-aa1c-bd8320a13a2d",
      "client_id": "4b176047-fdc5-4c74-9478-aeb5bcec0b2d",
      "topic_prefix": "satu_peta.",
      "kafka_brokers": "100.115.123.33:9092",
      "source_system": "SATU-PETA",
      "actor_field": "__user_name",
      "pk_field": "_id",
      "is_active": true,
      "created_at": "2026-07-01T16:33:53.275774+07:00",
      "updated_at": "2026-07-01T16:33:53.275774+07:00",
      "company_name": "Satu Peta Debezium"
    }
  ]
  ```

---

### E. Toggle Aktifkan/Nonaktifkan Stream Kafka (`PATCH /api/admin/kafka-config/:id/toggle`)

Endpoint ini digunakan untuk mengaktifkan atau menonaktifkan stream Kafka tertentu. Statusnya akan di-toggle otomatis (jika `true` menjadi `false`, dan sebaliknya).

* **Method**: `PATCH`
* **URL**: `http://localhost:8080/api/admin/kafka-config/{id_kafka_config}/toggle`
* **Contoh**: `http://localhost:8080/api/admin/kafka-config/e164415b-d1bf-45e3-a9ce-9efe52118855/toggle`

* **Response Sukses (`200 OK`)**:
  ```json
  {
    "message": "Status konfigurasi Kafka berhasil diperbarui",
    "id": "e164415b-d1bf-45e3-a9ce-9efe52118855",
    "client_id": "67e493e9-f7a1-441a-8229-f688cb876fd2",
    "kafka_brokers": "100.87.214.4:29092",
    "is_active": false
  }
  ```

* **Response Error — Config Tidak Ditemukan (`404`)**:
  ```json
  {
    "error": "Konfigurasi Kafka tidak ditemukan"
  }
  ```

---

### F. Ringkasan Dashboard (`GET /api/admin/summary`)

Endpoint ini digunakan untuk menampilkan statistik ringkasan di halaman utama Admin Panel, yaitu total klien terdaftar dan jumlah stream Kafka yang aktif.

* **Method**: `GET`
* **URL**: `http://localhost:8080/api/admin/summary`

* **Response Sukses (`200 OK`)**:
  ```json
  {
    "total_clients": 7,
    "active_streams": 2
  }
  ```

  | Field | Tipe | Keterangan |
  | :--- | :--- | :--- |
  | `total_clients` | integer | Jumlah total klien yang terdaftar di tabel `clients` |
  | `active_streams` | integer | Jumlah konfigurasi Kafka yang statusnya `is_active = true` |

---

## 4. Struktur File Kode Sumber

```
internal/
├── models/
│   ├── client.go             # Model database tabel `clients`
│   └── client_kafka.go       # Model database tabel `client_kafka_configs`
├── modules/
│   └── client/
│       ├── router.go         # Registrasi rute endpoint Admin
│       ├── handler.go        # Handler HTTP (request/response)
│       ├── service.go        # Logika bisnis (generate API key, mapping, summary)
│       └── repository.go     # Akses database (CRUD operations)
└── middleware/
    └── jwt.go                # Middleware autentikasi (JWTAuth, AdminAuth)
```

---

## 5. Cara Menjalankan

```bash
# 1. Install & tidy dependency Go
go mod tidy

# 2. Jalankan Server
go run main.go
```

Server akan berjalan di `http://localhost:8080` (default, sesuai konfigurasi `PORT` di `.env`).
