# 🛡️ AuditChain Gateway

AuditChain Gateway adalah *middleware* dan API Gateway berskala *Enterprise* yang menerima, memproses, dan mengunci log audit dari berbagai sistem klien (rumah sakit/SIMRS, data geospasial, dsb.) secara *immutable* (tidak dapat diubah) ke dalam jaringan **Hyperledger Fabric Blockchain**.

Sistem ini menggunakan arsitektur **multi-tenant (SaaS)**, ingestion via **Change Data Capture (CDC)** per klien, struktur data **Merkle Tree**, dan verifikasi berlapis untuk memastikan integritas data (Anti-Tampering) dengan performa *high-throughput*.

---

## ✨ Fitur Utama

- **🔌 Multi-Client CDC Ingestion:** Setiap klien memiliki konfigurasi Kafka sendiri (`ClientKafkaConfig`) — bukan Kafka terpusat. Gateway melakukan *reconciliation* otomatis setiap 15 detik untuk spawn/stop consumer goroutine per klien.
- **🌳 Merkle Tree Aggregation:** Menghemat biaya dan ruang *ledger* Blockchain dengan mengelompokkan log menjadi *batch*, membangun *proof chain* lengkap dari leaf ke root (mendukung batch berapa pun besarnya, termasuk kasus leaf ganjil/self-pairing), dan hanya mengirim *Merkle Root* ke jaringan Hyperledger Fabric.
- **🔗 Cryptographic Hashing (SHA3-256):** Setiap log dihitung hash-nya dari 8 field kanonik (actor, action, resource, timestamp, source system, authorization context, metadata, log ID). Local chain (`prevHash`) sudah tidak lagi menjadi bagian dari formula hash.
- **🕵️ 4-Layer Verification Engine:**
  1. **Layer 1** — Eksistensi log di database
  2. **Layer 2** — Re-hashing lokal terhadap PostgreSQL
  3. **Layer 3** — Verifikasi live ke *Universal Agent* yang berjalan di premise klien (`POST /verify/<table>/<id>`), hanya dijalankan untuk log **terbaru** per resource
  4. **Layer 4** — Rekonstruksi Merkle Proof dan pencocokan terhadap ledger Hyperledger Fabric
- **📊 Dashboard API:** Statistik, riwayat transaksi dengan pagination sungguhan, inventaris resource, verifikasi per-log/per-resource/per-range waktu, dan status integritas granular (`valid` / `tampered` / `pending` / `unreachable`) dengan detail `chain_issues` (mis. `merkle_mismatch`, `client_mismatch:<log_id>`).
- **📚 Interaktif API Docs:** Terintegrasi dengan **Swagger UI** untuk pengujian dan dokumentasi endpoint.

---

## 🏗️ Arsitektur Sistem

### Alur Data — Jalur Kafka CDC (jalur utama yang aktif)

```
[Database Klien] → Debezium CDC → [Kafka Broker milik klien]
        → Kafka Consumer per-klien (Gateway)
        → Hash langsung (SHA3-256) → PostgreSQL Gateway (status: HASHED)
```

Setiap klien (`ClientKafkaConfig`) punya broker, topic prefix, PK field, dan group ID sendiri. Consumer goroutine berjalan dengan context cancel per klien dan melakukan *reconciliation* berkala terhadap perubahan konfigurasi.

### Alur Data — Jalur HTTP Ingestion (tersedia, belum di-mount ke router)

```
[Client App] → POST /api/logs (API Key) → Normalisasi field dinamis
        → Redis Queue → Pipeline Worker (Hasher → Aggregator → Fabric Anchoring)
```

> ⚠️ **Catatan:** Modul `ingestion` sudah lengkap (handler, service, repository) namun route-nya **belum terdaftar** di `internal/api/router.go`. Saat ini seluruh ingestion produksi berjalan lewat jalur Kafka CDC.

### Pipeline Background (Ticker 10 detik)

1. **Hasher Engine** — `status=RECEIVED` → hitung SHA3-256 → `HASHED`
2. **Aggregator Engine** — kumpulkan log `HASHED` (batch 10) → bangun Merkle Tree + simpan `MerkleProof` per leaf → `AGGREGATED`
3. **Fabric Anchoring** — kirim Merkle Root unik ke chaincode `StoreMerkleRoot` → `ANCHORED` + `blockchain_tx_id`

### Status Lifecycle Log

```
RECEIVED → HASHED → AGGREGATED → ANCHORED
```
> Log yang masuk lewat Kafka CDC langsung menjadi `HASHED` (skip `RECEIVED`), karena hash dihitung inline saat message diproses.

---

## 🛠️ Tech Stack

- **Bahasa Pemrograman:** Go (Golang) 1.25
- **Web Framework:** Gin Web Framework
- **Database:** PostgreSQL (dengan GORM)
- **Message Broker:** Redis (jalur HTTP ingestion) & Apache Kafka + Debezium (jalur CDC, per-klien)
- **Blockchain:** Hyperledger Fabric v2.4+ (Gateway SDK)
- **Kriptografi:** SHA3-256 (Keccak)
- **Dokumentasi API:** Swaggo / Swagger
- **Agent Verifikasi Klien:** Repo terpisah (`auditchain-agent`), Go, mendukung Oracle (`go-ora`) dan PostgreSQL

---

## 🔐 Autentikasi

Sistem menggunakan tiga skema kredensial yang independen satu sama lain:

| Skema | Header | Kegunaan |
|---|---|---|
| **API Key** | `x-api-key` / `api-key` | Klien → Gateway (ingestion log) |
| **JWT Bearer** | `Authorization: Bearer <token>` | Dashboard/Auditor (2 jam expiry) |
| **verify_token** | `Authorization: Bearer <verify_token>` (disimpan di `agent_configs`) | Gateway → Universal Agent (Layer 3) |

`api_key` dan `verify_token` **tidak boleh tertukar** — arah otentikasinya berlawanan.

---

## 🚀 Instalasi & Konfigurasi

### Prasyarat
- Go 1.25+
- PostgreSQL
- Redis
- Akses ke Kafka broker milik masing-masing klien (untuk jalur CDC)
- Akses ke Node Hyperledger Fabric (Certificate, Private Key, MSP ID)

### Setup Environment

Buat file `.env` di *root* direktori:

```env
# Server
PORT=8080

# Database
DB_DSN=postgres://postgres:password@localhost:5433/test_blockchain?sslmode=disable

# Redis
REDIS_HOST=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0

# JWT
JWT_SECRET=ganti-dengan-secret-yang-kuat

# Admin
ADMIN_SECRET=ganti-dengan-secret-admin

# Hyperledger Fabric Gateway
FABRIC_MSP_ID=Org1MSP
FABRIC_PEER_ENDPOINT=localhost:7051
FABRIC_TLS_CERT_PATH=./crypto-config/tls/ca.crt
FABRIC_CERT_PATH=./crypto-config/users/Admin@org1/msp/signcerts/cert.pem
FABRIC_KEY_PATH=./crypto-config/users/Admin@org1/msp/keystore/priv_key.pem
FABRIC_CHANNEL=audit-channel
FABRIC_CHAINCODE=audit-contract
```

### Menjalankan Aplikasi

```bash
# 1. Clone repository
git clone <repo-url>

# 2. Unduh dependencies
go mod tidy

# 3. (Opsional) Generate ulang dokumentasi Swagger
swag init -g main.go

# 4. Jalankan server
go run main.go
```

Aplikasi berjalan di `http://localhost:8080` (atau sesuai `PORT`).
Swagger UI tersedia di `http://localhost:8080/swagger/index.html`.

### Menjalankan via Docker

```bash
docker-compose up -d --build      # Build + jalankan gateway + PostgreSQL
docker-compose logs -f api-gateway
```

---

## 📡 API Endpoints Utama

| Method | Endpoint | Auth | Deskripsi |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/auth/register` | ❌ | Registrasi user baru ke suatu client |
| `POST` | `/api/auth/login` | ❌ | Login → JWT token |
| `POST` | `/api/admin/clients` | 🔑 Admin Secret | Registrasi tenant baru + generate API Key |
| `POST` | `/api/admin/kafka-config` | 🔑 Admin Secret | Registrasi konfigurasi Kafka per klien |
| `PATCH` | `/api/admin/kafka-config/:id/toggle` | 🔑 Admin Secret | Aktif/nonaktifkan stream Kafka (tanpa hapus data) |
| `DELETE` | `/api/admin/kafka-config/:id` | 🔑 Admin Secret | Soft-delete konfigurasi Kafka |
| `GET` | `/api/dashboard/stats` | 🔐 JWT | Statistik total/anchored/pending |
| `GET` | `/api/dashboard/logs` | 🔐 JWT | Log terbaru (pagination: `page`, `page_size`, filter `integrity_status`) |
| `GET` | `/api/dashboard/logs/by-resource/:resource` | 🔐 JWT | Riwayat log per resource |
| `GET` | `/api/dashboard/verify/:log_id` | 🔐 JWT | Verifikasi 4-Layer untuk satu log (on-demand) |
| `GET` | `/api/dashboard/verify-resource/:resource` | 🔐 JWT | Verifikasi seluruh riwayat satu resource |
| `GET` | `/api/dashboard/verify-range?from=&to=` | 🔐 JWT | Verifikasi batch log dalam rentang waktu |
| `GET` | `/api/dashboard/fabric/:anchor_id` | 🔐 JWT | Ambil data raw dari Fabric World State |
| `POST` | `/api/dashboard/verify-data` | 🔐 JWT | Verifikasi integritas data aktual vs audit trail |
| `GET` | `/api/dashboard/inventory` | 🔐 JWT | Daftar resource unik yang termonitor |
| `POST` | `/api/dashboard/agent/config` | 🔐 JWT | Registrasi/update konfigurasi Universal Agent klien |
| `GET` | `/api/dashboard/agent/ping` | 🔐 JWT | Cek konektivitas Agent klien |

> Catatan: dua endpoint `logs/by-resource` dan `verify-resource` direncanakan digabung menjadi satu endpoint, namun masih tertunda menunggu penyelesaian debugging konfigurasi Agent.

---

## 🕵️ Verifikasi 4-Layer — Detail

```
Layer 1 (DB Existence)     → Cek log ada di PostgreSQL
Layer 2 (Re-Hash)          → Hitung ulang SHA3-256, bandingkan dengan hash tersimpan
Layer 3 (Agent Source)     → Panggil Universal Agent klien, bandingkan field aktual
                              (HANYA dijalankan untuk log TERBARU per resource)
Layer 4 (Merkle/Blockchain)→ Rekonstruksi Merkle Root dari proof chain,
                              bandingkan dengan root di ledger Hyperledger Fabric
```

Jika terjadi ketidakcocokan, status `tampered` disertai `chain_issues`:
- `merkle_mismatch` — rekonstruksi Merkle root tidak cocok dengan Fabric ledger (istilah ini dipilih alih-alih "blockchain tampered" karena gateway tidak bisa membuktikan secara kriptografis apakah manipulasi terjadi di ledger Fabric atau di tabel proof lokal).
- `client_mismatch:<log_id>` — data live di klien (via Agent) sudah berbeda dari log terbaru resource tersebut.

---

## 🛡️ Threat Model & Keamanan

Sistem ini kebal terhadap berbagai jenis serangan pada level database:

- **Modifikasi data di DB Gateway (Layer 2):** Jika data lokal diubah, mesin Re-Hashing akan mendeteksi ketidaksesuaian hash.
- **Modifikasi data + Re-Hash (Layer 4):** Jika hash ditimpa juga, verifikasi Merkle Tree terhadap Fabric ledger akan rusak.
- **Modifikasi data di sisi klien (Layer 3):** Jika data di database klien berubah tanpa melalui AuditChain, Agent akan melaporkan `client_mismatch` saat dibandingkan dengan log terbaru.
- **Modifikasi/penghapusan seluruh DB Gateway:** Merkle Root yang sudah di-anchor di Hyperledger Fabric tidak bisa dipalsukan ulang tanpa terdeteksi saat rekonstruksi proof gagal cocok dengan ledger.

### ⚠️ Isu Keamanan Pra-Produksi (Diketahui, Didefer hingga sebelum go-live `auditchain.id`)

- CORS saat ini `AllowAllOrigins = true` — perlu di-whitelist sebelum produksi
- Validasi `JWT_SECRET` kosong belum diterapkan saat startup
- Beberapa endpoint admin lama berpotensi tanpa autentikasi penuh — sedang direview
- Kredensial hardcoded di beberapa `docker-compose.yml` (mis. `POSTGRES_PASSWORD`) — akan dipindah ke secret manager
- Rencana deployment: `auditchain.id` (dashboard) + `api.auditchain.id` (gateway API) via Cloudflare Tunnel + Caddy reverse proxy

---

## 🧹 Dead Code / Item Teknis Tertunda

- `internal/modules/ingestion` — route belum terdaftar di router utama (lihat bagian Arsitektur)
- `kafkaconsumer/verifier.go` — tidak lagi digunakan, sudah ditandai untuk dihapus
- Cron job hybrid untuk verifikasi otomatis log yang baru ter-anchor — didesain namun menunggu persetujuan manager sebelum diaktifkan
- Frontend dashboard (`src/App.js`) perlu disesuaikan untuk membaca `res.data.data` sesuai response shape baru `GetRecentLogs` (`{"data": [...], "pagination": {...}, "note"?: "..."}`)

---

## 🧩 Ekosistem Terkait

| Repository | Fungsi |
|---|---|
| `auditchain-gateway-backend` (repo ini) | API Gateway, verifikasi, blockchain anchoring |
| `auditchain-agent` | Agent yang dipasang di sisi klien untuk verifikasi Layer 3 (mendukung Oracle & PostgreSQL) |
| Dashboard Frontend (React) | UI Auditor & Admin Panel |
| Landing Page (React + Vite) | Halaman promosi/pengenalan produk |

---

## 📄 Lisensi & Kontak

Internal project — hubungi tim pengembang untuk detail lisensi dan akses lebih lanjut.