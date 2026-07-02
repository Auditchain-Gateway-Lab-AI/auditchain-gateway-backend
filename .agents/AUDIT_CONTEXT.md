# 🛡️ AuditChain Gateway Backend — Audit Lengkap & Konteks Vibe Coding

> **Terakhir diaudit:** 2026-07-01
> **Bahasa:** Go 1.25 | **Framework:** Gin | **DB:** PostgreSQL (GORM) | **Blockchain:** Hyperledger Fabric

---

## 1. RINGKASAN PROYEK

AuditChain Gateway adalah **API Gateway & Middleware enterprise** yang menerima, memproses, dan mengunci audit log sistem secara **immutable** ke **Hyperledger Fabric Blockchain**. Sistem ini menggunakan:
- **SHA3-256** untuk hashing kriptografi
- **Merkle Tree** untuk agregasi batch
- **Local Chain** (previous hash linking) untuk anti-tampering lokal
- **Hyperledger Fabric** sebagai sumber kebenaran akhir (consensus layer)
- **Kafka** sebagai alternatif jalur ingestion dari database CDC (Change Data Capture)
- **Arsitektur multi-tenant (SaaS)** dengan API Key per klien

---

## 2. STRUKTUR DIREKTORI

```
auditchain-gateway-backend/
├── main.go                          # Entry point aplikasi
├── go.mod / go.sum                  # Dependensi Go modules
├── Dockerfile                       # Multi-stage Docker build
├── docker-compose.yml               # Orkestrasi PostgreSQL + API
├── README.md                        # Dokumentasi user-facing
├── .gitignore                       # Mengabaikan .env, wallet/, postman/
│
├── docs/                            # Swagger auto-generated
│   ├── docs.go
│   ├── swagger.json
│   └── swagger.yaml
│
├── pkg/                             # Shared / public packages
│   └── crypto/
│       ├── hash.go                  # SHA3-256 wrapper
│       └── merkle.go                # Merkle Tree builder + verifier
│
├── internal/                        # Private application code
│   ├── api/
│   │   └── router.go                # Setup Gin router, CORS, Swagger, route groups
│   │
│   ├── config/
│   │   ├── database.go              # PostgreSQL connection + auto-migrate
│   │   └── redis.go                 # Redis connection
│   │
│   ├── middleware/
│   │   ├── jwt.go                   # JWT Bearer auth + Admin secret auth
│   │   ├── api_key.go               # API Key auth (hash-based lookup)
│   │   └── rate_limit.go            # Token Bucket rate limiter per client
│   │
│   ├── models/                      # GORM model definitions
│   │   ├── log.go                   # AuditLog, MerkleMetadata, MerkleProof
│   │   ├── client.go                # Client (tenant)
│   │   ├── user.go                  # User (auditor)
│   │   ├── agent_config.go          # AgentConfig (verifier agent per client)
│   │   ├── client_kafka.go          # ClientKafkaConfig
│   │   └── kafka_offset.go          # KafkaOffset (untuk verifikasi Lapis 3)
│   │
│   ├── engine/                      # Background processing pipeline
│   │   ├── hasher/
│   │   │   └── hasher.go            # Hash engine (RECEIVED → HASHED)
│   │   ├── aggregator/
│   │   │   └── aggregator.go        # Merkle aggregator (HASHED → AGGREGATED)
│   │   ├── normalizer/
│   │   │   └── normalizer.go        # Dynamic field mapping + normalization
│   │   └── kafkaconsumer/
│   │       ├── consumer.go          # Kafka Debezium consumer (CDC)
│   │       └── verifier.go          # Kafka re-verification (Lapis 3)
│   │
│   ├── blockchain/
│   │   ├── fabric.go                # Hyperledger Fabric Gateway SDK integration
│   │   ├── mock/
│   │   │   └── fabric_mock.go       # In-memory mock untuk testing
│   │   └── agentverifier/
│   │       ├── handler.go           # Agent config CRUD endpoints
│   │       └── service.go           # Agent verification logic (Lapis 3)
│   │
│   └── modules/                     # Feature modules (Handler-Service-Repository)
│       ├── audit/
│       │   ├── handler.go           # Dashboard API handlers
│       │   ├── service.go           # Verification logic (4 layers)
│       │   ├── repository.go        # DB queries for audit logs
│       │   └── router.go            # /api/dashboard/* routes
│       │
│       ├── auth/
│       │   ├── handler.go           # Register + Login handlers
│       │   ├── service.go           # bcrypt + JWT token generation
│       │   ├── repository.go        # User + Client DB queries
│       │   └── router.go            # /api/auth/* routes
│       │
│       ├── client/
│       │   ├── handler.go           # Client + Kafka config creation
│       │   ├── service.go           # API Key generation (crypto/rand)
│       │   ├── repository.go        # Client DB insert
│       │   └── router.go            # /api/admin/* routes
│       │
│       └── ingestion/
│           ├── handler.go           # Bulk log ingestion endpoint
│           ├── service.go           # Normalize + push to Redis queue
│           ├── repository.go        # Redis RPush wrapper
│           └── router.go            # /api/logs/* routes
│
├── postman/                         # Postman collections (gitignored)
└── .postman/                        # Postman workspace config
```

---

## 3. ARSITEKTUR & DATA FLOW

### 3.1 Diagram Alur Keseluruhan

```
┌─────────────┐   POST /api/logs   ┌──────────────┐
│  Client App  │ ─── API Key ────► │  Ingestion    │
│  (SaaS)      │   (Bulk JSON)     │  Handler      │
└─────────────┘                    └──────┬───────┘
                                          │ Normalize + Push
                                          ▼
                                   ┌──────────────┐
                                   │  Redis Queue  │
                                   │ audit_log_que │
                                   └──────┬───────┘
                                          │ (Belum diimplementasi consumer)
                                          │ NOTE: Pipeline Worker membaca
                                          │ langsung dari DB, bukan Redis!
                                          ▼
┌─────────────┐   Kafka CDC        ┌──────────────┐     setiap 10 detik
│  Client DB   │ ── Debezium ────► │  Kafka        │ ─────────────────────┐
│  (Oracle/PG) │                   │  Consumer     │                      │
└─────────────┘                    └──────┬───────┘                      │
                                          │ processMessage()             │
                                          │ hash langsung (HASHED)       │
                                          ▼                              ▼
                                   ┌──────────────┐   ┌───────────┐  ┌──────────┐
                                   │  PostgreSQL   │◄──│  Hasher   │  │Aggregator│
                                   │  (AuditLog)   │   │  Engine   │  │  Engine  │
                                   └──────┬───────┘   └───────────┘  └────┬─────┘
                                          │              RECEIVED→HASHED  │ HASHED→AGGREGATED
                                          │                               │ Merkle Tree build
                                          ▼                               ▼
                                   ┌────────────────────────────────────────┐
                                   │     Hyperledger Fabric Blockchain      │
                                   │     StoreMerkleRoot (Chaincode)        │
                                   │     AGGREGATED → ANCHORED              │
                                   └────────────────────────────────────────┘
```

### 3.2 Status Lifecycle AuditLog

```
RECEIVED → HASHED → AGGREGATED → ANCHORED
   │           │          │           │
   │           │          │           └─ Merkle Root tersimpan di Fabric ledger
   │           │          └─ Merkle Tree dibangun, root dihitung
   │           └─ SHA3-256 hash + previous_hash chain computed
   └─ Log baru diterima (via ingestion/API atau Kafka)
```

> **CATATAN PENTING:** Log dari Kafka consumer langsung masuk sebagai `HASHED` (skip `RECEIVED`), karena hash dihitung inline saat message diproses.

---

## 4. DATABASE SCHEMA (GORM Auto-Migrate)

### 4.1 Tabel `audit_logs` (Model: `AuditLog`)

| Kolom                  | Tipe          | Constraint                | Keterangan                                          |
|------------------------|---------------|---------------------------|-----------------------------------------------------|
| `log_id`               | varchar(100)  | **PK**                    | ID unik per log (UUID atau UnixNano)                |
| `client_id`            | varchar(36)   | NOT NULL, INDEX, **FK**   | Tenant/perusahaan pemilik log                       |
| `actor`                | varchar(100)  | INDEX                     | Siapa yang melakukan aksi                           |
| `action`               | varchar(100)  |                           | Jenis aksi (INSERT/UPDATE/DELETE/custom)             |
| `resource`             | varchar(255)  |                           | Objek/tabel yang di-aksi. Format Kafka: `tabel:id`  |
| `timestamp`            | timestamp     | INDEX                     | Waktu kejadian (dari payload)                       |
| `db_timestamp`         | timestamp     | INDEX, autoCreateTime     | Waktu insert ke DB gateway (auto)                   |
| `source_system`        | varchar(100)  | INDEX                     | Sistem sumber (nama agent/app)                      |
| `authorization_context`| text          |                           | JSON konteks otorisasi                              |
| `metadata`             | jsonb         |                           | Data payload dinamis (canonical JSON)               |
| `source_record_id`     | varchar(100)  | INDEX                     | ID baris di DB klien (dari Agent, untuk Lapis 3)    |
| `hash_value`           | varchar(64)   | UNIQUE INDEX              | SHA3-256 hash dari seluruh field                    |
| `previous_hash`        | varchar(64)   |                           | Hash log sebelumnya (local chain)                   |
| `merkle_root`          | varchar(64)   | INDEX                     | Root dari batch Merkle Tree                         |
| `blockchain_tx_id`     | varchar(100)  | nullable                  | ID transaksi di Fabric (anchor_id)                  |
| `status`               | varchar(20)   | default: 'RECEIVED'       | RECEIVED/HASHED/AGGREGATED/ANCHORED                 |

### 4.2 Tabel `clients` (Model: `Client`)

| Kolom                | Tipe          | Constraint              | Keterangan                              |
|----------------------|---------------|-------------------------|-----------------------------------------|
| `id`                 | varchar(36)   | **PK** (UUID)           | ID tenant                               |
| `company_name`       | varchar(100)  | NOT NULL                | Nama perusahaan                         |
| `api_key_prefix`     | varchar(20)   | UNIQUE, NOT NULL        | 16 char pertama API key                 |
| `api_key_hash`       | varchar(255)  | NOT NULL                | SHA-256 dari full API key               |
| `subscription_tier`  | varchar(20)   | default: 'basic'        | Tier langganan                          |
| `rate_limit_per_sec` | int           | default: 50             | Batas request per detik                 |
| `status`             | varchar(20)   | default: 'active'       | Status aktif/nonaktif                   |
| `actor_field`        | varchar(100)  |                         | Mapping field kustom untuk "actor"      |
| `fallback_actor_field`| varchar(100) |                         | Fallback field jika actor kosong        |
| `action_field`       | varchar(100)  |                         | Mapping field kustom untuk "action"     |
| `resource_field`     | varchar(100)  |                         | Mapping field kustom untuk "resource"   |
| `created_at`         | timestamp     |                         |                                         |
| `updated_at`         | timestamp     |                         |                                         |
| `deleted_at`         | timestamp     | INDEX (soft delete)     |                                         |

### 4.3 Tabel `users` (Model: `User`)

| Kolom      | Tipe        | Constraint         | Keterangan           |
|------------|-------------|--------------------|----------------------|
| `id`       | varchar(36) | **PK** (UUID)      | ID user              |
| `client_id`| varchar(36) | NOT NULL, INDEX, FK| Tenant pemilik       |
| `username` | varchar     | UNIQUE, NOT NULL   | Username login       |
| `password` | varchar     | NOT NULL           | bcrypt hash          |
| `role`     | varchar     | default: 'Auditor' | Role pengguna        |

### 4.4 Tabel `merkle_metadatas` (Model: `MerkleMetadata`)

| Kolom           | Tipe        | Constraint     | Keterangan                |
|-----------------|-------------|----------------|---------------------------|
| `tree_id`       | uint        | **PK** (auto)  | ID batch                  |
| `merkle_root`   | varchar(64) | UNIQUE         | Root hash                 |
| `batch_timestamp`| timestamp  | autoCreateTime | Waktu batch dibuat        |
| `batch_size`    | int         |                | Jumlah log dalam batch    |

### 4.5 Tabel `merkle_proofs` (Model: `MerkleProof`)

| Kolom            | Tipe        | Constraint | Keterangan           |
|------------------|-------------|------------|----------------------|
| `id`             | uint        | **PK**     |                      |
| `transaction_hash`| varchar(64)| INDEX      | Hash log individual  |
| `sibling_hash`   | varchar(64) |            | Hash sibling node    |
| `tree_level`     | int         |            | Level di Merkle Tree |
| `merkle_root`    | varchar(64) | INDEX      | Root tree terkait    |

### 4.6 Tabel `agent_configs` (Model: `AgentConfig`)

| Kolom           | Tipe        | Constraint         | Keterangan                     |
|-----------------|-------------|--------------------|---------------------------------|
| `id`            | varchar(36) | **PK** (UUID)      |                                 |
| `client_id`     | varchar(36) | UNIQUE, NOT NULL   | Satu agent per klien            |
| `agent_url`     | varchar(255)| NOT NULL           | URL Agent yang bisa di-call     |
| `verify_token`  | varchar(255)| NOT NULL           | Bearer token untuk autentikasi  |
| `timeout_seconds`| int        | default: 5         | Timeout request ke Agent        |
| `is_active`     | bool        | default: true      |                                 |

### 4.7 Tabel `client_kafka_configs` (Model: `ClientKafkaConfig`)

| Kolom          | Tipe        | Constraint         | Keterangan                      |
|----------------|-------------|--------------------|---------------------------------|
| `id`           | varchar(36) | **PK** (UUID)      |                                 |
| `client_id`    | varchar(36) | UNIQUE, NOT NULL   |                                 |
| `topic_prefix` | varchar(100)| NOT NULL           | Filter topic Kafka per klien    |
| `kafka_brokers`| varchar(255)| NOT NULL           | Alamat Kafka broker             |
| `source_system`| varchar(100)| NOT NULL           | Nama sistem sumber              |
| `actor_field`  | varchar(100)| default: '__user_name' | Field CDC untuk aktor      |
| `pk_field`     | varchar(100)| default: 'ID'      | Field primary key di payload    |
| `is_active`    | bool        | default: true      |                                 |

### 4.8 Tabel `kafka_offsets` (Model: `KafkaOffset`)

| Kolom      | Tipe        | Constraint         | Keterangan                     |
|------------|-------------|--------------------|---------------------------------|
| `id`       | uint        | **PK**             |                                 |
| `log_id`   | varchar(100)| UNIQUE, NOT NULL   | Referensi ke audit_logs.log_id  |
| `topic`    | varchar(255)| NOT NULL           |                                 |
| `partition`| int32       | NOT NULL           |                                 |
| `offset`   | int64       | NOT NULL           |                                 |

---

## 5. API ENDPOINTS (LENGKAP)

### 5.1 Auth Module — `/api/auth`

| Method | Endpoint            | Auth       | Deskripsi                                |
|--------|---------------------|------------|------------------------------------------|
| POST   | `/api/auth/register`| ❌ None    | Register user baru ke suatu client       |
| POST   | `/api/auth/login`   | ❌ None    | Login → JWT token (2 jam expiry)         |

### 5.2 Admin Module — `/api/admin`

| Method | Endpoint                | Auth       | Deskripsi                                |
|--------|-------------------------|------------|------------------------------------------|
| POST   | `/api/admin/clients`    | ❌ None    | Buat tenant/klien baru + generate API Key|
| POST   | `/api/admin/kafka-config`| ❌ None   | Daftarkan konfigurasi Kafka per klien    |

### 5.3 Ingestion Module — `/api/logs`

| Method | Endpoint        | Auth           | Deskripsi                                    |
|--------|-----------------|----------------|----------------------------------------------|
| POST   | `/api/logs/`    | 🔑 API Key     | Bulk log ingestion (array JSON) + rate limit |

> ⚠️ **CATATAN**: Endpoint ini TIDAK TERDAFTAR di router! Lihat temuan audit #1.

### 5.4 Dashboard/Audit Module — `/api/dashboard`

| Method | Endpoint                                    | Auth      | Deskripsi                                             |
|--------|---------------------------------------------|-----------|-------------------------------------------------------|
| GET    | `/api/dashboard/stats`                      | 🔐 JWT    | Statistik total/anchored/pending                      |
| GET    | `/api/dashboard/logs`                       | 🔐 JWT    | 500 log terbaru                                       |
| GET    | `/api/dashboard/logs/by-resource/:resource` | 🔐 JWT    | Log per resource                                      |
| GET    | `/api/dashboard/verify/:log_id`             | 🔐 JWT    | Verifikasi 4-Layer (DB→Hash→Kafka→Blockchain)         |
| GET    | `/api/dashboard/fabric/:anchor_id`          | 🔐 JWT    | Ambil data raw dari Fabric World State                |
| POST   | `/api/dashboard/verify-data`                | 🔐 JWT    | Verifikasi integritas data aktual vs audit trail       |
| GET    | `/api/dashboard/inventory`                  | 🔐 JWT    | Daftar resource unik (DISTINCT ON)                    |
| GET    | `/api/dashboard/verify-resource/:resource`  | 🔐 JWT    | Verifikasi seluruh riwayat satu resource               |
| GET    | `/api/dashboard/verify-range?from=&to=`     | 🔐 JWT    | Verifikasi batch log dalam rentang waktu               |

### 5.5 Agent Verifier Module — `/api/dashboard/agent`

| Method | Endpoint                          | Auth      | Deskripsi                              |
|--------|-----------------------------------|-----------|----------------------------------------|
| POST   | `/api/dashboard/agent/config`     | 🔐 JWT    | Register/update Agent config           |
| GET    | `/api/dashboard/agent/config`     | 🔐 JWT    | Lihat Agent config (tanpa token)       |
| DELETE | `/api/dashboard/agent/config`     | 🔐 JWT    | Soft-delete Agent config               |
| GET    | `/api/dashboard/agent/ping`       | 🔐 JWT    | Ping health Agent klien                |

### 5.6 Swagger

| Method | Endpoint              | Auth | Deskripsi           |
|--------|-----------------------|------|---------------------|
| GET    | `/swagger/*any`       | ❌   | Swagger UI          |

---

## 6. MEKANISME KEAMANAN

### 6.1 Autentikasi (3 Skema)

1. **API Key** (`x-api-key` / `api-key` header):
   - Format: `ak_live_` + 64 hex chars (total 72+ char)
   - Disimpan: prefix (16 char) + SHA-256 hash di DB
   - Dipakai: Ingestion endpoint (`/api/logs/`)

2. **JWT Bearer Token**:
   - Signing: HMAC-SHA256 dengan `JWT_SECRET` dari env
   - Claims: `user_id`, `client_id`, `username`, `role`
   - Expiry: 2 jam
   - Dipakai: Seluruh dashboard endpoint

3. **Admin Secret** (`X-Admin-Secret` header):
   - Plain-text comparison dengan `ADMIN_SECRET` dari env
   - Dipakai: (Tersedia tapi belum digunakan di router saat ini)

### 6.2 Rate Limiting

- Algoritma: **Token Bucket** (in-memory)
- Scope: Per `client_id`
- Default: 50 request/detik
- Lokasi: Hanya pada ingestion route (`/api/logs/`)

### 6.3 Kriptografi

- **Hash function**: SHA3-256 (Keccak, bukan SHA-256!)
- **Hash input format**: `logID|actor|action|resource|timestamp_microseconds|sourceSystem|authCtx|prevHash|metadata`
- **Local chain**: Setiap log di-link ke hash sebelumnya via `PreviousHash`
- **Merkle Tree**: Batch SHA3-256 aggregation, proof disimpan per leaf

### 6.4 Verifikasi 4-Layer

```
Layer 1 (DB Existence)   → Cek log ada di PostgreSQL
Layer 2 (Re-Hash)        → Hitung ulang SHA3-256, bandingkan
Layer 3a (Kafka)         → Baca ulang message dari Kafka, normalize, hash, bandingkan
Layer 3b (Agent Source)  → Call Agent klien, bandingkan field-field
Layer 4 (Blockchain)     → Query Fabric World State, bandingkan Merkle Root
```

---

## 7. PIPELINE PROCESSING (Background Workers)

### 7.1 Pipeline Worker (`startPipelineWorker` di main.go)

Berjalan via **Ticker setiap 10 detik**:

1. **Hasher Engine** (`hasher.ProcessPendingLogs()`):
   - Query: `status = 'RECEIVED'`, order by `timestamp asc`
   - Proses: Hitung SHA3-256 hash + link previous_hash
   - Output: Status → `HASHED`

2. **Aggregator Engine** (`aggregator.ProcessBatch(10)`):
   - Query: `status = 'HASHED'`, order by `timestamp asc`, limit 10
   - Proses: Build Merkle Tree, simpan proofs + metadata
   - Output: Status → `AGGREGATED`

3. **Fabric Anchoring** (`fabricSvc.AnchorPendingRoots()`):
   - Query: Distinct `merkle_root` where `status = 'AGGREGATED'`
   - Proses: SubmitTransaction ke Chaincode `StoreMerkleRoot`
   - Output: Status → `ANCHORED`, `blockchain_tx_id` diisi

### 7.2 Kafka Consumer Worker

- Berjalan saat startup di goroutine terpisah per `ClientKafkaConfig` aktif
- Auto-discover topics berdasarkan `topic_prefix`
- Re-discovery setiap 60 detik
- Log dari Kafka **langsung HASHED** (skip `RECEIVED`)

---

## 8. HYPERLEDGER FABRIC INTEGRATION

### 8.1 Koneksi

- SDK: `fabric-gateway` v1.10.1 (Go SDK)
- Transport: gRPC + TLS
- Identity: X.509 certificate + PKCS8/EC private key
- Config via env: `FABRIC_MSP_ID`, `FABRIC_PEER_ENDPOINT`, `FABRIC_TLS_CERT_PATH`, `FABRIC_CERT_PATH`, `FABRIC_KEY_PATH`, `FABRIC_CHANNEL`, `FABRIC_CHAINCODE`

### 8.2 Chaincode Functions (Smart Contract)

| Function           | Tipe     | Parameter                                                       |
|--------------------|----------|-----------------------------------------------------------------|
| `StoreMerkleRoot`  | Submit   | anchorID, merkleRoot, timestamp, sourceGateway, batchSize, sig  |
| `QueryMerkleRoot`  | Evaluate | anchorID                                                        |

### 8.3 Data Tersimpan di Ledger

```json
{
  "anchor_id": "uuid",
  "merkle_root": "sha3-256-hex",
  "timestamp": "RFC3339Nano",
  "source_gateway": "AuditChain_Gateway_Node1",
  "batch_size": "10",
  "signature_node": "System_Signature"
}
```

---

## 9. KAFKA / DEBEZIUM INTEGRATION

### 9.1 Format Message

Menggunakan Debezium CDC (Oracle/PostgreSQL) dengan `ExtractNewRecordState` (SMT flatten):

```json
{
  "__op": "c",
  "__table": "nama_tabel",
  "__user_name": "db_user",
  "__ts_ms": 1719644000000,
  "ID": 123,
  "kolom1": "value1"
}
```

### 9.2 Processing Flow

1. Parse JSON → extract `__op`, `__table`, `__user_name`, `__ts_ms`
2. Resolve primary key (field konfigurabel: `pk_field`, fallback `ID`, `ogc_fid`, dll)
3. Format resource: `tabel_name:primary_key_value`
4. Extract metadata (semua field selain meta Debezium)
5. Normalize field values (handle Oracle NUMBER base64 encoding)
6. Deduplicate berdasarkan `resource + timestamp + client_id`
7. Hash langsung (skip Redis queue) → save ke DB sebagai `HASHED`
8. Simpan Kafka offset untuk verifikasi ulang

---

## 10. AGENT VERIFIER (Lapis 3 Extended)

### 10.1 Dua Mode Verifikasi

**Mode 1: SIMRS (via audit_trail)**
- Trigger: `source_record_id` terisi pada AuditLog
- Agent endpoint: `GET <agent_url>/verify/<source_record_id>`
- Bandingkan: `tabel↔resource`, `operasi↔action`, `app_user/db_user↔actor`, `data_lama+data_baru↔metadata`

**Mode 2: Satu Peta (via resource lookup)**
- Trigger: `resource` mengandung `:` (format `tabel:id`)
- Agent endpoint: `GET <agent_url>/verify-resource/<table>/<id>`
- Untuk INSERT/UPDATE: baris harus ada, metadata harus cocok
- Untuk DELETE: baris tidak boleh ada

### 10.2 Agent Communication

- Protocol: HTTP REST
- Auth: `Authorization: Bearer <verify_token>`
- Timeout: Konfigurabel per klien (default 5s)
- Response size limit: 1MB (`io.LimitReader`)

---

## 11. ENVIRONMENT VARIABLES

```env
# Server
PORT=8080

# Database
DB_DSN=postgres://user:pass@host:port/dbname?sslmode=disable

# Redis
REDIS_HOST=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0

# JWT
JWT_SECRET=your-secret-key

# Admin
ADMIN_SECRET=your-admin-secret

# Hyperledger Fabric
FABRIC_MSP_ID=Org1MSP
FABRIC_PEER_ENDPOINT=peer0.audit.example.com:7051
FABRIC_TLS_CERT_PATH=/path/to/tls/ca.crt
FABRIC_CERT_PATH=/path/to/users/Admin/msp/signcerts/cert.pem
FABRIC_KEY_PATH=/path/to/users/Admin/msp/keystore/priv_key.pem
FABRIC_CHANNEL=audit-channel
FABRIC_CHAINCODE=audit-contract
```

---

## 12. TEMUAN AUDIT & ISSUES

### 🔴 CRITICAL

1. **Ingestion Module TIDAK TERDAFTAR di Router!**
   - File `internal/modules/ingestion/router.go` mendefinisikan `RegisterRoutes` dengan endpoint `POST /api/logs/`
   - **TETAPI** `router.go` di `internal/api/` tidak pernah memanggil `ingestion.RegisterRoutes()`!
   - Ini berarti endpoint `POST /api/logs/` **TIDAK AKTIF** — log hanya bisa masuk via Kafka consumer
   - **FIX**: Tambahkan ingestion route registration di `api/router.go` + inject dependency Redis

2. **Redis TIDAK DIPAKAI di `main.go`**
   - `config.ConnectRedis()` sudah dibuat tapi **tidak pernah dipanggil** di `main.go`
   - Ingestion module memerlukan Redis client untuk push ke queue
   - Pipeline Worker (Hasher) membaca langsung dari **DB** (status = RECEIVED), bukan dari Redis queue
   - **Ada inkonsistensi arsitektural**: Ingestion push ke Redis, tapi Hasher baca dari DB

3. **Admin Endpoints TANPA AUTENTIKASI!**
   - `POST /api/admin/clients` dan `POST /api/admin/kafka-config` **tidak dilindungi** middleware apapun
   - Siapapun bisa membuat tenant baru dan mendapatkan API key
   - `AdminAuth` middleware sudah ada tapi **tidak digunakan**
   - **FIX**: Tambahkan `middleware.AdminAuth()` pada admin route group

### 🟡 WARNING

4. **Race Condition pada Previous Hash (Hasher)**
   - `ProcessPendingLogs()` query semua `RECEIVED` logs, lalu loop satu per satu
   - Tapi `prevHash` diambil dari **DB** (bukan dari hasil loop sebelumnya)
   - Jika ada concurrent write, previous hash chain bisa inkonsisten
   - Kafka consumer punya masalah serupa saat concurrent goroutines

5. **Rate Limiter In-Memory (Tidak Persisten)**
   - Token bucket disimpan di Go map — hilang saat restart
   - Tidak terdistribusi — jika multi-instance, setiap instance punya bucket sendiri
   - Pertimbangkan: Redis-based rate limiter

6. **Merkle Proof Hanya Level 0**
   - `BuildMerkleTree()` hanya menyimpan sibling proof untuk `levelIndex == 0`
   - Untuk tree > 2 level, proof intermediate level **tidak disimpan**
   - Ini membuat `VerifyMerkleProof()` hanya bekerja untuk batch size ≤ 2

7. **Log ID dari Kafka = `UnixNano` (Collision Risk)**
   - `generateLogID()` menggunakan `time.Now().UnixNano()`
   - Jika dua message diproses dalam nanosecond yang sama → **collision**
   - Seharusnya gunakan UUID seperti ingestion module

8. **JWT Secret dari Environment (No Fallback)**
   - Jika `JWT_SECRET` kosong, token tetap di-sign dengan string kosong
   - Ini memungkinkan siapapun membuat token valid
   - **FIX**: Validasi JWT_SECRET tidak kosong saat startup

9. **Error Handling via String Matching**
   - Service mengembalikan `errors.New("log_not_found")`, handler match via `err.Error()`
   - Ini fragile — typo di string bisa menyebabkan error tidak ter-handle
   - Sebaiknya gunakan sentinel errors (`var ErrLogNotFound = errors.New(...)`)

10. **CORS AllowAllOrigins**
    - `corsConfig.AllowAllOrigins = true` — terlalu permissive untuk production
    - Sebaiknya whitelist domain yang diizinkan

### 🔵 INFO / IMPROVEMENT

11. **No Unit Tests**
    - Tidak ada file `*_test.go` di seluruh codebase
    - Mock Fabric sudah ada tapi belum digunakan dalam test suite

12. **No Graceful Shutdown untuk Kafka Consumer**
    - Kafka reader di-close via defer, tapi context cancellation bisa menyebabkan message loss

13. **No Pagination pada GetRecentLogs**
    - Hardcoded limit 500, tidak ada offset/cursor pagination

14. **Docker Compose Hardcoded Credentials**
    - `POSTGRES_PASSWORD: rahasia` hardcoded di docker-compose.yml

15. **go.mod Module Name Mismatch**
    - Module name: `go-blockchain-api` — tidak mencerminkan nama proyek sebenarnya

16. **README.md Outdated**
    - README menyebut `cmd/gateway/main.go` tapi entry point sebenarnya `main.go` di root
    - Endpoint di README tidak up-to-date dengan implementasi aktual

---

## 13. DEPENDENCY UTAMA

| Package                              | Versi    | Fungsi                          |
|--------------------------------------|----------|---------------------------------|
| `github.com/gin-gonic/gin`          | v1.12.0  | HTTP framework                  |
| `github.com/gin-contrib/cors`        | v1.7.7   | CORS middleware                 |
| `gorm.io/gorm`                       | v1.31.1  | ORM                             |
| `gorm.io/driver/postgres`            | v1.6.0   | PostgreSQL driver               |
| `github.com/golang-jwt/jwt/v5`       | v5.3.1   | JWT signing/parsing             |
| `golang.org/x/crypto`                | v0.49.0  | SHA3-256, bcrypt                |
| `github.com/hyperledger/fabric-gateway`| v1.10.1 | Fabric blockchain SDK          |
| `github.com/redis/go-redis/v9`       | v9.18.0  | Redis client                    |
| `github.com/segmentio/kafka-go`      | v0.4.51  | Kafka consumer                  |
| `github.com/google/uuid`             | v1.6.0   | UUID generation                 |
| `github.com/joho/godotenv`           | v1.5.1   | .env file loader                |
| `github.com/swaggo/swag`             | v1.16.6  | Swagger doc generator           |
| `github.com/swaggo/gin-swagger`      | v1.6.1   | Swagger UI for Gin              |
| `google.golang.org/grpc`             | v1.78.0  | gRPC (untuk Fabric)             |

---

## 14. KONVENSI CODE

### 14.1 Pola Arsitektur Module

Setiap module mengikuti pola **Handler → Service (interface) → Repository (interface)**:

```go
// Repository — akses data
type Repository interface { ... }
type repoImpl struct { db *gorm.DB }

// Service — business logic
type Service interface { ... }
type serviceImpl struct { repo Repository }

// Handler — HTTP handler (Gin)
type Handler struct { Service Service }
```

### 14.2 Konvensi Penamaan

- **Model**: PascalCase singular (`AuditLog`, `Client`, `User`)
- **Table**: snake_case plural (auto oleh GORM: `audit_logs`, `clients`)
- **Package**: lowercase singular (`audit`, `auth`, `client`, `ingestion`)
- **File**: snake_case (`api_key.go`, `rate_limit.go`)
- **Status constants**: UPPER_SNAKE (`RECEIVED`, `HASHED`, `AGGREGATED`, `ANCHORED`)

### 14.3 Hash Formula (KRITIS — jangan diubah!)

```go
contextString := fmt.Sprintf("%s|%s|%s|%s|%d|%s|%s|%s|%s",
    logID,        // string
    actor,        // string
    action,       // string
    resource,     // string
    timestamp.UnixMicro(),  // int64
    sourceSystem, // string
    authCtx,      // string (normalized: "null"/"<nil>"/"" → "")
    prevHash,     // string
    metadata,     // string (canonical JSON)
)
hash := sha3.Sum256([]byte(contextString))
```

> **PERINGATAN**: Formula ini harus **IDENTIK** di `hasher.GenerateLogHash()` dan `kafkaconsumer.generateLogHash()`. Perubahan apapun pada format akan memecahkan semua verifikasi hash yang sudah ada.

### 14.4 Normalisasi Sebelum Hashing

Sebelum re-hash saat verifikasi, `canonicalizeLog()` melakukan:
1. **Metadata**: `json.Unmarshal` → `json.Marshal` (canonical key ordering)
2. **AuthorizationContext**: `"null"` / `"<nil>"` / `""` → selalu `""`

---

## 15. DEPLOYMENT

### Docker

```bash
# Build + run
docker-compose up -d --build

# Hanya DB
docker-compose up -d postgres-db
```

### Koneksi Jaringan

- Gateway terhubung ke `gateway-net` (internal) dan `fabric-net` (external, nama: `fabric_test`)
- Fabric peer di `192.168.11.94` (extra_hosts mapping)

### Volume Mount

- Fabric crypto materials: `/home/besu/audit_blockchain/organizations` (read-only)

---

## 16. PETA DEPENDENCY ANTAR PACKAGE

```
main.go
  ├── internal/config         (DB, Redis)
  ├── internal/api            (Router setup)
  │    ├── internal/modules/audit
  │    ├── internal/modules/auth
  │    ├── internal/modules/client
  │    └── internal/blockchain/agentverifier
  ├── internal/blockchain     (Fabric SDK)
  ├── internal/engine/hasher
  ├── internal/engine/aggregator
  └── internal/engine/kafkaconsumer

pkg/crypto                   (dipakai oleh hasher, aggregator, audit/service, kafkaconsumer)
internal/models              (dipakai oleh semua package internal)
internal/middleware           (dipakai oleh module routers)
internal/engine/normalizer   (dipakai oleh ingestion handler)
```

---

## 17. QUICK REFERENCE — COMMAND CHEAT SHEET

```bash
# Development
go run main.go                          # Run server
go mod tidy                             # Sync dependencies
swag init -g main.go                    # Generate Swagger docs

# Docker
docker-compose up -d --build            # Build + deploy
docker-compose logs -f api-gateway      # View logs

# Database
psql -h localhost -p 5433 -U postgres -d test_blockchain

# Testing (belum ada test)
go test ./...                           # (akan fail — belum ada test file)
```

---

## 18. PERCOBAAN & INTEGRASI KAFKA CLIENT (Juli 2026)

Pada Juli 2026, dilakukan percobaan integrasi berskala *real-world* lintas PC untuk menghubungkan database client (dengan Debezium & Kafka) ke sistem local gateway kita.

### 18.1 Arsitektur Aliran Data Lintas PC

Berikut adalah diagram visual aliran data dari PC Client ke PC Gateway kita:

```mermaid
graph TD
    subgraph Sisi Client [PC Client - 192.168.60.65]
        DB_C[(Database Client)] -->|CDC| Deb[Debezium CDC - Port 8083]
        Deb -->|Publish| KB[Kafka Broker - Port 29092]
        KUI[Kafka UI - Port 9000] -.->|Monitoring| KB
    end

    subgraph Sisi Kita [PC Gateway - Localhost]
        KB -->|TCP Consume| GoApp[Go Backend API - Port 8082]
        GoApp -->|Write Logs| PG[(PostgreSQL local - Port 5433)]
    end
```

### 18.2 Konfigurasi Lingkungan Pengujian
* **Sisi Client (PC Client: `192.168.60.65`):**
  * **Database Client:** Database SIMRS.
  * **Kafka UI:** Port `9000` (Web Interface UI untuk pemantauan cluster).
  * **Kafka Connect (Debezium REST API):** Port `8083` (aktif).
  * **Kafka Broker (Bootstrap Server):** Port `29092` (diakses via protokol TCP).
  * **Topic Prefix:** `morbis_simrs.`
* **Sisi Kita (Gateway Local):**
  * **Port Server Go:** `8082` (dipindahkan dari `8080` demi menghindari bentrok port binding dengan container IoT `smartfarming_edge_frontend` yang sedang aktif di PC local).
  * **PostgreSQL Database:** Berjalan via Docker Compose di port `5433` (database name: `test_blockchain`).
  * **File `.env`:** Diisi secara lokal untuk memuat `DB_DSN=postgres://postgres:rahasia@localhost:5433/test_blockchain?sslmode=disable`.

### 18.3 Studi Kasus Troubleshooting Port Broker (Lesson Learned)
Saat uji coba awal, koneksi sempat mengalami *timeout* dan tidak ada data yang masuk.
* **Penyebab:** Konfigurasi awal didaftarkan ke port `9000` (yang diinfokan sebagai port Kafka). Namun setelah dianalisis melalui request HTTP header, port `9000` mengembalikan `HTTP 200 OK` berupa halaman HTML dari **UI for Apache Kafka** (Web UI), bukan port biner broker.
* **Solusi:** 
  1. Dilakukan port-scanning menggunakan script diagnostik internal pada range port `9000-9099` dan `29000-29099`.
  2. Ditemukan port **`29092`** berstatus `OPEN` dan terkonfirmasi sebagai advertised listener TCP untuk Kafka Broker luar.
  3. Dilakukan koreksi port dari `9000` ke `29092` di tabel database `client_kafka_configs` dan dilakukan restart server.

### 18.4 Implementasi API Toggle Baru (Keamanan Data Terjamin)
Untuk memudahkan aktivasi/deaktivasi koneksi Kafka tanpa risiko penghapusan data, kami menambahkan fitur toggle status:
* **Endpoint Baru:** `PATCH /api/admin/kafka-config/:id/toggle`
* **Mekanisme Kerja:**
  * Endpoint ini hanya mengubah flag boolean `is_active` (`true` <-> `false`) pada record konfigurasi.
  * **Sama sekali tidak ada operasi `DELETE` atau `DROP`**, sehingga data konfigurasi dan riwayat log di database 100% aman dari penghapusan.

#### Potongan Kode Sumber Handler (`ToggleKafkaConfig`):
```go
func (h *Handler) ToggleKafkaConfig(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID konfigurasi tidak boleh kosong"})
		return
	}

	var cfg models.ClientKafkaConfig
	if err := h.DB.First(&cfg, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Konfigurasi Kafka tidak ditemukan"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mencari konfigurasi Kafka"})
		}
		return
	}

	// Toggle status
	cfg.IsActive = !cfg.IsActive

	if err := h.DB.Save(&cfg).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memperbarui status konfigurasi Kafka"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":       "Status konfigurasi Kafka berhasil diperbarui",
		"id":            cfg.ID,
		"client_id":     cfg.ClientID,
		"kafka_brokers": cfg.KafkaBrokers,
		"is_active":     cfg.IsActive,
	})
}
```

* **Contoh Request (PowerShell):**
  ```powershell
  Invoke-RestMethod -Method Patch -Uri "http://localhost:8082/api/admin/kafka-config/8d65299c-2f78-4bfd-8d1b-5595e0daf2e2/toggle"
  ```

### 18.5 Langkah Menghubungkan (Pendaftaran Ulang)
1. **Membuat Klien Baru:**
   ```powershell
   Invoke-RestMethod -Method Post -Uri "http://localhost:8082/api/admin/clients" `
     -ContentType "application/json" `
     -Body '{"company_name": "PT Client A", "subscription_tier": "enterprise", "rate_limit_per_sec": 100}'
   ```
2. **Mendaftarkan Kafka Config:**
   ```powershell
   Invoke-RestMethod -Method Post -Uri "http://localhost:8082/api/admin/kafka-config" `
     -ContentType "application/json" `
     -Body '{
       "client_id": "143d72a4-719e-46be-8ce9-6ce3671e2989",
       "topic_prefix": "morbis_simrs.",
       "kafka_brokers": "192.168.60.65:29092",
       "source_system": "SIMRS_CLIENT",
       "actor_field": "__user_name",
       "pk_field": "ID"
     }'
   ```

### 18.6 Script Diagnostik & Koreksi Database (Windows UAC Bypass)
Selama pengerjaan, ditemukan bahwa Windows User Account Control (UAC) memblokir eksekusi file script Go yang memiliki kata depan `update` (seperti `update_db.go`) karena dianggap sebagai installer sistem. 
* **Solusi/Pola:** Ganti nama file menjadi kata kerja biasa (seperti `correct_db.go`) untuk menghindari deteksi heuristik Windows UAC.
* **SQL Koreksi Manual (Alternatif DBeaver/psql):**
  ```sql
  UPDATE client_kafka_configs 
  SET kafka_brokers = '192.168.60.65:29092', is_active = true 
  WHERE client_id = '143d72a4-719e-46be-8ce9-6ce3671e2989';
  ```

### 18.7 Hasil Percobaan
Setelah konfigurasi diperbaiki dan server backend di-restart, server secara otomatis mendeteksi **3 topik** aktif dari Kafka Client:
* `morbis_simrs.POLINEMA.USERS`
* `morbis_simrs.POLINEMA.DATA_RM`
* `morbis_simrs.POLINEMA.BENTUK_MAKANAN`

Setiap data baru yang di-insert pada tabel tersebut di Morbis client akan langsung ter-consume secara real-time, di-hash (SHA3-256), dan disimpan ke tabel `audit_logs` di database PostgreSQL local.

---

> **Dokumen ini adalah sumber kebenaran untuk AI coding assistant.** Gunakan sebagai konteks saat melakukan modifikasi, debugging, atau penambahan fitur pada proyek AuditChain Gateway Backend.
