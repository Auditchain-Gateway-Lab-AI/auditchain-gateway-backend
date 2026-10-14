# Rencana Implementasi Flow Verifikasi End-to-End Baru

## 1. Tujuan dan batas pekerjaan

Dokumen ini menerjemahkan **maksud** diagram flow dari atasan: membandingkan kondisi terbaru di database klien dengan log terbaru di off-chain, lalu memakai bukti Merkle dan anchor Hyperledger Fabric untuk menilai keaslian jejak tersebut. Dokumen ini **belum** merupakan persetujuan untuk mengubah algoritma atau men-deploy-nya. Keputusan yang ambigu pada diagram harus disetujui terlebih dahulu pada Fase 0.

Target hasil: pengguna dapat membedakan (a) jejak Gateway/off-chain terbukti valid atau tidak, (b) data klien saat ini cocok dengan event klien terakhir atau tidak, dan (c) pemeriksaan belum dapat disimpulkan. Hasil tidak boleh menyatakan sumber mana yang *tampered* hanya karena dua hash berbeda.

Tidak termasuk: mengubah skema atau isi Oracle klien, mengubah pipeline CDC/Kafka/anchoring, menyalakan kembali MiniIO/MinIO, mengubah alur recovery, menyalakan worker duplikat di Mini PC, atau menghapus kompatibilitas verifikasi lama. Perubahan skema PostgreSQL, jika ternyata diperlukan, membutuhkan desain dan persetujuan terpisah; rencana awal memakai kontrak respons dan data yang sudah ada.

**Baseline yang diperiksa:** checkout backend `nama-fitur-baru` pada `0980c2d` (14 Oktober 2026). `.archify/` sudah untracked sebelum dokumen ini dibuat dan tidak disentuh. Baseline ini harus diperiksa ulang saat implementasi karena branch dapat berubah.

## 2. Yang benar-benar ada sekarang

1. `GET /api/dashboard/verify/:log_id` memakai JWT dan mengambil log dengan `log_id` **serta** `client_id` (`internal/modules/audit/router.go`, `handler.go`, `repository.go`).
2. `VerifyLogIntegrity` merekonstruksi hash daun dari isi audit log, lalu membandingkannya dengan `hash_value`; membaca anchor Fabric; merekonstruksi root dari hash daun dan `merkle_proofs`; kemudian, hanya untuk event klien terbaru yang relevan, membaca baris live melalui Agent (`internal/modules/audit/service.go`).
3. Hash daun log dari `internal/engine/hasher/hasher.go` **bukan** hash state baris klien dari `agentverifier.HashResourceState`. Field, preimage, dan tujuannya berbeda. Root Fabric adalah hash agregat batch, bukan salinan hash satu baris klien. Batch satu daun adalah kasus khusus ketika leaf = root.
4. Agent memilih `/verify-audit/<source_record_id>` bila ID audit sumber tersedia, selain itu `/verify/<table>/<id>` untuk `resource` berbentuk `TABLE:ID`. Pada mode resource, `INSERT`/`UPDATE` menuntut baris ada dan `DELETE` menuntut baris tidak ada (`internal/blockchain/agentverifier/service.go`).
5. `integrity_status` dan `source_status` sudah terpisah. Pemeriksaan manual dapat mencatat incident Gateway atau client-source. `VerifyResourceHistory`, verifikasi range, job terjadwal, dan tamper scanner adalah konsumen lain yang perlu dijaga konsistensinya.
6. Ada fungsi `verifyClientLogIntegrity` dalam `service.go`, tetapi tidak ditemukan pemanggilnya. Fungsi ini **bukan** kontrak aktif dan tidak boleh langsung diaktifkan: ia menyusun ulang daun dari metadata Agent dan dapat salah menyimpulkan kegagalan on-chain sebagai kerusakan data klien.
7. `VerifyResourceHistory` mengabaikan event `RECOVERY` untuk menentukan **event klien** terakhir, sedangkan riwayatnya tetap mencakup event recovery. Semantik ini harus dipertahankan dan diuji.
8. Pada pembandingan mode resource, implementasi sekarang melewati beberapa kolom ID serta kolom yang tidak ditemukan di respons Agent. Karena itu, `matched` sekarang belum identik dengan kesamaan byte seluruh baris. Kebijakan field, normalisasi, dan batas klaim harus dibakukan sebelum menjadikan hasil sebagai dasar atribusi tamper.
9. Ada fallback `legacy_field_fallback` untuk proof batch lama. Hasil ini tidak sama tingkat buktinya dengan proof yang lengkap; flow baru harus menampilkannya sebagai tingkat keyakinan terpisah, bukan diam-diam menyamakannya dengan bukti penuh.

## 3. Koreksi operasional terhadap diagram atasan

Diagram dipakai sebagai **urutan pertanyaan**, bukan rumus hash literal:

| Bagian diagram | Terjemahan implementasi yang aman |
|---|---|
| `hash value data client` vs `hash value latest log offchain` | Kanonisasi **state yang sebanding** pada klien dan metadata event klien terakhir; hitung/cek hash dengan skema state yang sama. Jangan bandingkan `ClientStateHash` dengan `AuditLog.HashValue` karena preimage berbeda. |
| `valid` setelah kedua hash sama | Artinya baru `SOURCE_MATCHED`; status end-to-end `VERIFIED` memerlukan hash log serta bukti Fabric yang sah. |
| `rekonstruksi merkle tree` | Rekonstruksi **root dari leaf log + proof batch yang tepat**. Tidak perlu membangun seluruh tree dari satu baris; jangan membandingkan hash baris langsung ke root. |
| `offchain selalu sama dengan blockchain` | Dihapus sebagai asumsi. Isi log, hash tersimpan, proof, dan root Fabric tetap diuji. |
| Tentukan sisi yang *tampered* | Hanya boleh jika ada bukti positif, versi/event sama, dan aturan atribusi yang disetujui. Jika tidak, beri `DIVERGENT`/`INCONCLUSIVE`, bukan tuduhan pasti. |

**Invarian matematika:** `log_leaf = H(envelope_log_kanonis)`; `root_kandidat = MerkleProof(log_leaf, siblings)`; yang diperbandingkan dengan Fabric adalah `root_kandidat == root_Fabric`. `state_hash = H(identitas_klien, resource, state_kanonis)` merupakan domain terpisah. Untuk membuktikan suatu *candidate state* terhadap anchor, harus ada aturan deterministik yang mengikat state itu ke leaf/envelope yang di-anchor; kesamaan nilai state hash saja tidak membuktikan keanggotaan Merkle.

## 4. Fase 0 — keputusan kontrak bersama atasan (wajib sebelum coding)

Catat jawaban dan contoh data untuk semua pertanyaan berikut dalam review dokumen/ADR singkat:

1. Yang dimaksud `hash value data client`: hash **state baris operasional** atau hash **event audit sumber**? Keduanya membutuhkan pembanding berbeda. Tetapkan cakupan awal: misalnya mode resource Morbis `INSERT`/`UPDATE`/`DELETE`; mode `source_record_id` diberi kontrak tersendiri.
2. Apakah sumber kebenaran yang di-anchor adalah **envelope log AuditChain**? Kode saat ini: ya. Fabric menyimpan root batch; ia tidak menyimpan row Oracle. Setujui bahwa diagram tidak berarti `hash(row) == root`.
3. Definisikan istilah hasil: `gateway_integrity`, `source_comparison`, dan `overall_assessment`, termasuk `PENDING`, `UNREACHABLE`, `NOT_COMPARABLE`, `LEGACY_PROOF`, serta data yang belum sinkron karena CDC.
4. Setujui ambang atribusi. Usulan konservatif: `offchain_compromised_suspected` hanya bila anchor/proof serta kandidat event tepercaya secara independen membuktikan isi berbeda; `client_divergence` hanya bila log terbukti di-anchor dan data live dari event yang **sama** berbeda. Label `suspected` bukan pembuktian pelaku/peretasan.
5. Putuskan apa yang boleh disimpulkan bila **keduanya tidak cocok**, **keduanya cocok**, proof hilang, Fabric tidak bisa dihubungi, Agent menolak, atau event klien lebih baru belum tertangkap CDC. Default: `INCONCLUSIVE`/`PENDING_SYNC`, bukan `VALID`/`TAMPERED`.
6. Setujui urutan perubahan API/UI dan kompatibilitas dengan dashboard Besu maupun Client Portal Mini PC. Tentukan apakah flow baru hanya untuk tombol Verify per-log atau juga history/range/scan. Rekomendasi: rollout per-log dulu; konsumen lain diselaraskan setelah hasil stabil.

**Gate 0:** keputusan tertulis, contoh payload yang representatif, dan matriks status disetujui. Jika belum, berhenti di rencana; jangan mengubah verifikasi produksi.

## 5. Fase 1 — baseline dan fixture tanpa menulis data produksi

1. Recheck branch/HEAD, `git status`, aturan repo, dan diff yang sudah ada. Bekukan contoh kontrak respons endpoint per-log, history, range, dan status incident; jangan mengambil token atau metadata sensitif ke fixture.
2. Petakan tepat satu jalur Morbis dari `Agent → Gateway → PostgreSQL log/proof → Fabric` untuk `INSERT`, `UPDATE`, `DELETE`, serta `RECOVERY`. Identifikasi kapan `source_record_id` ada/kosong, kapan event menjadi latest, dan cara menentukan urutan saat `timestamp`/`db_timestamp` kosong atau sama.
3. Kumpulkan fixture **sintetis**: batch Merkle 1, 2, 3+ daun; data match/mismatch; key case, angka, timestamp, null, kolom hilang, DELETE, Agent 401/403/503/timeout, Fabric unavailable, proof rusak, dan CDC tertinggal. Data asli klien tidak masuk repo.
4. Jalankan tes baseline Go dan simpan hasilnya. Semua kegagalan yang sudah ada dicatat terpisah dari regresi perubahan baru.

**Gate 1:** semua preimage/hash, identitas batch, endpoint, dan batas kepercayaan terdokumentasi. Belum ada perubahan skema/database.

## 6. Fase 2 — spesifikasi pembanding dan matriks keputusan

### 6.1 Pembanding yang benar

- `S`: state terkini dari Agent, dibaca **read-only** dan dibatasi verify policy lokal Agent.
- `O`: state yang direpresentasikan oleh metadata **event klien terakhir yang relevan** di PostgreSQL, bukan event `RECOVERY` internal. Untuk DELETE, representasinya adalah ketiadaan baris; untuk `INSERT`/`UPDATE`, tentukan proyeksi field yang disetujui.
- `L`: daun log yang dihitung ulang dari seluruh field kanonis event off-chain; cocokkan dengan `hash_value` tersimpan.
- `P`: proof milik `L`, dibatasi batch/root/tenant/log yang tepat; rekonstruksi root dan bandingkan dengan anchor `F` yang dibaca dari Fabric.
- `S == O` hanya bermakna jika identitas tenant, resource, primary key, aksi, proyeksi field, versi/waktu, dan algoritma kanonisasi sama. Jangan menganggap hash cocok jika perbandingan field parsial atau ada field wajib hilang.

### 6.2 Matriks hasil minimum

| Bukti `O → F` | Banding `S ↔ O` | Hasil yang aman |
|---|---|---|
| Terbukti, proof lengkap | Sama | `VERIFIED`: log ter-anchor dan state live cocok dengan event terakhir. |
| Terbukti, proof lengkap | Beda | `CLIENT_DIVERGENCE`: jejak ter-anchor, state live berbeda; periksa CDC lag/perubahan sah sebelum menyebut tamper. |
| Gagal hash lokal atau root | Apa pun | `GATEWAY_INTEGRITY_FAILED`/`OFFCHAIN_SUSPECTED`; tidak otomatis menyatakan state klien valid. |
| Kedua sumber berbeda; kandidat event sumber juga memiliki pembuktian independen yang sah ke anchor yang sama | Sesuai bukti | Baru boleh mempersempit atribusi menurut aturan Fase 0; sertakan evidence dan identitas batch. |
| Pending anchor/CDC | Apa pun | `PENDING`; jangan mengeluarkan vonis. |
| Fabric, proof, Agent, atau kebijakan baca tidak tersedia | Tidak dapat diuji | `INCONCLUSIVE` dengan penyebab spesifik; hindari `TAMPERED` palsu. |
| Legacy fallback saja | Sama atau beda | `LEGACY_LIMITED_PROOF`; pertahankan perilaku lama tetapi jangan mengklaim proof independen lengkap. |

Khusus cabang diagram yang awalnya `S == O` lalu menyatakan hanya salah satu cocok dengan Fabric: itu **tidak mungkin** bila kedua kandidat memakai representasi dan proof yang sama. Implementasi harus menganggapnya indikasi salah kontrak/versi/proof, bukan klasifikasi tamper.

**Gate 2:** matriks dan kontrak hash disetujui dengan unit test contoh **sebelum** perubahan service utama.

## 7. Fase 3 — perubahan backend bertahap

1. **Ekstrak hasil bukti terstruktur** di `internal/modules/audit/service.go`/file helper baru: `offchain_hash`, `anchor_proof`, `source_state`, `event_freshness`, dan `assessment`. Jadikan fungsi penilai deterministik agar seluruh cabang dapat diuji tanpa DB/Agent/Fabric. Jangan hilangkan status lama sebelum dashboard siap.
2. **Perketat pemilihan event latest** di `repository.go` dan `service.go`: scope `client_id + resource`, tie-break deterministik, aksi `RECOVERY` internal tidak menggantikan event klien, dan historical log tidak dibandingkan dengan row live sekarang. Uji event baru yang datang saat proses verifikasi.
3. **Samakan representasi state** di `internal/blockchain/agentverifier/service.go` dan helper kanonisasi yang sudah ada (`pkg/canonicalstate`). Tentukan allowlist field/proyeksi yang benar, penanganan field hilang/null/case/angka/waktu, serta error eksplisit jika metadata tidak dapat dibandingkan. Jangan mengubah `GenerateLogHash` atau format leaf historis tanpa strategi versi/migrasi yang disetujui.
4. **Pertahankan pembuktian off-chain → Fabric**: hitung ulang `L`, validasi proof batch, rekonstruksi root, baca `F`, cocokkan. Audit `GetProofsByHash` yang kini mencari lewat hash saja: bila ada kebutuhan isolasi batch/tenant lebih kuat, rancang query/kontraknya tanpa langsung migrasi data. Legacy fallback dibedakan jelas dari proof lengkap.
5. **Gabungkan hasil**, bukan mengganti verifikasi teman: kedua jalur menghasilkan assessment sesuai matriks. Agent unreachable/forbidden tidak menaikkan hasil log-Fabric yang valid menjadi log tampered. Tidak ada rekonstruksi Merkle dari hash state klien kecuali ada *candidate leaf* dan proof yang sah sesuai kontrak yang dipilih.
6. **Incident & status cache**: petakan ulang `integrity_status` hanya untuk integritas log/Fabric, `source_status` untuk state klien, dan assessment gabungan di respons. Jangan auto-buat incident/auto-recovery dari hasil `INCONCLUSIVE`, CDC lag, atau mismatch parsial. Uji idempotensi incident dan error saat persistensi gagal.
7. **Kontrak API** di `handler.go`, Swagger, dan tes kontrak: pertahankan field/status lama selama transisi; tambahkan field eksplisit seperti `verification_assessment`, `proof_level`, `comparison_basis`, `reason_code`, `checked_at` hanya setelah Fase 0 menyetujui nama dan maknanya. Endpoint Agent-facing `/api/agent/offchain-data` dan `/verify-merkle-root` diaudit otentikasi/tenant scope-nya sebelum dipakai sebagai bukti baru; header `X-Client-ID` saja tidak boleh menjadi dasar identitas tepercaya.
8. **Konsumen lain**: setelah per-log stabil, samakan `VerifyResourceHistory`, verify-range/job, tamper scanner, dan respons agregat. Jangan menduplikasi network call Agent untuk tiap historical event; jaga batas timeout dan beban.

**Gate 3:** tes unit dan kontrak lolos, hasil lama tidak berubah untuk fixture valid/historical kecuali perubahan semantik yang disetujui. Tidak ada perubahan Oracle dan tidak ada worker baru.

## 8. Fase 4 — Agent dan portal

- **Agent Mini PC:** gunakan endpoint read-only `/verify/:table/:record_id` dan verify policy yang sudah terpisah dari recovery policy. Perubahan Agent hanya jika Fase 0/2 membuktikan respons yang ada kurang; jangan menambah hak tulis, tabel Oracle, atau mengaktifkan recovery untuk keperluan verifikasi.
- **Portal Client dan Admin:** tampilkan tiga lapis terpisah: `Bukti log & Fabric`, `Data sumber saat ini`, `Kesimpulan`. `UNREACHABLE`/`NOT_COMPARABLE`/`PENDING_SYNC` harus tampak berbeda dari tamper. Teks “Verifikasi Berhasil” hanya untuk assessment yang memang lengkap; jika log-Fabric valid tetapi Agent gagal, tampilkan sukses kriptografi **dengan peringatan pemeriksaan sumber belum selesai**.
- Perubahan portal adalah pekerjaan repo frontend terkait, bukan otomatis dilakukan dengan patch backend ini. Bekukan contoh respons dan tes UI sebelum mengganti label/badge.

**Gate 4:** portal lama tetap bisa membaca respons selama transisi; review manual label untuk setiap baris matriks.

## 9. Fase 5 — verifikasi dan rollout aman

1. **Unit:** tabel keputusan lengkap; batch 1/2/3+; proof sibling kiri/kanan; proof hilang/tertukar; metadata lokal berubah; Agent beda/tidak ada/timeout/403; DELETE; source audit ID; recovery; historical; tenant silang; CDC lag; dua event berurutan; field hilang; legacy.
2. **Kontrak/integrasi lokal:** jalankan `gofmt`, `go test ./...`, `go test -race` pada paket terdampak jika layak, `go vet ./...`, build, `git diff --check`, dan tes handler/Swagger. Bandingkan baseline; kegagalan lingkungan dicatat, bukan dianggap lulus.
3. **Staging read-only:** pakai event uji yang telah diizinkan; periksa respons per-log, history, range, Agent langsung, dan bukti Fabric. Jangan menulis/menghapus baris Oracle untuk membuat kasus tamper. Pengujian perubahan data hanya di fixture/lingkungan uji dengan izin eksplisit.
4. **Observabilitas:** log `request_id`, `client_id`, `log_id`, `reason_code`, durasi Agent/Fabric, dan proof level tanpa token, password, atau data sensitif. Pantau rasio `INCONCLUSIVE`, mismatch, timeout, serta regresi latensi.
5. **Rollout:** feature flag/verifikasi versi baru bila perubahan hasil signifikan; mulai tenant/pilot terpilih, bandingkan hasil lama vs baru tanpa mengubah keputusan produksi (*shadow mode*) bila memungkinkan. Review selisih, lalu aktifkan bertahap. Rollback ke mode lama tidak boleh menghapus log atau incident historis.

**Gate 5 (selesai):** contoh match dan mismatch yang sah, DELETE, pending, tidak-terhubung, legacy, serta dua kemungkinan bukti rusak semuanya menghasilkan label yang benar; tidak ada klaim valid/tamper tanpa bukti; Besu dan Mini PC diuji pada deployment nyata yang disetujui.

## 10. Timeline estimasi dan urutan kerja

Estimasi adalah **jam kerja efektif**, bukan janji tanggal deployment. Bisa dipadatkan bila keputusan atasan, akses repo portal/Agent, serta staging sudah tersedia; gate tetap tidak boleh dilompati.

| Urutan | Kegiatan | Estimasi | Hasil / syarat lanjut |
|---|---|---:|---|
| 0 | Konfirmasi rumus dan klasifikasi dengan atasan | 1–2 jam + waktu persetujuan | Gate 0, keputusan tertulis |
| 1 | Baseline code/kontrak/fixture dan tes awal | 2–3 jam | Gate 1, peta alur dan fixture |
| 2 | Spesifikasi kanonisasi, proof, matriks status | 2–3 jam | Gate 2, test case disetujui |
| 3a | Refactor helper bukti dan pembanding state | 3–5 jam | Unit test komponen |
| 3b | Orkestrasi service, status/incident, API | 4–6 jam | Gate 3, tes kontrak |
| 4 | Penyesuaian Agent (jika perlu) dan dua portal | 3–6 jam, bergantung repo/UI | Gate 4, label benar |
| 5 | Integrasi, staging, shadow/pilot, rollout | 4–8 jam + jadwal operator | Gate 5, bukti E2E dan rollback |

Total perkiraan **19–33 jam kerja efektif**, dapat tersebar dalam beberapa sesi. Jika target harus lebih cepat, prioritas awal adalah Gate 0–3 untuk endpoint per-log, disusul portal dan rollout; hasil parsial **tidak** dinyatakan produksi selesai. Tidak ada merge/push/deploy otomatis dari pembuatan dokumen ini.

## 11. Definition of done dan daftar larangan

- Penilaian per-log mampu menjelaskan: event yang dibandingkan, waktu/versi pemeriksaan Agent, kecocokan state, hasil hash log, kualitas proof, root Fabric, dan sebab bila belum pasti.
- `S == O` saja tidak menghasilkan `VERIFIED`; `S != O` saja tidak menghasilkan tuduhan *tampered* ke salah satu pihak.
- Tidak ada persamaan literal `hash(row_client) == merkle_root_Fabric` untuk batch multi-leaf.
- `integrity_status`, `source_status`, incident, serta tombol/teks portal konsisten untuk per-log, history, dan range sesuai cakupan rollout.
- Data Oracle dan skema database tidak diubah oleh flow verifikasi. Recovery tetap jalur terpisah, berizin, dan tidak dipicu otomatis oleh hasil ini.
- Kontrak lama, legacy proof, tenant isolation, serta E2E Besu–Mini PC diuji sebelum klaim “aman berjalan”.

**Larangan implementasi:** menyalin diagram menjadi `if hash_client == merkle_root`; menandai off-chain selalu benar; memperlakukan 403/timeout sebagai bukti manipulasi; memakai event historis atau `RECOVERY` internal sebagai state klien terbaru; membuka endpoint Agent-facing tanpa autentikasi tepercaya; mengubah rumus hash historis tanpa versi dan migrasi; atau menguji tamper dengan memodifikasi data produksi.
