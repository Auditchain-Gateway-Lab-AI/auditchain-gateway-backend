# Sumber konfigurasi Fabric

`docker-compose-swarm.yaml` berasal dari file yang diunduh operator pada
30 Oktober 2026 dari VM Besu:

`/home/besu/audit_blockchain/docker/docker-compose-swarm.yaml`

Salinan ini disimpan di repository backend sebagai referensi berversi untuk
stack `audit-network`. Perubahan terhadap file unduhan dibatasi pada tiga
substitusi IP Tailscale ke LAN, termasuk komentar lokasi node:

| Node | IP lama | IP LAN |
|---|---|---|
| Besu / Semeru | `100.125.142.44` | `192.168.11.94` |
| Node 2 | `100.126.175.124` | `192.168.11.17` |
| Node 3 | `100.103.85.7` | `192.168.11.85` |

Hostname, TLS/MSP, path ledger, image tag, port, network, gossip bootstrap, dan
placement mengikuti file sumber. Jangan menambahkan isi `organizations`,
private key, atau data ledger ke folder ini. Warning gossip bootstrap yang
sudah teramati belum diperbaiki oleh perubahan mapping ini.

## Sinkronisasi file melalui GitHub Actions

1. Review, commit, push, lalu merge perubahan ke `main`.
2. Buka **Actions -> Sync Fabric LAN Config -> Run workflow** pada `main`.
3. Aktifkan `confirm_sync` dan jalankan workflow.
4. Workflow memakai environment `development` dan kredensial SSH/Tailscale
   yang sama dengan workflow backend. Pastikan konfigurasi environment itu
   tetap tersedia; tidak perlu menambahkan secret ledger atau private key Fabric.
5. Workflow memvalidasi stack, memeriksa mapping keempat service yang aktif,
   dan menolak jika file server berbeda di luar substitusi IP yang disetujui.
6. Jika ada perubahan file, workflow membuat backup di direktori tujuan dan
   mengganti file secara atomik. Jika sudah sama, workflow tidak mengubah file.

Workflow ini **hanya menyelaraskan file sumber**. Ia tidak menjalankan
`docker stack deploy`, restart container, mengganti channel, atau mengubah
anggota/alamat Docker Swarm. Backend workflow juga tidak me-deploy file ini.
Validasi stack tidak membuktikan transaksi, TLS, ataupun kesehatan jaringan.

Setelah workflow sukses, file server sudah berisi mapping LAN sehingga
deployment berikutnya yang memakai file tersebut tidak mengembalikan mapping
Tailscale lama. Ini bukan snapshot penuh service live: kebijakan update yang
diterapkan melalui CLI dan image digest aktif tetap perlu dibandingkan sebelum
deployment stack di masa mendatang. Image tag `2.5` pada sumber tetap mutable;
hindari deployment seluruh stack hanya untuk menyimpan perubahan IP.

## Pekerjaan tersisa

- Migrasi advertise/data-path Docker Swarm, terutama Node 3 yang teramati
  memakai alamat `10.0.2.15`.
- Inventarisasi peer tambahan `peer1.org1.audit.example.com:8051`, yang tidak
  ada pada file unduhan.
- Evaluasi konfigurasi gossip bootstrap dan kesiapan chaincode terpisah dari
  migrasi mapping.
- Pertahankan Tailscale sampai seluruh dependensi tersebut dimigrasikan.

Untuk membatalkan sinkronisasi file ini, review dan pulihkan backup yang
dicatat workflow melalui prosedur konfigurasi yang disetujui. Revert file
repository saja tidak membalik service yang sedang berjalan; workflow sync
ini sengaja hanya menerima migrasi LAN dan bukan alat rollback runtime.
