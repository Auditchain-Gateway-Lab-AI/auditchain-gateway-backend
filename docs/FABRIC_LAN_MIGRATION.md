# Migrasi koneksi backend Fabric ke LAN

## Lingkup perubahan

`docker-compose.yml` memetakan hostname Fabric pada container `api-gateway`
ke alamat LAN yang telah diuji dari VM Besu dan CLI Fabric:

| Hostname | Alamat sebelumnya | Alamat LAN |
|---|---|---|
| `peer0.org1.audit.example.com` | `100.125.142.44` | `192.168.11.94` |
| `peer0.org2.audit.example.com` | `100.126.175.124` | `192.168.11.17` |
| `peer0.org3.audit.example.com` | `100.103.85.7` | `192.168.11.85` |
| `orderer.audit.example.com` | `100.125.142.44` | `192.168.11.94` |

Endpoint backend tetap `peer0.org1.audit.example.com:7051`. Hostname dan material
TLS/MSP tetap dipakai; tidak perlu mengganti endpoint menjadi IP literal atau
mengganti sertifikat untuk perubahan mapping ini. Pastikan alamat LAN tetap
stabil melalui konfigurasi IP statis atau reservasi DHCP.

Ini hanya memindahkan resolusi hostname pada backend. Koneksi yang ditentukan
Docker DNS/overlay, alamat advertise/data-path Swarm, peer tambahan, serta
koneksi deployment melalui Tailscale tidak otomatis berpindah ke LAN.

## Deploy melalui workflow

1. Tinjau dan validasi perubahan lokal; commit dan push melalui proses repository.
2. Gabungkan perubahan ke `main` sesuai proses review yang berlaku.
3. Buka GitHub Actions dan pilih **Deploy Backend Development**. Nama file
   workflow-nya adalah `.github/workflows/deploy-production.yml`.
4. Pilih branch `main`, aktifkan `confirm_development`, isi
   `deployment_reason`, lalu jalankan **Run workflow**.
5. Workflow menjalankan validasi, kemudian memanggil `deploy.sh` dengan SHA
   commit terpilih dan service `api-gateway`. Script membuat ulang API memakai
   Compose terbaru dengan `--no-deps`, lalu memeriksa `/healthz` dan `/readyz`.

Perubahan ini tidak memerlukan perubahan secret `BACKEND_ENV` jika endpoint,
sertifikat, dan konfigurasi aplikasi di dalamnya tetap sesuai. Workflow yang
ada tetap menyinkronkan secret tersebut saat deployment. Tidak perlu mengubah
secret/variable SSH atau Tailscale untuk migrasi mapping Fabric ini.

Jeda API singkat dapat terjadi saat container diganti. Workflow ini tidak
me-redeploy stack Fabric dan tidak membuat ulang PostgreSQL. Workflow lama
**Deploy Backend Dev** (`deploy-dev.yml`, branch `ops-Petrus`) bukan jalur yang
dipakai untuk perubahan ini.

## Verifikasi setelah deployment

Jalankan pemeriksaan read-only berikut di VM Besu:

```bash
docker inspect auditchain-api \
  --format '{{json .HostConfig.ExtraHosts}}'

docker exec auditchain-api printenv FABRIC_PEER_ENDPOINT

curl -fsS --max-time 5 http://127.0.0.1:8080/healthz
curl -fsS --max-time 5 http://127.0.0.1:8080/readyz
```

Pastikan keempat mapping berisi IP LAN pada tabel dan endpoint masih hostname
Org1. Health/readiness saja belum membuktikan transaksi Fabric berhasil.
Lakukan satu CRUD pada data uji Morbis, tunggu log baru `ANCHORED`, lalu pastikan
block baru tersinkron ke peer Org1, Org2, dan Org3 melalui CLI. Bandingkan
height dan hash block pada posisi yang sama; jangan menganggap perbedaan
sesaat ketika transaksi masih berjalan sebagai kegagalan.

## Rollback backend

Jika perubahan mapping perlu dibatalkan, revert commit mapping melalui proses
repository, gabungkan ke `main`, kemudian jalankan workflow yang sama. Periksa
health/readiness dan transaksi lagi. Hindari edit Compose atau recreate API
manual di server karena perubahan akan menyimpang dari sumber deployment.

`deploy.sh` saat ini mengembalikan image dan environment ketika deployment
gagal, tetapi menggunakan Compose dari checkout saat itu. Rollback image
tersebut **tidak mengembalikan mapping jaringan Compose secara otomatis**.
Karena itu pemulihan mapping dilakukan lewat commit dan workflow.

## Pekerjaan infrastruktur yang masih terpisah

- Perubahan service CLI/peer yang sudah dilakukan pada server perlu disimpan
  pada sumber deployment Fabric. File yang ditunjukkan operator berada di
  `/home/besu/audit_blockchain/docker/docker-compose-swarm.yaml`; file itu
  tidak tersedia dalam repository backend ini. Jangan menyalin definisi stack
  perkiraan atau menjalankan ulang seluruh stack dari repository backend.
- Swarm/overlay masih menggunakan alamat lama; Node 3 teramati mengiklankan
  `10.0.2.15`. Migrasi advertise/data-path memerlukan prosedur tersendiri.
- Peer tambahan `peer1.org1.audit.example.com:8051` teramati memakai jalur
  Tailscale dan perlu diinventarisasi sebelum jalur tersebut dilepas.
- Pertahankan Tailscale sampai seluruh dependensi tersebut selesai diperiksa
  dan dimigrasikan. Keberhasilan deployment backend ini tidak berarti seluruh
  jaringan blockchain sudah bebas Tailscale.
