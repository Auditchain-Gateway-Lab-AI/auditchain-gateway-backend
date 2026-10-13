# Progres AuditChain Hari Ini

**Tanggal:** 13 Oktober 2026  
**Fokus:** memastikan Agent/Gateway Mini PC dan alur CDC Morbis ke backend Besu berjalan.

## Progres hari ini

Hari ini pekerjaan difokuskan pada pemeriksaan layanan yang sudah dideploy di Mini PC dan memastikan event Morbis benar-benar masuk ke backend Besu. Pemeriksaan dilakukan bertahap dari kondisi service, koneksi API, pembagian worker, lalu bukti data yang tersimpan.

Pertama, Agent pada Mini PC dipastikan berjalan pada branch `feat/Duplicate_Backend`, commit `743c262`. Container berstatus `Up (healthy)`, endpoint `/health` mengembalikan HTTP 200, dan log menyatakan koneksi Oracle berhasil serta recovery policy berhasil dimuat. Hasil ini menunjukkan Agent siap menerima pemeriksaan dasar, tetapi belum dengan sendirinya membuktikan transaksi recovery end-to-end.

Berikutnya, konektivitas Gateway diperiksa. Request `/readyz` mengembalikan status `ready`, sementara log `auditchain-gateway-runtime` menunjukkan request login, profil, statistik, inventory, dan latest verification runs berhasil dengan HTTP 200. Di sisi worker, `GATEWAY_BACKGROUND_WORKERS_ENABLED=false` terkonfirmasi pada Gateway Mini PC. Consumer CDC Morbis terlihat berjalan di backend Besu, sehingga Gateway Mini PC tidak menjalankan consumer Kafka kedua.

Setelah service siap, pengujian diarahkan khusus ke Morbis. Consumer pada Besu menemukan 23 topic dengan prefix `morbis_simrs.` dan melaporkan siap mengonsumsinya. Saat dipantau langsung, dua event berhasil diproses: `UPDATE AGAMA:725` dengan log ID `1791856465737206296` dan `INSERT AGAMA:923` dengan log ID `1791856493791453803`. Query PostgreSQL kemudian menemukan kedua event tersebut di `audit_logs` dengan client Morbis dan `source_system=SIMRS Morbis 1`. Ini membuktikan jalur CDC Morbis sampai ke penyimpanan audit untuk dua event yang diuji.

Pemeriksaan hasil juga menemukan pekerjaan yang belum selesai. Kedua row tercatat berstatus audit `ANCHORED`, tetapi `snapshot_status` masih `PENDING`, `integrity_status` masih `NOT_CHECKED`, dan query tidak menemukan pasangan pada `snapshot_outboxes`. Environment Besu menunjukkan `RECOVERY_MODE=agent_direct` bersama flag snapshot writer/required yang bernilai `true`. Jadi status dan perilaku snapshot perlu dicocokkan dengan mode runtime sebelum disimpulkan normal atau dianggap gagal. Kolom `ANCHORED` di PostgreSQL juga belum menjadi bukti pemeriksaan Fabric secara independen.

Terakhir, pemeriksaan portal menemukan bahwa browser sebelumnya masih membuka portal client lama di Besu melalui `100.125.142.44:3002`. Pengguna telah mengonfirmasi hal tersebut. Karena itu, portal client Mini PC belum bisa dinyatakan sudah teruji dari screenshot tersebut, walaupun log runtime Gateway menunjukkan beberapa request API berhasil.

## Kesimpulan progres

Agent Mini PC sehat, endpoint dasar Gateway merespons, worker Mini PC tidak berjalan ganda, dan dua event CDC Morbis berhasil masuk ke `audit_logs` pada backend Besu. Dengan demikian, **kesiapan service dan ingest CDC Morbis sudah terbukti untuk skenario yang diuji**. Pekerjaan belum dapat disebut selesai end-to-end karena portal client yang benar belum dibuktikan, status snapshot/integritas belum ditutup, dan anchor Fabric belum diperiksa secara independen.

## Selanjutnya yang dikerjakan

Langkah berikutnya adalah memastikan browser membuka portal client yang benar-benar dituju dan memeriksa Request URL API di tab Network. Setelah host portal dipastikan, lakukan satu perubahan uji yang disepakati di SIMRS Morbis, lalu cocokkan event sumber, topic Kafka, log consumer, dan row `audit_logs`. Pada event terbaru itu jalankan verifikasi ke Agent dan periksa bukti anchor/root Fabric secara terpisah.

Secara paralel, telusuri kenapa event pada mode `agent_direct` berstatus `PENDING` tanpa snapshot outbox. Jika mode tersebut memang tidak menggunakan snapshot legacy, rapikan kontrak/status agar kondisi itu tidak tampil seperti pekerjaan tertunda. Jika snapshot memang diwajibkan untuk jalur yang sedang dipakai, telusuri worker, konfigurasi, dan antrean outbox lalu perbaiki penyebabnya. Jangan mengubah flag produksi hanya untuk mengganti tampilan status sebelum alur yang diharapkan dipastikan.

## Narasi siap dimasukkan ke PPT

Pada 13 Oktober, kami memeriksa layanan Agent dan Gateway di Mini PC serta alur CDC Morbis menuju backend Besu. Agent terpantau sehat, koneksi Oracle berhasil, endpoint Gateway merespons, dan worker background Mini PC tetap nonaktif agar tidak membuat consumer Kafka ganda. Consumer Besu menemukan 23 topic Morbis dan berhasil mencatat dua event uji, yaitu perubahan `AGAMA:725` dan penambahan `AGAMA:923`, ke tabel audit. Ini membuktikan ingest CDC Morbis berjalan untuk event yang diuji. Pemeriksaan lanjutan menemukan status snapshot masih `PENDING` dan integritas belum diperiksa, sementara browser yang sempat diuji ternyata masih membuka portal lama di Besu. Berikutnya kami akan memastikan portal client Mini PC, menuntaskan arti status snapshot pada mode `agent_direct`, lalu memverifikasi satu event terbaru terhadap Agent dan bukti Fabric sebelum menyatakan alur end-to-end selesai.

## Bukti yang tersedia

- Agent Mini PC: container `Up (healthy)`, `/health` HTTP 200, koneksi Oracle berhasil, recovery policy dimuat.
- Gateway: `/readyz` HTTP 200 dan log request API dasar berstatus HTTP 200.
- Worker: `GATEWAY_BACKGROUND_WORKERS_ENABLED=false` pada Gateway Mini PC.
- CDC Morbis: 23 topic ditemukan; dua event live tercatat dan ditemukan di `audit_logs`.
- Status dua event: `ANCHORED`, `snapshot_status=PENDING`, `integrity_status=NOT_CHECKED`; belum ada bukti pemeriksaan Fabric independen.
- Portal: screenshot sebelumnya mengarah ke portal Besu lama, bukan bukti portal client Mini PC.
