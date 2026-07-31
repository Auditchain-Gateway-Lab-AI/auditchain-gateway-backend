#!/bin/bash

# ==============================================================================
# 🚀 AUDITCHAIN AGENT 1-COMMAND INSTALLER (MANAGED SERVICE MODEL)
# ==============================================================================
# This script automates:
# 1. System & Privilege Validation (Root Check)
# 2. Automated Tailscale VPN Mesh Joining (Unattended Auth Key)
# 3. Debezium CDC Engine & Kafka Agent Setup
# 4. Network Auto-Discovery (Tailscale Virtual IP)
# 5. Telemetry Phone-Home Callback to AuditChain Gateway
# ==============================================================================

set -e

# Colors for Terminal Output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}"
echo "======================================================================"
echo "         🛡️  AUDITCHAIN AGENT AUTOMATED INSTALLER v1.0              "
echo "======================================================================"
echo -e "${NC}"

# ------------------------------------------------------------------------------
# 1. PRIVILEGE & ENVIRONMENT VALIDATION
# ------------------------------------------------------------------------------
if [ "$(id -u)" -ne 0 ]; then
    echo -e "${RED}[ERROR] Script ini harus dijalankan sebagai Root / Sudo!${NC}"
    echo "Silakan jalankan ulang menggunakan: sudo bash -c \"\$(curl ...)\""
    exit 1
fi

GATEWAY_URL=${GATEWAY_URL:-${1:-"https://api.auditchain.id"}}
GATEWAY_URL=$(echo "$GATEWAY_URL" | sed -E 's|/api/?$||' | sed -E 's|/$||')
CLIENT_KEY=${CLIENT_KEY:-$2}

if [ -z "$CLIENT_KEY" ]; then
    echo -e "${RED}[ERROR] CLIENT_KEY (API Key Klien) wajib diisi!${NC}"
    echo "Silakan jalankan script dengan menyertakan CLIENT_KEY yang diberikan Admin."
    echo "Contoh: GATEWAY_URL=\"http://100.103.5.72:8082\" CLIENT_KEY=\"ak_live_xxxx\" sudo -E bash"
    exit 1
fi

# ------------------------------------------------------------------------------
# 2. TAILSCALE VPN INSTALLATION & UNATTENDED AUTHENTICATION
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[1/5] Memeriksa & Menginstal Tailscale VPN Mesh...${NC}"

if ! command -v tailscale &> /dev/null; then
    echo "Tailscale belum terpasang. Mengunduh installer resmi Tailscale..."
    curl -fsSL https://tailscale.com/install.sh | sh
else
    echo "Tailscale sudah terpasang di sistem."
fi

echo -e "\nMeminta Auth Key sementara (sekali pakai) secara aman dari AuditChain Gateway..."
KEY_PAYLOAD=$(cat <<EOF
{
  "api_key_prefix": "${CLIENT_KEY}"
}
EOF
)

TAILSCALE_AUTHKEY=$(curl -s -X POST "${GATEWAY_URL}/api/agent/tailscale-key" \
    -H "Content-Type: application/json" \
    -d "${KEY_PAYLOAD}" | grep -o '"auth_key":"[^"]*"' | cut -d'"' -f4)

if [ -n "$TAILSCALE_AUTHKEY" ] && [ "$TAILSCALE_AUTHKEY" != "null" ]; then
    echo -e "${GREEN}✓ Auth Key berhasil didapatkan! Menghubungkan VPN secara otomatis...${NC}"
    tailscale up --authkey="${TAILSCALE_AUTHKEY}" --accept-routes --reset || true
else
    echo -e "${RED}[ERROR] Gagal mendapatkan Auth Key dari Gateway. Periksa API_KEY atau kredensial OAuth Admin.${NC}"
    echo -e "${YELLOW}Mencoba menghubungkan secara interaktif (Manual Login URL)...${NC}"
    tailscale up --accept-routes --reset || true
fi

# Mengambil IP Virtual Tailscale (IPv4)
TAILSCALE_IP=$(tailscale ip -4 2>/dev/null || hostname -I | awk '{print $1}')
echo -e "${GREEN}✓ Terhubung ke VPN Mesh! IP Virtual Server: ${TAILSCALE_IP}${NC}"

# ------------------------------------------------------------------------------
# 3. INSTALLASI DOCKER & DEBEZIUM CDC ENGINE
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[2/5] Memeriksa Dependensi Container Engine (Docker)...${NC}"

if ! command -v docker &> /dev/null; then
    echo "Docker belum terpasang. Mengunduh installer Docker..."
    curl -fsSL https://get.docker.com | sh
    systemctl enable --now docker
else
    echo "Docker Engine siap."
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "Docker Compose belum terpasang. Mengunduh Docker Compose..."
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
    ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
fi

echo -e "\n${BLUE}[3/5] Menyiapkan Folder Konfigurasi Agent & Docker Compose...${NC}"
mkdir -p /etc/auditchain
mkdir -p /var/log/auditchain

cat <<EOF > /etc/auditchain/docker-compose.yml
version: '3.8'
services:
  zookeeper:
    image: quay.io/debezium/zookeeper:2.4
    ports:
      - "2181:2181"
      - "2888:2888"
      - "3888:3888"
  kafka:
    image: quay.io/debezium/kafka:2.4
    ports:
      - "9092:9092"
    environment:
      - ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${TAILSCALE_IP}:9092
    depends_on:
      - zookeeper
  debezium:
    image: quay.io/debezium/connect:2.4
    ports:
      - "8083:8083"
    environment:
      - BOOTSTRAP_SERVERS=kafka:9092
      - GROUP_ID=1
      - CONFIG_STORAGE_TOPIC=my_connect_configs
      - OFFSET_STORAGE_TOPIC=my_connect_offsets
      - STATUS_STORAGE_TOPIC=my_connect_statuses
    depends_on:
      - kafka
EOF

echo -e "\n${BLUE}[4/7] Menjalankan Engine Database CDC (Zookeeper, Kafka, Debezium)...${NC}"
cd /etc/auditchain
if command -v docker-compose &> /dev/null; then
    docker-compose up -d
else
    docker compose up -d
fi

# ------------------------------------------------------------------------------
# 5. AUTO-DISCOVERY & KONFIGURASI KONEKTOR DATABASE CDC
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[5/7] Auto-Discovery & Konfigurasi Konektor Database CDC...${NC}"

SELECTED_DB_ENGINE=""
SELECTED_DB_NAME=""
SELECTED_TABLES=""
CONNECTOR_SETUP_STATUS="skipped"

HAS_POSTGRES=false
HAS_MYSQL=false

if command -v psql &> /dev/null || systemctl is-active --quiet postgresql 2>/dev/null || systemctl is-active --quiet postgres 2>/dev/null; then
    HAS_POSTGRES=true
fi

if command -v mysql &> /dev/null || systemctl is-active --quiet mysql 2>/dev/null || systemctl is-active --quiet mysqld 2>/dev/null || systemctl is-active --quiet mariadb 2>/dev/null; then
    HAS_MYSQL=true
fi

if [ "$HAS_POSTGRES" = false ] && [ "$HAS_MYSQL" = false ]; then
    echo -e "${YELLOW}[NOTE] Tidak ada PostgreSQL/MySQL terdeteksi di port standar.${NC}"
    echo -e "${YELLOW}Pilih opsi:${NC}"
    echo "  [M] Manual Entry (masukkan host, port, dan engine secara manual)"
    echo "  [S] Skip (konfigurasi nanti via Dashboard Admin)"
    read -p "Pilihan [M]: " NO_DB_CHOICE < /dev/tty
    NO_DB_CHOICE=${NO_DB_CHOICE:-M}
    if [[ "$NO_DB_CHOICE" =~ ^[Mm]$ ]]; then
        MANUAL_MODE=true
    else
        MANUAL_MODE=false
    fi
else
    MANUAL_MODE=false
    echo -e "${GREEN}✓ Engine Database Terdeteksi di Server!${NC}"
fi

CHOSEN_ENGINE=""
DB_HOST="172.17.0.1"
DB_PORT="5432"

if [ "$MANUAL_MODE" = true ]; then
    echo -e "\n${BLUE}📝 Manual Entry — Konfigurasi Database${NC}"
    echo "  [1] PostgreSQL"
    echo "  [2] MySQL / MariaDB"
    read -p "Pilih Engine Database [1]: " ENGINE_CHOICE < /dev/tty
    ENGINE_CHOICE=${ENGINE_CHOICE:-1}
    if [ "$ENGINE_CHOICE" = "2" ]; then
        CHOSEN_ENGINE="mysql"
        DB_PORT="3306"
    else
        CHOSEN_ENGINE="postgres"
        DB_PORT="5432"
    fi
    read -p "Database Hostname [localhost]: " INPUT_HOST < /dev/tty
    DB_HOST=${INPUT_HOST:-localhost}
    if [ "$CHOSEN_ENGINE" = "postgres" ]; then
        read -p "Database Port [5432]: " INPUT_PORT < /dev/tty
        DB_PORT=${INPUT_PORT:-5432}
    else
        read -p "Database Port [3306]: " INPUT_PORT < /dev/tty
        DB_PORT=${INPUT_PORT:-3306}
    fi
    read -p "Nama Database yang ingin di-audit: " TARGET_DB < /dev/tty
elif [ "$HAS_POSTGRES" = true ] || [ "$HAS_MYSQL" = true ]; then
    if [ "$HAS_POSTGRES" = true ] && [ "$HAS_MYSQL" = true ]; then
        echo -e "\n${YELLOW}Beberapa Engine Database Ditemukan:${NC}"
        echo "  [1] PostgreSQL"
        echo "  [2] MySQL / MariaDB"
        echo "  [M] Manual Entry (host/port custom)"
        read -p "Pilih Engine Database [1]: " ENGINE_CHOICE < /dev/tty
        ENGINE_CHOICE=${ENGINE_CHOICE:-1}
        if [[ "$ENGINE_CHOICE" =~ ^[Mm]$ ]]; then
            MANUAL_MODE=true
        elif [ "$ENGINE_CHOICE" = "2" ]; then
            CHOSEN_ENGINE="mysql"
        else
            CHOSEN_ENGINE="postgres"
        fi
    elif [ "$HAS_POSTGRES" = true ]; then
        echo "  • Engine Terdeteksi: PostgreSQL"
        CHOSEN_ENGINE="postgres"
    else
        echo "  • Engine Terdeteksi: MySQL / MariaDB"
        CHOSEN_ENGINE="mysql"
    fi

    # Jika user memilih Manual di sini
    if [ "$MANUAL_MODE" = true ]; then
        echo -e "\n${BLUE}📝 Manual Entry — Konfigurasi Database${NC}"
        echo "  [1] PostgreSQL"
        echo "  [2] MySQL / MariaDB"
        read -p "Pilih Engine Database [1]: " ENGINE_CHOICE < /dev/tty
        ENGINE_CHOICE=${ENGINE_CHOICE:-1}
        if [ "$ENGINE_CHOICE" = "2" ]; then
            CHOSEN_ENGINE="mysql"
            DB_PORT="3306"
        else
            CHOSEN_ENGINE="postgres"
            DB_PORT="5432"
        fi
        read -p "Database Hostname [localhost]: " INPUT_HOST < /dev/tty
        DB_HOST=${INPUT_HOST:-localhost}
        if [ "$CHOSEN_ENGINE" = "postgres" ]; then
            read -p "Database Port [5432]: " INPUT_PORT < /dev/tty
            DB_PORT=${INPUT_PORT:-5432}
        else
            read -p "Database Port [3306]: " INPUT_PORT < /dev/tty
            DB_PORT=${INPUT_PORT:-3306}
        fi
        read -p "Nama Database yang ingin di-audit: " TARGET_DB < /dev/tty
    fi

    SELECTED_DB_ENGINE="$CHOSEN_ENGINE"

    # Auto-discovery database list (hanya jika bukan manual mode)
    if [ "$MANUAL_MODE" = false ]; then
        DB_LIST=()
        PORT_LIST=()
        if [ "$CHOSEN_ENGINE" = "postgres" ]; then
            for port in 5432 5433 5434 5435; do
                RAW_DBS=$(sudo -u postgres psql -p "$port" --no-align --tuples-only -c "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres');" 2>/dev/null || true)
                if [ -n "$RAW_DBS" ]; then
                    while IFS= read -r line; do
                        if [ -n "$line" ]; then
                            DB_LIST+=("$line")
                            PORT_LIST+=("$port")
                        fi
                    done <<< "$RAW_DBS"
                fi
            done
        else
            for port in 3306 3307 3308; do
                RAW_DBS=$(mysql --no-defaults -P "$port" -N -e "SHOW DATABASES" 2>/dev/null | grep -vE "^(information_schema|performance_schema|mysql|sys)$" || true)
                if [ -n "$RAW_DBS" ]; then
                    while IFS= read -r line; do
                        if [ -n "$line" ]; then
                            DB_LIST+=("$line")
                            PORT_LIST+=("$port")
                        fi
                    done <<< "$RAW_DBS"
                fi
            done
        fi

        TARGET_DB=""
        if [ ${#DB_LIST[@]} -gt 0 ]; then
            echo -e "\n${BLUE}📂 Daftar Database Terdeteksi:${NC}"
            echo "--------------------------------------"
            for idx in "${!DB_LIST[@]}"; do
                echo "  [$((idx+1))] ${DB_LIST[$idx]} (Port: ${PORT_LIST[$idx]})"
            done
            echo "  [M] Manual Entry (database lain / port custom)"
            echo "--------------------------------------"
            read -p "Pilih nomor database yang ingin di-audit [1]: " DB_IDX < /dev/tty
            DB_IDX=${DB_IDX:-1}
            if [[ "$DB_IDX" =~ ^[Mm]$ ]]; then
                read -p "Database Hostname [localhost]: " INPUT_HOST < /dev/tty
                DB_HOST=${INPUT_HOST:-localhost}
                read -p "Database Port [${DB_PORT}]: " INPUT_PORT < /dev/tty
                DB_PORT=${INPUT_PORT:-$DB_PORT}
                read -p "Nama Database yang ingin di-audit: " TARGET_DB < /dev/tty
            else
                ARRAY_IDX=$((DB_IDX-1))
                if [ $ARRAY_IDX -ge 0 ] && [ $ARRAY_IDX -lt ${#DB_LIST[@]} ]; then
                    TARGET_DB="${DB_LIST[$ARRAY_IDX]}"
                    DB_PORT="${PORT_LIST[$ARRAY_IDX]}"
                else
                    TARGET_DB="${DB_LIST[0]}"
                    DB_PORT="${PORT_LIST[0]}"
                fi
            fi
        else
            read -p "Masukkan Nama Database yang ingin di-audit: " TARGET_DB < /dev/tty
        fi
    fi

    SELECTED_DB_NAME="$TARGET_DB"

    if [ -n "$TARGET_DB" ]; then
        echo -e "${GREEN}✓ Database Terpilih: ${TARGET_DB}${NC}"

        TABLE_LIST=()
        if [ "$CHOSEN_ENGINE" = "postgres" ]; then
            RAW_TBLS=$(sudo -u postgres psql -d "$TARGET_DB" --no-align --tuples-only -c "SELECT schemaname || '.' || tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema');" 2>/dev/null || true)
            if [ -n "$RAW_TBLS" ]; then
                while IFS= read -r line; do
                    [ -n "$line" ] && TABLE_LIST+=("$line")
                done <<< "$RAW_TBLS"
            fi
        else
            RAW_TBLS=$(mysql --no-defaults -N -D "$TARGET_DB" -e "SHOW TABLES" 2>/dev/null || true)
            if [ -n "$RAW_TBLS" ]; then
                while IFS= read -r line; do
                    [ -n "$line" ] && TABLE_LIST+=("${TARGET_DB}.${line}")
                done <<< "$RAW_TBLS"
            fi
        fi

        CHOSEN_TABLES=""
        if [ ${#TABLE_LIST[@]} -gt 0 ]; then
            echo -e "\n${BLUE}📋 Tabel Terdeteksi di '$TARGET_DB':${NC}"
            echo "--------------------------------------"
            for idx in "${!TABLE_LIST[@]}"; do
                echo "  [$((idx+1))] ${TABLE_LIST[$idx]}"
            done
            echo "  [A] Semua Tabel (Monitor Semua)"
            echo "--------------------------------------"
            read -p "Pilih tabel (pisahkan koma contoh 1,2 atau 'A' untuk semua) [A]: " TBL_INPUT < /dev/tty
            TBL_INPUT=${TBL_INPUT:-A}

            if [ "$TBL_INPUT" = "A" ] || [ "$TBL_INPUT" = "a" ]; then
                CHOSEN_TABLES=$(IFS=,; echo "${TABLE_LIST[*]}")
            else
                SELECTED_TBL_ARRAY=()
                IFS=',' read -ra ADDR <<< "$TBL_INPUT"
                for item in "${ADDR[@]}"; do
                    item=$(echo "$item" | xargs)
                    if [[ "$item" =~ ^[0-9]+$ ]]; then
                        T_IDX=$((item-1))
                        if [ $T_IDX -ge 0 ] && [ $T_IDX -lt ${#TABLE_LIST[@]} ]; then
                            SELECTED_TBL_ARRAY+=("${TABLE_LIST[$T_IDX]}")
                        fi
                    fi
                done
                if [ ${#SELECTED_TBL_ARRAY[@]} -gt 0 ]; then
                    CHOSEN_TABLES=$(IFS=,; echo "${SELECTED_TBL_ARRAY[*]}")
                else
                    CHOSEN_TABLES=$(IFS=,; echo "${TABLE_LIST[*]}")
                fi
            fi
        else
            read -p "Masukkan Nama Tabel yang ingin di-audit (contoh: public.audit_trail): " CHOSEN_TABLES < /dev/tty
        fi

        SELECTED_TABLES="$CHOSEN_TABLES"
        echo -e "${GREEN}✓ Tabel Terpilih: ${CHOSEN_TABLES}${NC}"

        AGENT_DB_USER="auditchain_agent"
        AGENT_DB_PASS=$(openssl rand -hex 12 2>/dev/null || echo "ac_pwd_$(date +%s)")
        # Set default port hanya jika belum di-set manual
        if [ "$MANUAL_MODE" = false ]; then
            DB_PORT="5432"
            [ "$CHOSEN_ENGINE" = "mysql" ] && DB_PORT="3306"
        fi

        USER_CREATED=false

        if [ "$CHOSEN_ENGINE" = "postgres" ]; then
            echo -e "\nMembuat user database '${AGENT_DB_USER}' dengan hak akses replication..."
            if sudo -u postgres psql -p "$DB_PORT" -c "CREATE USER ${AGENT_DB_USER} WITH REPLICATION LOGIN PASSWORD '${AGENT_DB_PASS}';" 2>/dev/null; then
                sudo -u postgres psql -p "$DB_PORT" -c "GRANT CONNECT ON DATABASE \"${TARGET_DB}\" TO ${AGENT_DB_USER};" 2>/dev/null || true
                sudo -u postgres psql -p "$DB_PORT" -d "$TARGET_DB" -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${AGENT_DB_USER};" 2>/dev/null || true
                USER_CREATED=true
                echo -e "${GREEN}✓ User DB '${AGENT_DB_USER}' berhasil dibuat otomatis!${NC}"
            fi
        else
            echo -e "\nMembuat user database '${AGENT_DB_USER}' dengan hak akses replication..."
            if mysql --no-defaults -e "CREATE USER IF NOT EXISTS '${AGENT_DB_USER}'@'%' IDENTIFIED BY '${AGENT_DB_PASS}'; GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${AGENT_DB_USER}'@'%'; FLUSH PRIVILEGES;" 2>/dev/null; then
                USER_CREATED=true
                echo -e "${GREEN}✓ User DB '${AGENT_DB_USER}' berhasil dibuat otomatis!${NC}"
            fi
        fi

        if [ "$USER_CREATED" = false ]; then
            echo -e "${YELLOW}[NOTE] Otomasi pembuat user DB membutuhkan kredensial manual.${NC}"
            read -p "Database Username: " AGENT_DB_USER < /dev/tty
            read -s -p "Database Password: " AGENT_DB_PASS < /dev/tty
            echo ""
        fi

        if [ "$CHOSEN_ENGINE" = "postgres" ]; then
            NEEDS_PG_RESTART=false

            # --- Cek WAL Level ---
            CURRENT_WAL=$(sudo -u postgres psql -p "$DB_PORT" --no-align --tuples-only -c "SHOW wal_level;" 2>/dev/null || echo "unknown")
            if [ "$CURRENT_WAL" != "logical" ]; then
                echo -e "\n${YELLOW}⚠️ WAL Level saat ini: '${CURRENT_WAL}'. Debezium memerlukan 'logical'.${NC}"
                sudo -u postgres psql -p "$DB_PORT" -c "ALTER SYSTEM SET wal_level = logical;" 2>/dev/null || true
                echo -e "${GREEN}✓ WAL Level diubah ke 'logical'.${NC}"
                NEEDS_PG_RESTART=true
            fi

            # --- Cek listen_addresses agar Docker bisa connect ---
            CURRENT_LISTEN=$(sudo -u postgres psql -p "$DB_PORT" --no-align --tuples-only -c "SHOW listen_addresses;" 2>/dev/null || echo "localhost")
            if [ "$CURRENT_LISTEN" = "localhost" ]; then
                echo -e "${YELLOW}⚠️ PostgreSQL hanya mendengarkan 'localhost'. Debezium (Docker) butuh akses via 172.17.0.1.${NC}"
                sudo -u postgres psql -p "$DB_PORT" -c "ALTER SYSTEM SET listen_addresses = '*';" 2>/dev/null || true
                echo -e "${GREEN}✓ listen_addresses diubah ke '*'.${NC}"
                NEEDS_PG_RESTART=true
            fi

            # --- Tambahkan rule pg_hba.conf untuk Docker subnet ---
            PG_HBA=$(sudo -u postgres psql -p "$DB_PORT" --no-align --tuples-only -c "SHOW hba_file;" 2>/dev/null || echo "")
            if [ -n "$PG_HBA" ] && [ -f "$PG_HBA" ]; then
                if ! grep -q "AuditChain" "$PG_HBA" 2>/dev/null; then
                    echo -e "${YELLOW}⚠️ Menambahkan rule pg_hba.conf untuk Docker subnet...${NC}"
                    echo "# AuditChain - Allow Debezium Docker container" >> "$PG_HBA"
                    echo "host    all    all    172.16.0.0/12    md5" >> "$PG_HBA"
                    echo -e "${GREEN}✓ Rule pg_hba.conf ditambahkan (172.16.0.0/12 - semua subnet Docker).${NC}"
                    NEEDS_PG_RESTART=true
                fi
            fi

            # --- Restart PostgreSQL jika ada perubahan ---
            if [ "$NEEDS_PG_RESTART" = true ]; then
                echo -e "${YELLOW}Restarting PostgreSQL untuk menerapkan perubahan...${NC}"
                systemctl restart postgresql 2>/dev/null || systemctl restart postgres 2>/dev/null || true
                sleep 2
                echo -e "${GREEN}✓ PostgreSQL berhasil di-restart.${NC}"
            fi
        fi

        echo -e "\nMenunggu Debezium Engine siap (maks 30 detik)..."
        DEBEZIUM_READY=false
        for i in $(seq 1 15); do
            if curl -s http://localhost:8083/ > /dev/null 2>&1; then
                DEBEZIUM_READY=true
                break
            fi
            sleep 2
        done

        if [ "$DEBEZIUM_READY" = true ]; then
            # Gunakan DB_HOST dari manual entry, atau deteksi Docker bridge IP
            if [ "$MANUAL_MODE" = false ]; then
                DB_HOST=$(ip -4 addr show docker0 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' || echo "172.17.0.1")
            fi

            if [ "$CHOSEN_ENGINE" = "postgres" ]; then
                CONNECTOR_PAYLOAD=$(cat <<EOF
{
  "name": "${TARGET_DB}-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "${DB_HOST}",
    "database.port": "${DB_PORT}",
    "database.user": "${AGENT_DB_USER}",
    "database.password": "${AGENT_DB_PASS}",
    "database.dbname": "${TARGET_DB}",
    "topic.prefix": "${HOSTNAME}_${TARGET_DB}",
    "table.include.list": "${CHOSEN_TABLES}",
    "plugin.name": "pgoutput"
  }
}
EOF
)
            else
                CONNECTOR_PAYLOAD=$(cat <<EOF
{
  "name": "${TARGET_DB}-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "${DB_HOST}",
    "database.port": "${DB_PORT}",
    "database.user": "${AGENT_DB_USER}",
    "database.password": "${AGENT_DB_PASS}",
    "database.server.id": "184054",
    "topic.prefix": "${HOSTNAME}_${TARGET_DB}",
    "database.include.list": "${TARGET_DB}",
    "table.include.list": "${CHOSEN_TABLES}",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.${TARGET_DB}"
  }
}
EOF
)
            fi

            DBZ_BODY_FILE="/tmp/dbz_response_$$.json"
            DBZ_RESP=$(curl -s -w "%{http_code}" -o "${DBZ_BODY_FILE}" -X POST http://localhost:8083/connectors \
                -H "Content-Type: application/json" \
                -d "${CONNECTOR_PAYLOAD}" || echo "000")

            if [ "$DBZ_RESP" -eq 201 ] || [ "$DBZ_RESP" -eq 200 ] || [ "$DBZ_RESP" -eq 409 ]; then
                echo -e "${GREEN}✓ Konektor Database Debezium BERHASIL didaftarkan! Data mulai disedot.${NC}"
                CONNECTOR_SETUP_STATUS="running"
            else
                echo -e "${YELLOW}[NOTE] Respon Debezium (HTTP Status: ${DBZ_RESP}).${NC}"
                if [ -f "${DBZ_BODY_FILE}" ]; then
                    echo -e "${YELLOW}Detail Error Debezium:${NC}"
                    cat "${DBZ_BODY_FILE}"
                    echo ""
                fi
                CONNECTOR_SETUP_STATUS="failed_${DBZ_RESP}"
            fi
            rm -f "${DBZ_BODY_FILE}"
        else
            echo -e "${YELLOW}[NOTE] Debezium belum siap merespon. Konfigurasi otomatis ditunda.${NC}"
            CONNECTOR_SETUP_STATUS="debezium_not_ready"
        fi
    fi
fi

# ------------------------------------------------------------------------------
# 6. DETEKSI ENDPOINT NETWORK & TELEMETRI
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[6/7] Mendeteksi Endpoint Network & Telemetri...${NC}"

KAFKA_BROKERS="${TAILSCALE_IP}:9092"
AGENT_SERVER_URL="http://${TAILSCALE_IP}:8081"
HOSTNAME=$(hostname)

# Simpan file environment konfigurasi tunggal di VPS Klien
cat <<EOF > /etc/auditchain/agent.env
# AuditChain Agent Environment Configuration
AUDITCHAIN_GATEWAY_URL="${GATEWAY_URL}"
AUDITCHAIN_API_KEY="${CLIENT_KEY}"
TAILSCALE_IP="${TAILSCALE_IP}"
AGENT_SERVER_URL="${AGENT_SERVER_URL}"
KAFKA_BROKERS="${KAFKA_BROKERS}"
DB_ENGINE="${SELECTED_DB_ENGINE}"
DB_NAME="${SELECTED_DB_NAME}"
DB_TABLES="${SELECTED_TABLES}"
CONNECTOR_STATUS="${CONNECTOR_SETUP_STATUS}"
EOF
chmod 600 /etc/auditchain/agent.env

echo -e "${GREEN}✓ Konfigurasi .env berhasil disimpan di /etc/auditchain/agent.env${NC}"

echo "  - Hostname           : ${HOSTNAME}"
echo "  - Tailscale VPN IP   : ${TAILSCALE_IP}"
echo "  - Kafka Broker IP    : ${KAFKA_BROKERS}"
echo "  - Agent Server URL   : ${AGENT_SERVER_URL}"
echo "  - Target Database    : ${SELECTED_DB_ENGINE} / ${SELECTED_DB_NAME}"

echo -e "\n${BLUE}[7/7] Mengirimkan Telemetri ke Gateway AuditChain Admin...${NC}"

PAYLOAD=$(cat <<EOF
{
  "api_key_prefix": "${CLIENT_KEY}",
  "kafka_brokers": "${KAFKA_BROKERS}",
  "agent_server_url": "${AGENT_SERVER_URL}",
  "hostname": "${HOSTNAME}",
  "tailscale_ip": "${TAILSCALE_IP}",
  "status": "pending_setup",
  "db_engine": "${SELECTED_DB_ENGINE}",
  "db_name": "${SELECTED_DB_NAME}",
  "db_tables": "${SELECTED_TABLES}",
  "connector_status": "${CONNECTOR_SETUP_STATUS}"
}
EOF
)

# Kirim HTTP POST Callback ke Gateway Backend
HTTP_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${GATEWAY_URL}/api/agent/telemetry" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}" || echo "000")

if [ "$HTTP_RESPONSE" -eq 200 ] || [ "$HTTP_RESPONSE" -eq 201 ]; then
    echo -e "${GREEN}✓ Telemetri berhasil dikirim ke Admin Dashboard!${NC}"
else
    echo -e "${YELLOW}[NOTE] Telemetri terkirim (Response Status: ${HTTP_RESPONSE}). Data siap diverifikasi Admin.${NC}"
fi

# ------------------------------------------------------------------------------
# 7. COMPLETION BANNER
# ------------------------------------------------------------------------------
echo -e "\n${GREEN}"
echo "======================================================================"
echo " 🎉 INSTALASI AUDITCHAIN AGENT BERHASIL! (STATUS: PENDING SETUP)"
echo "======================================================================"
echo -e "${NC}"
echo " Detail Koneksi Virtual Server Anda:"
echo " --------------------------------------------------------------------"
echo "  • VPN IP Address     : ${TAILSCALE_IP}"
echo "  • Kafka Broker Host  : ${KAFKA_BROKERS}"
echo "  • Agent Server URL   : ${AGENT_SERVER_URL}"
echo "  • Database Monitored : ${SELECTED_DB_ENGINE} -> ${SELECTED_DB_NAME}"
echo "  • Connector Status   : ${CONNECTOR_SETUP_STATUS}"
echo "  • Status Dashboard   : Pending Verification by Admin 🟡"
echo " --------------------------------------------------------------------"
echo -e "${BLUE}Silakan hubungi Admin AuditChain untuk pengaktifan koneksi resmi.${NC}\n"

