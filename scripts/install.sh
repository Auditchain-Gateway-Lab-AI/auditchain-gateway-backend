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

# ==============================================================================
# MODE --update: Pembaruan Ringan untuk Klien yang Sudah Terinstall
# ==============================================================================
if [ "$1" = "--update" ] || [ "${UPDATE_MODE}" = "true" ]; then
    echo -e "\n${BLUE}======================================================================${NC}"
    echo -e "${BLUE}         🔄 AUDITCHAIN AGENT UPDATE MODE                            ${NC}"
    echo -e "${BLUE}======================================================================${NC}\n"

    if [ ! -f /etc/auditchain/docker-compose.yml ]; then
        echo -e "${RED}[ERROR] Auditchain Agent belum terinstall di server ini!${NC}"
        echo -e "Jalankan installer penuh terlebih dahulu (tanpa --update)."
        exit 1
    fi

    TAILSCALE_IP=$(tailscale ip -4 2>/dev/null || hostname -I | awk '{print $1}')
    echo -e "${GREEN}✓ Tailscale IP: ${TAILSCALE_IP}${NC}"

    echo -e "\n${YELLOW}📦 Memperbarui Docker Compose (restart policy + healthcheck)...${NC}"

    cat <<UPDATEEOF > /etc/auditchain/docker-compose.yml
version: '3.8'
services:
  zookeeper:
    image: quay.io/debezium/zookeeper:2.4
    restart: always
    ports:
      - "2181:2181"
      - "2888:2888"
      - "3888:3888"
    healthcheck:
      test: ["CMD-SHELL", "echo ruok | nc localhost 2181 | grep imok"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
  kafka:
    image: quay.io/debezium/kafka:2.4
    restart: always
    ports:
      - "9092:9092"
    environment:
      - ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${TAILSCALE_IP}:9092
    depends_on:
      zookeeper:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 30s
  debezium:
    image: quay.io/debezium/connect:2.4
    restart: always
    volumes:
      - /etc/auditchain/jdbc-drivers/ojdbc8.jar:/kafka/connect/debezium-connector-oracle/ojdbc8.jar
    ports:
      - "8083:8083"
    environment:
      - BOOTSTRAP_SERVERS=kafka:9092
      - GROUP_ID=1
      - CONFIG_STORAGE_TOPIC=my_connect_configs
      - OFFSET_STORAGE_TOPIC=my_connect_offsets
      - STATUS_STORAGE_TOPIC=my_connect_statuses
    depends_on:
      kafka:
        condition: service_healthy
UPDATEEOF

    echo -e "${GREEN}✓ docker-compose.yml berhasil diperbarui!${NC}"

    echo -e "\n${YELLOW}🔄 Menerapkan perubahan (recreate container)...${NC}"
    cd /etc/auditchain
    if command -v docker-compose &> /dev/null; then
        docker-compose up -d
    else
        docker compose up -d
    fi

    echo -e "\n${GREEN}======================================================================${NC}"
    echo -e "${GREEN}  ✅ UPDATE BERHASIL! Container sekarang Tahan Mati Lampu.           ${NC}"
    echo -e "${GREEN}======================================================================${NC}"
    echo -e "  • restart: always  → Container otomatis hidup saat server boot"
    echo -e "  • healthcheck      → Urutan nyala dijaga: Zookeeper → Kafka → Debezium"
    echo -e "  • Data CDC         → Tidak hilang (offset tersimpan di Kafka)\n"
    exit 0
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
mkdir -p /etc/auditchain/jdbc-drivers

echo "Mengunduh driver JDBC yang dibutuhkan..."
curl -fsSL "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.1.0.0/ojdbc8-21.1.0.0.jar" -o /etc/auditchain/jdbc-drivers/ojdbc8.jar 2>/dev/null || true

cat <<EOF > /etc/auditchain/docker-compose.yml
version: '3.8'
services:
  zookeeper:
    image: quay.io/debezium/zookeeper:2.4
    restart: always
    ports:
      - "2181:2181"
      - "2888:2888"
      - "3888:3888"
    healthcheck:
      test: ["CMD-SHELL", "echo ruok | nc localhost 2181 | grep imok"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
  kafka:
    image: quay.io/debezium/kafka:2.4
    restart: always
    ports:
      - "9092:9092"
    environment:
      - ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${TAILSCALE_IP}:9092
    depends_on:
      zookeeper:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 30s
  debezium:
    image: quay.io/debezium/connect:2.4
    restart: always
    volumes:
      - /etc/auditchain/jdbc-drivers/ojdbc8.jar:/kafka/connect/debezium-connector-oracle/ojdbc8.jar
    ports:
      - "8083:8083"
    environment:
      - BOOTSTRAP_SERVERS=kafka:9092
      - GROUP_ID=1
      - CONFIG_STORAGE_TOPIC=my_connect_configs
      - OFFSET_STORAGE_TOPIC=my_connect_offsets
      - STATUS_STORAGE_TOPIC=my_connect_statuses
    depends_on:
      kafka:
        condition: service_healthy
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
HAS_ORACLE=false

if command -v psql &> /dev/null || systemctl is-active --quiet postgresql 2>/dev/null || systemctl is-active --quiet postgres 2>/dev/null || [ -n "$(docker ps --filter "ancestor=postgres" -q 2>/dev/null)" ]; then
    HAS_POSTGRES=true
fi

if command -v mysql &> /dev/null || systemctl is-active --quiet mysql 2>/dev/null || systemctl is-active --quiet mysqld 2>/dev/null || systemctl is-active --quiet mariadb 2>/dev/null || [ -n "$(docker ps --filter "ancestor=mysql" -q 2>/dev/null)" ]; then
    HAS_MYSQL=true
fi

if command -v sqlplus &> /dev/null || systemctl is-active --quiet oracle* 2>/dev/null || [ -n "$(docker ps --format '{{.Image}}' 2>/dev/null | grep -i oracle)" ]; then
    HAS_ORACLE=true
fi

HAS_MONGODB=false
if command -v mongosh &> /dev/null || command -v mongo &> /dev/null || systemctl is-active --quiet mongod 2>/dev/null || [ -n "$(docker ps --filter "ancestor=mongo" -q 2>/dev/null)" ]; then
    HAS_MONGODB=true
fi

HAS_SQLSERVER=false
if command -v sqlcmd &> /dev/null || systemctl is-active --quiet mssql-server 2>/dev/null || [ -n "$(docker ps --format '{{.Image}}' 2>/dev/null | grep -iE 'mssql|sqlserver')" ]; then
    HAS_SQLSERVER=true
fi

if [ "$HAS_POSTGRES" = false ] && [ "$HAS_MYSQL" = false ] && [ "$HAS_ORACLE" = false ] && [ "$HAS_MONGODB" = false ] && [ "$HAS_SQLSERVER" = false ]; then
    echo -e "${YELLOW}[NOTE] Tidak ada Database Engine terdeteksi di server ini.${NC}"
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
    echo "  [3] Oracle Database"
    echo "  [4] MongoDB"
    echo "  [5] SQL Server"
    read -p "Pilih Engine Database [1]: " ENGINE_CHOICE < /dev/tty
    ENGINE_CHOICE=${ENGINE_CHOICE:-1}
    if [ "$ENGINE_CHOICE" = "5" ]; then
        CHOSEN_ENGINE="sqlserver"
        DB_PORT="1433"
    elif [ "$ENGINE_CHOICE" = "4" ]; then
        CHOSEN_ENGINE="mongodb"
        DB_PORT="27017"
    elif [ "$ENGINE_CHOICE" = "3" ]; then
        CHOSEN_ENGINE="oracle"
        DB_PORT="1521"
    elif [ "$ENGINE_CHOICE" = "2" ]; then
        CHOSEN_ENGINE="mysql"
        DB_PORT="3306"
    else
        CHOSEN_ENGINE="postgres"
        DB_PORT="5432"
    fi
    read -p "Database Hostname [localhost]: " INPUT_HOST < /dev/tty
    DB_HOST=${INPUT_HOST:-localhost}
    if [ "$CHOSEN_ENGINE" = "mongodb" ]; then
        read -p "Database Port [27017]: " INPUT_PORT < /dev/tty
        DB_PORT=${INPUT_PORT:-27017}
    elif [ "$CHOSEN_ENGINE" = "sqlserver" ]; then
        read -p "Database Port [1433]: " INPUT_PORT < /dev/tty
        DB_PORT=${INPUT_PORT:-1433}
    elif [ "$CHOSEN_ENGINE" = "oracle" ]; then
        read -p "Database Port [1521]: " INPUT_PORT < /dev/tty
        DB_PORT=${INPUT_PORT:-1521}
    elif [ "$CHOSEN_ENGINE" = "postgres" ]; then
        read -p "Database Port [5432]: " INPUT_PORT < /dev/tty
        DB_PORT=${INPUT_PORT:-5432}
    else
        read -p "Database Port [3306]: " INPUT_PORT < /dev/tty
        DB_PORT=${INPUT_PORT:-3306}
    fi
    read -p "Nama Database (SID/Service Name/DB Name) yang ingin di-audit: " TARGET_DB < /dev/tty
elif [ "$HAS_POSTGRES" = true ] || [ "$HAS_MYSQL" = true ] || [ "$HAS_ORACLE" = true ] || [ "$HAS_MONGODB" = true ] || [ "$HAS_SQLSERVER" = true ]; then
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
    elif [ "$HAS_MYSQL" = true ]; then
        echo "  • Engine Terdeteksi: MySQL / MariaDB"
        CHOSEN_ENGINE="mysql"
    elif [ "$HAS_SQLSERVER" = true ]; then
        echo "  • Engine Terdeteksi: SQL Server"
        CHOSEN_ENGINE="sqlserver"
        DB_PORT="1433"
    elif [ "$HAS_ORACLE" = true ]; then
        echo "  • Engine Terdeteksi: Oracle Database (Membutuhkan Input SID/Service Name)"
        CHOSEN_ENGINE="oracle"
        DB_PORT="1521"
        MANUAL_MODE=false
    elif [ "$HAS_MONGODB" = true ]; then
        echo "  • Engine Terdeteksi: MongoDB (Membutuhkan Input Database Name)"
        CHOSEN_ENGINE="mongodb"
        DB_PORT="27017"
        MANUAL_MODE=false
    else
        echo "  • Engine Terdeteksi: Tidak Diketahui"
        MANUAL_MODE=true
    fi

    # Jika user memilih Manual di sini
    if [ "$MANUAL_MODE" = true ]; then
        echo -e "\n${BLUE}📝 Manual Entry — Konfigurasi Database${NC}"
        echo "  [1] PostgreSQL"
        echo "  [2] MySQL / MariaDB"
        echo "  [3] Oracle Database"
        echo "  [4] MongoDB"
        echo "  [5] SQL Server"
        read -p "Pilih Engine Database [1]: " ENGINE_CHOICE < /dev/tty
        ENGINE_CHOICE=${ENGINE_CHOICE:-1}
        if [ "$ENGINE_CHOICE" = "5" ]; then
            CHOSEN_ENGINE="sqlserver"
            DB_PORT="1433"
        elif [ "$ENGINE_CHOICE" = "4" ]; then
            CHOSEN_ENGINE="mongodb"
            DB_PORT="27017"
        elif [ "$ENGINE_CHOICE" = "3" ]; then
            CHOSEN_ENGINE="oracle"
            DB_PORT="1521"
        elif [ "$ENGINE_CHOICE" = "2" ]; then
            CHOSEN_ENGINE="mysql"
            DB_PORT="3306"
        else
            CHOSEN_ENGINE="postgres"
            DB_PORT="5432"
        fi
        read -p "Database Hostname [localhost]: " INPUT_HOST < /dev/tty
        DB_HOST=${INPUT_HOST:-localhost}
        if [ "$CHOSEN_ENGINE" = "mongodb" ]; then
            read -p "Database Port [27017]: " INPUT_PORT < /dev/tty
            DB_PORT=${INPUT_PORT:-27017}
        elif [ "$CHOSEN_ENGINE" = "sqlserver" ]; then
            read -p "Database Port [1433]: " INPUT_PORT < /dev/tty
            DB_PORT=${INPUT_PORT:-1433}
        elif [ "$CHOSEN_ENGINE" = "oracle" ]; then
            read -p "Database Port [1521]: " INPUT_PORT < /dev/tty
            DB_PORT=${INPUT_PORT:-1521}
        elif [ "$CHOSEN_ENGINE" = "postgres" ]; then
            read -p "Database Port [5432]: " INPUT_PORT < /dev/tty
            DB_PORT=${INPUT_PORT:-5432}
        else
            read -p "Database Port [3306]: " INPUT_PORT < /dev/tty
            DB_PORT=${INPUT_PORT:-3306}
        fi
        read -p "Nama Database (SID/Service Name/DB Name) yang ingin di-audit: " TARGET_DB < /dev/tty
    fi
fi

SELECTED_DB_ENGINE="$CHOSEN_ENGINE"

    # Auto-discovery database list (hanya jika bukan manual mode)
    if [ "$MANUAL_MODE" = false ]; then
        DB_LIST=()
        PORT_LIST=()
        LABEL_LIST=()

        if [ "$CHOSEN_ENGINE" = "postgres" ]; then
            # --- 1. Native PostgreSQL Scan ---
            for port in 5432 5433 5434 5435; do
                RAW_DBS=$(sudo -u postgres psql -p "$port" --no-align --tuples-only -c "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres');" 2>/dev/null || true)
                if [ -n "$RAW_DBS" ]; then
                    while IFS= read -r line; do
                        if [ -n "$line" ]; then
                            DB_LIST+=("$line")
                            PORT_LIST+=("$port")
                            LABEL_LIST+=("Native")
                        fi
                    done <<< "$RAW_DBS"
                fi
            done

            # --- 2. Docker PostgreSQL Scan ---
            if command -v docker >/dev/null 2>&1; then
                DOCKER_CONTAINERS=$(docker ps --filter "ancestor=postgres" --format "{{.ID}}|{{.Names}}" 2>/dev/null || true)
                if [ -n "$DOCKER_CONTAINERS" ]; then
                    while IFS='|' read -r c_id c_name; do
                        RAW_DBS=$(docker exec "$c_id" psql -U postgres --no-align --tuples-only -c "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres');" 2>/dev/null || true)
                        if [ -n "$RAW_DBS" ]; then
                            MAPPED_PORT=$(docker port "$c_id" 5432 2>/dev/null | grep -oP ':\K\d+' | head -n 1)
                            MAPPED_PORT=${MAPPED_PORT:-"?"}
                            while IFS= read -r line; do
                                if [ -n "$line" ]; then
                                    DB_LIST+=("$line")
                                    PORT_LIST+=("$MAPPED_PORT")
                                    LABEL_LIST+=("Docker: $c_name")
                                fi
                            done <<< "$RAW_DBS"
                        fi
                    done <<< "$DOCKER_CONTAINERS"
                fi
            fi

        elif [ "$CHOSEN_ENGINE" = "mysql" ]; then
            # --- 1. Native MySQL Scan ---
            for port in 3306 3307 3308; do
                RAW_DBS=$(mysql --no-defaults -P "$port" -N -e "SHOW DATABASES" 2>/dev/null | grep -vE "^(information_schema|performance_schema|mysql|sys)$" || true)
                if [ -n "$RAW_DBS" ]; then
                    while IFS= read -r line; do
                        if [ -n "$line" ]; then
                            DB_LIST+=("$line")
                            PORT_LIST+=("$port")
                            LABEL_LIST+=("Native")
                        fi
                    done <<< "$RAW_DBS"
                fi
            done

            # --- 2. Docker MySQL Scan ---
            if command -v docker >/dev/null 2>&1; then
                DOCKER_CONTAINERS=$(docker ps --filter "ancestor=mysql" --format "{{.ID}}|{{.Names}}" 2>/dev/null || true)
                if [ -n "$DOCKER_CONTAINERS" ]; then
                    while IFS='|' read -r c_id c_name; do
                        # Coba koneksi root tanpa password
                        RAW_DBS=$(docker exec "$c_id" mysql -u root -N -e "SHOW DATABASES;" 2>/dev/null | grep -vE "^(information_schema|performance_schema|mysql|sys)$" || true)
                        if [ -n "$RAW_DBS" ]; then
                            MAPPED_PORT=$(docker port "$c_id" 3306 2>/dev/null | grep -oP ':\K\d+' | head -n 1)
                            MAPPED_PORT=${MAPPED_PORT:-"?"}
                            while IFS= read -r line; do
                                if [ -n "$line" ]; then
                                    DB_LIST+=("$line")
                                    PORT_LIST+=("$MAPPED_PORT")
                                    LABEL_LIST+=("Docker: $c_name")
                                fi
                            done <<< "$RAW_DBS"
                        fi
                    done <<< "$DOCKER_CONTAINERS"
                fi
            fi

        elif [ "$CHOSEN_ENGINE" = "mongodb" ]; then
            # --- 1. Native MongoDB Scan ---
            RAW_DBS=$(mongosh --quiet --eval "db.getMongo().getDBNames().join('\n')" 2>/dev/null || mongo --quiet --eval "db.getMongo().getDBNames().join('\n')" 2>/dev/null | grep -vE "^(admin|config|local)$" || true)
            if [ -n "$RAW_DBS" ]; then
                while IFS= read -r line; do
                    if [ -n "$line" ]; then
                        DB_LIST+=("$line")
                        PORT_LIST+=("27017")
                        LABEL_LIST+=("Native")
                    fi
                done <<< "$RAW_DBS"
            fi
            
            # --- 2. Docker MongoDB Scan ---
            if command -v docker >/dev/null 2>&1; then
                DOCKER_CONTAINERS=$(docker ps --filter "ancestor=mongo" --format "{{.ID}}|{{.Names}}" 2>/dev/null || true)
                if [ -n "$DOCKER_CONTAINERS" ]; then
                    while IFS='|' read -r c_id c_name; do
                        RAW_DBS=$(docker exec "$c_id" mongosh --quiet --eval "db.getMongo().getDBNames().join('\n')" 2>/dev/null || docker exec "$c_id" mongo --quiet --eval "db.getMongo().getDBNames().join('\n')" 2>/dev/null | grep -vE "^(admin|config|local)$" || true)
                        if [ -n "$RAW_DBS" ]; then
                            MAPPED_PORT=$(docker port "$c_id" 27017 2>/dev/null | grep -oP ':\K\d+' | head -n 1)
                            MAPPED_PORT=${MAPPED_PORT:-"?"}
                            while IFS= read -r line; do
                                if [ -n "$line" ]; then
                                    DB_LIST+=("$line")
                                    PORT_LIST+=("$MAPPED_PORT")
                                    LABEL_LIST+=("Docker: $c_name")
                                fi
                            done <<< "$RAW_DBS"
                        fi
                    done <<< "$DOCKER_CONTAINERS"
                fi
            fi

        elif [ "$CHOSEN_ENGINE" = "sqlserver" ]; then
            # Hanya deteksi instance (port), karena otentikasi wajib untuk baca DB list
            if command -v docker >/dev/null 2>&1; then
                DOCKER_CONTAINERS=$(docker ps --format "{{.ID}}|{{.Names}}|{{.Image}}" 2>/dev/null | grep -iE "mssql|sqlserver" || true)
                if [ -n "$DOCKER_CONTAINERS" ]; then
                    while IFS='|' read -r c_id c_name c_img; do
                        MAPPED_PORT=$(docker port "$c_id" 1433 2>/dev/null | grep -oP ':\K\d+' | head -n 1)
                        MAPPED_PORT=${MAPPED_PORT:-"1433"}
                        DB_LIST+=("<INPUT_MANUAL>")
                        PORT_LIST+=("$MAPPED_PORT")
                        LABEL_LIST+=("Docker: $c_name")
                    done <<< "$DOCKER_CONTAINERS"
                fi
            fi
            # Native fallback jika sqlcmd ada dan bukan docker
            if [ ${#DB_LIST[@]} -eq 0 ] && command -v sqlcmd &> /dev/null; then
                DB_LIST+=("<INPUT_MANUAL>")
                PORT_LIST+=("1433")
                LABEL_LIST+=("Native")
            fi

        elif [ "$CHOSEN_ENGINE" = "oracle" ]; then
            # Sama seperti SQL Server, deteksi instance docker
            if command -v docker >/dev/null 2>&1; then
                DOCKER_CONTAINERS=$(docker ps --format "{{.ID}}|{{.Names}}|{{.Image}}" 2>/dev/null | grep -i "oracle" || true)
                if [ -n "$DOCKER_CONTAINERS" ]; then
                    while IFS='|' read -r c_id c_name c_img; do
                        MAPPED_PORT=$(docker port "$c_id" 1521 2>/dev/null | grep -oP ':\K\d+' | head -n 1)
                        MAPPED_PORT=${MAPPED_PORT:-"1521"}
                        DB_LIST+=("<INPUT_MANUAL>")
                        PORT_LIST+=("$MAPPED_PORT")
                        LABEL_LIST+=("Docker: $c_name")
                    done <<< "$DOCKER_CONTAINERS"
                fi
            fi
        fi

        TARGET_DB=""
        if [ ${#DB_LIST[@]} -gt 0 ]; then
            echo -e "\n${BLUE}📂 Daftar Database Terdeteksi:${NC}"
            echo "--------------------------------------"
            for idx in "${!DB_LIST[@]}"; do
                if [ "${DB_LIST[$idx]}" = "<INPUT_MANUAL>" ]; then
                    echo "  [$((idx+1))] [Input Nama Database Manual] (Port: ${PORT_LIST[$idx]} | ${LABEL_LIST[$idx]})"
                else
                    echo "  [$((idx+1))] ${DB_LIST[$idx]} (Port: ${PORT_LIST[$idx]} | ${LABEL_LIST[$idx]})"
                fi
            done
            echo "  [M] Manual Entry (database lain / port custom)"
            echo "--------------------------------------"
            read -p "Pilih nomor database yang ingin di-audit [1]: " DB_IDX < /dev/tty
            DB_IDX=${DB_IDX:-1}
            if [[ "$DB_IDX" =~ ^[Mm]$ ]]; then
                MANUAL_MODE=true
                echo -e "\n${BLUE}📝 Manual Entry — Konfigurasi Database${NC}"
                echo "  [1] PostgreSQL"
                echo "  [2] MySQL / MariaDB"
                echo "  [3] Oracle Database"
                echo "  [4] MongoDB"
                echo "  [5] SQL Server"
                read -p "Pilih Engine Database [1]: " ENGINE_CHOICE < /dev/tty
                ENGINE_CHOICE=${ENGINE_CHOICE:-1}
                if [ "$ENGINE_CHOICE" = "5" ]; then
                    CHOSEN_ENGINE="sqlserver"
                    DB_PORT="1433"
                elif [ "$ENGINE_CHOICE" = "4" ]; then
                    CHOSEN_ENGINE="mongodb"
                    DB_PORT="27017"
                elif [ "$ENGINE_CHOICE" = "3" ]; then
                    CHOSEN_ENGINE="oracle"
                    DB_PORT="1521"
                elif [ "$ENGINE_CHOICE" = "2" ]; then
                    CHOSEN_ENGINE="mysql"
                    DB_PORT="3306"
                else
                    CHOSEN_ENGINE="postgres"
                    DB_PORT="5432"
                fi
                SELECTED_DB_ENGINE="$CHOSEN_ENGINE"
                
                read -p "Database Hostname [localhost]: " INPUT_HOST < /dev/tty
                DB_HOST=${INPUT_HOST:-localhost}
                read -p "Database Port [${DB_PORT}]: " INPUT_PORT < /dev/tty
                DB_PORT=${INPUT_PORT:-$DB_PORT}
                read -p "Nama Database yang ingin di-audit: " TARGET_DB < /dev/tty
            else
                ARRAY_IDX=$((DB_IDX-1))
                if [ $ARRAY_IDX -ge 0 ] && [ $ARRAY_IDX -lt ${#DB_LIST[@]} ]; then
                    if [ "${DB_LIST[$ARRAY_IDX]}" = "<INPUT_MANUAL>" ]; then
                        read -p "Nama Database (SID/Service Name/DB Name) yang ingin di-audit: " TARGET_DB < /dev/tty
                    else
                        TARGET_DB="${DB_LIST[$ARRAY_IDX]}"
                    fi
                    DB_PORT="${PORT_LIST[$ARRAY_IDX]}"
                else
                    if [ "${DB_LIST[0]}" = "<INPUT_MANUAL>" ]; then
                        read -p "Nama Database (SID/Service Name/DB Name) yang ingin di-audit: " TARGET_DB < /dev/tty
                    else
                        TARGET_DB="${DB_LIST[0]}"
                    fi
                    DB_PORT="${PORT_LIST[0]}"
                fi

                # Jika berasal dari Docker dan portnya tidak tertebak
                if [ "$DB_PORT" = "?" ]; then
                    echo -e "\n${YELLOW}⚠️ Tidak dapat mendeteksi Port Host (Port Mapping) untuk Docker Container ini.${NC}"
                    read -p "Masukkan Port Host yang ter-mapping ke container ini: " INPUT_PORT < /dev/tty
                    DB_PORT=${INPUT_PORT}
                    # Default Hostname untuk akses Docker via TCP Host
                    DB_HOST="127.0.0.1"
                fi
            fi
        else
            read -p "Masukkan Nama Database yang ingin di-audit: " TARGET_DB < /dev/tty
        fi
    fi

    SELECTED_DB_NAME="$TARGET_DB"
    ORACLE_PDB=""

    if [ "$CHOSEN_ENGINE" = "oracle" ]; then
        echo -e "\n${YELLOW}ℹ️  Oracle versi baru (19c+) menggunakan arsitektur CDB/PDB.${NC}"
        read -p "Apakah tabel aplikasi Anda berada di dalam Pluggable Database (PDB) misal XEPDB1? (y/N): " USE_PDB < /dev/tty
        if [[ "$USE_PDB" =~ ^[Yy]$ ]]; then
            read -p "Masukkan nama Pluggable Database (PDB): " ORACLE_PDB < /dev/tty
        fi
    fi

    if [ -n "$TARGET_DB" ]; then
        echo -e "${GREEN}✓ Database Terpilih: ${TARGET_DB}${NC}"
        if [ -n "$ORACLE_PDB" ]; then
            echo -e "${GREEN}✓ PDB Terpilih: ${ORACLE_PDB}${NC}"
            # Kirim telemetry PDB as TARGET_DB so Gateway registers the PDB name correctly
            SELECTED_DB_NAME="$ORACLE_PDB"
        fi

        TABLE_LIST=()
        if [ "$CHOSEN_ENGINE" = "postgres" ]; then
            RAW_TBLS=$(sudo -u postgres psql -d "$TARGET_DB" --no-align --tuples-only -c "SELECT schemaname || '.' || tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema');" 2>/dev/null || true)
            if [ -n "$RAW_TBLS" ]; then
                while IFS= read -r line; do
                    [ -n "$line" ] && TABLE_LIST+=("$line")
                done <<< "$RAW_TBLS"
            fi
        elif [ "$CHOSEN_ENGINE" = "mysql" ]; then
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
            read -p "Masukkan Nama Tabel/Koleksi yang ingin di-audit (contoh: public.audit_trail atau targetdb.koleksi): " CHOSEN_TABLES < /dev/tty
        fi

        SELECTED_TABLES="$CHOSEN_TABLES"
        echo -e "${GREEN}✓ Tabel Terpilih: ${CHOSEN_TABLES}${NC}"

        AGENT_DB_USER="auditchain_agent"
        AGENT_DB_PASS=$(openssl rand -hex 12 2>/dev/null || echo "ac_pwd_$(date +%s)")
        # Set default port hanya jika belum di-set manual
        if [ "$MANUAL_MODE" = false ]; then
            DB_PORT="5432"
            [ "$CHOSEN_ENGINE" = "mysql" ] && DB_PORT="3306"
            [ "$CHOSEN_ENGINE" = "sqlserver" ] && DB_PORT="1433"
            [ "$CHOSEN_ENGINE" = "mongodb" ] && DB_PORT="27017"
            [ "$CHOSEN_ENGINE" = "oracle" ] && DB_PORT="1521"
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
            # Buat Publication untuk Debezium (membutuhkan superuser)
            echo -e "Membuat Publication CDC untuk Debezium..."
            sudo -u postgres psql -p "$DB_PORT" -d "$TARGET_DB" -c "DROP PUBLICATION IF EXISTS dbz_publication;" 2>/dev/null || true
            sudo -u postgres psql -p "$DB_PORT" -d "$TARGET_DB" -c "CREATE PUBLICATION dbz_publication FOR TABLE ${CHOSEN_TABLES};" 2>/dev/null || true
        else
            echo -e "\nMembuat user database '${AGENT_DB_USER}' dengan hak akses replication..."
            if mysql --no-defaults -e "CREATE USER IF NOT EXISTS '${AGENT_DB_USER}'@'%' IDENTIFIED BY '${AGENT_DB_PASS}'; GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${AGENT_DB_USER}'@'%'; FLUSH PRIVILEGES;" 2>/dev/null; then
                USER_CREATED=true
                echo -e "${GREEN}✓ User DB '${AGENT_DB_USER}' berhasil dibuat otomatis!${NC}"
            fi
        fi

        if [ "$USER_CREATED" = false ]; then
            echo -e "${YELLOW}[NOTE] Otomasi pembuat user DB tidak tersedia (mis. database di Docker).${NC}"
            echo -e "${YELLOW}Silakan masukkan kredensial database yang sudah ada:${NC}"

            MAX_RETRIES=3
            RETRY_COUNT=0
            CONN_OK=false

            while [ "$CONN_OK" = false ] && [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
                RETRY_COUNT=$((RETRY_COUNT+1))
                if [ $RETRY_COUNT -gt 1 ]; then
                    echo -e "\n${YELLOW}🔄 Percobaan ke-${RETRY_COUNT} dari ${MAX_RETRIES}...${NC}"
                fi

                read -p "Database Username: " AGENT_DB_USER < /dev/tty
                read -p "Database Password (terlihat): " AGENT_DB_PASS < /dev/tty

                echo -e "\n${BLUE}🔌 Menguji koneksi ke ${DB_HOST}:${DB_PORT}/${TARGET_DB} sebagai '${AGENT_DB_USER}'...${NC}"

                if [ "$CHOSEN_ENGINE" = "postgres" ]; then
                    TEST_RESULT=$(PGPASSWORD="$AGENT_DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$AGENT_DB_USER" -d "$TARGET_DB" -c "SELECT 1;" --no-align --tuples-only 2>&1)
                    if echo "$TEST_RESULT" | grep -q "^1$"; then
                        CONN_OK=true
                        echo -e "${GREEN}✓ Koneksi berhasil! Kredensial valid.${NC}"
                    else
                        echo -e "${RED}✗ Koneksi gagal: ${TEST_RESULT}${NC}"
                        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                            echo -e "${YELLOW}Silakan periksa kembali username dan password Anda.${NC}"
                        fi
                    fi
                elif [ "$CHOSEN_ENGINE" = "mysql" ]; then
                    TEST_RESULT=$(mysql --no-defaults -h "$DB_HOST" -P "$DB_PORT" -u "$AGENT_DB_USER" -p"$AGENT_DB_PASS" -D "$TARGET_DB" -e "SELECT 1;" 2>&1)
                    if [ $? -eq 0 ]; then
                        CONN_OK=true
                        echo -e "${GREEN}✓ Koneksi berhasil! Kredensial valid.${NC}"
                    else
                        echo -e "${RED}✗ Koneksi gagal: ${TEST_RESULT}${NC}"
                        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                            echo -e "${YELLOW}Silakan periksa kembali username dan password Anda.${NC}"
                        fi
                    fi
                elif [ "$CHOSEN_ENGINE" = "sqlserver" ]; then
                    TEST_RESULT=$(sqlcmd -S "$DB_HOST,$DB_PORT" -U "$AGENT_DB_USER" -P "$AGENT_DB_PASS" -d "$TARGET_DB" -Q "SELECT 1" -h -1 -W 2>&1)
                    if echo "$TEST_RESULT" | grep -q "^1$"; then
                        CONN_OK=true
                        echo -e "${GREEN}✓ Koneksi berhasil! Kredensial valid.${NC}"
                    else
                        echo -e "${RED}✗ Koneksi gagal: ${TEST_RESULT}${NC}"
                        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                            echo -e "${YELLOW}Silakan periksa kembali username dan password Anda.${NC}"
                        fi
                    fi
                else
                    # Skip test for Oracle and MongoDB since they require specific clients
                    CONN_OK=true
                    echo -e "${GREEN}✓ [Skip Test] Asumsi kredensial ${CHOSEN_ENGINE} valid karena klien native tidak tersedia.${NC}"
                fi
            done

            if [ "$CONN_OK" = false ]; then
                echo -e "${RED}✗ Gagal terkoneksi setelah ${MAX_RETRIES} percobaan. Proses dibatalkan.${NC}"
                echo -e "${YELLOW}Tip: Pastikan database bisa diakses dari host ini via TCP: psql -h ${DB_HOST} -p ${DB_PORT} -U <user> -d ${TARGET_DB}${NC}"
            fi
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

        if [ "$CHOSEN_ENGINE" = "oracle" ]; then
            echo -e "\n${YELLOW}⚠️ PERHATIAN: Oracle Database memerlukan konfigurasi tambahan!${NC}"
            echo -e "Pastikan database telah berada di mode ARCHIVELOG dan fitur LogMiner diaktifkan."
            echo -e "Anda WAJIB menyalin file driver JDBC (ojdbc8.jar) ke folder /etc/auditchain/jdbc-drivers/ di server ini,"
            echo -e "kemudian restart Debezium (docker restart auditchain-debezium) agar konektor bisa berjalan."
            sleep 3
        elif [ "$CHOSEN_ENGINE" = "mongodb" ]; then
            echo -e "\n${YELLOW}⚠️ PERHATIAN: MongoDB memerlukan konfigurasi tambahan!${NC}"
            echo -e "Debezium MongoDB Connector mewajibkan MongoDB berjalan dalam mode Replica Set (meskipun standalone / 1 node)."
            echo -e "Pastikan MongoDB dijalankan dengan opsi --replSet dan sudah diinisialisasi (rs.initiate())."
            sleep 3
        elif [ "$CHOSEN_ENGINE" = "sqlserver" ]; then
            echo -e "\n${YELLOW}⚠️ PERHATIAN: SQL Server memerlukan konfigurasi tambahan!${NC}"
            echo -e "Debezium SQL Server Connector mewajibkan fitur CDC diaktifkan pada database dan tabel target."
            echo -e "Jalankan perintah berikut di SSMS atau sqlcmd:"
            echo -e "  ${BLUE}USE ${TARGET_DB}; EXEC sys.sp_cdc_enable_db;${NC}"
            echo -e "  ${BLUE}EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='<nama_tabel>', @role_name=NULL;${NC}"
            echo -e "Pastikan SQL Server Agent service juga berjalan (diperlukan untuk CDC job)."
            sleep 3
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
    "plugin.name": "pgoutput",
    "publication.autocreate.mode": "disabled",
    "publication.name": "dbz_publication",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,table,ts_ms"
  }
}
EOF
)
            elif [ "$CHOSEN_ENGINE" = "oracle" ]; then
                PDB_CONFIG=""
                if [ -n "$ORACLE_PDB" ]; then
                    PDB_CONFIG="\"database.pdb.name\": \"${ORACLE_PDB}\","
                fi
                # User is highly likely uppercase in Oracle
                UPPER_USER=$(echo "$AGENT_DB_USER" | tr '[:lower:]' '[:upper:]')
                
                CONNECTOR_PAYLOAD=$(cat <<EOF
{
  "name": "${TARGET_DB}-connector",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "tasks.max": "1",
    "database.hostname": "${DB_HOST}",
    "database.port": "${DB_PORT}",
    "database.user": "${AGENT_DB_USER}",
    "database.password": "${AGENT_DB_PASS}",
    "database.dbname": "${TARGET_DB}",
    ${PDB_CONFIG}
    "topic.prefix": "${HOSTNAME}_${SELECTED_DB_NAME}",
    "schema.include.list": "${UPPER_USER},${AGENT_DB_USER}",
    "table.include.list": "${CHOSEN_TABLES}",
    "database.tablename.case.insensitive": "false",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.${SELECTED_DB_NAME}",
    "log.mining.strategy": "online_catalog",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,table,ts_ms"
  }
}
EOF
)
            elif [ "$CHOSEN_ENGINE" = "sqlserver" ]; then
                CONNECTOR_PAYLOAD=$(cat <<EOF
{
  "name": "${TARGET_DB}-connector",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "tasks.max": "1",
    "database.hostname": "${DB_HOST}",
    "database.port": "${DB_PORT}",
    "database.user": "${AGENT_DB_USER}",
    "database.password": "${AGENT_DB_PASS}",
    "database.names": "${TARGET_DB}",
    "topic.prefix": "${HOSTNAME}_${TARGET_DB}",
    "table.include.list": "${CHOSEN_TABLES}",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.${TARGET_DB}",
    "database.encrypt": "false",
    "database.trustServerCertificate": "true",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,table,ts_ms"
  }
}
EOF
)
            elif [ "$CHOSEN_ENGINE" = "mongodb" ]; then
                CONNECTOR_PAYLOAD=$(cat <<EOF
{
  "name": "${TARGET_DB}-connector",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "tasks.max": "1",
    "mongodb.connection.string": "mongodb://${AGENT_DB_USER}:${AGENT_DB_PASS}@${DB_HOST}:${DB_PORT}/?replicaSet=rs0",
    "topic.prefix": "${HOSTNAME}_${TARGET_DB}",
    "collection.include.list": "${CHOSEN_TABLES}",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "drop",
    "transforms.unwrap.add.fields": "op,collection,ts_ms"
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
    "schema.history.internal.kafka.topic": "schema-changes.${TARGET_DB}",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,table,ts_ms"
  }
}
EOF
)
            fi

            # Hapus konektor jika sudah ada sebelumnya agar konfigurasi baru (SMT) bisa masuk
            curl -s -X DELETE "http://localhost:8083/connectors/${TARGET_DB}-connector" >/dev/null 2>&1 || true
            sleep 1

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

# ------------------------------------------------------------------------------
# 6. DETEKSI ENDPOINT NETWORK & TELEMETRI
# ------------------------------------------------------------------------------
echo -e "\n${BLUE}[6/7] Mendeteksi Endpoint Network & Telemetri...${NC}"

KAFKA_BROKERS="${TAILSCALE_IP}:9092"
AGENT_SERVER_URL="http://${TAILSCALE_IP}:8083"
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

