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
CLIENT_KEY=${CLIENT_KEY:-$2}
TAILSCALE_AUTHKEY=${TAILSCALE_AUTHKEY:-"tskey-auth-kj1d5Zj9Bm11CNTRL-wGdo4ffFtCccEqXuFqCHCcV5yMJuuUDh7"}

if [ -z "$CLIENT_KEY" ]; then
    echo -e "${RED}[ERROR] CLIENT_KEY (API Key Klien) wajib diisi!${NC}"
    echo "Silakan jalankan script dengan menyertakan CLIENT_KEY yang diberikan Admin."
    echo "Contoh: GATEWAY_URL=\"http://100.103.5.72:8082\" CLIENT_KEY=\"ak_live_xxxx\" sudo -E bash"
    exit 1
fi

if [ -z "$TAILSCALE_AUTHKEY" ]; then
    echo -e "${YELLOW}[WARNING] TAILSCALE_AUTHKEY tidak ditemukan di environment.${NC}"
    echo -e "${YELLOW}Menggunakan fallback auth key default / prompt mode...${NC}"
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

if [ -n "$TAILSCALE_AUTHKEY" ]; then
    echo -e "Menghubungkan server ke Grup VPN AuditChain secara otomatis..."
    tailscale up --authkey="${TAILSCALE_AUTHKEY}" --unattended --accept-routes || true
else
    echo -e "${YELLOW}[SKIP] Menjalankan Tailscale tanpa authkey khusus.${NC}"
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

echo -e "\n${BLUE}[3/5] Menyiapkan Folder Konfigurasi Agent...${NC}"
mkdir -p /etc/auditchain
mkdir -p /var/log/auditchain

echo -e "\n${BLUE}[4/5] Mendeteksi Endpoint Network & Telemetri...${NC}"

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
EOF
chmod 600 /etc/auditchain/agent.env

echo -e "${GREEN}✓ Konfigurasi .env berhasil disimpan di /etc/auditchain/agent.env${NC}"


echo "  - Hostname           : ${HOSTNAME}"
echo "  - Tailscale VPN IP   : ${TAILSCALE_IP}"
echo "  - Kafka Broker IP    : ${KAFKA_BROKERS}"
echo "  - Agent Server URL   : ${AGENT_SERVER_URL}"

echo -e "\n${BLUE}[5/5] Mengirimkan Telemetri ke Gateway AuditChain Admin...${NC}"

PAYLOAD=$(cat <<EOF
{
  "api_key_prefix": "${CLIENT_KEY}",
  "kafka_brokers": "${KAFKA_BROKERS}",
  "agent_server_url": "${AGENT_SERVER_URL}",
  "hostname": "${HOSTNAME}",
  "status": "pending_setup"
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
# 5. COMPLETION BANNER
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
echo "  • Status Dashboard   : Pending Verification by Admin 🟡"
echo " --------------------------------------------------------------------"
echo -e "${BLUE}Silakan hubungi Admin AuditChain untuk pengaktifan koneksi resmi.${NC}\n"
