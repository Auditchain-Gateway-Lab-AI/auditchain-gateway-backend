#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
BRANCH="${DEPLOY_BRANCH:-$CURRENT_BRANCH}"

# Cek local changes di server, tolak deploy kalau ada uncommitted changes
if [ -n "$(git status --porcelain)" ]; then
  echo "Deploy stopped: local changes exist on the server."
  echo "Commit, stash, or discard them first, then run this script again."
  git status --short
  exit 1
fi

echo "Deploying auditchain-gateway-backend from branch: $BRANCH"

git fetch origin "$BRANCH"
if [ "$CURRENT_BRANCH" != "$BRANCH" ]; then
  git checkout "$BRANCH"
fi
git pull --ff-only origin "$BRANCH"

# Pilih docker compose atau docker-compose
if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  echo "Deploy failed: docker compose or docker-compose is not installed."
  exit 1
fi

# Rebuild dan restart hanya service yang di-build (api-gateway)
# postgres-db memakai image jadi, tidak perlu rebuild
$COMPOSE up -d --build
$COMPOSE ps

echo "Deploy complete."
