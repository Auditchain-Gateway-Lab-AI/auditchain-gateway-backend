#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

DEPLOY_MODE="${DEPLOY_MODE:-development}"
CURRENT_BRANCH="$(git branch --show-current)"
BRANCH="${DEPLOY_BRANCH:-$CURRENT_BRANCH}"
EXPECTED_SHA="${EXPECTED_SHA:-}"
SERVICE="${DEPLOY_SERVICE:-api-gateway}"
CONTAINER_NAME="${DEPLOY_CONTAINER_NAME:-auditchain-api}"
HEALTH_URL="${DEPLOY_HEALTH_URL:-http://127.0.0.1:8080/healthz}"
READY_URL="${DEPLOY_READY_URL:-http://127.0.0.1:8080/readyz}"
HEALTH_RETRIES="${DEPLOY_HEALTH_RETRIES:-60}"
HEALTH_INTERVAL_SECONDS="${DEPLOY_HEALTH_INTERVAL_SECONDS:-2}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-}"

if [[ "$DEPLOY_MODE" == "production" ]]; then
	umask 077
	if [[ "$BRANCH" != "main" ]]; then
		echo "Deploy failed: production deployment is restricted to main." >&2
		exit 1
	fi
	if [[ ! "$EXPECTED_SHA" =~ ^[0-9a-fA-F]{40}$ ]]; then
		echo "Deploy failed: EXPECTED_SHA must be a 40-character commit SHA." >&2
		exit 1
	fi
	if [[ "$SERVICE" != "api-gateway" ]]; then
		echo "Deploy failed: production deployment only supports api-gateway." >&2
		exit 1
	fi
	if [[ -z "$BACKEND_ENV_FILE" || ! -s "$BACKEND_ENV_FILE" ]]; then
		echo "Deploy failed: BACKEND_ENV_FILE is required for production." >&2
		exit 1
	fi
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
	echo "Deploy failed: $PROJECT_DIR is not a Git worktree." >&2
	exit 1
fi

if [[ -n "$(git status --porcelain=v1 --untracked-files=no)" ]]; then
	echo "Deploy stopped: tracked local changes exist on the server." >&2
	git status --short --untracked-files=no >&2
	exit 1
fi

if docker compose version >/dev/null 2>&1; then
	compose() { docker compose "$@"; }
elif command -v docker-compose >/dev/null 2>&1; then
	compose() { docker-compose "$@"; }
else
	echo "Deploy failed: docker compose or docker-compose is not installed." >&2
	exit 1
fi

SHARED_DIR="${DEPLOY_SHARED_DIR:-$(cd .. && pwd)/shared}"
ENV_TARGET="$SHARED_DIR/backend.env"
ENV_BACKUP_DIR="$SHARED_DIR/env-backups"
LOCK_FILE="${DEPLOY_LOCK_FILE:-$SHARED_DIR/backend-deploy.lock}"
ENV_CHANGED=false
ENV_PREVIOUS_BACKUP=""
ENV_PREVIOUS_REPOSITORY_BACKUP=""
ENV_PREVIOUS_LINK_TARGET=""
OLD_IMAGE_ID=""
OLD_IMAGE_REF=""
NEW_CONTAINER_STARTED=false
TRAP_RUNNING=false

mkdir -p "$SHARED_DIR" "$ENV_BACKUP_DIR"
exec 9>"$LOCK_FILE"
if command -v flock >/dev/null 2>&1; then
	if ! flock -n 9; then
		echo "Deploy stopped: another backend deployment is already running." >&2
		exit 1
	fi
else
	echo "Deploy failed: flock is required to protect the deployment." >&2
	exit 1
fi

cleanup() {
	if [[ -n "$BACKEND_ENV_FILE" && -f "$BACKEND_ENV_FILE" ]]; then
		rm -f -- "$BACKEND_ENV_FILE"
	fi
}

restore_environment() {
	if [[ "$ENV_CHANGED" != true ]]; then
		return 0
	fi

	if [[ -n "$ENV_PREVIOUS_BACKUP" && -f "$ENV_PREVIOUS_BACKUP" ]]; then
		install -m 600 "$ENV_PREVIOUS_BACKUP" "$ENV_TARGET"
	else
		rm -f -- "$ENV_TARGET"
	fi

	if [[ -n "$ENV_PREVIOUS_REPOSITORY_BACKUP" && -f "$ENV_PREVIOUS_REPOSITORY_BACKUP" ]]; then
		rm -f -- "$PROJECT_DIR/.env"
		install -m 600 "$ENV_PREVIOUS_REPOSITORY_BACKUP" "$PROJECT_DIR/.env"
	elif [[ -n "$ENV_PREVIOUS_LINK_TARGET" ]]; then
		rm -f -- "$PROJECT_DIR/.env"
		ln -s "$ENV_PREVIOUS_LINK_TARGET" "$PROJECT_DIR/.env"
	elif [[ -n "$ENV_PREVIOUS_BACKUP" && -f "$ENV_PREVIOUS_BACKUP" ]]; then
		rm -f -- "$PROJECT_DIR/.env"
		ln -s "$ENV_TARGET" "$PROJECT_DIR/.env"
	else
		rm -f -- "$PROJECT_DIR/.env"
	fi
	echo "Previous backend environment restored."
}

rollback_container() {
	if [[ "$NEW_CONTAINER_STARTED" != true || -z "$OLD_IMAGE_ID" || -z "$OLD_IMAGE_REF" ]]; then
		echo "No previous API image was available for automatic rollback." >&2
		return 1
	fi

	echo "Rolling back api-gateway to the previous image..."
	docker tag "$OLD_IMAGE_ID" "$OLD_IMAGE_REF"
	compose up -d --no-deps --no-build --force-recreate "$SERVICE"
	wait_for_health "rollback"
}

on_exit() {
	local status=$?
	if [[ "$TRAP_RUNNING" == true ]]; then
		return "$status"
	fi
	TRAP_RUNNING=true
	trap - EXIT

	if (( status != 0 )); then
		set +e
		restore_environment
		rollback_container
	fi
	cleanup
	exit "$status"
}

wait_for_health() {
	local label="${1:-deployment}"
	local attempt=1
	local health_status=""

	while (( attempt <= HEALTH_RETRIES )); do
		health_status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "$CONTAINER_NAME" 2>/dev/null || true)"
		if [[ "$health_status" == "healthy" ]] && curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null 2>&1 && curl -fsS --max-time 5 "$READY_URL" >/dev/null 2>&1; then
			echo "$label health check passed on attempt $attempt."
			return 0
		fi
		echo "$label is not ready (health=$health_status); retry $attempt/$HEALTH_RETRIES..."
		sleep "$HEALTH_INTERVAL_SECONDS"
		((attempt += 1))
	done

	echo "$label health check failed after $HEALTH_RETRIES attempts." >&2
	return 1
}

validate_environment_file() {
	local env_file="$1"

	if [[ ! -s "$env_file" ]]; then
		echo "Deploy failed: backend environment file is empty." >&2
		return 1
	fi

	# Reject malformed lines, duplicate keys, and empty mandatory values without
	# printing the corresponding secret value.
	if ! awk '
		/^[[:space:]]*$/ || /^[[:space:]]*#/ { next }
		{
			line = $0
			sub(/^[[:space:]]*(export[[:space:]]+)?/, "", line)
			if (line !~ /^[A-Za-z_][A-Za-z0-9_]*=/) {
				exit 1
			}
			key = line
			sub(/=.*/, "", key)
			if (seen[key]++) {
				exit 1
			}
			value = line
			sub(/^[^=]*=/, "", value)
			if ((key == "APP_ENV" || key == "PORT" || key == "DB_DSN" || key == "JWT_SECRET") && value ~ /^[[:space:]]*$/) {
				exit 1
			}
		}
	' "$env_file"; then
		echo "Deploy failed: backend environment is malformed, contains duplicate keys, or has an empty mandatory value." >&2
		return 1
	fi

	for required_key in APP_ENV PORT DB_DSN JWT_SECRET; do
		if ! grep -Eq "^[[:space:]]*(export[[:space:]]+)?${required_key}=" "$env_file"; then
			echo "Deploy failed: backend environment is missing required key ${required_key}." >&2
			return 1
		fi
	done
	if [[ "$DEPLOY_MODE" == "production" ]] && ! grep -Eq "^[[:space:]]*(export[[:space:]]+)?APP_ENV=production$" "$env_file"; then
		echo "Deploy failed: production backend environment must set APP_ENV=production." >&2
		return 1
	fi
}

sync_environment() {
	if [[ -z "$BACKEND_ENV_FILE" ]]; then
		return 0
	fi

	validate_environment_file "$BACKEND_ENV_FILE"
	chmod 600 "$BACKEND_ENV_FILE"
	if [[ -d "$PROJECT_DIR/.env" && ! -L "$PROJECT_DIR/.env" ]]; then
		echo "Deploy failed: $PROJECT_DIR/.env is a directory, expected a file or symlink." >&2
		return 1
	fi

	local stamp
	stamp="$(date -u +%Y%m%d-%H%M%S)-$$"
	if [[ -e "$ENV_TARGET" || -L "$ENV_TARGET" ]]; then
		ENV_PREVIOUS_BACKUP="$ENV_BACKUP_DIR/backend.env.$stamp"
		cp -p -- "$ENV_TARGET" "$ENV_PREVIOUS_BACKUP"
		chmod 600 "$ENV_PREVIOUS_BACKUP"
	fi
	if [[ -e "$PROJECT_DIR/.env" && ! -L "$PROJECT_DIR/.env" ]]; then
		ENV_PREVIOUS_REPOSITORY_BACKUP="$ENV_BACKUP_DIR/repository-env.$stamp"
		cp -p -- "$PROJECT_DIR/.env" "$ENV_PREVIOUS_REPOSITORY_BACKUP"
		chmod 600 "$ENV_PREVIOUS_REPOSITORY_BACKUP"
	elif [[ -L "$PROJECT_DIR/.env" ]]; then
		ENV_PREVIOUS_LINK_TARGET="$(readlink -- "$PROJECT_DIR/.env")"
	fi

	local new_env
	new_env="$SHARED_DIR/.backend.env.new.$$"
	install -m 600 "$BACKEND_ENV_FILE" "$new_env"
	mv -f -- "$new_env" "$ENV_TARGET"
	ENV_CHANGED=true
	rm -f -- "$PROJECT_DIR/.env"
	ln -sfn "$ENV_TARGET" "$PROJECT_DIR/.env"
	echo "Backend environment validated and installed without printing its values."
}

trap on_exit EXIT

echo "Deploying $SERVICE from branch $BRANCH in $DEPLOY_MODE mode."

if ! command -v curl >/dev/null 2>&1; then
	echo "Deploy failed: curl is required for deployment health checks." >&2
	exit 1
fi

git fetch origin "$BRANCH"
REMOTE_SHA="$(git rev-parse "origin/$BRANCH")"
if [[ -n "$EXPECTED_SHA" && "$REMOTE_SHA" != "$EXPECTED_SHA" ]]; then
	echo "Deploy stopped: origin/$BRANCH is $REMOTE_SHA, expected $EXPECTED_SHA." >&2
	exit 1
fi

if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
	git checkout "$BRANCH"
else
	git checkout -b "$BRANCH" --track "origin/$BRANCH"
fi
git pull --ff-only origin "$BRANCH"

DEPLOYED_SHA="$(git rev-parse HEAD)"
if [[ -n "$EXPECTED_SHA" && "$DEPLOYED_SHA" != "$EXPECTED_SHA" ]]; then
	echo "Deploy stopped: checked out $DEPLOYED_SHA, expected $EXPECTED_SHA." >&2
	exit 1
fi
echo "Deploying commit $DEPLOYED_SHA."

if [[ -n "$BACKEND_ENV_FILE" ]]; then
	sync_environment
	compose config --quiet
fi

OLD_IMAGE_ID="$(docker inspect --format '{{.Image}}' "$CONTAINER_NAME" 2>/dev/null || true)"
OLD_IMAGE_REF="$(docker inspect --format '{{.Config.Image}}' "$CONTAINER_NAME" 2>/dev/null || true)"

if [[ "$DEPLOY_MODE" == "production" ]]; then
	for state_container in auditchain-postgres auditchain-minio; do
		state="$(docker inspect --format '{{.State.Status}}' "$state_container" 2>/dev/null || true)"
		if [[ "$state" != "running" ]]; then
			echo "Deploy failed: required stateful container $state_container is not running." >&2
			exit 1
		fi
	done
	api_state="$(docker inspect --format '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || true)"
	if [[ "$api_state" != "running" ]]; then
		echo "Deploy failed: current API container $CONTAINER_NAME is not running; automatic rollback baseline is unavailable." >&2
		exit 1
	fi
fi

compose build "$SERVICE"
NEW_CONTAINER_STARTED=true
if [[ "$DEPLOY_MODE" == "production" ]]; then
	compose up -d --no-deps --no-build --force-recreate "$SERVICE"
	wait_for_health "deployment"
else
	compose up -d --no-build --force-recreate "$SERVICE"
	wait_for_health "development deployment"
fi

compose ps "$SERVICE"
echo "Deploy complete for commit $DEPLOYED_SHA in $DEPLOY_MODE mode."
