#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
ENVIRONMENT=""

usage() {
  cat <<EOF
Usage:
  $SCRIPT_NAME [--environment <name>] <command> [args...]

Commands:
  context
      Show whoami, project status, and all service statuses.
  status [service]
      Show status for all services, or one service.
  logs <service> [lines] [filter]
      Show recent logs (non-streaming). Default lines: 100.
  errors <service> [lines]
      Show recent error-focused logs. Default lines: 150.
  deploy <service|api|worker> [path] [--attach]
      Deploy a path to a service. If path is omitted for api/worker aliases:
      api -> apps/api, worker -> apps/worker.
  vars <service>
      List variables in KV format.
  link <service>
      Link current directory to a service.
  restart <service>
      Restart latest deployment for a service (skips confirmation).
  redeploy <service>
      Redeploy latest deployment for a service (skips confirmation).
  ssh <service> [command...]
      Open shell (or run command) over Railway SSH.

Alias resolution:
  api    -> \${PANCCRE_RAILWAY_API_SERVICE:-panccre-api}
  worker -> \${PANCCRE_RAILWAY_WORKER_SERVICE:-panccre-worker}

Examples:
  $SCRIPT_NAME context
  $SCRIPT_NAME --environment production logs api 200 "@level:error OR traceback"
  $SCRIPT_NAME deploy worker
  $SCRIPT_NAME deploy panccre-worker apps/worker --attach
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

run_cmd() {
  echo "+ $*" >&2
  "$@"
}

require_railway() {
  command -v railway >/dev/null 2>&1 || die "railway CLI is not installed or not on PATH"
}

resolve_service() {
  local input="${1:-}"
  case "$input" in
    api)
      echo "${PANCCRE_RAILWAY_API_SERVICE:-panccre-api}"
      ;;
    worker)
      echo "${PANCCRE_RAILWAY_WORKER_SERVICE:-panccre-worker}"
      ;;
    *)
      echo "$input"
      ;;
  esac
}

default_path_for_alias() {
  local input="${1:-}"
  case "$input" in
    api)
      echo "apps/api"
      ;;
    worker)
      echo "apps/worker"
      ;;
    *)
      echo ""
      ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -e|--environment)
      [[ $# -ge 2 ]] || die "missing value for $1"
      ENVIRONMENT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
  esac
done

[[ $# -ge 1 ]] || {
  usage
  exit 1
}

require_railway

COMMAND="$1"
shift

case "$COMMAND" in
  context)
    run_cmd railway whoami
    run_cmd railway status
    if [[ -n "$ENVIRONMENT" ]]; then
      run_cmd railway service status --all --environment "$ENVIRONMENT"
    else
      run_cmd railway service status --all
    fi
    ;;
  status)
    if [[ $# -eq 0 ]]; then
      if [[ -n "$ENVIRONMENT" ]]; then
        run_cmd railway service status --all --environment "$ENVIRONMENT"
      else
        run_cmd railway service status --all
      fi
    else
      service="$(resolve_service "$1")"
      if [[ -n "$ENVIRONMENT" ]]; then
        run_cmd railway service status --service "$service" --environment "$ENVIRONMENT"
      else
        run_cmd railway service status --service "$service"
      fi
    fi
    ;;
  logs)
    [[ $# -ge 1 ]] || die "logs requires <service> [lines] [filter]"
    service="$(resolve_service "$1")"
    shift
    lines="${1:-100}"
    if [[ $# -gt 0 ]]; then
      shift
    fi
    filter="${*:-}"
    if [[ -n "$ENVIRONMENT" ]]; then
      cmd=(railway service logs --service "$service" --environment "$ENVIRONMENT" --lines "$lines")
    else
      cmd=(railway service logs --service "$service" --lines "$lines")
    fi
    if [[ -n "$filter" ]]; then
      cmd+=(--filter "$filter")
    fi
    run_cmd "${cmd[@]}"
    ;;
  errors)
    [[ $# -ge 1 ]] || die "errors requires <service> [lines]"
    service="$(resolve_service "$1")"
    lines="${2:-150}"
    if [[ -n "$ENVIRONMENT" ]]; then
      run_cmd railway service logs --service "$service" --environment "$ENVIRONMENT" --lines "$lines" --filter "@level:error OR @level:fatal OR traceback OR exception"
    else
      run_cmd railway service logs --service "$service" --lines "$lines" --filter "@level:error OR @level:fatal OR traceback OR exception"
    fi
    ;;
  deploy)
    [[ $# -ge 1 ]] || die "deploy requires <service|api|worker> [path] [--attach]"
    alias_or_service="$1"
    service="$(resolve_service "$alias_or_service")"
    shift

    path=""
    attach="false"

    if [[ $# -gt 0 && "$1" != "--attach" ]]; then
      path="$1"
      shift
    else
      path="$(default_path_for_alias "$alias_or_service")"
    fi

    [[ -n "$path" ]] || die "path is required for non-api/worker service names"

    if [[ $# -gt 0 ]]; then
      [[ "$1" == "--attach" ]] || die "unexpected argument: $1"
      attach="true"
    fi

    if [[ "$attach" == "true" ]]; then
      if [[ -n "$ENVIRONMENT" ]]; then
        cmd=(railway up --service "$service" --environment "$ENVIRONMENT" "$path")
      else
        cmd=(railway up --service "$service" "$path")
      fi
    else
      if [[ -n "$ENVIRONMENT" ]]; then
        cmd=(railway up --service "$service" --environment "$ENVIRONMENT" --detach "$path")
      else
        cmd=(railway up --service "$service" --detach "$path")
      fi
    fi
    run_cmd "${cmd[@]}"
    ;;
  vars)
    [[ $# -eq 1 ]] || die "vars requires <service>"
    service="$(resolve_service "$1")"
    if [[ -n "$ENVIRONMENT" ]]; then
      run_cmd railway variable list --service "$service" --environment "$ENVIRONMENT" --kv
    else
      run_cmd railway variable list --service "$service" --kv
    fi
    ;;
  link)
    [[ $# -eq 1 ]] || die "link requires <service>"
    service="$(resolve_service "$1")"
    run_cmd railway service link "$service"
    ;;
  restart)
    [[ $# -eq 1 ]] || die "restart requires <service>"
    service="$(resolve_service "$1")"
    run_cmd railway service restart --service "$service" --yes
    ;;
  redeploy)
    [[ $# -eq 1 ]] || die "redeploy requires <service>"
    service="$(resolve_service "$1")"
    run_cmd railway service redeploy --service "$service" --yes
    ;;
  ssh)
    [[ $# -ge 1 ]] || die "ssh requires <service> [command...]"
    service="$(resolve_service "$1")"
    shift
    if [[ $# -eq 0 ]]; then
      if [[ -n "$ENVIRONMENT" ]]; then
        run_cmd railway ssh --service "$service" --environment "$ENVIRONMENT"
      else
        run_cmd railway ssh --service "$service"
      fi
    else
      if [[ -n "$ENVIRONMENT" ]]; then
        run_cmd railway ssh --service "$service" --environment "$ENVIRONMENT" "$@"
      else
        run_cmd railway ssh --service "$service" "$@"
      fi
    fi
    ;;
  *)
    die "unknown command: $COMMAND"
    ;;
esac
