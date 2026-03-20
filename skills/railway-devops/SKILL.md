---
name: railway-devops
description: Run and debug Railway workflows with the Railway CLI, including project/service linking, API and worker deploys, status checks, log triage, SSH debugging, and variable management. Use when a user asks to deploy to Railway, inspect Railway logs, troubleshoot failed deployments, restart services, or verify Railway runtime configuration for this repository.
---

# Railway DevOps

## Overview

Use this skill to execute repeatable Railway deploy and debugging workflows quickly. Prefer the bundled helper script for high-frequency tasks (status, logs, deploy, restart, SSH).

## Quick Start

1. Confirm account and project context.
   - `railway whoami`
   - `railway status`
   - `railway service status --all`
2. Use the helper script for everyday workflows.
   - `./skills/railway-devops/scripts/railway_fast.sh context`
   - `./skills/railway-devops/scripts/railway_fast.sh status`
   - `./skills/railway-devops/scripts/railway_fast.sh logs api 120`
3. Deploy only the changed service.
   - `./skills/railway-devops/scripts/railway_fast.sh deploy api`
   - `./skills/railway-devops/scripts/railway_fast.sh deploy worker`

## Workflow

### 1) Establish Railway context

- Verify login and linked project before mutating state.
- Run `railway link` if the directory is not linked to a project.
- Run `railway service link <service-name>` if commands fail due to missing linked service.

### 2) Deploy

- Deploy API from `apps/api`.
- Deploy worker from `apps/worker`.
- Prefer detached deploy mode unless actively watching build/deploy output.

### 3) Validate rollout

- Check status with `railway service status --all`.
- Pull recent logs with explicit `--lines` and optional `--filter`.
- Confirm API health endpoint when service domain is known.

### 4) Triage incidents

- Pull focused error logs first (`@level:error`, `@level:fatal`, `traceback`, `exception`).
- Use `railway ssh` for runtime-only failures (filesystem, process, env mismatch).
- Use `restart` for transient runtime issues.
- Use `redeploy` when artifact/build state may be stale.

## Bundled Resources

- `scripts/railway_fast.sh`: Wrapper for status, logs, deploy, vars, ssh, restart, and redeploy.
- `references/panccre-railway.md`: Project-specific service roots, aliases, and critical variables.

## Operating Rules

- Keep commands non-destructive by default.
- Avoid printing secrets from variable output in final summaries.
- Report exact command, service, and environment for every mutating action.
