# AGENTS.md - Konflux Perf&Scale

Based on [Global Engineering AGENTS.md guide](https://gitlab.cee.redhat.com/global-engineering/wg-agentic-sdlc/-/tree/main/best-practices/repo-scaffolding?ref_type=heads).

## Purpose

Grafana dashboards and operational tools for the Konflux Performance & Scale team.

## Build & Test Commands

- Build Grafonnet dashboards: `grafonnet-workdir/build.sh`
- Lint Python: `black --check . && flake8`
- Format Python: `black .`
- Lint shell scripts: `shellcheck <script>`
- No automated test suite exists in this repo

## Key Conventions

- After merging dashboard changes, update the ref in infra-deployments repo with `update-infra-ref.sh <sha>`. The script expects a sibling `../infra-deployments` checkout.
- When creating a dashboard in the Grafana UI to persist in git, run `cleanup-dashboard.sh` to strip datasource fields from the JSON.
- Grafonnet vendor dependencies are committed in `grafonnet-workdir/vendor/`; run `jb install` only when updating them.
- Long-term goal is to have all dashboards created by Jsonnet via Grafonnet, not hand-edited JSON.

## Repo Layout

- `grafana/` - Dashboard JSONs: in-cluster ones deployed via infra-deployments, plus generic Probe dashboards deployed manually to <https://grafana.corp.redhat.com/dashboards?orgId=26>.
- `grafonnet-workdir/` - Jsonnet/Grafonnet sources; `build.sh` outputs to `grafana/`.
- `tools/oomkill-and-crashloopbackoff-detector/` - Parallel OOMKilled/CrashLoopBackOff scanner across OpenShift clusters.
- `tools/tasks-and-steps-resource-analyzer/` - Extracts per-task/step Memory & CPU metrics from Prometheus and recommends resource limits.

## Testing

- Grafonnet dashboards: verify they build with `grafonnet-workdir/build.sh`.
- Python code: must pass `black` and `flake8`.
- Shell scripts: must pass `shellcheck`.

## Agent skills

### Issue tracker

Jira — project KONFLUX, component Performance, on redhat.atlassian.net. See `docs/agents/issue-tracker.md`.

### Triage labels

Not used. This repo does not follow a triage workflow.

### Domain docs

Single-context layout. See `docs/agents/domain.md`.
