Konflux Perf&Scale team repository
==================================

Repository structure
--------------------

 * `grafana/` - Grafana dashboard JSONs deployed to in-cluster Grafana instances via infra-deployments.
 * `grafonnet-workdir/` - Jsonnet/Grafonnet source code for generating dashboards hosted on grafana.corp.redhat.com. See `grafonnet-workdir/README.md` for build instructions.
 * `tools/` - Operational scripts:
   * `oomkill-and-crashloopbackoff-detector/` - Parallel OOMKilled/CrashLoopBackOff detector across OpenShift clusters with forensic artifact collection and HTML reports.
   * `tasks-and-steps-resource-analyzer/` - Extracts Memory/CPU usage metrics (Max, P95, P90, Median) per Tekton task/step from Prometheus, with resource limit recommendations.
 * `cleanup-dashboard.sh` - Strips `datasource` fields from dashboard JSONs before committing.
 * `update-infra-ref.sh` - Updates the infra-deployments kustomization reference to a given commit SHA.

In Konflux cluster Grafana dashboards
-------------------------------------

These dashboards appear in Grafana instances deployed in Konflux clusters and are stored in `grafana/` in this repository.

To add a dashboard, you just follow <https://github.com/redhat-appstudio/infra-deployments/blob/main/components/monitoring/README.md#teams-repository>.

In a nutshell:

1. Remove all `"datasource": {...}` from the dashboard json. You can use the script: `./cleanup-dashboard.sh grafana/dashboards/rhtap-performance.json`
2. Add dashboard json to `grafana/dashboards/` directory.
3. Add sections for it to `grafana/dashboard.yaml` and `grafana/kustomization.yaml` files.
4. Once merged, take git commit sha and create a infra-deployments PR to change the commit sha in `components/monitoring/grafana/base/dashboards/performance/kustomization.yaml` file. You can do the change with `./update-infra-ref.sh <sha>`.

Other dashboards
----------------

These dashboards live in <https://grafana.corp.redhat.com/> and are generated from Jsonnet/Grafonnet source in `grafonnet-workdir/`. See `grafonnet-workdir/README.md` for how to build them.

Our grafana.corp.redhat.com organization is "Konflux perf&scale" and it was initially requested in ticket [RITM2070980](https://redhat.service-now.com/surl.do?n=RITM2070980).

Data source we use for Probes dashboard is Perf Dept Integration Lab PostgreSQL DB schema requested in [INTLAB-459](https://issues.redhat.com/browse/INTLAB-459).

Access to our Grafana organization is guarded by these LDAP groups (ask owners of these groups to be added):

 * For admin access: <https://rover.redhat.com/groups/group/konflux-perfscale-grafanacorp-admins>
 * For user, read-only access: <https://rover.redhat.com/groups/group/konflux-perfscale-grafanacorp-users>

AI agent skills
---------------

This repo uses the [`skills` CLI](https://github.com/mattpocock/skills) to manage reusable AI agent skills. Installed skills live in `.agents/skills/` with symlinks in `.claude/skills/`, and versions are tracked in `skills-lock.json`.

After cloning, restore installed skills from the lock file:

```bash
npx skills@latest experimental_install
```

Other useful commands:

```bash
npx skills@latest list                      # List installed skills
npx skills@latest add mattpocock/skills     # Add skills from a package
npx skills@latest update                    # Update to latest versions
npx skills@latest remove                    # Remove a skill
```
