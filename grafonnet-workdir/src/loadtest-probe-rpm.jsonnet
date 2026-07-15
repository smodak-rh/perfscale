local probes = import 'probes.libsonnet';

// Define dashboard variables
local memberClusters = [
  'https://api.stone-prod-p02.hjvn.p1.openshiftapps.com:6443/',
  'https://api.stone-prd-rh01.pg1f.p1.openshiftapps.com:6443/',
  'https://api.kflux-rhel-p01.fc38.p1.openshiftapps.com:6443/',
  'https://api.kflux-fedora-01.84db.p1.openshiftapps.com:6443/',
];
local testPhaseStubs = [
  '__results_measurements_HandleUser_',
  '__results_measurements_createApplication_',
  '__results_measurements_createComponent_',
  '__results_measurements_createImageRepository_',
  '__results_measurements_waitForImageRepositoryReady_',
  '__results_measurements_createIntegrationTestScenario_',
  '__results_measurements_createReleasePlanAdmission_',
  '__results_measurements_createReleasePlan_',
  '__results_measurements_getPaCPullNumber_',
  '__results_measurements_validateApplication_',
  '__results_measurements_validateComponentBuildSA_',  // TODO: Delme later, I was renamed to just __results_measurements_validateComponent_...
  '__results_measurements_validateComponent_',
  '__results_measurements_validateIntegrationTestScenario_',
  '__results_measurements_validatePipelineRunCondition_',
  '__results_measurements_validatePipelineRunCreation_',
  '__results_measurements_validatePipelineRunSignature_',
  '__results_measurements_validateReleaseCondition_',
  '__results_measurements_validateReleaseCreation_',
  '__results_measurements_validateReleasePipelineRunCondition_',
  '__results_measurements_validateReleasePipelineRunCreation_',
  '__results_measurements_validateReleasePlanAdmission_',
  '__results_measurements_validateReleasePlan_',
  '__results_measurements_validateSnapshotCreation_',
  '__results_measurements_validateTestPipelineRunCondition_',
  '__results_measurements_validateTestPipelineRunCreation_',
];
local taskRunStubs = [
  '__results_durations_stats_taskruns__build_calculate_deps__',
  '__results_durations_stats_taskruns__build_check_noarch__',
  '__results_durations_stats_taskruns__build_get_rpm_sources__',
  '__results_durations_stats_taskruns__build_git_clone_oci_ta__',
  '__results_durations_stats_taskruns__build_import_to_quay__',
  '__results_durations_stats_taskruns__build_init__',
  '__results_durations_stats_taskruns__build_rpmbuild__',
  '__results_durations_stats_taskruns__build_show_sbom__',
  '__results_durations_stats_taskruns__build_summary__',
  '__results_durations_stats_taskruns__build_get_build_target__',
];
local platformTaskRunStubs = [
  '__results_durations_stats_platformtaskruns__build_rpmbuild_linux_amd64__',
  '__results_durations_stats_platformtaskruns__build_calculate_deps_linux_amd64__',
  '__results_durations_stats_platformtaskruns__build_rpmbuild_linux_arm64__',
  '__results_durations_stats_platformtaskruns__build_calculate_deps_linux_arm64__',
  '__results_durations_stats_platformtaskruns__build_rpmbuild_linux_s390x__',
  '__results_durations_stats_platformtaskruns__build_calculate_deps_linux_s390x__',
  '__results_durations_stats_platformtaskruns__build_rpmbuild_linux_ppc64le__',
  '__results_durations_stats_platformtaskruns__build_calculate_deps_linux_ppc64le__',
];


probes.completeDashboard(
  dashboardName='Konflux clusters loadtest RPM probe results',
  dashboardDescription='Dashboard visualizes Konflux clusters loadtest RPM probe results. Related Horreum test is https://horreum.corp.redhat.com/test/372 with filter by label `.parameters.options.ComponentRepoUrl ~ libecpg*`.',
  dashboardUid='Konflux_clusters_loadtest_RPM_probe_res',
  testId=372,
  componentRepoUrl='%/libecpg%',
  memberClusters=memberClusters,
  testPhaseStubs=testPhaseStubs,
  taskRunStubs=taskRunStubs,
  platformTaskRunStubs=platformTaskRunStubs,
)
