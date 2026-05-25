local grafonnet = import 'github.com/grafana/grafonnet/gen/grafonnet-latest/main.libsonnet';
// Horreum label names for measurements.taskruns only (memory/cpu mean) — Grafana panels avoid
// __measurements_steps__* noise (KONFLUX-12065); step data remains in Horreum/PostgreSQL.
// Regenerate from e2e-tests ci-scripts/config/horreum-schema.json when that schema changes:
//   jq -n --slurpfile s PATH/horreum-schema.json '{ "taskRunMemoryLabels": ([$s[0].labels[] | .name | select(test("^__measurements_taskruns__") and test("__memory_mean$"))] | sort), "taskRunCpuLabels": ([$s[0].labels[] | .name | select(test("^__measurements_taskruns__") and test("__cpu_mean$"))] | sort) }' > grafonnet-workdir/src/horreum_task_run_labels.json
local horreumTaskRunLabels = import 'horreum_task_run_labels.json';

// Just some shortcuts
local dashboard = grafonnet.dashboard;
local timeSeries = grafonnet.panel.timeSeries;
local stat = grafonnet.panel.stat;
local table = grafonnet.panel.table;
local row = grafonnet.panel.row;
local pieChart = grafonnet.panel.pieChart;

{
  // Define "datasource" variable
  datasourceVar()::
    grafonnet.dashboard.variable.datasource.new(
      'datasource',
      'postgres',
    )
    + grafonnet.dashboard.variable.datasource.withRegex('.*grafana-postgresql-datasource.*')  // TODO
    + grafonnet.dashboard.variable.custom.generalOptions.withLabel('Datasource')
    + grafonnet.dashboard.variable.custom.generalOptions.withDescription(
      'Description'
    )
    + grafonnet.dashboard.variable.custom.generalOptions.withCurrent('grafana-postgresql-datasource'),


  // Define "member_cluster" multi-select variable
  memberClusterVar(values)::
    grafonnet.dashboard.variable.custom.new(
      'member_cluster',
      values=[if std.endsWith(v, '/') then std.slice(v, 0, std.length(v) - 1, 1) else v for v in values],
    )
    + grafonnet.dashboard.variable.custom.generalOptions.withLabel('Member cluster')
    + grafonnet.dashboard.variable.custom.generalOptions.withDescription(
      'Description'
    )
    + grafonnet.dashboard.variable.custom.selectionOptions.withMulti()
    + grafonnet.dashboard.variable.query.selectionOptions.withIncludeAll()
    + grafonnet.dashboard.variable.custom.generalOptions.withCurrent('all'),


  // Define "smoothing" variable
  smoothingVar()::
    grafonnet.dashboard.variable.custom.new(
      'smoothing',
      values=[
        'Off',
        '3 hours',
        '12 hours',
        '1 day',
        '3 days',
      ],
    )
    + grafonnet.dashboard.variable.custom.generalOptions.withLabel('Smoothing')
    + grafonnet.dashboard.variable.custom.generalOptions.withDescription(
      'Description'
    )
    + grafonnet.dashboard.variable.custom.generalOptions.withCurrent('Off'),


  joinExtraFilters(extraFilters)::
    local string = std.join(' AND ', [i for i in extraFilters if i != '']);
    if string == '' then
      ''
    else
      'AND ' + string,


  durationQuery(testId, fieldName, extraFilters):: {
    rawSql: |||
      SELECT
          EXTRACT(EPOCH FROM start) AS "time",
          CASE
              WHEN '${smoothing}' = '3 hours' THEN AVG((label_values->>'%s')::DOUBLE PRECISION) OVER (ORDER BY start RANGE '3 hours' PRECEDING)
              WHEN '${smoothing}' = '12 hours' THEN AVG((label_values->>'%s')::DOUBLE PRECISION) OVER (ORDER BY start RANGE '12 hours' PRECEDING)
              WHEN '${smoothing}' = '1 day' THEN AVG((label_values->>'%s')::DOUBLE PRECISION) OVER (ORDER BY start RANGE '1 day' PRECEDING)
              WHEN '${smoothing}' = '3 days' THEN AVG((label_values->>'%s')::DOUBLE PRECISION) OVER (ORDER BY start RANGE '3 days' PRECEDING)
              ELSE (label_values->>'%s')::DOUBLE PRECISION
          END AS "value",
          '%s' as "metric"
      FROM
          data
      WHERE
          horreum_testid = %g
          AND rtrim(label_values->>'.metadata.env.MEMBER_CLUSTER', '/') = rtrim(${member_cluster}, '/')
          AND $__timeFilter(start)
          %s
      ORDER BY
          start;
    ||| % [fieldName, fieldName, fieldName, fieldName, fieldName, fieldName, testId, $.joinExtraFilters(extraFilters)],
    format: 'time_series',
  },


  durationsQuery(testId, fieldNames, extraFilters)::
    timeSeries.queryOptions.withTargets(
      [self.durationQuery(testId, fieldName, extraFilters) for fieldName in fieldNames],
    ),


  errorsTableQuery(testId, extraFilters):: {
    rawSql: |||
      SELECT
          EXTRACT(EPOCH FROM start) AS "time",
          label_values->>'__results_errors_error_reasons_simple' AS "Error reasons"
      FROM
          data
      WHERE
          horreum_testid = %g
          AND rtrim(label_values->>'.metadata.env.MEMBER_CLUSTER', '/') = rtrim(${member_cluster}, '/')
          AND $__timeFilter(start)
          %s
      ORDER BY
          start DESC;
    ||| % [testId, $.joinExtraFilters(extraFilters)],
    format: 'time_series',
  },


  errorsPieQuery(testId, extraFilters):: {
    rawSql: |||
      SELECT
          COALESCE(
              CASE
                  WHEN label_values ? '__results_errors_error_reasons_simple' THEN
                      regexp_replace(label_values->>'__results_errors_error_reasons_simple', '[0-9]+x ', '', 'g')
                  ELSE
                      NULL
              END,
              '') AS "Error",
          COUNT(*) AS "Count"
      FROM
          data
      WHERE
          horreum_testid = %g
          AND rtrim(label_values->>'.metadata.env.MEMBER_CLUSTER', '/') = rtrim(${member_cluster}, '/')
          AND $__timeFilter(start)
          %s
      GROUP BY
          "Error"
      ORDER BY
          "Error" ASC;
    ||| % [testId, $.joinExtraFilters(extraFilters)],
    format: 'table',
  },


  durationsPanel(testId, fieldNames, fieldUnit, panelName='', extraFilters=[])::
    local title = if panelName == '' then std.join(',', fieldNames) else panelName;
    timeSeries.new('${member_cluster} %s' % title)
    + timeSeries.queryOptions.withDatasource(
      type='postgres',
      uid='${datasource}',
    )
    + timeSeries.fieldConfig.defaults.custom.withInsertNulls(5400000)
    + timeSeries.gridPos.withH(8)
    + timeSeries.gridPos.withW(24)
    + timeSeries.panelOptions.withMaxPerRow(4)
    + timeSeries.panelOptions.withRepeatDirection(value='h')
    + timeSeries.panelOptions.withRepeat('member_cluster')
    + timeSeries.queryOptions.withTransformations([])
    + timeSeries.standardOptions.withMin(0)
    + timeSeries.standardOptions.withUnit(fieldUnit)
    + self.durationsQuery(testId, fieldNames, extraFilters),


  errorsCountPanel(testId, fieldNames, panelName='', extraFilters=[])::
    local title = if panelName == '' then std.join(',', fieldNames) else panelName;
    stat.new('${member_cluster} %s' % title)
    + stat.queryOptions.withDatasource(
      type='postgres',
      uid='${datasource}',
    )
    + stat.gridPos.withH(8)
    + stat.gridPos.withW(24)
    + stat.options.reduceOptions.withCalcs(['mean'])
    + stat.options.reduceOptions.withValues(false)
    + stat.panelOptions.withMaxPerRow(4)
    + stat.panelOptions.withRepeatDirection(value='h')
    + stat.panelOptions.withRepeat('member_cluster')
    + stat.queryOptions.withTransformations([])
    + stat.standardOptions.color.withMode('thresholds')
    + stat.standardOptions.thresholds.withMode('absolute')
    + stat.standardOptions.thresholds.withSteps([{ color: 'green', value: null }, { color: 'red', value: 0.1 }])
    + stat.standardOptions.withMin(0)
    + stat.standardOptions.withUnit('percentunit')
    + self.durationsQuery(testId, fieldNames, extraFilters),


  errorsTablePanel(testId, extraFilters=[])::
    table.new('${member_cluster} Error reasons detail')
    + table.queryOptions.withDatasource(
      type='postgres',
      uid='${datasource}',
    )
    + table.fieldConfig.defaults.custom.withFilterable()
    + table.gridPos.withH(10)
    + table.gridPos.withW(24)
    + table.options.footer.withEnablePagination()
    + table.panelOptions.withMaxPerRow(4)
    + table.panelOptions.withRepeatDirection(value='h')
    + table.panelOptions.withRepeat('member_cluster')
    + table.queryOptions.withTransformations([])
    + table.standardOptions.withMin(0)
    + table.standardOptions.withUnit('string')
    + table.queryOptions.withTargets([self.errorsTableQuery(testId, extraFilters)]),


  errorsPiePanel(testId, extraFilters=[])::
    pieChart.new('${member_cluster} Error reasons overall')
    + pieChart.queryOptions.withDatasource(
      type='postgres',
      uid='${datasource}',
    )
    + pieChart.gridPos.withH(10)
    + pieChart.gridPos.withW(24)
    + pieChart.options.reduceOptions.withValues(true)
    + pieChart.options.withDisplayLabels(['value'])
    + pieChart.panelOptions.withMaxPerRow(4)
    + pieChart.panelOptions.withRepeatDirection(value='h')
    + pieChart.panelOptions.withRepeat('member_cluster')
    + pieChart.queryOptions.withTransformations([])
    + pieChart.standardOptions.withMin(0)
    + pieChart.standardOptions.withNoValue('no error detected')
    + pieChart.standardOptions.withUnit('none')
    + pieChart.queryOptions.withTargets([self.errorsPieQuery(testId, extraFilters)]),


  // Task run memory and CPU — Horreum labels for measurements.taskruns only (KONFLUX-12065)
  taskRunMemoryLabels: horreumTaskRunLabels.taskRunMemoryLabels,
  taskRunCpuLabels: horreumTaskRunLabels.taskRunCpuLabels,

  completeDashboard(
    dashboardName='',
    dashboardUid='',
    dashboardDescription='',
    testId=0,
    componentRepoUrl='',
    templatingDir='',
    memberClusters=[],
    testPhaseStubs=[],
    taskRunStubs=[],
    platformTaskRunStubs=[],
  )::
    local componentRepoUrlFilter = if std.findSubstr('%', componentRepoUrl) == [] then "label_values->>'.parameters.options.ComponentRepoUrl' = '%s'" % [componentRepoUrl] else "label_values->>'.parameters.options.ComponentRepoUrl' LIKE '%s'" % [componentRepoUrl];
    local passingFilter = "label_values->>'.results.measurements.KPI.mean' != '-1'";
    local templatingDirFilter =
      if templatingDir == 'SingleArchLike' then
        "(label_values->>'__parameters_options_PipelineRepoTemplatingSourceDir' IS NULL OR label_values->>'__parameters_options_PipelineRepoTemplatingSourceDir' = '' OR label_values->>'__parameters_options_PipelineRepoTemplatingSourceDir' = 'nodejs-devfile-sample-test/' OR label_values->>'__parameters_options_PipelineRepoTemplatingSourceDir' = 'nodejs-devfile-sample-SingleArch/')"
      else if templatingDir == 'MultiArchLike' then
        "(label_values->>'__parameters_options_PipelineRepoTemplatingSourceDir' = 'nodejs-devfile-sample-MultiArch/' OR label_values->>'__parameters_options_PipelineRepoTemplatingSourceDir' = 'nodejs-devfile-sample-MultiArch-only-ARM-and-AMD/')" else '';
    local extraFilters = [componentRepoUrlFilter, passingFilter, templatingDirFilter];
    dashboard.new(dashboardName)
    + dashboard.withUid(dashboardUid)
    + dashboard.withDescription(dashboardDescription)
    + dashboard.time.withFrom(value='now-7d')
    + dashboard.withVariables([
      self.datasourceVar(),
      self.memberClusterVar(memberClusters),
      self.smoothingVar(),
    ])
    + dashboard.withPanels([
      // Main panels
      row.new('KPI durations'),
      self.durationsPanel(testId, ['__results_measurements_KPI_mean'], 's', 'Mean duration', extraFilters=extraFilters),
      row.new('KPI errors'),
      self.errorsCountPanel(testId, ['__results_measurements_KPI_errors'], 'Failure rate', extraFilters=[i for i in extraFilters if i != passingFilter]),
      row.new('Errors table'),
      self.errorsTablePanel(testId, extraFilters=[i for i in extraFilters if i != passingFilter]),
      row.new('Errors pie-chart'),
      self.errorsPiePanel(testId, extraFilters=[i for i in extraFilters if i != passingFilter]),
      // Panels splitting test actions
      row.new('Duration by test phase'),
      self.durationsPanel(testId, [i + 'pass_duration_mean' for i in testPhaseStubs], 's', 'Duration by test phase', extraFilters=extraFilters),
      row.new('Error rate by test phase'),
      self.durationsPanel(testId, [i + 'error_rate' for i in testPhaseStubs], 'none', 'Error rate by test phase', extraFilters=[i for i in extraFilters if i != passingFilter]),
      // Panels showing per task data
      row.new('Overall duration by task run'),
      self.durationsPanel(testId, [i + 'passed_duration_mean' for i in taskRunStubs], 's', 'Overall duration by task run', extraFilters=extraFilters),
      row.new('Running duration by task run'),
      self.durationsPanel(testId, [i + 'passed_running_mean' for i in taskRunStubs], 's', 'Running duration by task run', extraFilters=extraFilters),
      row.new('Scheduled duration by task run'),
      self.durationsPanel(testId, [i + 'passed_scheduled_mean' for i in taskRunStubs], 's', 'Scheduled duration by task run', extraFilters=extraFilters),
      row.new('Idle duration by task run'),
      self.durationsPanel(testId, [i + 'passed_idle_mean' for i in taskRunStubs], 's', 'Idle duration by task run', extraFilters=extraFilters),
      row.new('Count of task runs'),
      self.durationsPanel(testId, [i + 'passed_duration_samples' for i in taskRunStubs], 'none', 'Count of task runs', extraFilters=extraFilters),
      // Panels showing per task architecture data
      row.new('Overall duration by platform task run'),
      self.durationsPanel(testId, [i + 'passed_duration_mean' for i in platformTaskRunStubs], 's', 'Overall duration by platform task run', extraFilters=extraFilters),
      row.new('Running duration by platform task run'),
      self.durationsPanel(testId, [i + 'passed_running_mean' for i in platformTaskRunStubs], 's', 'Running duration by platform task run', extraFilters=extraFilters),
      row.new('Scheduled duration by platform task run'),
      self.durationsPanel(testId, [i + 'passed_scheduled_mean' for i in platformTaskRunStubs], 's', 'Scheduled duration by platform task run', extraFilters=extraFilters),
      row.new('Idle duration by platform task run'),
      self.durationsPanel(testId, [i + 'passed_idle_mean' for i in platformTaskRunStubs], 's', 'Idle duration by platform task run', extraFilters=extraFilters),
      row.new('Count of platform task runs'),
      self.durationsPanel(testId, [i + 'passed_duration_samples' for i in platformTaskRunStubs], 'none', 'Count of platform task runs', extraFilters=extraFilters),
      // Task run memory and CPU — taskruns only per KONFLUX-12065 (steps remain in Horreum, not graphed here)
      row.new('Task run memory (mean)'),
      self.durationsPanel(testId, self.taskRunMemoryLabels, 'bytes', 'Task run memory', extraFilters=extraFilters),
      row.new('Task run CPU (mean)'),
      self.durationsPanel(testId, self.taskRunCpuLabels, 'short', 'Task run CPU', extraFilters=extraFilters),
    ]),

}
