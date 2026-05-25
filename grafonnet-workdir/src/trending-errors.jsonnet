local probes = import 'probes.libsonnet';
local grafonnet = import 'github.com/grafana/grafonnet/gen/grafonnet-latest/main.libsonnet';

local dashboard = grafonnet.dashboard;
local timeSeries = grafonnet.panel.timeSeries;
local row = grafonnet.panel.row;

local testId = 372;

local memberClusters = [
  'https://api.kfluxfedorap01.toli.p1.openshiftapps.com:6443/',
  'https://api.kflux-ocp-p01.7ayg.p1.openshiftapps.com:6443/',
  'https://api.kflux-prd-rh02.0fk9.p1.openshiftapps.com:6443/',
  'https://api.kflux-prd-rh03.nnv1.p1.openshiftapps.com:6443/',
  'https://api.kflux-rhel-p01.fc38.p1.openshiftapps.com:6443/',
  'https://api.stone-prd-rh01.pg1f.p1.openshiftapps.com:6443/',
  'https://api.stone-prod-p01.wcfb.p1.openshiftapps.com:6443/',
  'https://api.stone-prod-p02.hjvn.p1.openshiftapps.com:6443/',
  'https://api.stone-stage-p01.hpmt.p1.openshiftapps.com:6443/',
  'https://api.stone-stg-rh01.l2vh.p1.openshiftapps.com:6443/',
];

local query(sql, format='time_series') = {
  rawSql: sql,
  format: format,
  datasource: {
    type: 'postgres',
    uid: '${datasource}',
  },
};

local grandTotalRunsQuery(testId) = [
  query(|||
    SELECT
        $__timeGroupAlias(start, '1h'),
        COUNT(*) AS "passes"
    FROM
        data
    WHERE
        horreum_testid = %g
        AND $__timeFilter(start)
        AND label_values->>'.results.measurements.KPI.mean' != '-1'
    GROUP BY 1
    ORDER BY 1;
  ||| % testId),
  query(|||
    SELECT
        $__timeGroupAlias(start, '1h'),
        COUNT(*) AS "failures"
    FROM
        data
    WHERE
        horreum_testid = %g
        AND $__timeFilter(start)
        AND label_values->>'.results.measurements.KPI.mean' = '-1'
    GROUP BY 1
    ORDER BY 1;
  ||| % testId),
];

local grandTotalErrorsByReasonQuery(testId) = [
  query(|||
    SELECT
        $__timeGroupAlias(start, '1h'),
        COALESCE(
            CASE
                WHEN label_values ? '__results_errors_error_reasons_simple' AND label_values->>'__results_errors_error_reasons_simple' = '' THEN
                    'no error detected'
                WHEN label_values ? '__results_errors_error_reasons_simple' THEN
                    regexp_replace(label_values->>'__results_errors_error_reasons_simple', '[0-9]+\s?x ', '', 'g')
                ELSE
                    NULL
            END,
            NULL) AS "metric",
        COUNT(*) AS "count"
    FROM
        data
    WHERE
        horreum_testid = %g
        AND label_values ? '__results_errors_error_reasons_simple'
        AND label_values->>'__results_errors_error_reasons_simple' != ''
        AND $__timeFilter(start)
    GROUP BY 1,2
    ORDER BY 1;
  ||| % testId),
];

local grandTotalErrorsByCauseQuery(testId) = [
  query(|||
    SELECT
        $__timeGroupAlias(start, '1h'),
        COALESCE(
            CASE
                WHEN label_values ? '__results_errors_error_caused_by_simple' AND label_values->>'__results_errors_error_caused_by_simple' = '' THEN
                    'no error detected'
                WHEN label_values ? '__results_errors_error_caused_by_simple' THEN
                    label_values->>'__results_errors_error_caused_by_simple'
                ELSE
                    NULL
            END,
            NULL) AS "metric",
        COUNT(*) AS "count"
    FROM
        data
    WHERE
        horreum_testid = %g
        AND label_values ? '__results_errors_error_caused_by_simple'
        AND label_values->>'__results_errors_error_caused_by_simple' != ''
        AND $__timeFilter(start)
    GROUP BY 1,2
    ORDER BY 1;
  ||| % testId),
];

local perClusterRunsQuery(testId) = [
  query(|||
    SELECT
        $__timeGroupAlias(start, '1h'),
        COUNT(*) AS "passes"
    FROM
        data
    WHERE
        horreum_testid = %g
        AND rtrim(label_values->>'.metadata.env.MEMBER_CLUSTER', '/') = rtrim(${member_cluster}, '/')
        AND $__timeFilter(start)
        AND label_values->>'.results.measurements.KPI.mean' != '-1'
    GROUP BY 1
    ORDER BY 1;
  ||| % testId),
  query(|||
    SELECT
        $__timeGroupAlias(start, '1h'),
        COUNT(*) AS "failures"
    FROM
        data
    WHERE
        horreum_testid = %g
        AND rtrim(label_values->>'.metadata.env.MEMBER_CLUSTER', '/') = rtrim(${member_cluster}, '/')
        AND $__timeFilter(start)
        AND label_values->>'.results.measurements.KPI.mean' = '-1'
    GROUP BY 1
    ORDER BY 1;
  ||| % testId),
];

local perClusterErrorsByReasonQuery(testId) = [
  query(|||
    SELECT
        $__timeGroupAlias(start, '1h'),
        COALESCE(
            CASE
                WHEN label_values ? '__results_errors_error_reasons_simple' AND label_values->>'__results_errors_error_reasons_simple' = '' THEN
                    'no error detected'
                WHEN label_values ? '__results_errors_error_reasons_simple' THEN
                    regexp_replace(label_values->>'__results_errors_error_reasons_simple', '[0-9]+\s?x ', '', 'g')
                ELSE
                    NULL
            END,
            NULL) AS "metric",
        COUNT(*) AS "count"
    FROM
        data
    WHERE
        horreum_testid = %g
        AND rtrim(label_values->>'.metadata.env.MEMBER_CLUSTER', '/') = rtrim(${member_cluster}, '/')
        AND label_values ? '__results_errors_error_reasons_simple'
        AND label_values->>'__results_errors_error_reasons_simple' != ''
        AND $__timeFilter(start)
    GROUP BY 1,2
    ORDER BY 1;
  ||| % testId),
];

dashboard.new('Trending errors')
+ dashboard.withUid('Trending_errors')
+ dashboard.withDescription('Dashboard for tracking error trends.')
+ dashboard.time.withFrom(value='now-24h')
+ dashboard.withVariables([
  probes.datasourceVar(),
  probes.memberClusterVar(memberClusters),
])
+ dashboard.withPanels([
  timeSeries.new('Runs count grand total')
  + timeSeries.queryOptions.withTargets(grandTotalRunsQuery(testId))
  + timeSeries.gridPos.withH(13)
  + timeSeries.gridPos.withW(24)
  + timeSeries.fieldConfig.defaults.custom.withLineInterpolation('smooth')
  + timeSeries.fieldConfig.defaults.custom.withFillOpacity(20)
  + timeSeries.fieldConfig.defaults.custom.stacking.withMode('normal')
  + timeSeries.standardOptions.withUnit('none')
  + timeSeries.standardOptions.withMin(0)
  + timeSeries.standardOptions.withDecimals(0),

  timeSeries.new('Errors by reason grand total')
  + timeSeries.queryOptions.withTargets(grandTotalErrorsByReasonQuery(testId))
  + timeSeries.gridPos.withH(13)
  + timeSeries.gridPos.withW(24)
  + timeSeries.gridPos.withY(13)
  + timeSeries.fieldConfig.defaults.custom.withDrawStyle('bars')
  + timeSeries.fieldConfig.defaults.custom.withLineInterpolation('stepBefore')
  + timeSeries.fieldConfig.defaults.custom.withFillOpacity(20)
  + timeSeries.fieldConfig.defaults.custom.stacking.withMode('normal')
  + timeSeries.standardOptions.withUnit('none')
  + timeSeries.standardOptions.withMin(0)
  + timeSeries.standardOptions.withDecimals(0),

  timeSeries.new('Errors by cause grand total')
  + timeSeries.queryOptions.withTargets(grandTotalErrorsByCauseQuery(testId))
  + timeSeries.gridPos.withH(13)
  + timeSeries.gridPos.withW(24)
  + timeSeries.gridPos.withY(26)
  + timeSeries.fieldConfig.defaults.custom.withDrawStyle('bars')
  + timeSeries.fieldConfig.defaults.custom.withLineInterpolation('stepBefore')
  + timeSeries.fieldConfig.defaults.custom.withFillOpacity(20)
  + timeSeries.fieldConfig.defaults.custom.stacking.withMode('normal')
  + timeSeries.standardOptions.withUnit('none')
  + timeSeries.standardOptions.withMin(0)
  + timeSeries.standardOptions.withDecimals(0),

  row.new('Runs count per cluster')
  + row.withPanels([
    timeSeries.new('Runs count for ${member_cluster}')
    + timeSeries.queryOptions.withTargets(perClusterRunsQuery(testId))
    + timeSeries.panelOptions.withRepeat('member_cluster')
    + timeSeries.gridPos.withH(9)
    + timeSeries.gridPos.withW(6)
    + timeSeries.fieldConfig.defaults.custom.withLineInterpolation('smooth')
    + timeSeries.fieldConfig.defaults.custom.withShowPoints('always')
    + timeSeries.fieldConfig.defaults.custom.withSpanNulls(3600000)
    + timeSeries.standardOptions.withUnit('none'),
  ])
  + row.gridPos.withY(39),

  row.new('Errors by reason per cluster')
  + row.withPanels([
    timeSeries.new('Errors by reason for ${member_cluster}')
    + timeSeries.queryOptions.withTargets(perClusterErrorsByReasonQuery(testId))
    + timeSeries.panelOptions.withRepeat('member_cluster')
    + timeSeries.gridPos.withH(9)
    + timeSeries.gridPos.withW(6)
    + timeSeries.fieldConfig.defaults.custom.withDrawStyle('bars')
    + timeSeries.fieldConfig.defaults.custom.withLineInterpolation('stepBefore')
    + timeSeries.fieldConfig.defaults.custom.withFillOpacity(20)
    + timeSeries.standardOptions.withUnit('none'),
  ])
  + row.gridPos.withY(40),
])
