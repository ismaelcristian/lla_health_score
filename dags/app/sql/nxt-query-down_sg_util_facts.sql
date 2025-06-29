SELECT
    HUB_ID,
    CMTS_ID,
    CABLE_MAC,
    MAX(HourlyMaxUtilization) AS dsMaxUtilization,
    AVG(HourlyMaxUtilization) AS dsAvgUtilization,
    SUM(CASE WHEN HourlyMaxUtilization > 80 THEN 1 ELSE 0 END) AS dsAboveThresholdCount,
    COUNT(HourlyMaxUtilization) AS dsTotalCount,
    DATE_FORMAT(MAX(START_TIME), '%%Y-%%m-%%dT%%TZ') AS ISO_TIME_S
FROM (
    SELECT
        HUB_ID,
        CMTS_ID,
        CABLE_MAC,
        MAX(SCQAM_UTILIZATION) AS HourlyMaxUtilization,
        MAX(START_TIME) AS START_TIME
    FROM
        cabletica.DOWN_SG_UTIL_FACTS
    WHERE
        START_TIME BETWEEN DATE_SUB(NOW(), INTERVAL 7 DAY) AND NOW()
    GROUP BY
        HUB_ID,
        CMTS_ID,
        CABLE_MAC,
        DATE(START_TIME),
        HOUR(START_TIME)
) AS HourlyData
GROUP BY
    HUB_ID,
    CMTS_ID,
    CABLE_MAC;
