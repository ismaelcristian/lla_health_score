SELECT
    HUB_ID,
    CMTS_ID,
    CABLE_MAC,
    SG_ID,
    MAX(HourlyMaxUtilization) AS usMaxUtilization,
    AVG(HourlyMaxUtilization) AS usAvgUtilization,
    SUM(CASE WHEN HourlyMaxUtilization > 80 THEN 1 ELSE 0 END) AS usAboveThresholdCount,
    COUNT(HourlyMaxUtilization) AS usTotalCount,
    DATE_FORMAT(MAX(START_TIME), '%%Y-%%m-%%dT%%TZ') AS ISO_TIME_S
FROM (
    SELECT
        HUB_ID,
        CMTS_ID,
        CABLE_MAC,
        SG_ID,
        MAX(SCQAM_UTILIZATION) AS HourlyMaxUtilization,
        MAX(START_TIME) AS START_TIME
    FROM
        cabletica.UP_SG_UTIL_FACTS
    WHERE
        START_TIME BETWEEN DATE_SUB(NOW(), INTERVAL 7 DAY) AND NOW()
    GROUP BY
        HUB_ID,
        CMTS_ID,
        CABLE_MAC,
        SG_ID,
        DATE(START_TIME),
        HOUR(START_TIME)
) AS HourlyData
GROUP BY
    HUB_ID,
    CMTS_ID,
    CABLE_MAC,
    SG_ID;
