SELECT CMTS_ID,CABLE_MAC, MIN(CHANNEL_NAME) AS REF_CHAN, CM_MAC 
FROM cabletica.CM_SCQ_UP_DAY_FACTS csudf 
GROUP BY CMTS_ID, CABLE_MAC, CM_MAC
