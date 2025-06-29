SELECT
  ucc.CMTS_ID,
  ucc.CABLE_MAC,
  ucc.SG_ID,
  MIN(ucc.CHANNEL_NAME) AS REF_CHAN,
  COUNT(ucc.CHANNEL_NAME) AS UCHC,
  MIN(ctpfn.FIBER_NODE) AS FIBER_NODE  -- pick only one fiber node
FROM cabletica.UP_CHANNEL_CONFIG ucc
JOIN cabletica.CHANNEL_TO_PHY_FIBER_NODE ctpfn
  ON ucc.CMTS_ID = ctpfn.CMTS_ID
 AND ucc.CHANNEL_NAME = ctpfn.CHANNEL_NAME
WHERE ctpfn.DIRECTION = 'UP'
GROUP BY
  ucc.CMTS_ID,
  ucc.CABLE_MAC,
  ucc.SG_ID;
