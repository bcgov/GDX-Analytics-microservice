SELECT * FROM (
  -- based off of the explore https://analytics.gov.bc.ca/explore/snowplow_web_block/sbc_online_appointments?qid=CUcFP7cBkxnOBdqLolIG2H&origin_space=37&toggle=fil,pik,vis
  -- using the stable name PDT BM_snowplow_web_block_sbc_online_appointments
  SELECT
      sbc_online_appointments.min_timestamp AS "sbc_online_appointments.min_timestamp",
      sbc_online_appointments.max_timestamp AS "sbc_online_appointments.max_timestamp",
      sbc_online_appointments.session_id AS "sbc_online_appointments.session_id",
      sbc_online_appointments.appointment_step AS "sbc_online_appointments.appointment_step",
      sbc_online_appointments.client_id AS "sbc_online_appointments.client_id",
      sbc_online_appointments.appointment_id AS "sbc_online_appointments.appointment_id"
  FROM looker.BM_snowplow_web_block_sbc_online_appointments AS sbc_online_appointments
  WHERE ((( sbc_online_appointments.min_timestamp  ) >= ((DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ))) AND ( sbc_online_appointments.min_timestamp  ) < ((DATEADD(day,1, DATEADD(day,-1, DATE_TRUNC(''day'',GETDATE()) ) )))))
  ORDER BY
      1
)