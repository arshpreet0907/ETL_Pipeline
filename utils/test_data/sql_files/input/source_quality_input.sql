USE vehicle_manufacturing_src;

SHOW TABLES;

SELECT * FROM quality_checks;


INSERT INTO quality_checks
  (vehicle_id, check_date, inspector_id, station, test_type,
   result, defect_code, rework_hours, pass_fail,
   created_at)
VALUES
  (1,'2021-03-04','INSP18','STATION_8','SAFETY','MINOR_ISSUE',NULL,NULL,'PASS','2021-03-04 06:00:00');