USE vehicle_manufacturing_src;

SHOW TABLES;

SELECT * FROM parts
WHERE created_at >= "2021-03-17 11:00:00"
ORDER BY created_at ASC;


INSERT INTO parts
  (vehicle_id, part_number, part_name, supplier_code, quantity,
   unit_cost, currency, install_time_min, defect_flag, batch_number,
   created_at)
VALUES
  (1,'PN2294','Radiator','SUP013',7,2322.04,'EUR',36,0,'BATCH331','2021-09-06 11:00:00');