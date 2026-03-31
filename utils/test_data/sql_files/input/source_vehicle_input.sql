USE vehicle_manufacturing_src;

SHOW TABLES;

SELECT * FROM vehicles;

INSERT INTO vehicles
  (vin, model_code, variant, color_code, engine_type, plant_code,
   line_number, production_date, shift, status, quality_score,
   weight_kg, created_at, updated_at)
VALUES
  ('AA7C9A3BB536A4DEC','SUV_Z3','SPORT','GY005','EV_MOTOR','PLANT_C',4,'2022-01-11','MORNING','IN_PROGRESS',80.33,1409.1,'2022-01-11 09:00:00','2022-02-08 09:00:00');