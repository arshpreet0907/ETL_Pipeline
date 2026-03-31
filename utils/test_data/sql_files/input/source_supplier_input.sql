USE vehicle_manufacturing_src;

SHOW TABLES;

SELECT * FROM suppliers;

INSERT INTO suppliers
  (supplier_code, supplier_name, country, tier, rating,
   contract_start, contract_end, is_active,
   created_at, updated_at)
VALUES
  ('SUP001','Supplier_1 GmbH','Germany',3,3.19,'2018-11-24','2020-07-09',1,'2019-01-01 00:00:00',NULL);