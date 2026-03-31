USE vehicle_manufacturing_dw;
SHOW TABLES ;

select * from suppliers;
select * from vehicles;
select * from parts;
select * from quality_checks;
select * from etl_run_log;

# run following only when starting to put data in all tables
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE quality_checks;
TRUNCATE TABLE parts;
TRUNCATE TABLE vehicles;
TRUNCATE TABLE suppliers;
SET FOREIGN_KEY_CHECKS = 1;

# SUPPLIERS  (SCD Type 2)
CREATE TABLE suppliers (
  supplier_sk             INT            NOT NULL AUTO_INCREMENT,  # SCD2 surrogate key
  supplier_id             INT            NOT NULL,                 # source PK
  supplier_code           VARCHAR(10)    NOT NULL,
  supplier_name           VARCHAR(100)   NOT NULL,
  country_of_origin       VARCHAR(50)    NOT NULL,
  supplier_tier           TINYINT        NOT NULL,
  tier_label              VARCHAR(12)    NOT NULL,
  performance_rating      DECIMAL(3,2)   NOT NULL,
  contract_start_date     DATE           NOT NULL,
  contract_end_date       DATE           NOT NULL,
  contract_duration_days  INT            NOT NULL,
  active_status           VARCHAR(8)     NOT NULL,
  valid_from              DATE           NOT NULL,
  valid_to                DATE           NOT NULL DEFAULT '9999-12-31',
  is_current              TINYINT(1)     NOT NULL DEFAULT 1,
  created_at              DATETIME       NOT NULL,
  dw_inserted_at          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at              DATETIME       DEFAULT NULL,
  PRIMARY KEY (supplier_sk),
  UNIQUE KEY uq_supplier_code_current (supplier_code, is_current)
) ENGINE=InnoDB;

DESC suppliers;

# VEHICLES
CREATE TABLE vehicles (
  vehicle_sk              INT            NOT NULL AUTO_INCREMENT,
  src_vehicle_id          INT            NOT NULL,
  vin_number              VARCHAR(17)    NOT NULL,
  model_code              VARCHAR(20)    NOT NULL,
  model_variant_name      VARCHAR(45)    NOT NULL,
  color_code              VARCHAR(10)    NOT NULL,
  engine_type             VARCHAR(20)    NOT NULL,
  manufacturing_plant     VARCHAR(10)    NOT NULL,
  production_date         DATE           NOT NULL,
  production_year         SMALLINT       NOT NULL,
  production_month        TINYINT        NOT NULL,
  production_shift        VARCHAR(12)    NOT NULL,
  production_status       VARCHAR(15)    NOT NULL,
  quality_score           DECIMAL(5,2)   NOT NULL,
  quality_tier            VARCHAR(12)    NOT NULL,
  gross_weight_kg         DECIMAL(8,1)   NOT NULL,
  weight_category         VARCHAR(6)     NOT NULL,
  is_electric_vehicle     TINYINT(1)     NOT NULL,
  created_at              DATETIME       NOT NULL,
  dw_inserted_at          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at              DATETIME       DEFAULT NULL,
  PRIMARY KEY (vehicle_sk),
  UNIQUE KEY uq_vin_number (vin_number),
  UNIQUE KEY uq_src_id (src_vehicle_id)
) ENGINE=InnoDB;

DESC vehicles;

# PARTS
CREATE TABLE parts (
  part_id             INT            NOT NULL,
  vehicle_id          INT            NOT NULL,
  part_number         VARCHAR(20)    NOT NULL,
  component_name      VARCHAR(100)   NOT NULL,
  supplier_code       VARCHAR(10)    NOT NULL,
  quantity_used       SMALLINT       NOT NULL,
  unit_cost_eur       DECIMAL(10,2)  NOT NULL,
  total_cost_eur      DECIMAL(12,2)  NOT NULL,
  cost_tier           VARCHAR(12)    NOT NULL,
  installation_hrs    DECIMAL(5,2)   NOT NULL,
  has_defect_flag     TINYINT(1)     NOT NULL,
  batch_number        VARCHAR(20)    NOT NULL,
  created_at          DATETIME       NOT NULL,
  dw_inserted_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (part_id),
  CONSTRAINT fk_dw_parts_vehicle  FOREIGN KEY (vehicle_id)   REFERENCES vehicles(src_vehicle_id),
  CONSTRAINT fk_dw_parts_supplier FOREIGN KEY (supplier_code) REFERENCES suppliers(supplier_code)
) ENGINE=InnoDB;

DESC parts;

# QUALITY_CHECKS
CREATE TABLE quality_checks (
  qc_id               INT            NOT NULL,
  vehicle_id          INT            NOT NULL,
  inspection_date     DATE           NOT NULL,
  inspection_year     SMALLINT       NOT NULL,
  inspector_code      VARCHAR(10)    NOT NULL,
  inspection_station  VARCHAR(20)    NOT NULL,
  test_category       VARCHAR(15)    NOT NULL,
  inspection_result   VARCHAR(15)    NOT NULL,
  defect_code         VARCHAR(10)    DEFAULT NULL,
  has_defect          TINYINT(1)     NOT NULL,
  rework_hours        DECIMAL(5,2)   DEFAULT NULL,
  rework_cost_usd     DECIMAL(8,2)   NOT NULL,
  is_passed           TINYINT(1)     NOT NULL,
  created_at          DATETIME       NOT NULL,
  dw_inserted_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (qc_id),
  CONSTRAINT fk_dw_qc_vehicle FOREIGN KEY (vehicle_id) REFERENCES vehicles(src_vehicle_id)
) ENGINE=InnoDB;

DESC quality_checks;

# ETL RUN LOG
CREATE TABLE etl_run_log (
  run_id          INT             NOT NULL AUTO_INCREMENT,
  run_start       DATETIME        NOT NULL,
  run_end         DATETIME        DEFAULT NULL,
  pipeline_name   VARCHAR(100)    NOT NULL,
  table_name      VARCHAR(50)     NOT NULL,
  load_type       ENUM('FULL','INCREMENTAL') NOT NULL,
  watermark_from  DATETIME        DEFAULT NULL,
  watermark_to    DATETIME        DEFAULT NULL,
  rows_extracted  INT             DEFAULT 0,
  rows_inserted   INT             DEFAULT 0,
  rows_updated    INT             DEFAULT 0,
  rows_failed     INT             DEFAULT 0,
  status          ENUM('RUNNING','SUCCESS','PARTIAL','FAILED') NOT NULL,
  error_message   TEXT            DEFAULT NULL,
  PRIMARY KEY (run_id)
) ENGINE=InnoDB;

DESC etl_run_log;