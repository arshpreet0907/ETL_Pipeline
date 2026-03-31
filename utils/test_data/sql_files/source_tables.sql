USE vehicle_manufacturing_src;
SHOW TABLES ;

select count(*) from suppliers;
select count(*) from vehicles;
select count(*) from parts;
select count(*) from quality_checks;

# run following only when starting to put data in all tables
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE quality_checks;
TRUNCATE TABLE parts;
TRUNCATE TABLE vehicles;
TRUNCATE TABLE suppliers;
SET FOREIGN_KEY_CHECKS = 1;

# SUPPLIERS
CREATE TABLE suppliers (
  supplier_id     INT            NOT NULL AUTO_INCREMENT,
  supplier_code   VARCHAR(10)    NOT NULL,
  supplier_name   VARCHAR(100)   NOT NULL,
  country         VARCHAR(50)    NOT NULL,
  tier            TINYINT        NOT NULL,
  rating          DECIMAL(3,2)   NOT NULL,
  contract_start  DATE           NOT NULL,
  contract_end    DATE           NOT NULL,
  is_active       TINYINT(1)     NOT NULL DEFAULT 1,
  created_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME       DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (supplier_id),
  UNIQUE KEY uq_supplier_code (supplier_code),
  CONSTRAINT chk_tier    CHECK (tier IN (1,2,3)),
  CONSTRAINT chk_rating  CHECK (rating BETWEEN 0 AND 5),
  CONSTRAINT chk_dates   CHECK (contract_end > contract_start),
  CONSTRAINT chk_active  CHECK (is_active IN (0,1))
) ENGINE=InnoDB;

DESC suppliers;

# VEHICLES
CREATE TABLE vehicles (
  vehicle_id      INT            NOT NULL AUTO_INCREMENT,
  vin             VARCHAR(17)    NOT NULL,
  model_code      VARCHAR(20)    NOT NULL,
  variant         VARCHAR(20)    NOT NULL,
  color_code      VARCHAR(10)    NOT NULL,
  engine_type     VARCHAR(20)    NOT NULL,
  plant_code      VARCHAR(10)    NOT NULL,
  line_number     TINYINT        NOT NULL,
  production_date DATE           NOT NULL,
  shift           ENUM('MORNING','AFTERNOON','NIGHT') NOT NULL,
  status          ENUM('COMPLETED','IN_PROGRESS','ON_HOLD','REJECTED') NOT NULL,
  quality_score   DECIMAL(5,2)   NOT NULL,
  weight_kg       DECIMAL(8,1)   NOT NULL,
  created_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME       DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (vehicle_id),
  UNIQUE KEY uq_vin (vin),
  CONSTRAINT chk_quality CHECK (quality_score BETWEEN 0 AND 100),
  CONSTRAINT chk_weight  CHECK (weight_kg > 0)
) ENGINE=InnoDB;

DESC vehicles;

# PARTS
CREATE TABLE parts (
  part_id          INT            NOT NULL AUTO_INCREMENT,
  vehicle_id       INT            NOT NULL,
  part_number      VARCHAR(20)    NOT NULL,
  part_name        VARCHAR(100)   NOT NULL,
  supplier_code    VARCHAR(10)    NOT NULL,
  quantity         SMALLINT       NOT NULL,
  unit_cost        DECIMAL(10,2)  NOT NULL,
  currency         VARCHAR(3)     NOT NULL DEFAULT 'EUR',
  install_time_min SMALLINT       NOT NULL,
  defect_flag      TINYINT(1)     NOT NULL DEFAULT 0,
  batch_number     VARCHAR(20)    NOT NULL,
  created_at       DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (part_id),
  CONSTRAINT fk_parts_vehicle  FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id),
  CONSTRAINT fk_parts_supplier FOREIGN KEY (supplier_code) REFERENCES suppliers(supplier_code),
  CONSTRAINT chk_qty   CHECK (quantity >= 1),
  CONSTRAINT chk_cost  CHECK (unit_cost > 0),
  CONSTRAINT chk_dflg  CHECK (defect_flag IN (0,1))
) ENGINE=InnoDB;

DESC parts;

# QUALITY_CHECKS
CREATE TABLE quality_checks (
  check_id        INT            NOT NULL AUTO_INCREMENT,
  vehicle_id      INT            NOT NULL,
  check_date      DATE           NOT NULL,
  inspector_id    VARCHAR(10)    NOT NULL,
  station         VARCHAR(20)    NOT NULL,
  test_type       ENUM('PAINT','ELECTRICAL','STRUCTURAL','EMISSIONS','SAFETY') NOT NULL,
  result          ENUM('OK','MINOR_ISSUE','MAJOR_ISSUE') NOT NULL,
  defect_code     VARCHAR(10)    DEFAULT NULL,
  rework_hours    DECIMAL(5,2)   DEFAULT NULL,
  pass_fail       ENUM('PASS','FAIL') NOT NULL,
  created_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (check_id),
  CONSTRAINT fk_qc_vehicle FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id),
  CONSTRAINT chk_rework CHECK (rework_hours IS NULL OR rework_hours >= 0)
) ENGINE=InnoDB;

DESC quality_checks;