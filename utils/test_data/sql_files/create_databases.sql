# source database
CREATE DATABASE IF NOT EXISTS vehicle_manufacturing_src
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE vehicle_manufacturing_src;

# target database
CREATE DATABASE IF NOT EXISTS vehicle_manufacturing_dw
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE vehicle_manufacturing_dw;

SHOW DATABASES ;