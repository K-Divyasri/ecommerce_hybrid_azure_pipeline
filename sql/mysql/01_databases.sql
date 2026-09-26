-- Runs once, the first time the MySQL container starts with an empty volume.
-- olist      = the OLTP "live store" (the shop's application database) + ETL control tables
-- analytics  = serving tables written back by Spark and pandas (what BI tools read)
CREATE DATABASE IF NOT EXISTS olist CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS analytics CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- the image already created user 'olist' (MYSQL_USER); give it both databases
GRANT ALL PRIVILEGES ON olist.* TO 'olist'@'%';
GRANT ALL PRIVILEGES ON analytics.* TO 'olist'@'%';
FLUSH PRIVILEGES;
