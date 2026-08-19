CREATE DATABASE IF NOT EXISTS GLOBALPARTNERS_RAW;
CREATE DATABASE IF NOT EXISTS GLOBALPARTNERS_CLEANED;
CREATE DATABASE IF NOT EXISTS GLOBALPARTNERS_TRANSFORMED;

USE GLOBALPARTNERS_RAW;

CREATE TABLE IF NOT EXISTS date_dim_raw (
	date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    week INT,
    day_of_week VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(100)
);
drop table order_items_raw;
CREATE TABLE IF NOT EXISTS order_items_raw (
	app_name VARCHAR(100),
    restaurant_id VARCHAR(100),
    creation_time_utc DATETIME,
    order_id VARCHAR(100), 
    user_id VARCHAR(100),
    printed_card_number VARCHAR(100),
    is_loyalty BOOLEAN,
    currency VARCHAR(10),
    lineitem_id VARCHAR(100) PRIMARY KEY, 
    item_category VARCHAR(100),
    item_name VARCHAR(255),
    item_price DECIMAL(10, 2),
    item_quantity INT,
    INDEX idx_order_id (order_id)
);


CREATE TABLE IF NOT EXISTS order_item_options_raw (
    order_id VARCHAR(100),
    lineitem_id VARCHAR(100),
    option_group_name VARCHAR(100),
    option_name VARCHAR(255),
    option_price DECIMAL(10, 2),
    option_quantity INT,
    id INT AUTO_INCREMENT PRIMARY KEY,
    INDEX idx_lineitem (lineitem_id)
);

-- 1. Load Date Dimension

-- Clear table before loading
TRUNCATE TABLE date_dim_raw;

-- Load with on-the-fly transformation for booleans and blank strings
LOAD DATA LOCAL INFILE '/Users/dionnedm29/Documents/DE\ Academy/End_to_End_Projects/GlobalPartners/data/date_dim.csv'
INTO TABLE date_dim_raw
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
  @date_key,
  @year, 
  @month, 
  @week, 
  @day_of_week, 
  @is_weekend, 
  @is_holiday, 
  @holiday_name
)
SET
  date_key = STR_TO_DATE(@date_key, '%d-%m-%Y'),
  day_of_week = @day_of_week,
  week = NULLIF(@week, ''),
  month = @month,
  year = NULLIF(@year, ''),
  -- Converts string 'True'/'False' (or '1'/'0') to actual 1/0 Booleans
  is_weekend = CASE 
				WHEN LOWER(@is_weekend) IN ('true', '1', 'yes') THEN 1 
                ELSE 0 
               END,
  is_holiday = CASE 
                WHEN LOWER(@is_holiday) IN ('true', '1', 'yes') THEN 1 
                ELSE 0 
               END,
  -- Converts blank/empty holiday_name entries into strict database NULLs
  holiday_name = NULLIF(TRIM(@holiday_name), '');

-- Verify data load successfully ran
SELECT * FROM date_dim_raw LIMIT 5;
SELECT COUNT(*) FROM date_dim_raw;

-- 2. Load Order Items

-- Clear table before loading
TRUNCATE TABLE order_items_raw;

LOAD DATA LOCAL INFILE '/Users/dionnedm29/Documents/DE\ Academy/End_to_End_Projects/GlobalPartners/data/order_items.csv'
INTO TABLE order_items_raw
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
	app_name,
    restaurant_id,
    @creation_time_utc,
    order_id, 
    user_id,
    printed_card_number,
    @is_loyalty,
    currency,
    lineitem_id, 
    item_category,
    item_name,
    item_price,
    item_quantity
)
SET
  creation_time_utc = STR_TO_DATE(LEFT(@creation_time_utc, 19), '%Y-%m-%dT%H:%i:%s'),
  -- user_id = NULLIF(@user_id, ''),
--   printed_card_number = NULLIF(@printed_card_number, ''),
  -- Converts string 'True'/'False' (or '1'/'0') to actual 1/0 Booleans
  is_loyalty = CASE 
				WHEN LOWER(@is_loyalty) IN ('true', '1', 'yes') THEN 1 
                ELSE 0 
               END;
 
 -- Verify data load successfully ran
 SELECT * FROM order_items_raw LIMIT 5;
 SELECT COUNT(*) FROM order_items_raw;          

-- 3. Load Order Item Options

-- Clear table before loading
TRUNCATE TABLE order_item_options_raw;

LOAD DATA LOCAL INFILE '/Users/dionnedm29/Documents/DE\ Academy/End_to_End_Projects/GlobalPartners/data/order_item_options.csv'
INTO TABLE order_item_options_raw
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- Verify data load successfully ran
SELECT * FROM order_item_options_raw LIMIT 5;
SELECT COUNT(*) FROM order_item_options_raw; 




