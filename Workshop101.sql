-- Lab 3, Part 1, Step 2 
-- Create lab schema

CREATE SCHEMA nyc_uber_rides.<yourname>;

-- Lab 3, Part 1, Step 4
-- Confirm lab schema

SHOW TABLES FROM nyc_uber_rides.<yourname>;

-- Lab 3, Part 2, Step 1
-- Create external table

CREATE TABLE
 rides_raw_data (
   dispatching_base varchar,
   pickup_date varchar,
   affiliated_base_num varchar,
   location_id varchar,
   year_month varchar
 )
WITH
 (
   format = 'csv',
   type = 'hive',
   external_location = 's3://starburst101-handsonlab-nyc-uber-rides/year_month',
   skip_header_line_count = 1,
   partitioned_by = array['year_month']
 );

call system.sync_partition_metadata('<yourname>', 'rides_raw_data', 'ADD');

-- Lab 3, Part 2, Step 2
-- Validate data

SELECT *
FROM rides_raw_data
LIMIT 10;

SELECT DISTINCT
 (year_month)
FROM
 rides_raw_data
ORDER BY
 year_month;

-- Lab 3, Part 3, Step 1
-- Create the structure table

CREATE TABLE
 ride_pickups (
   dispatching_base varchar,
   pickup_date timestamp (6),
   affiliated_base_num varchar,
   location_id integer,
   year_month varchar
 )
WITH
 (
   type = 'iceberg',
   format = 'orc',
   partitioning = array['year_month']
 );

-- Lab 3, Part 3, Step 2
-- Insert data into the structure table

INSERT INTO
 ride_pickups
SELECT
 dispatching_base,
 cast(pickup_date as timestamp (6)),
 affiliated_base_num,
 cast(location_id as INT),
 year_month
FROM
 rides_raw_data;

-- Lab 3, Part 3, Step 3
-- Validate the structure table

SELECT * FROM ride_pickups LIMIT 10;

DESCRIBE ride_pickups;

-- Lab 3, Part 4, Step 2
-- Create the consume table

CREATE TABLE
 rides_by_zone
WITH
 (type = 'iceberg', format = 'orc') AS
SELECT
 p.dispatching_base,
 p.pickup_date,
 p.location_id,
 date_format(p.pickup_date, '%W') weekday,
 date_format(p.pickup_date, '%M') month,
 l.borough,
 l.zone
FROM
 ride_pickups p
 INNER JOIN taxi_zone_lookup.taxi_zones.zone_lookup l ON p.location_id = l.location_id;

 SELECT * FROM rides_by_zone LIMIT 10;

-- Lab 3, Part 4, Step 3
-- Run interactive analytics

SELECT
 COUNT(*) AS total_rides,
 borough
FROM
 rides_by_zone
GROUP BY
 borough
ORDER BY
 COUNT(*) DESC;


 SELECT
 borough,
 weekday,
 COUNT(*) AS total_rides,
 RANK() OVER (PARTITION BY borough ORDER BY COUNT(weekday) DESC
 ) AS rank_column
FROM
 rides_by_zone
GROUP BY
 borough,
 weekday
ORDER BY
 borough,
 COUNT(*) DESC;


WITH
 weekly AS (
   SELECT
     borough,
     weekday,
     COUNT(*) AS total_rides,
     RANK() OVER (PARTITION BY borough ORDER BY COUNT(weekday) DESC
     ) AS rank_column
   FROM
     rides_by_zone
   GROUP BY
     borough,
     weekday
   ORDER BY
     borough,
     COUNT(*) DESC
 )
SELECT
 borough,
 weekday,
 total_rides
FROM
 weekly
WHERE
 rank_column = 1;


-- Lab 3, Part 4, Step 4
-- Create the marketing views

CREATE OR REPLACE VIEW borough_most_pop_weekday_vw AS
WITH
 weekly AS (
   SELECT
     borough,
     weekday,
     COUNT(*) AS total_rides,
     RANK() OVER (PARTITION BY borough ORDER BY COUNT(weekday) DESC
     ) AS rank_column
   FROM
     rides_by_zone
   GROUP BY
     borough,
     weekday
   ORDER BY
     borough,
     COUNT(*) DESC
 )
SELECT
 borough,
 weekday,
 total_rides
FROM
 weekly
WHERE
 rank_column = 1;

SELECT * FROM borough_most_pop_weekday_vw;

CREATE OR REPLACE VIEW borough_most_pop_month_vw AS
WITH
 monthly AS (
   SELECT
     borough,
     month,
     COUNT(*) AS total_rides,
     rank() OVER (PARTITION BY borough ORDER BY COUNT(month) DESC
     ) AS rank_column
   FROM
     rides_by_zone
   GROUP BY
     borough,
     month
   ORDER BY
     borough,
     COUNT(*) DESC
 )
SELECT
 borough,
 month,
 total_rides
FROM
 monthly
WHERE
 rank_column = 1;

SELECT * FROM borough_most_pop_month_vw;

-- Lab 3, Part 5, Step 2
-- Test the marketing role

SELECT * FROM borough_most_pop_month_vw;

-- Show query that does not work with marketing role

CREATE OR REPLACE VIEW
  borough_most_pop_weekday_vw AS
WITH
  weekly AS (
    SELECT
      borough,
      weekday,
      COUNT(*) AS total_rides,
      RANK() OVER (
        PARTITION BY
          borough
        ORDER BY
          COUNT(weekday) DESC
      ) AS rank_column
    FROM
      rides_by_zone
    GROUP BY
      borough,
      weekday
    ORDER BY
      borough,
      COUNT(*) DESC
  )
SELECT
  borough,
  weekday,
  total_rides
FROM
  weekly


