SELECT * FROM dim_store_details;

-- find store number per country 

SELECT 
    country_code, 
    COUNT(store_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_no_stores DESC;
	
	
-- find number of stores per location 

SELECT 
    locality, 
    COUNT(store_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality 
ORDER BY 
    total_no_stores DESC;
	
-- months that produced the most amount of sales
SELECT 
    SUM(ot.product_quantity * dp.product_price) AS total_sales, 
    ddt.month
FROM 
    orders_table ot
JOIN 
    dim_products dp ON ot.product_code = dp.product_code
JOIN 
    dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
GROUP BY 
    ddt.month
ORDER BY 
    total_sales DESC;


	
-- SELECT * FROM dim_store_details;

--find number of sales online and offline
SELECT 
    COUNT(ot.product_code) AS numbers_of_sales, 
    SUM(ot.product_quantity) AS product_quantity_count,
    CASE 
        WHEN dsd.store_code LIKE 'WEB%' THEN 'Online'
        ELSE 'Offline'
    END AS location
FROM 
    orders_table ot
JOIN 
    dim_store_details dsd ON ot.store_code = dsd.store_code
GROUP BY 
    CASE 
        WHEN dsd.store_code LIKE 'WEB%' THEN 'Online'
        ELSE 'Offline'
    END;


--what percentage of sales came from each location.

SELECT 
    COUNT(ot.product_code) AS numbers_of_sales, 
    SUM(ot.product_quantity) AS product_quantity_count, 
    SUM(ot.product_quantity * dp.product_price) AS total_sales_value,
    dsd.store_type AS location,
    (SUM(ot.product_quantity * dp.product_price) / 
        (SELECT SUM(ot2.product_quantity * dp2.product_price)
         FROM orders_table ot2
         JOIN dim_products dp2 ON ot2.product_code = dp2.product_code)) * 100 AS sales_percentage
FROM 
    orders_table ot
JOIN 
    dim_products dp ON ot.product_code = dp.product_code
JOIN 
    dim_store_details dsd ON ot.store_code = dsd.store_code
GROUP BY 
    dsd.store_type;


SELECT * FROM dim_store_details;

-- which month had the highest cost of sales

SELECT 
    SUM(ot.product_quantity * dp.product_price) AS total_sales,
    ddt.year,
    ddt.month
FROM 
    orders_table ot
JOIN 
    dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
JOIN 
    dim_products dp ON ot.product_code = dp.product_code
GROUP BY 
    ddt.year,
    ddt.month
ORDER BY 
    total_sales DESC;

-- staff numbers in each location 

SELECT 
    SUM(ds.staff_numbers) AS total_staff_numbers,
    ds.country_code
FROM 
    dim_store_details ds
GROUP BY 
    ds.country_code
ORDER BY 
    total_staff_numbers DESC;



-- which german store is selling the most 

SELECT 
    SUM(ot.product_quantity * dp.product_price) AS total_sales,
    dsd.store_type,
    dsd.country_code
FROM 
    orders_table ot
JOIN 
    dim_products dp ON ot.product_code = dp.product_code
JOIN 
    dim_store_details dsd ON ot.store_code = dsd.store_code
WHERE 
    dsd.country_code = 'DE'
GROUP BY 
    dsd.store_type, dsd.country_code
ORDER BY 
    total_sales DESC;


-- how quickly is the comapny making sales

WITH SalesWithTimestamps AS (
    SELECT 
        ot.product_code,
        ddt.year,
        CAST(
            CONCAT(
                ddt.year, '-', 
                LPAD(ddt.month::TEXT, 2, '0'), '-', 
                LPAD(ddt.day::TEXT, 2, '0'), ' ', 
                CASE 
                    WHEN ddt.time_period = 'Morning' THEN '09:00:00'
                    WHEN ddt.time_period = 'Afternoon' THEN '14:00:00'
                    WHEN ddt.time_period = 'Evening' THEN '18:00:00'
                    WHEN ddt.time_period = 'Night' THEN '21:00:00'
                    ELSE '00:00:00' -- Default to midnight if none of the above
                END
            ) AS TIMESTAMP
        ) AS order_timestamp
    FROM 
        orders_table ot
    JOIN 
        dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
),

SalesTimeDifferences AS (
    SELECT 
        year,
        order_timestamp,
        LEAD(order_timestamp) OVER (PARTITION BY year ORDER BY order_timestamp) AS next_order_timestamp,
        EXTRACT(EPOCH FROM (LEAD(order_timestamp) OVER (PARTITION BY year ORDER BY order_timestamp) - order_timestamp)) AS time_diff_seconds
    FROM 
        SalesWithTimestamps
)

SELECT 
    year,
    json_build_object(
        'hours', FLOOR(AVG(time_diff_seconds) / 3600),
        'minutes', FLOOR((AVG(time_diff_seconds) % 3600) / 60),
        'seconds', FLOOR(AVG(time_diff_seconds) % 60),
        'milliseconds', FLOOR((AVG(time_diff_seconds) * 1000) % 1000)
    ) AS actual_time_taken
FROM 
    SalesTimeDifferences
WHERE 
    time_diff_seconds IS NOT NULL
GROUP BY 
    year
ORDER BY 
    year;

