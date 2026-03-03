WITH pivoted_data AS (
    SELECT 
        field_id,
        event_timestamp,
        MAX(CASE WHEN measurement_type = 'air_temperature' THEN value END) AS air_temperature_c,
        MAX(CASE WHEN measurement_type = 'air_humidity' THEN value END) AS air_humidity_pct,
        MAX(CASE WHEN measurement_type = 'soil_temperature' THEN value END) AS soil_temperature_c,
        MAX(CASE WHEN measurement_type = 'soil_moisture' THEN value END) AS soil_moisture_pct,
        MAX(CASE WHEN measurement_type = 'precipitation' THEN value END) AS precipitation_mm,
        MAX(CASE WHEN measurement_type = 'solar_radiation' THEN value END) AS solar_radiation_wm2,
        (COUNT(DISTINCT measurement_type) = 6) AS is_record_complete
    FROM raw_sensor_readings
    GROUP BY field_id, event_timestamp
)
INSERT INTO trusted_sensor_readings (
    field_id, event_timestamp, air_temperature_c, air_humidity_pct,
    soil_temperature_c, soil_moisture_pct, precipitation_mm, solar_radiation_wm2,
    air_temperature_status, soil_moisture_status, precipitation_status,
    phenological_phase, is_record_complete
)
SELECT 
    field_id,
    event_timestamp,
    air_temperature_c,
    air_humidity_pct,
    soil_temperature_c,
    soil_moisture_pct,
    precipitation_mm,
    solar_radiation_wm2,
    CASE 
        WHEN air_temperature_c < 15.0 THEN 'critical_low'
        WHEN air_temperature_c >= 15.0 AND air_temperature_c < 20.0 THEN 'alert_low'
        WHEN air_temperature_c >= 20.0 AND air_temperature_c <= 30.0 THEN 'ideal'
        WHEN air_temperature_c > 30.0 AND air_temperature_c <= 35.0 THEN 'alert_high'
        WHEN air_temperature_c > 35.0 THEN 'critical_high'
        ELSE 'unknown'
    END AS air_temperature_status,
    CASE 
        WHEN soil_moisture_pct < 40.0 THEN 'critical_low'
        WHEN soil_moisture_pct >= 40.0 AND soil_moisture_pct < 60.0 THEN 'alert_low'
        WHEN soil_moisture_pct >= 60.0 AND soil_moisture_pct <= 80.0 THEN 'ideal'
        WHEN soil_moisture_pct > 80.0 THEN 'critical_high'
        ELSE 'unknown'
    END AS soil_moisture_status,
    CASE 
        WHEN precipitation_mm = 0 THEN 'dry'
        WHEN precipitation_mm > 0 AND precipitation_mm <= 10 THEN 'light_rain'
        WHEN precipitation_mm > 10 THEN 'heavy_rain'
        ELSE 'unknown'
    END AS precipitation_status,
    'vegetative' AS phenological_phase,
    is_record_complete
FROM pivoted_data
ON CONFLICT (field_id, event_timestamp) DO UPDATE SET 
    air_temperature_c = EXCLUDED.air_temperature_c,
    air_humidity_pct = EXCLUDED.air_humidity_pct,
    soil_temperature_c = EXCLUDED.soil_temperature_c,
    soil_moisture_pct = EXCLUDED.soil_moisture_pct,
    precipitation_mm = EXCLUDED.precipitation_mm,
    solar_radiation_wm2 = EXCLUDED.solar_radiation_wm2,
    air_temperature_status = EXCLUDED.air_temperature_status,
    soil_moisture_status = EXCLUDED.soil_moisture_status,
    precipitation_status = EXCLUDED.precipitation_status,
    is_record_complete = EXCLUDED.is_record_complete,
    processing_timestamp = CURRENT_TIMESTAMP;

/* 
It worked exactly as I wanted.
In the RAW file we had 5,184 rows. 
If we divide 5,184 by 6, we get exactly 864. 
In other words, successfully grouped the data without losing any reads.
*/