DROP TABLE IF EXISTS trusted_sensor_readings;

CREATE TABLE trusted_sensor_readings (
    trusted_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    field_id VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    
    air_temperature_c DECIMAL(5,2),
    air_humidity_pct DECIMAL(5,2),
    soil_temperature_c DECIMAL(5,2),
    soil_moisture_pct DECIMAL(5,2),
    precipitation_mm DECIMAL(6,2),
    solar_radiation_wm2 DECIMAL(6,2),
    
    air_temperature_status VARCHAR(20),
    soil_moisture_status VARCHAR(20),
    precipitation_status VARCHAR(20),
    
    phenological_phase VARCHAR(50), 
    
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_record_complete BOOLEAN DEFAULT FALSE 
);

CREATE INDEX idx_trusted_field_time ON trusted_sensor_readings(field_id, event_timestamp);
CREATE INDEX idx_trusted_status ON trusted_sensor_readings(soil_moisture_status);
CREATE UNIQUE INDEX uniq_field_time ON trusted_sensor_readings(field_id, event_timestamp);

COMMENT ON TABLE trusted_sensor_readings IS 'Tabela consolidada (Wide). Uma linha por timestamp/campo com métricas pivotadas e status calculados.';