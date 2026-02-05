DROP TABLE IF EXISTS raw_sensor_readings;

CREATE TABLE raw_sensor_readings (
    reading_id UUID PRIMARY KEY,         
    field_id VARCHAR(50) NOT NULL,        
    source_type VARCHAR(20) NOT NULL,     
    source_name VARCHAR(50),             
    measurement_type VARCHAR(50) NOT NULL,
    
    value DECIMAL(10, 2),                 
    unit VARCHAR(20),                     
    
    event_timestamp TIMESTAMP NOT NULL,   
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    raw_payload JSONB                     
);

CREATE INDEX idx_raw_event_time ON raw_sensor_readings(event_timestamp);
CREATE INDEX idx_raw_measurement ON raw_sensor_readings(measurement_type);

COMMENT ON TABLE raw_sensor_readings IS 'Tabela de ingestão imutável. Contém dados brutos de sensores e APIs sem regras de negócio aplicadas.';