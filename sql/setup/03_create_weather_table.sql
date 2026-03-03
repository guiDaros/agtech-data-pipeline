-- Garante a função de UUID
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS raw_weather_forecast (
    forecast_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    location_name VARCHAR(100) DEFAULT 'Ames-IA',
    forecast_timestamp TIMESTAMP NOT NULL,
    temperature_c DECIMAL(5,2),
    humidity_pct DECIMAL(5,2),
    rain_mm_3h DECIMAL(6,2) DEFAULT 0.0,
    weather_condition VARCHAR(100),
    raw_payload JSONB,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_forecast_time ON raw_weather_forecast(location_name, forecast_timestamp);

COMMENT ON TABLE raw_weather_forecast IS 'Tabela RAW para dados da API OpenWeather. Inclui coluna JSONB para schema-on-read.';

/* 
I used Ames, Iowa as the city for the data because it's the world capital of agronomy, 
and from what I could research, it has excellent data in APIs.
*/