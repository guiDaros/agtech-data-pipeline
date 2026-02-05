import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid

def generate_sensor_data(days=7, field_id="FIELD_001"):
    """
    This script generates synthetic sensor data with a granularity of 5 minutes.
    which is sufficient to create a basic correlation between the variables
    """
    start_date = datetime.now() - timedelta(days=days)
    periods = days * 24 * 12  # 12 readings per hour (5-minute intervals)
    timestamps = [start_date + timedelta(minutes=5 * i) for i in range(periods)]
    
    data = []
    
    # base for simulation
    for ts in timestamps:
        hour = ts.hour
        # Thermal simulation: warmer around 2PM and cooler around 4AM
        temp_base = 25 + 5 * np.sin(np.pi * (hour - 10) / 12)
        air_temp = temp_base + np.random.normal(0, 0.5)
        soil_temp = temp_base * 0.9 + np.random.normal(0, 0.3)
        
        # Humidity: inversely proportional to temperature
        air_hum = max(0, min(100, 70 - 10 * np.sin(np.pi * (hour - 10) / 12) + np.random.normal(0, 2)))
        soil_moist = max(0, min(100, 30 + np.random.normal(0, 1))) # Soil moisture with low variability
        
        # Rain: sporadic events, with a 98% probability of being zero
        precip = 0.0 if np.random.random() > 0.02 else np.random.uniform(0.5, 5.0)
        
        # Radiation: daytime only
        solar = max(0, 800 * np.sin(np.pi * (hour - 6) / 12)) if 6 <= hour <= 18 else 0
        
        # Structure aligned with the defined RAW schema.
        readings = [
            ("air_temperature", air_temp, "°C"),
            ("soil_temperature", soil_temp, "°C"),
            ("air_humidity", air_hum, "%"),
            ("soil_moisture", soil_moist, "%"),
            ("precipitation", precip, "mm"),
            ("solar_radiation", solar, "W/m²")
        ]
        
        for m_type, val, unit in readings:
            data.append({
                "reading_id": str(uuid.uuid4()),
                "field_id": field_id,
                "source_type": "sensor",
                "source_name": f"sensor_{m_type}",
                "measurement_type": m_type,
                "value": round(val, 2),
                "unit": unit,
                "event_timestamp": ts,
                "ingestion_timestamp": datetime.now(),
                "raw_payload": f'{{"value": {round(val, 2)}, "unit": "{unit}", "sensor": "{m_type}"}}'
            })
            
    return pd.DataFrame(data)

if __name__ == "__main__":
    df = generate_sensor_data(days=3)
    df.to_csv("data/raw_sensors_preview.csv", index=False)
    print(f"✅ Simulação concluída: {len(df)} linhas geradas em 'data/raw_sensors_preview.csv'")