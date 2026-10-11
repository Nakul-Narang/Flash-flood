import json
from kafka import KafkaConsumer, KafkaProducer

# Configure Consumer to read raw sensor data from the edge
consumer = KafkaConsumer(
    "sensors.telemetry.raw",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="flash-flood-engine-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# Configure Producer to publish risk predictions and alerts
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

alert_topic = "ml.predictions.alerts"

print("AI Prediction Engine & Consumer active. Analyzing telemetry streams...")

for message in consumer:
  data = message.value
  water_level = data.get("water_level_m", 0.0)
  rainfall = data.get("rainfall_intensity_mmhr", 0.0)
  device_id = data.get("device_id")

  # Real-time flash flood risk heuristic engine tailored for hilly terrain
  risk_level = "NORMAL"
  if water_level > 5.0 or (water_level > 4.0 and rainfall > 25.0):
    risk_level = "CRITICAL_FLASH_FLOOD_WARNING"
  elif water_level > 4.2 or rainfall > 20.0:
    risk_level = "ELEVATED_RISK"

  alert_payload = {
      "device_id": device_id,
      "timestamp": data.get("timestamp"),
      "water_level_m": water_level,
      "rainfall_intensity_mmhr": rainfall,
      "risk_level": risk_level,
  }

  if risk_level != "NORMAL":
    producer.send(alert_topic, value=alert_payload)
    print(f"ALERT PUBLISHED [{risk_level}]: {alert_payload}")
  else:
    print(f"Processed normal reading for {device_id}: {water_level}m")
  
