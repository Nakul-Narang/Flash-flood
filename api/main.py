import json
from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer
import threading

app = FastAPI(
    title="NDRF Flash Flood Early Warning API",
    description=(
        "Hilly Region Flash Flood Prediction & Monitoring System (SIH Problem"
        " #26192)"
    ),
    version="1.0.0",
)

# In-memory storage for latest system state (simulating cache/database layer)
latest_telemetry = {}
active_alerts = []


def background_kafka_consumer():
  """Background thread to consume telemetry and alerts from Kafka topics."""
  try:
    consumer = KafkaConsumer(
        "sensors.telemetry.raw",
        "ml.predictions.alerts",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="fastapi-dashboard-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    for message in consumer:
      topic = message.topic
      data = message.value
      if topic == "sensors.telemetry.raw":
        device_id = data.get("device_id")
        latest_telemetry[device_id] = data
      elif topic == "ml.predictions.alerts":
        active_alerts.insert(0, data)
        # Keep only the latest 50 alerts in memory
        if len(active_alerts) > 50:
          active_alerts.pop()
  except Exception as e:
    print(f"Kafka background consumer error: {e}")


@app.on_event("startup")
def startup_event():
  # Start the background Kafka listener thread
  t = threading.Thread(target=background_kafka_consumer, daemon=True)
  t.start()


@app.get("/")
def read_root():
  return {
      "system": "NDRF Flash Flood Early Warning System",
      "status": "Operational",
  }


@app.get("/telemetry/latest")
def get_latest_telemetry():
  """Fetch the latest live readings from all deployed field sensors."""
  if not latest_telemetry:
    return {"message": "No telemetry data received yet."}
  return {"sensor_count": len(latest_telemetry), "data": latest_telemetry}


@app.get("/alerts/active")
def get_active_alerts():
  """Fetch active critical flash flood warnings and elevated risk notifications."""
  return {"alert_count": len(active_alerts), "alerts": active_alerts}
      
