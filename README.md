# 🚗 Car Telemetry Real-Time Streaming Dashboard

A real-time IoT data pipeline that simulates live vehicle telemetry, streams it through Azure's cloud infrastructure, and visualizes it on a live-updating Power BI dashboard — with automated fault detection and severity alerting.

![Status](https://img.shields.io/badge/status-active-success)
![Azure](https://img.shields.io/badge/Azure-IoT%20Hub%20%7C%20Stream%20Analytics-0078D4)
![Power BI](https://img.shields.io/badge/Power%20BI-Streaming%20Dataset-F2C811)
![Docker](https://img.shields.io/badge/Docker-Simulator-2496ED)

---

## 📊 Power BI Dashboards — *start here*

> 🖼️ *Dashboard screenshots coming soon.*

The core deliverable of this project is two Power BI dashboards, built and maintained by **[eludehany](https://github.com/eludehany)**:

| Dashboard | Description | File |
|---|---|---|
| 🔴 **Live Streaming Dashboard** | Real-time vehicle telemetry — speed, engine temperature, oil pressure, battery, fuel, and per-tyre readings — updating automatically with **no manual refresh**, powered by an Azure Stream Analytics → Power BI streaming dataset. | [`01_Streaming_Car_Telemetry.pbix`](./01_Streaming_Car_Telemetry.pbix) |
| 📈 **Batch Analytics Dashboard** | Historical, aggregated analysis of vehicle telemetry — trends, fault statistics, and fleet-level summaries — built on dbt-modeled tables from the batch pipeline. | [`02_Batch_Car_Telemetry.pbix`](./02_Batch_Car_Telemetry.pbix) |

**To open either dashboard:** download the `.pbix` file above and open it in [Power BI Desktop](https://www.microsoft.com/en-us/power-platform/products/power-bi/desktop). The streaming dashboard requires an active connection to the Power BI streaming dataset described below to show live data.

---

## 📖 Overview

This project simulates a car's onboard sensors (engine, oil, battery, fuel, tyres, speed) and processes that telemetry through **two complementary pipelines**, mirroring how real data platforms combine both patterns:

- **Batch pipeline** — telemetry is landed, transformed with **dbt**, and orchestrated with **Airflow** into a structured warehouse (Postgres) for historical analysis and reporting.
- **Streaming pipeline** — telemetry flows live through **Azure IoT Hub → Azure Stream Analytics → Power BI**, powering a dashboard that updates in real time with no manual refresh.

**Pipeline:**

```
                     ┌────────────────────────────────────────────┐
                     │              Python Simulator                │
                     │         (generates vehicle telemetry)        │
                     └───────────────┬───────────────┬──────────────┘
                                      │               │
                     BATCH MODE       │               │      STREAM MODE
                                      ▼               ▼
                      ┌───────────────────┐   ┌────────────────────┐
                      │     Postgres       │   │   Azure IoT Hub     │
                      │  (raw landing)      │   └──────────┬─────────┘
                      └─────────┬──────────┘               │
                                │                            ▼
                                ▼                 ┌────────────────────────┐
                      ┌───────────────────┐        │ Azure Stream Analytics  │
                      │   dbt (transform)   │        │ real-time query &       │
                      └─────────┬──────────┘        │ fault classification    │
                                │                    └──────────┬──────────────┘
                                ▼                               │
                      ┌───────────────────┐        ┌────────────┴────────────┐
                      │  Airflow (orchestr.)│        ▼                        ▼
                      └─────────┬──────────┘  Azure SQL Database    Power BI Streaming
                                │              (archive + alerts)      Dataset (live)
                                ▼
                    Batch dashboard (02_Batch_Car_Telemetry.pbix)
```

---

## 🏗️ Architecture

| Component | Role |
|---|---|
| **Simulator** (`simulator/`) | Python service (containerized with Docker) that generates realistic, continuously-varying vehicle sensor data. Can run in **batch** mode (writes to Postgres) or **stream** mode (pushes to Azure IoT Hub via `azure-iot-device`), controlled by the `OUTPUT_MODE` environment variable. |
| **Postgres** (`postgres/`) | Raw landing zone for the batch pipeline — stores simulator output before transformation. |
| **dbt** (`dbt_project/`) | Transforms raw batch telemetry into clean, modeled tables (staging → marts) ready for reporting. |
| **Airflow** (`airflow/`) | Orchestrates the batch pipeline end-to-end — scheduling simulator runs, dbt builds, and data quality checks. |
| **Azure IoT Hub** | Cloud ingestion endpoint for the streaming pipeline. Registers the simulated vehicle as a device (`car_001`) and receives its live telemetry stream. |
| **Azure Stream Analytics** | The real-time processing engine. Runs a continuous SQL-like query over the incoming stream to classify subsystem health, compute severity levels, and route data to multiple outputs simultaneously. |
| **Azure SQL Database** | Durable storage for the streaming pipeline — full telemetry log plus a dedicated fault-alerts table. |
| **Power BI Streaming Dataset** | Live output target for the streaming pipeline. Powers a dashboard that updates automatically with no manual refresh, using Power BI's push/streaming API. |

---

## 📊 Telemetry Signals

The simulator emits the following signals per tick:

- **Engine:** `engine_temp`, `rpm`
- **Drivetrain:** `speed_kmh`, `drive_state`, `gear`, `throttle`
- **Fluids:** `oil_pressure`, `oil_temp`, `fuel_level`, `fuel_flow`
- **Electrical:** `battery_voltage`, `alternator_on`, `ignition_on`
- **Tyres:** per-wheel `tyre_pressure` and `tyre_temp` (FL/FR/RL/RR)
- **Diagnostics:** `fault_active`, `fault_type`, `check_engine`, `abs_active`

---

## ⚙️ Stream Analytics Query Logic

The job runs two parallel output streams from a single input:

### 1. `caralerts` → Azure SQL (fault-triggered only)
Fires **only when `fault_active = 1`**. For each fault it computes:
- `subsystem` — which system is affected (ENGINE / OIL / BATTERY / FUEL / TYRES)
- `message` — a human-readable description of the fault
- `min_tyre_pressure` / `max_tyre_temp` — worst-case values across all four tyres
- Per-subsystem health status (`NORMAL` / `WARNING` / `CRITICAL`) based on safety thresholds
- `severity` — an overall vehicle health rating combining all subsystems

### 2. `powerbioutput` → Power BI (continuous stream)
Pushes **every telemetry tick, unfiltered**, so the dashboard stays live and animated at all times — not just during fault events. Tyre pressure/temperature arrays are flattened into individual FL/FR/RL/RR columns for direct visualization.

---

## 🖥️ Dashboards

This project ships **two Power BI dashboards**, one per pipeline:

### `01_Streaming_Car_Telemetry.pbix` — Live dashboard
Built on the Power BI **streaming dataset**, connected live to the Stream Analytics output. Includes auto-updating tiles for:
- Current speed, engine temperature, oil pressure, battery voltage, fuel level
- Per-tyre pressure/temperature readouts
- Live severity/status indicators
- Time-series trend of key metrics

> ⚠️ Streaming tiles require the **Power BI Service** (app.powerbi.com), not Power BI Desktop — Desktop reports need manual refresh and don't support push-streaming datasets.

### `02_Batch_Car_Telemetry.pbix` — Historical/analytical dashboard
Built on the **dbt-modeled tables** produced by the batch pipeline. Used for deeper, non-real-time analysis — trends over time, aggregate fault statistics, and fleet-level summaries — the kind of reporting that complements the live operational view above.

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop
- Git
- An Azure subscription with an IoT Hub, Stream Analytics Job, and SQL Database provisioned
- A Power BI account

### 1. Clone the repository
```bash
git clone https://github.com/<your-username>/car-telemetry-streaming.git
cd car-telemetry-streaming
```

### 2. Configure environment variables
Copy the example env file and fill in your Azure IoT Hub device connection string:
```bash
cp .env.example .env
```
```ini
IOTHUB_CONNECTION_STRING=HostName=<your-hub>.azure-devices.net;DeviceId=car_001;SharedAccessKey=<your-key>
OUTPUT_MODE=stream
```

### 3. Start the simulator
```bash
docker compose up simulator
```

### 4. Start the Stream Analytics job
In the Azure Portal, start the Stream Analytics job (`Start job → Now`).

### 5. View the live dashboard
Open the dashboard in **Power BI Service** — tiles connected to the streaming dataset update automatically as data arrives.

---

## 📂 Project Structure

```
.
├── simulator/
│   ├── car_simulator.py       # Telemetry generator + IoT Hub client
│   ├── azure_config.py        # Reads connection settings from environment (gitignored)
│   └── Dockerfile
├── dbt_project/                # SQL transformations for historical data
├── airflow/                    # Batch/orchestration workflows
├── postgres/                   # Batch pipeline raw landing zone
├── 01_Streaming_Car_Telemetry.pbix  # Live streaming dashboard
├── 02_Batch_Car_Telemetry.pbix      # Historical/analytical dashboard
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## 🔐 Security Notes

- `.env` and `simulator/azure_config.py` are **gitignored** — never commit connection strings, SAS keys, or credentials.
- Rotate your IoT Hub device keys periodically via **Azure Portal → IoT Hub → Devices → Manage keys**.

---

## 🧰 Tech Stack

`Azure IoT Hub` · `Azure Stream Analytics` · `Azure SQL Database` · `Power BI` · `Python` · `Docker` · `dbt` · `Airflow`

---

## 📌 Roadmap / Possible Extensions

- [ ] Migrate Power BI output from Streaming Dataset to Push Dataset (Microsoft is retiring the classic streaming output in Oct 2027)
- [ ] Add anomaly detection using historical patterns
- [ ] Extend simulator to support a fleet of multiple vehicles
- [ ] Add automated alerting (email/SMS) on CRITICAL severity events

---

## 👤 Author

Built as part of the **DEPI (Digital Egypt Pioneers Initiative)** Microsoft Data Engineering track.
