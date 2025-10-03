# Drives smart monitoring

A tool to monitor disk health via SMART, compute a normalized **health %**, and publish via MQTT with optional Home Assistant auto-discovery.

## Features

- Scans local block devices (HDD, SATA SSD, NVMe)
- Parses SMART data (via `smartctl`) and computes a health score (0–100)
- Supports periodic publishing via MQTT
- Publishes per-drive JSON state + scalar topics (health %, SMART status)
- Optional **Home Assistant MQTT Discovery**:
  - One HA device per host
  - One sensor per drive for health %
  - One binary sensor per drive for SMART problem
- Availability support (via heartbeat) so HA marks device offline if the script stops
- Output modes: human-readable table, JSON, verbose breakdown

## Requirements

- [smartmontools](https://www.smartmontools.org/) (`smartctl` must be in PATH)
- Python ≥ 3.8
- Optional: [paho-mqtt](https://pypi.org/project/paho-mqtt/) if using MQTT / HA features

## Installation

```bash
git clone https://github.com/giovi321/drives-smart-monitoring.git
cd drives-smart-monitoring
python3 -m venv venv
source venv/bin/activate
pip install paho-mqtt
```

Install `smartctl`:

```bash
sudo apt-get install smartmontools
```

## Usage

```bash
./drive_health_score.py [OPTIONS]
```

### Key options

- `--once` : run one scan and exit  
- `--no-mqtt` : disable MQTT (for local-only output)  
- `--interval SEC` : seconds between scans (default 3600)  
- `--broker HOST` / `--port N` : MQTT broker settings  
- `--client-id ID` : MQTT client ID  
- `--base-topic TOPIC` : root MQTT topic (default `servers/smart`)  
- `--retain` : publish MQTT messages with retain  
- `--ha-discovery` : enable Home Assistant MQTT Discovery  
- `--ha-prefix` : HA discovery prefix (default `homeassistant`)  
- `--ha-node` : override HA device name  
- `--format {table,json}` : output style  
- `--verbose` : for table, show penalty breakdown  
- `--top N` : show top N penalties  
- `--json-verbose` : include summary & settings in JSON root  
- `--ignore-samsung-181` : skip SMART 181 penalty on Samsung SSDs  

## MQTT Topics & Payloads

For each drive:

- JSON state:

```
<base-topic>/<hostname>/<drive_slug>/state
```

Example payload:

```json
{
  "device": "/dev/sda",
  "model": "ST4000DM004",
  "serial": "ZGY9PQ0D",
  "health_percent": 91.3,
  "metrics": {
    "power_on_hours": 33263,
    "reallocated": 0,
    "pending": 0,
    "crc_errors": 0,
    "breakdown": [ ... ],
    "total_penalty": 8.7
  },
  "smart_overall_passed": true,
  "timestamp": 1759513173
}
```

- Scalar topics (retained):

```
<base-topic>/<hostname>/<drive_slug>/health_percent   → e.g. "91.3"
<base-topic>/<hostname>/<drive_slug>/problem          → "ON" / "OFF"
<base-topic>/<hostname>/availability                  → "online" / "offline"
<base-topic>/<hostname>/summary                       → JSON summary of all drives
```

## Home Assistant Discovery

When `--ha-discovery` is enabled:

- Publishes retained config topics under `<ha-prefix>/sensor/...` and `<ha-prefix>/binary_sensor/...`
- All sensors belong to a single HA “device” (the host)
- Each drive has:
  - A sensor for health %
  - A binary sensor for SMART overall status
- Entities use scalar topics above; JSON state is attached as attributes
- Availability: entity status follows `<base-topic>/<hostname>/availability`

## Examples

Run once, local output:

```bash
./drive_health_score.py --no-mqtt --once
```

Periodic with MQTT + HA discovery:

```bash
./drive_health_score.py   --broker mqtt.local --port 1883   --retain --interval 900   --ha-discovery --ha-node GC01SRVR
```

## Systemd Service Example

```ini
[Unit]
Description=Drive SMART Health Monitoring

[Service]
ExecStart=/home/user/drives-smart-monitoring/venv/bin/python3   /home/user/drives-smart-monitoring/drive_health_score.py   --broker mqtt.local --retain --ha-discovery
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable & start:

```bash
sudo systemctl enable drive-health.service
sudo systemctl start drive-health.service
```

## Limitations

- Health % is heuristic, not a guarantee. Always keep backups.
- Some USB enclosures block SMART passthrough.
- Vendor-specific attributes may differ; scoring is best-effort.

# License
The content of this repository is licensed under the [WTFPL](http://www.wtfpl.net/).

```
Copyright © 2024 giovi321
This work is free. You can redistribute it and/or modify it under the
terms of the Do What The Fuck You Want To Public License, Version 2,
as published by Sam Hocevar. See the LICENSE file for more details.
```
