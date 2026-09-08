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
- `--ignore-nvme-used` : damp the NVMe endurance penalty to 40% of its weight  
- `--ignore-samsung-181` : deprecated no-op, kept so existing unit files keep working  
- `--prune-stale-topics` : one-time cleanup of drive slugs left by earlier versions  

## MQTT Topics & Payloads

For each drive:

- JSON state:

```
<base-topic>/<hostname>/<serial>/state
```

The per-drive path segment is the drive serial. It is deliberately not the device name: Linux assigns
`sd*` by discovery order, so a drive that moves from `sdc` to `sdi` would otherwise become a second
sensor while the first froze at its last reading. WWN is used when a drive publishes no usable serial,
and the kernel device name only as a last resort.

Do not put the hostname in `--base-topic`. The program appends it, so `--base-topic servers/smart` on
host `foo` publishes to `servers/smart/foo/...`. Copying a unit file between hosts without editing
`--base-topic` makes the new host publish into the old one's tree.

Example payload:

```json
{
  "device": "/dev/sda",
  "model": "ST4000DM004",
  "serial": "XXXXXXXX",
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
<base-topic>/<hostname>/<serial>/health_percent   → e.g. "91.3"
<base-topic>/<hostname>/<serial>/problem          → "ON" / "OFF"
<base-topic>/<hostname>/availability              → "online" / "offline"
<base-topic>/<hostname>/summary                   → JSON summary of all drives
<base-topic>/<hostname>/known_drives              → JSON list of serials published
```

`known_drives` is bookkeeping. Because every drive topic is retained, a broker replays a removed
drive's last reading forever, so a drive that has been pulled keeps a plausible health value and
nothing looks wrong. Each run compares the current drives against this list and deletes the retained
topics and discovery configs of any that have gone away. `availability` does not cover this: it is the
publisher's last-will, so it only fires when the whole process dies, never when one drive disappears.

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
./drive_health_score.py   --broker mqtt.example.lan --port 1883   --retain --interval 900   --ha-discovery
```

## Systemd Service Example

```ini
[Unit]
Description=Drive SMART Health Monitoring
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
ExecStart=/home/user/drives-smart-monitoring/venv/bin/python3 /home/user/drives-smart-monitoring/drive_health_score.py \
--broker mqtt.example.lan --port 1883 --username username --password password \
--base-topic servers/smart \
--interval 43200 --ha-discovery --heartbeat-sec 30 --retain
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Three things in that unit are deliberate:

- `--base-topic` contains no hostname. The program appends the hostname itself, so the unit file can be
  copied to another host unchanged. Put a hostname there, forget to edit it on the next host, and that
  host publishes its own drives into the first host's topic tree.
- `Wants=network-online.target` with `StartLimitIntervalSec=0` and a non-trivial `RestartSec`. Without
  them, a boot where the network comes up after the service does will spend every retry inside systemd's
  default 10-second start-limit window in about one second, after which systemd gives up permanently and
  the host goes quiet. Because the health topics are retained, Home Assistant keeps showing the last
  value, so a dead publisher looks exactly like a healthy one.
- No `--ignore-*` flags. `--ignore-samsung-181` is a no-op now, and `--ignore-nvme-used` damps the one
  signal that tells you a solid-state drive is running out of writes.

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
