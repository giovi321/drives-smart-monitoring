# drive_health_score.py — CLI usage (v9.3)

```text
usage: drive_health_score.py [-h] [--broker BROKER] [--port PORT] [--username USERNAME] [--password PASSWORD] [--tls] [--cafile CAFILE] [--insecure]
                             [--client-id CLIENT_ID] [--base-topic BASE_TOPIC] [--qos {0,1,2}] [--retain] [--no-mqtt] [--ha-discovery]
                             [--ha-prefix HA_PREFIX] [--ha-node HA_NODE] [--heartbeat-sec HEARTBEAT_SEC] [--ha-heartbeat-expire HA_HEARTBEAT_EXPIRE]
                             [--once] [--interval INTERVAL] [--format {table,json}] [--verbose] [--top TOP] [--json-verbose] [--ignore-samsung-181]

DRIVE_HEALTH_SCORE(v9.3)                             User Commands                            DRIVE_HEALTH_SCORE(v9.3)

NAME
    drive_health_score.py — compute a per-drive SMART health index and optionally publish via MQTT

SYNOPSIS
    drive_health_score.py [OPTIONS]

DESCRIPTION
    drive_health_score.py scans local block devices, queries SMART data using smartctl, computes a 0–100 “health%” score
    per drive (SSD/NVMe/HDD) using multiple attributes, and prints results to stdout in table or JSON. Optionally, it
    publishes per-drive payloads and a summary to MQTT. The scoring avoids vendor “Overall SMART PASSED” and instead
    focuses on concrete indicators (reallocated/pending/uncorrectable sector counts, CRC errors, SSD wear, etc.).

    Key properties:
      • Works with ATA/SATA HDDs and SSDs, and NVMe devices
      • Uses smartctl JSON output (preferred) and falls back to /dev enumeration
      • Separate scoring logic per device type (HDD vs. SATA SSD vs. NVMe)
      • No age-based penalties
      • Optional suppression of Samsung SATA SSD attribute 181 (Program_Fail_Cnt_Total)
      • Human-readable table (with optional per-attribute breakdown) or JSON output (with optional verbose root summary)
      • MQTT output can be toggled off; with --no-mqtt, it runs one cycle and exits
      • Home Assistant MQTT Discovery: one HA device per host, one sensor per drive, plus a binary_sensor “problem”
      • LWT availability so HA marks device offline if the process dies
      • Heartbeat topic and HA heartbeat binary_sensor to detect a stalled script without touching per-drive sensors

INSTALLATION
    Requirements:
      • smartmontools (smartctl in $PATH)
      • Python 3.8+ recommended (tested newer)
      • Optional: paho-mqtt (pip install paho-mqtt) if MQTT publishing is desired

    Example (Debian/Ubuntu):
      apt-get install smartmontools
      python3 -m venv venv
      . venv/bin/activate
      pip install paho-mqtt

ALGORITHM OVERVIEW
    The script computes a penalty sum from relevant SMART indicators, then Health% = clamp(100 - total_penalty, 0..100).
    Different device classes use different sources:

    NVMe (nvme_smart_health_information_log):
      • percentage_used: up to 60 penalty (1:1 within [0..60])
      • available_spare: spare<100 → 2*(100 - spare), up to 20 penalty
      • media/data integrity errors: 5*sqrt(errors), capped at 50
      • controller_busy_time (minutes): 0.001 per minute, capped at 5
      • error log entries: 0.001 per entry, capped at 3

    SATA SSD (ATA SMART):
      • Prefer Device Statistics “Percentage Used Endurance Indicator”: up to 60 penalty (1:1 within [0..60])
        If missing: estimate TBW from attribute 241 (Total_LBAs_Written * 512 / 1e12 TB) vs. capacity-based baseline
        (≈ capacity_GiB/512 * 150 TB minimum 50 TB). Penalty up to 60 proportionally.
      • Available spare (232): if 0<spare<100, 2*(100 - spare), capped at 30
      • Media wearout indicator (233): 0.3*(100 - mwi), capped at 30
      • Program_Fail_Cnt_Total (181): 10 * value, capped at 40 (may be ignored on Samsung with --ignore-samsung-181)
      • Erase_Fail_Count_Total (182): 10 * value, capped at 40
      • Reallocated sectors (5): 5 + 2*sqrt(realloc), capped at 30 if realloc>0
      • Pending sectors (197): 12 + 5*sqrt(pending), capped at 35 if pending>0
      • Offline+Reported Uncorrectable (198 + 187): 10 + 3*sqrt(sum), capped at 40 if sum>0
      • Wear Leveling Count (177): if >200, 0.01*(wlc-200), capped at 10
      • CRC errors (199): 0.05*crc, capped at 5

    HDD (ATA SMART):
      • Reallocated sectors (5): 4*sqrt(realloc), capped at 40
      • Pending sectors (197): 7*sqrt(pending), capped at 45
      • Offline+Reported Uncorrectable (198 + 187): 4*sqrt(sum), capped at 35
      • Reallocated event count (196): 1:1, capped at 10
      • Seek error rate RAW (7): 2*log10(1+raw), capped at 8 (heuristic)
      • Spin retry count (10): 10*value, capped at 20
      • Load cycle count (193): penalty up to 10 relative to a cap (IronWolf/Pro: 600k, others: 300k)
      • CRC errors (199): 0.05*crc, capped at 5

    The per-drive breakdown lists each penalty component contributing to the total penalty.

MODES
    Single-shot:
      • --once             : run one collection/publish cycle then exit
      • --no-mqtt          : disables MQTT; also implies single-shot behavior (run once and exit)

    Periodic:
      • default if MQTT is enabled and --once was not passed
      • controlled by --interval (seconds)

OPTIONS
    MQTT:
      --broker HOST          MQTT broker (default: localhost)
      --port N               MQTT port (default: 1883)
      --username USER        MQTT username
      --password PASS        MQTT password
      --tls                  Enable TLS for MQTT
      --cafile PATH          CA file path for TLS
      --insecure             Allow insecure TLS (tls_insecure_set)
      --client-id ID         MQTT client id (default: smart-health-<hostname>)
      --base-topic TOPIC     Base MQTT topic (default: servers/smart)
      --qos {0,1,2}          MQTT QoS (default: 0)
      --retain               Publish with retain
      --no-mqtt              Do not publish to MQTT; run one cycle and exit

    Home Assistant:
      --ha-discovery         Enable Home Assistant MQTT Discovery (retained config)
      --ha-prefix PREFIX     Discovery prefix (default: homeassistant)
      --ha-node NAME         Override HA device name (default: <hostname>)

    Heartbeat:
      --heartbeat-sec N      Publish "alive" to <base-topic>/<host>/heartbeat every N seconds (default: 30, unretained)
      --ha-heartbeat-expire S
                             Heartbeat binary_sensor expire_after seconds (default: 2*N+5)

    Scheduling:
      --once                 Run once and exit (also implied by --no-mqtt)
      --interval SEC         Interval seconds for periodic mode (default: 3600)

    Output:
      --format {table,json}  Output format (default: table)
      --verbose              With table: include per-attribute penalty breakdown for each drive
      --top N                With table+verbose: limit breakdown lines to top N penalties
      --json-verbose         With JSON format: include scoring_version, settings, summary (min/max/avg/penalty_sum)

    Scoring modifiers:
      --ignore-samsung-181   Ignore SMART 181 Program_Fail_Cnt_Total on Samsung SATA SSDs

OUTPUT
    MQTT topics (per host/drive):
      <base-topic>/<host>/<drive>/state            JSON payload with all metrics
      <base-topic>/<host>/<drive>/health_percent   Number payload health% (retained)
      <base-topic>/<host>/<drive>/problem          "ON"/"OFF" SMART overall status (retained)
      <base-topic>/<host>/availability             "online"/"offline" (retained, LWT)
      <base-topic>/<host>/heartbeat                "alive" (unretained, periodic)

    Home Assistant discovery (retained):
      <ha-prefix>/sensor/<uid>/config
      <ha-prefix>/binary_sensor/<uid>/config   (per-drive SMART problem + one host heartbeat)

EXAMPLES
    Run periodic with HA discovery and heartbeat:
      drive_health_score.py --broker mqtt.local --interval 900 --retain --ha-discovery --heartbeat-sec 30

EXIT STATUS
    0  Success
    1  Unhandled error during execution

NOTES
    • LWT is configured so entities flip to unavailable if the process dies or disconnects unexpectedly.
    • Heartbeat binary_sensor becomes unavailable if heartbeats stop and expire_after elapses.

SECURITY
    • If using MQTT over TLS, consider providing --cafile and avoid --insecure in production.
    • Credentials passed via CLI could be visible in process listings; prefer environment variables or protected shells.

LIMITATIONS
    • Vendor-specific SMART meanings vary; thresholds/weights are heuristics.
    • USB enclosures may block SMART passthrough; use native SATA/NVMe connections for best results.

AUTHOR
    giovi321, 2025 (github.com/giovi321)

VERSION
    v9.3
```

