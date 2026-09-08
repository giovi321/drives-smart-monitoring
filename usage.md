# drive_health_score.py — CLI usage (v10.0)

```text
usage: drive_health_score.py [-h] [--broker BROKER] [--port PORT] [--username USERNAME] [--password PASSWORD] [--tls] [--cafile CAFILE] [--insecure]
                             [--client-id CLIENT_ID] [--base-topic BASE_TOPIC] [--qos {0,1,2}] [--retain] [--no-mqtt] [--ha-discovery] [--ha-prefix HA_PREFIX]
                             [--ha-node HA_NODE] [--heartbeat-sec HEARTBEAT_SEC] [--ha-heartbeat-expire HA_HEARTBEAT_EXPIRE] [--once] [--interval INTERVAL]
                             [--format {table,json}] [--verbose] [--top TOP] [--json-verbose] [--ignore-samsung-181] [--ignore-nvme-used]
                             [--prune-stale-topics]

DRIVE_HEALTH_SCORE(v10.0)                             User Commands                            DRIVE_HEALTH_SCORE(v10.0)

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
      • Vendor-encoded attributes are scored on the NORMALISED value, never on the raw. Only attributes whose raw
        is a genuine count on every vendor may be read as a count; the list is enforced in code (RAW_IS_COUNT)
      • Per-drive identity is the SERIAL, so a drive keeps one entity when Linux renames it sd*
      • Retained topics and discovery configs of drives that have gone away are deleted, not left to replay
      • Human-readable table (with optional per-attribute breakdown) or JSON output (with optional verbose root summary)
      • MQTT output can be toggled off; with --no-mqtt, it runs one cycle and exits
      • Home Assistant MQTT Discovery: one HA device per host, one sensor per drive, plus a binary_sensor “problem”
      • LWT availability so HA marks device offline if the process dies. Note this tracks the PUBLISHER, so it
        cannot report a single drive going away; expire_after and topic pruning cover that
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

    RAW VERSUS NORMALISED
      A SMART attribute has a raw field and a normalised value/worst/threshold triple. For a count like
      Reallocated_Sector_Ct the raw is the count. For a rate like Seek_Error_Rate it is a vendor-packed
      bitfield: on Seagate the low 32 bits are total seeks and the high 16 are seek errors, so a healthy
      drive reads in the hundreds of millions. Reading such a raw as an error tally penalises a perfectly
      good drive, which is what produced 56.7% on a drive with 0 reallocated, 0 pending, 0 uncorrectable,
      0 CRC and smartctl -H PASSED.
      Only ids in RAW_IS_COUNT may be read as counts, and get_attr_count() raises on anything else.
      Everything else is scored by attr_norm_penalty(): no penalty while more than half the margin above
      the threshold remains, ramping to the cap at the threshold. Western Digital normalises to 200 rather
      than 100 and the ramp accounts for it.

    ANY DEVICE
      • smartctl -H FAILED floors the score at 5. The firmware fails a drive on internal criteria that need
        not show up in any scored attribute, so this cannot be left to the attribute penalties alone.

    NVMe (nvme_smart_health_information_log):
      • percentage_used: 0.6*pct + 0.02*max(0, pct-80)^2, capped at 75. Superlinear past 80% so the end of
        rated life still resolves: 50% used costs 30, 80% costs 48, 94% costs 60.
        --ignore-nvme-used multiplies this by 0.4. It no longer removes it.
      • available_spare: measured against the drive's own available_spare_threshold, not against 100.
        No penalty above half the margin, ramping to 35 at the threshold.
      • critical_warning: 60 if any of spare-below-threshold, reliability-degraded or media-read-only is
        set, else 15. This is the NVMe equivalent of failing smartctl -H.
      • media/data integrity errors: 5*sqrt(errors), capped at 50
      • error log entries: 0.001 per entry, capped at 3
      • controller_busy_time: recorded, NOT scored. It is a workload odometer that rises with normal use.

    SATA SSD (ATA SMART):
      Endurance is taken from exactly one source, in this order, and never added up twice:
        1. Device Statistics “Percentage Used Endurance Indicator”
        2. the normalised value of the first wear attribute present among 177, 173, 233, 231, 202. A value
           pinned at the top of the scale with a zero raw on a drive with over 1000 power-on hours is
           treated as not implemented and skipped, not as an unworn drive.
        3. TBW from attribute 241 against a capacity baseline (≈ capacity_GiB/512 * 150 TB, minimum 50 TB),
           but only when plausible. Attribute 241's UNIT is vendor-defined: some drives report 32 MB or GB
           units, so 241*512 can understate writes by orders of magnitude. Anything below 1 GB per 1000
           power-on hours is rejected as a unit mismatch.
        If none of the three yields a figure, the drive scores a flat 5 as “endurance unknown” and says so
        in the breakdown, rather than silently scoring zero wear and publishing 100%.
      • endurance penalty: same curve as NVMe percentage_used
      • Available spare (232) and Used_Rsvd_Blk_Cnt_Tot (179): normalised, capped at 30 and 25
      • Program_Fail_Cnt_Total (181): NORMALISED, capped at 40. Samsung packs a rate in this raw
        (260,204,355 observed at normalised 93), so reading the raw charged the full cap to healthy drives.
      • Erase_Fail_Count_Total (182): normalised, capped at 40
      • Reallocated sectors (5): 5 + 2*sqrt(realloc), capped at 30 if realloc>0
      • Pending sectors (197): 12 + 5*sqrt(pending), capped at 35 if pending>0
      • Offline Uncorrectable (198): 10 + 3*sqrt(n), capped at 40 if n>0
      • Uncorrectable_Error_Cnt (187): 6 + 2*sqrt(n), capped at 25 if n>0. Scored separately from 198.
      • CRC errors (199): 0.05*crc, capped at 5

    HDD (ATA SMART):
      • Reallocated sectors (5): 4*sqrt(realloc), capped at 40
      • Pending sectors (197): 7*sqrt(pending), capped at 45
      • Offline Uncorrectable (198): 6*sqrt(n), capped at 35. A genuine sector count.
      • Reported_Uncorrect (187), scored separately from 198 and in two parts:
          normalised value, capped at 25 — the vendor's own statement of remaining margin
          raw count, sqrt(n) capped at 10, HALVED when 5, 197 and 198 are all zero
        187 counts errors reported to the HOST, not sectors. On Seagate that includes link, cable and
        command-timeout events which leave no media defect, so summing it with 198 asserted sector damage
        the drive never claimed. Without sector-level corroboration the indicated action is to check cabling
        and run a long self-test, not to replace, hence the halving.
      • Reallocated event count (196): 1:1, capped at 10
      • Seek_Error_Rate (7) and Raw_Read_Error_Rate (1): NORMALISED, capped at 8 and 6
      • Spin retry count (10): 10*value, capped at 20
      • Load cycle count (193): up to 5 relative to a rating (NAS/enterprise families 600k, others 300k).
        Half its former weight: it is an odometer, not an error counter, and a WD Green at 1.58M cycles with
        zero errors on every counter was losing a tenth of the whole scale for parking its heads.
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
      --cafile PATH          CA cert file path for TLS
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
      --ignore-samsung-181   Deprecated no-op. Attribute 181 is scored on its normalised value now, so the
                                        false alarm this suppressed cannot happen. Still accepted so existing unit
                                        files keep working; prints a note and does nothing.
      --ignore-nvme-used     Damp the NVMe 'percentage_used' endurance penalty to 40% of its weight. It no
                                        longer removes the penalty: a flag in a unit file must not be able to assert
                                        perfect health over the drive's own end-of-life estimate.
      --prune-stale-topics   One-time cleanup. Also sweep this host's own topic subtree for drive slugs left
                                        by earlier versions (device-letter identities, drives since removed) and delete
                                        their retained topics and discovery configs. Steady-state pruning uses the
                                        retained manifest and needs no flag.

OUTPUT
    MQTT topics (per host/drive):
      <base-topic>/<host>/<serial>/state            JSON payload with all metrics
      <base-topic>/<host>/<serial>/health_percent   Number payload health% (retained)
      <base-topic>/<host>/<serial>/problem          "ON"/"OFF" SMART overall status (retained)
      <base-topic>/<host>/availability              "online"/"offline" (retained, LWT)
      <base-topic>/<host>/heartbeat                 "alive" (unretained, periodic)
      <base-topic>/<host>/known_drives              JSON list of the serials published (retained)

    <serial> is the drive serial, slugified. It used to be "<device-letter>_<serial>", which is not stable:
    Linux assigns sd* by discovery order, so every letter a drive ever held minted another topic tree and
    another Home Assistant entity, and nothing removed the old ones. WWN is used when a drive publishes no
    usable serial, and the device letter only as a last resort. The current device letter stays in the state
    payload and in the entity name, where a moving value is information rather than a new identity.

    known_drives is the bookkeeping topic that lets the next run tell what has gone away. Anything under
    <base-topic>/<host>/ that was published before and is not present now has its state topics and its
    discovery configs deleted with empty retained payloads.

    NOTE ON --base-topic: the hostname is appended by the program, so do NOT put it in --base-topic as well.
    Copying a unit file between hosts without editing --base-topic makes the new host publish its own drives
    into the donor host's topic tree, which is how hostA/smart/hostC/* came to exist. A mismatch
    between the first segment of --base-topic and the hostname prints a warning at startup.

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
      It tracks the PUBLISHER, so it says nothing about a single drive disappearing.
    • Heartbeat binary_sensor becomes unavailable if heartbeats stop and expire_after elapses.
    • Per-drive sensors carry expire_after too, so a drive that stops reporting goes unavailable instead of
      freezing at its last reading. Note that expire_after restarts its timer whenever Home Assistant reads
      the retained message again, which happens on every Home Assistant restart, so expire_after alone does
      not retire a stale value permanently. Deleting the retained topic does, which is what the pruning in
      publish/prune_vanished_drives is for.

SECURITY
    • If using MQTT over TLS, consider providing --cafile and avoid --insecure in production.
    • Credentials passed via CLI could be visible in process listings; prefer environment variables or protected shells.

LIMITATIONS
    • Vendor-specific SMART meanings vary; thresholds/weights are heuristics.
    • USB enclosures may block SMART passthrough; use native SATA/NVMe connections for best results.

AUTHOR
    giovi321, 2025 (github.com/giovi321)

VERSION
    v10.0

options:
  -h, --help            show this help message and exit
  --broker BROKER       MQTT broker (default: localhost)
  --port PORT           MQTT port (default: 1883)
  --username USERNAME   MQTT username
  --password PASSWORD   MQTT password
  --tls                 Enable TLS for MQTT
  --cafile CAFILE       CA file path for TLS
  --insecure            Allow insecure TLS (tls_insecure_set)
  --client-id CLIENT_ID
                                   MQTT client id
  --base-topic BASE_TOPIC
                                   Base MQTT topic
  --qos {0,1,2}         MQTT QoS (default: 0)
  --retain              Publish with retain
  --no-mqtt             Disable MQTT; run one cycle and exit
  --ha-discovery        Enable Home Assistant MQTT Discovery
  --ha-prefix HA_PREFIX
                                   Home Assistant discovery prefix (default: homeassistant)
  --ha-node HA_NODE     Override HA device name (default: <hostname>)
  --heartbeat-sec HEARTBEAT_SEC
                                   Publish heartbeat every N seconds (default: 30)
  --ha-heartbeat-expire HA_HEARTBEAT_EXPIRE
                                   expire_after for heartbeat binary_sensor (default: 2*heartbeat+5)
  --once                Run once and exit (also implied by --no-mqtt)
  --interval INTERVAL   Seconds between runs in periodic mode (default: 3600)
  --format {table,json}
                                   Output format (default: table)
  --verbose             (table) Print per-attribute penalty breakdowns
  --top TOP             (table+verbose) Show only top N penalties
  --json-verbose        (json) Include scoring_version, settings, and summary at root
  --ignore-samsung-181  Deprecated no-op. Attribute 181 is scored on its normalised value now, so the false alarm this suppressed no longer happens.
                                   Accepted so existing unit files keep working.
  --ignore-nvme-used    Damp the NVMe percentage_used endurance penalty to 40% of its weight. It no longer silences endurance entirely: a flag must
                                   not assert perfect health over the drive own end-of-life estimate.
  --prune-stale-topics  One-time cleanup: also sweep this host's own topic subtree for drive slugs left behind by earlier versions (device-letter
                                   identities, removed drives) and delete their retained topics and discovery configs.
```
