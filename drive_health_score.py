#!/usr/bin/env python3
# drive_health_score_v9.py
# v9.3: remove --ha-expire, add MQTT heartbeat + HA heartbeat binary_sensor.
# No other behavior changed.

import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
from math import log10

# Optional MQTT
try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

SCORING_VERSION = "v9.3"

MAN = f"""\
DRIVE_HEALTH_SCORE({SCORING_VERSION})                             User Commands                            DRIVE_HEALTH_SCORE({SCORING_VERSION})

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
      --cafile PATH          CA cert file path for TLS
      --insecure             Allow insecure TLS (tls_insecure_set)
      --client-id ID         MQTT client id (default: smart-health-<hostname>)
      --base-topic TOPIC     Base MQTT topic (default: servers/smart)
      --qos {{0,1,2}}          MQTT QoS (default: 0)
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
      --format {{table,json}}  Output format (default: table)
      --verbose              With table: include per-attribute penalty breakdown for each drive
      --top N                With table+verbose: limit breakdown lines to top N penalties
      --json-verbose         With JSON format: include scoring_version, settings, summary (min/max/avg/penalty_sum)

    Scoring modifiers:
      --ignore-samsung-181   Ignore SMART 181 (Program_Fail_Cnt_Total) on Samsung SATA SSDs
      --ignore-nvme-used     Ignore NVMe 'percentage_used' in health scoring

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
    {SCORING_VERSION}

"""

# --------------------------- helpers ---------------------------------
def sh(cmd, allow_nonzero=False):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0 and not allow_nonzero:
        raise RuntimeError(f"cmd failed: {' '.join(cmd)}\n{p.stderr}")
    return p.stdout, p.stderr, p.returncode


def smart_scan():
    try:
        out, _, _ = sh(["smartctl", "--scan-open", "-j"], allow_nonzero=True)
        data = json.loads(out) if out else {}
        if data and "devices" in data:
            return sorted({d["name"] for d in data["devices"] if not d.get("open_error")})
    except Exception:
        pass
    devs = []
    try:
        for base in os.listdir("/dev"):
            if base.startswith("sd") and len(base) == 3:
                devs.append(f"/dev/{base}")
            if base.startswith("nvme") and base.endswith("n1"):
                devs.append(f"/dev/{base}")
    except Exception:
        pass
    return sorted(set(devs))


def read_smart(dev):
    try:
        out, err, rc = sh(["smartctl", "-a", "-j", dev], allow_nonzero=True)
        data = json.loads(out)
        data.setdefault("_reader", {})["exit_status"] = data.get("smartctl", {}).get("exit_status", rc)
        if err:
            data["_reader"]["stderr"] = err.strip()
        return data
    except Exception as e:
        return {"device": {"name": dev}, "error": str(e)}


def get_attr(ata_json, attr_id, default=0):
    tbl = ata_json.get("ata_smart_attributes", {}).get("table", [])
    for row in tbl:
        if row.get("id") == attr_id:
            raw = row.get("raw", {}).get("value")
            if raw is None:
                raw_str = row.get("raw", {}).get("string", "")
                num = ""
                for ch in raw_str:
                    if ch.isdigit():
                        num += ch
                    elif num:
                        break
                try:
                    return int(num) if num else default
                except Exception:
                    return default
            try:
                return int(raw)
            except Exception:
                return default
    return default


def get(d, path, default=None):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def lbas_to_bytes(v):
    try:
        return int(v) * 512
    except Exception:
        return 0


def is_ssd(d):
    rr = d.get("rotation_rate")
    if rr == 0 or rr == "Solid State Device":
        return True
    if "nvme_smart_health_information_log" in d:
        return True
    return "solid state device" in json.dumps(d).lower()


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def add_penalty(breakdown, key, value, penalty, note=None):
    breakdown.append(
        {
            "key": key,
            "value": value,
            "penalty": round(float(max(0.0, penalty)), 2),
            "note": note,
        }
    )


def total_penalty(breakdown):
    return round(sum(i.get("penalty", 0.0) for i in breakdown), 2)


# --------------------------- scoring ---------------------------------
def score_nvme(d, settings):
    score = 100.0
    breakdown = []
    nlog = d.get("nvme_smart_health_information_log", {}) or {}

    # Endurance wear
    pct_used = nlog.get("percentage_used", 0) or 0
    if not settings.get("ignore_nvme_used", False):
        base = min(60.0, float(pct_used))
        score -= base
        add_penalty(breakdown, "nvme_percentage_used", pct_used, base, "% used endurance")
    else:
        # record value but no penalty
        add_penalty(breakdown, "nvme_percentage_used", pct_used, 0.0, "ignored by --ignore-nvme-used")

    spare = nlog.get("available_spare", 100)
    if spare is not None and spare < 100:
        pen = min(20.0, 2.0 * (100 - float(spare)))
        score -= pen
        add_penalty(breakdown, "nvme_available_spare", spare, pen, "spare below 100")

    media_err = nlog.get("media_errors", nlog.get("media_and_data_integrity_errors", 0)) or 0
    pen = min(50.0, 5.0 * (float(media_err) ** 0.5))
    score -= pen
    add_penalty(breakdown, "nvme_media_errors", media_err, pen, "media/data integrity errors")

    busy_min = float(nlog.get("controller_busy_time", 0) or 0)
    if busy_min > 0:
        pen = min(5.0, 0.001 * busy_min)
        score -= pen
        add_penalty(breakdown, "nvme_controller_busy_minutes", busy_min, pen, "controller busy")

    err_entries = get(d, ["nvme_error_log", "error_count"], None)
    if err_entries is None:
        err_entries = nlog.get("num_err_log_entries", nlog.get("error_info_log_entries", 0)) or 0
    pen = min(3.0, 0.001 * float(err_entries))
    score -= pen
    add_penalty(breakdown, "nvme_error_log_entries", err_entries, pen, "error log entries")

    return clamp(round(score, 1)), {"breakdown": breakdown}


def score_ata_ssd(d, settings):
    score = 100.0
    breakdown = []

    model = d.get("model_name", "") or d.get("device", {}).get("model_name", "")
    is_samsung = "samsung" in model.lower()

    percent_used = get(
        d,
        ["device_statistics", "pages", "sata_smart_attributes", "Percentage Used Endurance Indicator"],
        0,
    ) or 0
    if percent_used > 0:
        pen = min(60.0, float(percent_used))
        score -= pen
        add_penalty(breakdown, "attr_PercentageUsed", percent_used, pen, "device statistics % used")
    else:
        cap_bytes = get(d, ["user_capacity", "bytes"], 0) or 0
        baseline_tb = max(50.0, (cap_bytes / (512 * 2**30)) * 150.0)
        lba_w = get_attr(d, 241, 0)
        tb_written = lbas_to_bytes(lba_w) / 1e12
        wear_frac = min(2.0, tb_written / baseline_tb)
        pen = min(60.0, 60.0 * wear_frac)
        score -= pen
        add_penalty(
            breakdown,
            "attr_241_TBW_decimal_TB",
            round(tb_written, 2),
            pen,
            f"baseline {round(baseline_tb, 1)} TB",
        )

    spare = get_attr(d, 232, 100)
    if 0 < spare < 100:
        pen = min(30.0, 2.0 * (100 - float(spare)))
        score -= pen
        add_penalty(breakdown, "attr_232_AvailableSpare", spare, pen, "spare below 100")

    mwi = get_attr(d, 233, 0)
    if 0 < mwi <= 100:
        pen = min(30.0, 0.3 * (100 - float(mwi)))
        score -= pen
        add_penalty(breakdown, "attr_233_MediaWearoutIndicator", mwi, pen, "lower means more wear")

    prog_fail = get_attr(d, 181, 0)
    if is_samsung and settings.get("ignore_samsung_181", False):
        add_penalty(
            breakdown,
            "attr_181_ProgramFailTotal",
            prog_fail,
            0.0,
            "ignored due to --ignore-samsung-181",
        )
    else:
        pen = min(40.0, 10.0 * float(prog_fail))
        score -= pen
        add_penalty(breakdown, "attr_181_ProgramFailTotal", prog_fail, pen)

    erase_fail = get_attr(d, 182, 0)
    pen = min(40.0, 10.0 * float(erase_fail))
    score -= pen
    add_penalty(breakdown, "attr_182_EraseFailTotal", erase_fail, pen)

    realloc = get_attr(d, 5, 0)
    pend = get_attr(d, 197, 0)
    offu = get_attr(d, 198, 0)
    repu = get_attr(d, 187, 0)
    if realloc > 0:
        pen = min(30.0, 5.0 + 2.0 * (float(realloc) ** 0.5))
        score -= pen
        add_penalty(breakdown, "attr_005_ReallocatedSectors", realloc, pen)
    if pend > 0:
        pen = min(35.0, 12.0 + 5.0 * (float(pend) ** 0.5))
        score -= pen
        add_penalty(breakdown, "attr_197_CurrentPending", pend, pen)
    if (offu + repu) > 0:
        pen = min(40.0, 10.0 + 3.0 * ((float(offu + repu)) ** 0.5))
        score -= pen
        add_penalty(breakdown, "uncorrect_sum(attr198+187)", offu + repu, pen)

    wlc = get_attr(d, 177, 0)
    if wlc > 200:
        pen = min(10.0, 0.01 * (wlc - 200))
        score -= pen
        add_penalty(breakdown, "attr_177_WearLevelingCount", wlc, pen, ">200 cycles heuristic")

    crc = get_attr(d, 199, 0)
    pen = min(5.0, 0.05 * float(crc))
    score -= pen
    add_penalty(breakdown, "attr_199_CRC_Errors", crc, pen)

    return clamp(round(score, 1)), {"breakdown": breakdown}


def score_hdd(d):
    score = 100.0
    breakdown = []

    realloc = get_attr(d, 5, 0)
    pend = get_attr(d, 197, 0)
    offu = get_attr(d, 198, 0)
    repu = get_attr(d, 187, 0)

    pen = min(40.0, 4.0 * (float(realloc) ** 0.5))
    score -= pen
    add_penalty(breakdown, "attr_005_ReallocatedSectors", realloc, pen)

    pen = min(45.0, 7.0 * (float(pend) ** 0.5))
    score -= pen
    add_penalty(breakdown, "attr_197_CurrentPending", pend, pen)

    pen = min(35.0, 4.0 * (float(offu + repu) ** 0.5))
    score -= pen
    add_penalty(breakdown, "uncorrect_sum(attr198+187)", offu + repu, pen)

    re_event = get_attr(d, 196, 0)
    pen = min(10.0, float(re_event))
    score -= pen
    add_penalty(breakdown, "attr_196_ReallocEvents", re_event, pen)

    seek_raw = get_attr(d, 7, 0)
    if seek_raw > 0:
        pen = min(8.0, 2.0 * log10(1 + float(seek_raw)))
        score -= pen
        add_penalty(breakdown, "attr_007_SeekErrorRate_raw", seek_raw, pen)

    spin_retry = get_attr(d, 10, 0)
    pen = min(20.0, 10.0 * float(spin_retry))
    score -= pen
    add_penalty(breakdown, "attr_010_SpinRetry", spin_retry, pen)

    lcc = get_attr(d, 193, 0)
    model = d.get("model_name", "") or d.get("device", {}).get("model_name", "")
    family = d.get("model_family", "") or ""
    lcc_cap = 600000 if ("ironwolf" in model.lower() or "ironwolf" in family.lower() or "pro" in model.lower()) else 300000
    pen = min(10.0, 10.0 * min(1.5, lcc / max(1, lcc_cap)))
    score -= pen
    add_penalty(breakdown, "attr_193_LoadCycleCount", lcc, pen, f"cap {lcc_cap}")

    crc = get_attr(d, 199, 0)
    pen = min(5.0, 0.05 * float(crc))
    score -= pen
    add_penalty(breakdown, "attr_199_CRC_Errors", crc, pen)

    return clamp(round(score, 1)), {"breakdown": breakdown}


def compute_score(d, settings):
    if "nvme_smart_health_information_log" in d:
        t = "SSD"
        score, details = score_nvme(d, settings)
    elif is_ssd(d):
        t = "SSD"
        score, details = score_ata_ssd(d, settings)
    else:
        t = "HDD"
        score, details = score_hdd(d)
    return t, score, details


# --------------------------- MQTT ------------------------------------
class MQTTReconnectWorker:
    def __init__(self, client, *, host, port, keepalive, max_backoff=300):
        self.client = client
        self._connect_kwargs = {"host": host, "port": port, "keepalive": keepalive}
        self._max_backoff = max_backoff
        self._backoff = 1.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._trigger = threading.Event()
        self._scheduled = False
        self._thread = threading.Thread(target=self._run, name="mqtt-reconnect", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._trigger.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def reset_backoff(self):
        with self._lock:
            self._backoff = 1.0

    def schedule(self, *, reset_backoff=False):
        with self._lock:
            if reset_backoff:
                self._backoff = 1.0
            if self._scheduled or self._stop.is_set():
                return
            self._scheduled = True
        self._trigger.set()

    def _next_delay(self):
        with self._lock:
            delay = self._backoff
            self._backoff = min(self._backoff * 2.0, float(self._max_backoff))
            return delay

    def _run(self):
        while not self._stop.is_set():
            self._trigger.wait()
            self._trigger.clear()
            if self._stop.is_set():
                break
            while not self._stop.is_set():
                rc = None
                try:
                    rc = self.client.reconnect()
                except Exception:
                    try:
                        rc = self.client.connect(**self._connect_kwargs)
                    except Exception:
                        rc = mqtt.MQTT_ERR_NO_CONN if mqtt is not None else 1
                if rc == mqtt.MQTT_ERR_SUCCESS:
                    try:
                        # Drive the network stack to kick off the handshake.
                        self.client.loop(timeout=0.1)
                    except Exception:
                        pass
                    break
                delay = self._next_delay()
                print(f"MQTT reconnect failed (rc={rc}); retrying in {int(delay)}s", file=sys.stderr)
                time.sleep(delay)
            with self._lock:
                self._scheduled = False


def _reason_code_value(code):
    if code is None:
        return None
    if hasattr(code, "value"):
        return code.value
    try:
        return int(code)
    except Exception:
        return None


def make_client(args, host):
    if mqtt is None:
        raise RuntimeError("paho-mqtt not installed and MQTT requested. Use --no-mqtt or install paho-mqtt.")

    try:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=args.client_id,
            protocol=mqtt.MQTTv311,
            transport="tcp",
        )
    except Exception:
        client = mqtt.Client(client_id=args.client_id, protocol=mqtt.MQTTv311, transport="tcp")

    if args.username:
        client.username_pw_set(args.username, args.password)
    if args.tls:
        if args.cafile:
            client.tls_set(ca_certs=args.cafile)
        else:
            client.tls_set()
        if getattr(args, "insecure", False):
            client.tls_insecure_set(True)

    # Last Will: mark host offline if we disconnect unexpectedly
    client.will_set(f"{args.base_topic}/{host}/availability", "offline", qos=args.qos, retain=True)

    keepalive = 60
    worker = MQTTReconnectWorker(client, host=args.broker, port=args.port, keepalive=keepalive)

    state = {"worker": worker, "expected_disconnect": False}

    def _on_connect(client, userdata, flags, reason_code, properties=None):
        code = _reason_code_value(reason_code)
        worker = userdata.get("worker") if isinstance(userdata, dict) else None
        if code not in (None, mqtt.MQTT_ERR_SUCCESS, getattr(mqtt, "CONNACK_ACCEPTED", 0)):
            print(f"MQTT connect failed (rc={code}); scheduling reconnect", file=sys.stderr)
            if worker is not None:
                worker.schedule()
            return
        if worker is not None:
            worker.reset_backoff()

    def _on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
        if isinstance(userdata, dict) and userdata.get("expected_disconnect"):
            return
        code = _reason_code_value(reason_code)
        if code in (None, mqtt.MQTT_ERR_SUCCESS):
            return
        print(f"MQTT disconnected unexpectedly (rc={code}); scheduling reconnect", file=sys.stderr)
        worker = userdata.get("worker") if isinstance(userdata, dict) else None
        if worker is not None:
            worker.schedule(reset_backoff=False)

    client.user_data_set(state)
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect

    client.connect(args.broker, args.port, keepalive=keepalive)
    return client, worker, state


def publish(client, topic, payload, qos=0, retain=False):
    client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=qos, retain=retain)


def _slug(x):
    s = "".join(ch if ch.isalnum() else "_" for ch in str(x))
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_").lower()

def ha_discovery_publish(client, ha_prefix, base_topic, host, results, qos=0, retain=True, node_name=None,
                         heartbeat_expire=None):
    """
    One HA device per host. One sensor and one binary_sensor per drive.
    Plus a host heartbeat binary_sensor with expire_after.
    """
    node = node_name or host
    avail_topic = f"{base_topic}/{host}/availability"
    hb_topic = f"{base_topic}/{host}/heartbeat"

    device_obj = {
        "identifiers": [f"smart_{host}"],
        "name": node,
        "manufacturer": "smart-health",
        "model": f"drive_health_score {SCORING_VERSION}",
        "sw_version": SCORING_VERSION,
    }

    for r in results:
        devpath = r.get("device", "")
        serial = r.get("serial") or ""
        base = os.path.basename(devpath) or devpath or "drive"
        slug = _slug(f"{base}_{serial}") if serial else _slug(base)

        state_json_topic = f"{base_topic}/{host}/{slug}/state"
        health_topic     = f"{base_topic}/{host}/{slug}/health_percent"
        problem_topic    = f"{base_topic}/{host}/{slug}/problem"

        # sensor: health %
        sensor_uid = _slug(f"{host}_{slug}_health")
        sensor_cfg_topic = f"{ha_prefix}/sensor/{sensor_uid}/config"
        sensor_cfg = {
            "name": f"{slug} health",
            "unique_id": sensor_uid,
            "state_topic": health_topic,
            "unit_of_measurement": "%",
            "state_class": "measurement",
            "availability_topic": avail_topic,
            "json_attributes_topic": state_json_topic,
            "device": device_obj,
            "icon": "mdi:harddisk",
        }
        publish(client, sensor_cfg_topic, sensor_cfg, qos=qos, retain=retain)

        # binary_sensor: overall SMART problem
        bin_uid = _slug(f"{host}_{slug}_smart_problem")
        bin_cfg_topic = f"{ha_prefix}/binary_sensor/{bin_uid}/config"
        bin_cfg = {
            "name": f"{slug} SMART problem",
            "unique_id": bin_uid,
            "state_topic": problem_topic,
            "device_class": "problem",
            "payload_on": "ON",
            "payload_off": "OFF",
            "availability_topic": avail_topic,
            "device": device_obj,
            "icon": "mdi:alert",
        }
        publish(client, bin_cfg_topic, bin_cfg, qos=qos, retain=retain)

    # Host heartbeat binary_sensor
    hb_uid = _slug(f"{host}_heartbeat")
    hb_cfg_topic = f"{ha_prefix}/binary_sensor/{hb_uid}/config"
    hb_cfg = {
        "name": f"{node} heartbeat",
        "unique_id": hb_uid,
        "state_topic": hb_topic,
        "device_class": "connectivity",
        "payload_on": "alive",
        "availability_topic": avail_topic,
        "device": device_obj,
        "icon": "mdi:heart-pulse",
    }
    if isinstance(heartbeat_expire, int) and heartbeat_expire > 0:
        hb_cfg["expire_after"] = int(heartbeat_expire)
    publish(client, hb_cfg_topic, hb_cfg, qos=qos, retain=retain)


def ha_publish_availability(client, base_topic, host, online=True, qos=0, retain=True):
    client.publish(f"{base_topic}/{host}/availability", "online" if online else "offline", qos=qos, retain=retain)

def publish_heartbeat(client, base_topic, host, qos=0):
    # unretained heartbeat so HA expire_after works
    client.publish(f"{base_topic}/{host}/heartbeat", "alive", qos=qos, retain=False)


# --------------------------- output -----------------------------------
def human_table(rows):
    cols = ["device", "type", "health%", "model", "serial", "power_on_h", "realloc", "pend", "uncorr", "crc"]
    headers = {
        "device": "DEVICE",
        "type": "TYPE",
        "health%": "HEALTH%",
        "model": "MODEL",
        "serial": "SERIAL",
        "power_on_h": "POH",
        "realloc": "RELOC",
        "pend": "PEND",
        "uncorr": "UNCORR",
        "crc": "CRC",
    }
    widths = {c: len(headers[c]) for c in cols}

    def strv(x):
        return "" if x is None else str(x)

    for r in rows:
        m = r.get("metrics", {})
        widths["device"] = max(widths["device"], len(strv(r.get("device"))))
        widths["type"] = max(widths["type"], len(strv(r.get("type"))))
        widths["health%"] = max(widths["health%"], len(strv(r.get("health_percent"))))
        widths["model"] = max(widths["model"], len(strv(r.get("model"))))
        widths["serial"] = max(widths["serial"], len(strv(r.get("serial"))))
        widths["power_on_h"] = max(widths["power_on_h"], len(strv(m.get("power_on_hours"))))
        widths["realloc"] = max(widths["realloc"], len(strv(m.get("reallocated", 0))))
        widths["pend"] = max(widths["pend"], len(strv(m.get("pending", 0))))
        uncorr = (m.get("offline_uncorrect", 0) or 0) + (m.get("reported_uncorrect", 0) or 0)
        widths["uncorr"] = max(widths["uncorr"], len(strv(uncorr)))
        widths["crc"] = max(widths["crc"], len(strv(m.get("crc_errors", 0))))

    line = "  ".join(headers[c].ljust(widths[c]) for c in cols)
    sep = "  ".join("-" * widths[c] for c in cols)
    out = [line, sep]

    for r in rows:
        m = r.get("metrics", {})
        row = [
            strv(r.get("device")).ljust(widths["device"]),
            strv(r.get("type")).ljust(widths["type"]),
            strv(r.get("health_percent")).rjust(widths["health%"]),
            strv(r.get("model")).ljust(widths["model"]),
            strv(r.get("serial")).ljust(widths["serial"]),
            strv(m.get("power_on_hours")).rjust(widths["power_on_h"]),
            strv(m.get("reallocated", 0)).rjust(widths["realloc"]),
            strv(m.get("pending", 0)).rjust(widths["pend"]),
            strv((m.get("offline_uncorrect", 0) or 0) + (m.get("reported_uncorrect", 0) or 0)).rjust(widths["uncorr"]),
            strv(m.get("crc_errors", 0)).rjust(widths["crc"]),
        ]
        out.append("  ".join(row))

    return "\n".join(out)


def human_breakdown(rows, top_n=None):
    lines = []
    for r in rows:
        lines.append(f"\n== {r.get('device')}  {r.get('type')}  health={r.get('health_percent')}% ==")
        b = r.get("metrics", {}).get("breakdown", [])
        if not b:
            lines.append("  no penalties")
            continue
        b = sorted(b, key=lambda x: x.get("penalty", 0), reverse=True)
        if top_n:
            b = b[:top_n]
        for item in b:
            key = item.get("key")
            value = item.get("value")
            pen = item.get("penalty")
            note = item.get("note")
            lines.append(f"  - {key}: value={value} -> penalty={pen}" + (f"  ({note})" if note else ""))
        lines.append(f"  total penalty: {total_penalty(r.get('metrics', {}).get('breakdown', []))}")
    return "\n".join(lines)


# --------------------------- collection --------------------------------
def collect(host, settings):
    results = []
    for dev in smart_scan():
        s = read_smart(dev)
        devname = s.get("device", {}).get("name") or dev
        model = s.get("model_name", "") or s.get("device", {}).get("model_name", "")
        family = s.get("model_family", "") or ""
        serial = s.get("serial_number", "") or s.get("device", {}).get("serial_number", "")
        dtype, score, details = compute_score(s, settings)
        bdown = details.get("breakdown", [])
        poh = get(s, ["power_on_time", "hours"], None)
        if poh is None:
            poh = get_attr(s, 9, 0) or get(s, ["nvme_smart_health_information_log", "power_on_hours"], 0) or 0

        m = {
            "power_on_hours": poh,
            "reallocated": get_attr(s, 5, 0),
            "pending": get_attr(s, 197, 0),
            "offline_uncorrect": get_attr(s, 198, 0),
            "reported_uncorrect": get_attr(s, 187, 0),
            "crc_errors": get_attr(s, 199, 0),
            "breakdown": bdown,
            "total_penalty": total_penalty(bdown),
        }

        results.append(
            {
                "host": host,
                "device": devname,
                "model": model,
                "family": family,
                "serial": serial,
                "type": dtype,
                "health_percent": score,
                "metrics": m,
                "smart_overall_passed": bool(get(s, ["smart_status", "passed"], True)),
                "timestamp": int(time.time()),
            }
        )
    return results


def publish_all(client, base_topic, host, qos, retain, results, payload_root):
    for payload in results:
        serial = payload.get("serial") or ""
        base = os.path.basename(payload["device"])
        slug = _slug(f"{base}_{serial}") if serial else _slug(base)

        state_topic  = f"{base_topic}/{host}/{slug}/state"
        health_topic = f"{base_topic}/{host}/{slug}/health_percent"
        problem_topic= f"{base_topic}/{host}/{slug}/problem"

        # JSON state
        publish(client, state_topic, payload, qos=qos, retain=retain)
        # health% numeric
        client.publish(health_topic, str(payload.get("health_percent", "")), qos=qos, retain=retain)
        # problem flag
        smart_ok = payload.get("smart_overall_passed", True)
        client.publish(problem_topic, "OFF" if smart_ok else "ON", qos=qos, retain=retain)

    publish(client, f"{base_topic}/{host}/summary", payload_root, qos=qos, retain=retain)


# --------------------------- main --------------------------------------
def main():
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=MAN,
    )
    # MQTT
    ap.add_argument("--broker", default="localhost", help="MQTT broker (default: localhost)")
    ap.add_argument("--port", type=int, default=1883, help="MQTT port (default: 1883)")
    ap.add_argument("--username", help="MQTT username")
    ap.add_argument("--password", help="MQTT password")
    ap.add_argument("--tls", action="store_true", help="Enable TLS for MQTT")
    ap.add_argument("--cafile", help="CA file path for TLS")
    ap.add_argument("--insecure", action="store_true", help="Allow insecure TLS (tls_insecure_set)")
    ap.add_argument("--client-id", default=f"smart-health-{socket.gethostname()}", help="MQTT client id")
    ap.add_argument("--base-topic", default="servers/smart", help="Base MQTT topic")
    ap.add_argument("--qos", type=int, default=0, choices=[0, 1, 2], help="MQTT QoS (default: 0)")
    ap.add_argument("--retain", action="store_true", help="Publish with retain")
    ap.add_argument("--no-mqtt", action="store_true", help="Disable MQTT; run one cycle and exit")

    # Home Assistant discovery
    ap.add_argument("--ha-discovery", action="store_true", help="Enable Home Assistant MQTT Discovery")
    ap.add_argument("--ha-prefix", default="homeassistant", help="Home Assistant discovery prefix (default: homeassistant)")
    ap.add_argument("--ha-node", default=None, help="Override HA device name (default: <hostname>)")

    # Heartbeat
    ap.add_argument("--heartbeat-sec", type=int, default=30, help="Publish heartbeat every N seconds (default: 30)")
    ap.add_argument("--ha-heartbeat-expire", type=int, default=None,
                    help="expire_after for heartbeat binary_sensor (default: 2*heartbeat+5)")

    # Scheduling
    ap.add_argument("--once", action="store_true", help="Run once and exit (also implied by --no-mqtt)")
    ap.add_argument("--interval", type=int, default=3600, help="Seconds between runs in periodic mode (default: 3600)")

    # Output
    ap.add_argument("--format", choices=["table", "json"], default="table", help="Output format (default: table)")
    ap.add_argument("--verbose", action="store_true", help="(table) Print per-attribute penalty breakdowns")
    ap.add_argument("--top", type=int, help="(table+verbose) Show only top N penalties")
    ap.add_argument("--json-verbose", action="store_true",
                    help="(json) Include scoring_version, settings, and summary at root")

    # Scoring knobs
    ap.add_argument("--ignore-samsung-181", action="store_true", help="Ignore SMART 181 Program_Fail_Cnt_Total on Samsung SATA SSDs")
    ap.add_argument("--ignore-nvme-used", action="store_true", help="Ignore NVMe 'percentage_used' in health scoring")

    args = ap.parse_args()

    settings = {
    "ignore_samsung_181": bool(args.ignore_samsung_181),
    "ignore_nvme_used": bool(args.ignore_nvme_used),
}

    host = socket.gethostname()
    heartbeat_expire = args.ha_heartbeat_expire if args.ha_heartbeat_expire is not None else (2 * args.heartbeat_sec + 5)

    client = None
    reconnect_worker = None
    mqtt_state = None
    if not args.no_mqtt:
        if mqtt is None:
            print("paho-mqtt not installed; proceeding with --no-mqtt", file=sys.stderr)
            args.no_mqtt = True
        else:
            client, reconnect_worker, mqtt_state = make_client(args, host)
            # Online for HA availability
            ha_publish_availability(client, args.base_topic, host, online=True, qos=args.qos, retain=True)

    def build_payload(results):
        root = {
            "host": host,
            "timestamp": int(time.time()),
            "format": args.format,
            "scoring_version": SCORING_VERSION if args.json_verbose or args.format == "json" else None,
            "settings": settings if args.json_verbose or args.format == "json" else None,
            "drives": results,
        }
        if args.json_verbose or args.format == "json":
            root["summary"] = {
                "count": len(results),
                "min_health": min((r["health_percent"] for r in results), default=None),
                "max_health": max((r["health_percent"] for r in results), default=None),
                "avg_health": round(sum(r["health_percent"] for r in results) / len(results), 2) if results else None,
                "total_penalty_sum": round(sum(r["metrics"].get("total_penalty", 0.0) for r in results), 2),
            }
        return {k: v for k, v in root.items() if v is not None}

    def cycle():
        results = collect(host, settings)
        payload_root = build_payload(results)

        if args.format == "json":
            print(json.dumps(payload_root, indent=2, ensure_ascii=False))
        else:
            print(human_table(results))
            if args.verbose:
                print(human_breakdown(results, top_n=args.top))

        # MQTT publish (HA discovery first, then states)
        if not args.no_mqtt and client is not None:
            if args.ha_discovery:
                ha_discovery_publish(
                    client,
                    args.ha_prefix,
                    args.base_topic,
                    host,
                    results,
                    qos=args.qos,
                    retain=True,
                    node_name=(args.ha_node or host),
                    heartbeat_expire=heartbeat_expire,
                )
            publish_all(client, args.base_topic, host, args.qos, args.retain, results, payload_root)
            # initial heartbeat right after a scan
            publish_heartbeat(client, args.base_topic, host, qos=args.qos)
            client.loop(timeout=1.0)

    # Single-run if --once OR --no-mqtt
    if args.once or args.no_mqtt:
        cycle()
        if client is not None:
            try:
                ha_publish_availability(client, args.base_topic, host, online=False, qos=args.qos, retain=True)
            except Exception:
                pass
            try:
                if mqtt_state is not None:
                    mqtt_state["expected_disconnect"] = True
                client.disconnect()
            except Exception:
                pass
            if reconnect_worker is not None:
                reconnect_worker.stop()
        return

    # Periodic loop (MQTT mode) with heartbeat ticks
    next_scan = 0.0
    next_hb = 0.0
    while True:
        now = time.time()
        try:
            if not args.no_mqtt and client is not None:
                try:
                    client.loop(timeout=0.1)
                except Exception:
                    pass
            if now >= next_scan:
                cycle()
                next_scan = now + max(10, args.interval)
            if not args.no_mqtt and client is not None and now >= next_hb:
                publish_heartbeat(client, args.base_topic, host, qos=args.qos)
                client.loop(timeout=0.1)
                next_hb = now + max(5, args.heartbeat_sec)
        except Exception as e:
            if not args.no_mqtt and client is not None:
                publish(client, f"{args.base_topic}/{host}/error",
                        {"host": host, "error": str(e), "timestamp": int(time.time())}, qos=args.qos, retain=False)
            print(f"ERROR: {e}", file=sys.stderr)
        time.sleep(1)


if __name__ == "__main__":
    main()
