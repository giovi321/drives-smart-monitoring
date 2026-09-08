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

SCORING_VERSION = "v10.0"

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


def get_attr_norm(ata_json, attr_id):
    """
    Return (value, worst, thresh) for a SMART attribute, or (None, None, None).

    These are the NORMALISED figures, and for vendor-encoded attributes they are the
    only comparable view. The RAW of attribute 7 (Seek_Error_Rate) on Seagate is a
    packed 48-bit field, low 32 bits total seeks and high 16 bits seek errors, not a
    count. Reading it as a count charges a large penalty to a perfectly healthy drive.
    Attribute 1 (Raw_Read_Error_Rate) is encoded the same way and should be treated
    identically if it is ever scored.
    """
    tbl = ata_json.get("ata_smart_attributes", {}).get("table", [])
    for row in tbl:
        if row.get("id") == attr_id:
            return row.get("value"), row.get("worst"), row.get("thresh")
    return None, None, None


# Attributes whose RAW field is a plain count on every vendor in the fleet.
# Anything NOT listed here must be scored on the normalised value, because its raw is
# vendor-encoded and reading it as a count charges nonsense penalties to healthy drives.
#
# Measured 2026-09-08 across host A, host B and host C:
#    id  attribute                raw as read              what the raw actually is
#     1  Raw_Read_Error_Rate      244073641   Seagate      packed 48-bit, not a count
#     7  Seek_Error_Rate          370858355   Seagate      packed 48-bit, not a count
#     9  Power_On_Hours           85607288165789 Seagate    packed, prints '21917 (77 220 0)'
#   181  Program_Fail_Cnt_Total   260204355   Samsung      packed rate; val=93 worst=1
#   188  Command_Timeout          4294967295  Samsung      0xFFFFFFFF sentinel
#   190  Airflow_Temperature_Cel  974782513   Seagate      temp with min/max packed in
#   194  Temperature_Celsius      477531602984 Samsung     temp with min/max/count packed in
#   195  Hardware_ECC_Recovered   30792939    Seagate      mirrors attribute 1's packed raw
#   240  Head_Flying_Hours        3153304859190316 Seagate packed time triple
#   241  Total_LBAs_Written       2333070     Samsung      vendor UNITS, not 512-byte LBAs
#
# 241/242 are counts but the unit is vendor-defined, so they get a plausibility gate of
# their own rather than a place on this list.
RAW_IS_COUNT = frozenset({4, 5, 10, 12, 174, 177, 179, 182, 183, 184, 187,
                          191, 192, 193, 196, 197, 198, 199, 235})


def get_attr_count(ata_json, attr_id, default=0):
    """
    Raw value of an attribute whose raw is a genuine count on all vendors.

    Refuses any id outside RAW_IS_COUNT. This is the guard against the defect class
    that made a healthy Seagate read 56.7% (attribute 7) and a healthy Samsung read
    59.9% (attribute 181): a vendor-packed raw read as an error tally. If an
    attribute is not on the list, score it with attr_norm_penalty() instead.
    """
    if attr_id not in RAW_IS_COUNT:
        raise AssertionError(
            f"attribute {attr_id} raw is not a portable count; "
            "score it on the normalised value via attr_norm_penalty()"
        )
    return get_attr(ata_json, attr_id, default)


def attr_norm_penalty(d, attr_id, cap, start=0.5):
    """
    Penalty from an attribute's NORMALISED value measured against its own threshold.

    Returns (penalty, value, thresh); (None, None, None) when the attribute is absent.

    The normalised value is the vendor's own verdict: near 100 when new, falling to
    the threshold at the point the vendor calls the drive failed. It is the only view
    that is comparable across makes, and the only one available for the packed
    attributes above. No penalty while more than `start` of the margin above the
    threshold remains; from there it ramps linearly to `cap` as the value reaches the
    threshold, and stays at `cap` below it.
    """
    val, worst, thresh = get_attr_norm(d, attr_id)
    if val is None or thresh is None:
        return None, None, None
    # Not every vendor normalises to 100. host A /dev/sdb (WDC WD20EZRX, Western
    # Digital Green) reports val=200 worst=200 thresh=140 on attribute 5, so a fixed
    # margin of 100-thresh goes negative and the ramp collapses into a cliff at the
    # threshold. Infer the top of the scale instead.
    top = 200.0 if (int(val) > 100 or (worst is not None and int(worst) > 100)) else 100.0
    margin = max(1.0, top - float(thresh))
    headroom = (float(val) - float(thresh)) / margin
    if headroom >= start:
        return 0.0, val, thresh
    return min(float(cap), (float(cap) / start) * (start - max(0.0, headroom))), val, thresh


def get_attr_hours(ata_json, attr_id=9):
    """
    Power-on hours from attribute 9, read from the RENDERED STRING, never the raw.

    Attribute 9 is another packed field on Seagate. Measured 2026-09-08: host A
    /dev/sda raw 85607288165789 rendering as '21917 (77 220 0)', host C /dev/sda raw
    4630915342935480 rendering as '9656h+17m+58.219s'. get_attr() would return the
    packed integer, so the old fallback in collect() would have published a fourteen
    digit hour count. smartctl has already decoded the leading figure, so use that.
    """
    tbl = ata_json.get("ata_smart_attributes", {}).get("table", [])
    for row in tbl:
        if row.get("id") != attr_id:
            continue
        raw_str = str(row.get("raw", {}).get("string", "") or "")
        num = ""
        for ch in raw_str:
            if ch.isdigit():
                num += ch
            elif num:
                break
        try:
            return int(num) if num else 0
        except Exception:
            return 0
    return 0


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

    # ---------------------------------------------------------------- endurance
    # The old term was min(60, percentage_used), which is flat above 60% used: a drive
    # at 60% of rated endurance and one at 94% both landed on exactly 40.0.
    #
    # host C on 2026-09-08 is the case in point. Its two boot NVMe drives, a mirror
    # pair, read percentage_used 94 (nvme0) and 81 (nvme1).
    # Both scored 40.0 unflagged, and both published 100.0% in production because that
    # host runs --ignore-nvme-used. Retained topics still hold the pre-flag values,
    # 63.0 and 60.0, from when percentage_used was 37 and 40. So the only endurance
    # signal on the pair was switched off while it was climbing, and Home Assistant
    # has been showing perfect health for two drives near the end of their rated life.
    #
    # The curve is superlinear past 80% so the last stretch, where the decision
    # actually gets made, still resolves. 50% used costs 30, 80% costs 48, 94% costs
    # 60, 100% costs 65.
    pct_used = nlog.get("percentage_used", 0) or 0
    pen = min(75.0, 0.6 * float(pct_used) + 0.02 * max(0.0, float(pct_used) - 80.0) ** 2)
    if settings.get("ignore_nvme_used", False):
        # --ignore-nvme-used now DAMPS the endurance term to 40% of its weight instead
        # of deleting it.
        #
        # Deleting it is what produced the worst reading in the fleet: host C
        # publishing 100.0% for two boot NVMe drives at 94% and 81% of rated endurance.
        # A flag in a unit file must not be able to assert perfect health over the
        # drive's own end-of-life estimate.
        #
        # A multiplier rather than a cap, so the ordering survives. A cap would put 81%
        # used and 94% used back on the same number, which is the flattening this
        # curve was reshaped to remove. Damped: 3% used costs 0.7, 50% costs 12,
        # 81% costs 19, 94% costs 24.
        #
        # The flag was probably a response to the old linear 1:1 penalty, where 30%
        # used meant a 70% score. The reshaped curve is gentle early and steep late, so
        # most of the reason to reach for this flag has gone. Prefer dropping it.
        pen *= 0.4
        note = f"{pct_used}% used, damped to 40% by --ignore-nvme-used"
    else:
        note = "% used endurance"
    score -= pen
    add_penalty(breakdown, "nvme_percentage_used", pct_used, pen, note)

    # available_spare is compared against the drive OWN threshold, not against 100.
    # The old term, min(20, 2*(100-spare)), saturated at spare=90, so every value from
    # 90 down to 0 scored identically and the one boundary that matters, the vendor
    # available_spare_threshold, was invisible. It reads 32 on the host C pair and 10
    # on the host B Samsung, so "below 100" and "in trouble" are far apart.
    spare = nlog.get("available_spare", 100)
    spare_thresh = nlog.get("available_spare_threshold", 10)
    if spare is not None:
        top = 100.0
        floor = float(spare_thresh if spare_thresh is not None else 10)
        margin = max(1.0, top - floor)
        headroom = (float(spare) - floor) / margin
        if headroom < 0.5:
            pen = min(35.0, 70.0 * (0.5 - max(0.0, headroom)))
            score -= pen
            add_penalty(breakdown, "nvme_available_spare", spare, pen,
                        f"threshold {spare_thresh}")

    # critical_warning is the drive own failure bitmask and was not scored at all.
    # Bit 0 spare below threshold, 1 temperature out of range, 2 reliability degraded,
    # 3 media in read-only, 4 volatile memory backup failed, 5 persistent memory
    # read-only. Any of bits 0, 2, 3 set is the NVMe equivalent of failing
    # smartctl -H, and it needs to dominate the score rather than contribute to it.
    cw = int(nlog.get("critical_warning", 0) or 0)
    if cw:
        bits = {0: "available spare below threshold", 1: "temperature out of range",
                2: "reliability degraded", 3: "media is read-only",
                4: "volatile memory backup failed", 5: "persistent memory read-only"}
        set_bits = [bits.get(i, f"bit {i}") for i in range(8) if cw & (1 << i)]
        pen = 60.0 if (cw & 0b101101) else 15.0
        score -= pen
        add_penalty(breakdown, "nvme_critical_warning", cw, pen, "; ".join(set_bits))

    media_err = nlog.get("media_errors", nlog.get("media_and_data_integrity_errors", 0)) or 0
    pen = min(50.0, 5.0 * (float(media_err) ** 0.5))
    score -= pen
    add_penalty(breakdown, "nvme_media_errors", media_err, pen, "media/data integrity errors")

    # controller_busy_time is gone from the score.
    #
    # It is a workload odometer in minutes, monotonically increasing with use, and
    # carries no information about failure. min(5, 0.001*minutes) reached its full
    # 5-point cap at 5000 minutes, or 83 hours of accumulated I/O, so any drive that
    # has ever done real work paid the maximum forever. host B /dev/nvme0 was
    # losing the full 5.0 for 11,903 minutes across 3,943 power-on hours: normal
    # service, scored as a defect. Same mistake in kind as reading a total-seeks field
    # as a seek-error count. It is recorded for context, at zero weight.
    busy_min = float(nlog.get("controller_busy_time", 0) or 0)
    add_penalty(breakdown, "nvme_controller_busy_minutes", busy_min, 0.0,
                "workload odometer, not scored")

    # Error log entries stay tiny and are now capped explicitly. The count includes
    # benign administrative rejections and on many controllers saturates at the log
    # depth: host B /dev/nvme0 reports exactly 255. It is a hint, not a verdict.
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

    # ---------------------------------------------------------------- endurance
    # Wear is measured ONCE, from the best source the drive offers, in this order.
    # It used to be measured twice and added up: a TBW estimate plus a separate
    # attribute-177 cycle penalty. host A /dev/sdj (Samsung 850 PRO) paid 13.42 for
    # TBW and another 6.32 for wear levelling, 19.74 in total, for one physical
    # quantity that Samsung itself puts at 14% consumed (attribute 177 normalised 86,
    # raw 832 cycles against roughly 6000 rated for this MLC part).
    #
    # 1. "Percentage Used Endurance Indicator" from device statistics: an ATA-standard
    #    figure, already a percentage, no vendor decoding.
    # 2. The normalised value of whichever wear attribute the drive publishes. This is
    #    the vendor counting down its own rating and needs no guess about TBW ratings
    #    or cell type. Note the id varies: 177 on the 850 PRO, 173 on the PM883-class
    #    MZ7LH512, 233 on Intel, 231 on some others. Only 177 was read before, so
    #    /dev/sdi published no wear signal at all.
    # 3. A TBW estimate from attribute 241, last, and only when it is plausible.
    #    Attribute 241 is a count but its UNIT is vendor-defined. host A /dev/sdi
    #    reports raw 2,333,070, which times 512 is 1.19 GB after 8,172 hours; the real
    #    figure is around 75 TB in 32 MB units. host B /dev/sda reports 0.30 GB
    #    after 13,321 hours of Home Assistant recorder writes. Both silently scored
    #    zero wear, so both SSDs were published as healthy with no endurance
    #    monitoring whatsoever. Anything implying less than 1 GB written per 1000
    #    power-on hours is rejected as a unit mismatch rather than believed.
    wear_pct = None
    wear_src = None

    ds_pct = get(
        d,
        ["device_statistics", "pages", "sata_smart_attributes", "Percentage Used Endurance Indicator"],
        0,
    ) or 0
    if ds_pct > 0:
        wear_pct = float(ds_pct)
        wear_src = "device_statistics_percentage_used"

    poh_hours = get(d, ["power_on_time", "hours"], 0) or 0
    if wear_pct is None:
        for wid in (177, 173, 233, 231, 202):
            wval, wworst, wthresh = get_attr_norm(d, wid)
            if wval is None:
                continue
            top = 200.0 if int(wval) > 100 else 100.0
            # A pristine normalised value with a zero raw on a drive that has been
            # powered for years means the attribute is NOT IMPLEMENTED, not that the
            # NAND is untouched. host B /dev/sda (NGFF 2280 512GB SSD, an SM22xx
            # class controller) reports val=100 worst=100 thresh=50 and raw=0 on
            # attribute 177 after 13,321 hours of Home Assistant recorder writes. Left
            # unchecked that reads as 0% wear and publishes 100.0%, which is the same
            # class of mistake as everything else in this review: a field that means
            # nothing on this vendor taken at face value.
            if int(wval) >= int(top) and get_attr(d, wid, 0) == 0 and poh_hours > 1000:
                continue
            wear_pct = max(0.0, min(100.0, (top - float(wval)) / top * 100.0))
            wear_src = f"attr_{wid:03d}_normalised_{wval}"
            break

    if wear_pct is None:
        cap_bytes = get(d, ["user_capacity", "bytes"], 0) or 0
        tb_written = lbas_to_bytes(get_attr(d, 241, 0)) / 1e12
        plausible = tb_written * 1e12 >= max(1e9, (poh_hours / 1000.0) * 1e9)
        if plausible and cap_bytes > 0:
            baseline_tb = max(50.0, (cap_bytes / (512 * 2**30)) * 150.0)
            wear_pct = min(200.0, 100.0 * tb_written / baseline_tb)
            wear_src = f"attr_241_TBW_{round(tb_written, 2)}TB_of_{round(baseline_tb, 1)}TB"
        else:
            # Unknown endurance is not the same as no wear, and must not publish 100.0%.
            #
            # Both of the drives that land here are real: host A /dev/sdi reported
            # attribute 241 raw 2,333,070, which times 512 is 1.19 GB after 8,172
            # hours (the true figure is near 75 TB in 32 MB units), and host B
            # /dev/sda reported 0.30 GB after 13,321 hours. Both silently scored zero
            # wear and published as perfectly healthy, so neither SSD was actually
            # being monitored for endurance at all.
            #
            # A fixed 5 points: enough that the drive never reads 100.0 and surfaces in
            # a dashboard sorted by health, small enough that it cannot be mistaken for
            # a fault or trip a replacement threshold. The note says why.
            pen = 5.0
            score -= pen
            add_penalty(breakdown, "endurance_unknown", round(tb_written, 4), pen,
                        f"attr 241 implies {round(tb_written * 1000, 2)} GB over {poh_hours}h, "
                        "rejected as a unit mismatch; no usable wear attribute published")

    if wear_pct is not None:
        # Superlinear past 80%. A linear term capped at 60 gave the same 40.0 score to
        # a drive at 60% of rated endurance and one at 94%, which is precisely where
        # the resolution matters. host C nvme0/nvme1 sit at 94% and 81%.
        pen = min(75.0, 0.6 * wear_pct + 0.02 * max(0.0, wear_pct - 80.0) ** 2)
        score -= pen
        add_penalty(breakdown, "endurance_percent_used", round(wear_pct, 1), pen, wear_src)

    # ---------------------------------------------------------------- spare blocks
    # Both of these read the NORMALISED value now. They used to read the raw, which is
    # meaningless for either: on Samsung neither attribute exists at all, so the old
    # code fell through to its defaults (232 -> 100, 233 -> 0) and both tests failed
    # closed. That was luck, not correctness; on a drive that does publish them with a
    # packed raw it would have charged up to 60 points of nonsense.
    pen, sval, sthresh = attr_norm_penalty(d, 232, cap=30.0, start=0.9)
    if pen:
        score -= pen
        add_penalty(breakdown, "attr_232_AvailableSpare_norm", sval, pen, f"thresh {sthresh}")

    pen, uval, uthresh = attr_norm_penalty(d, 179, cap=25.0, start=0.9)
    if pen:
        score -= pen
        add_penalty(breakdown, "attr_179_UsedReservedBlocks_norm", uval, pen, f"thresh {uthresh}")

    # ---------------------------------------------------------------- program/erase
    # Attribute 181 is scored on the NORMALISED value. Reading its raw was the second
    # instance of the defect this review was called for, and the more damaging one
    # because it fires by default.
    #
    # host A /dev/sdi, SAMSUNG MZ7LH512HALU, 2026-09-08: attribute 181 raw
    # 260,204,355 with normalised value 93 and threshold 0. The old term
    # min(40, 10*raw) therefore charged the full 40-point cap to a drive with zero
    # reallocated, zero pending, zero uncorrectable, 2 CRC and smartctl -H PASSED, for
    # a published health of 59.9%. Samsung packs a rate field there; it is not a
    # program-failure tally.
    #
    # --ignore-samsung-181 existed to paper over exactly this, and is now a no-op. It
    # was the wrong shape of fix twice over: it lived in each host unit file rather
    # than the code, and it discarded the attribute entirely, so a Samsung SSD that
    # really was failing programs would have been silenced along with the false alarm.
    pen, pval, pthresh = attr_norm_penalty(d, 181, cap=40.0)
    if pen:
        score -= pen
        add_penalty(breakdown, "attr_181_ProgramFailTotal_norm", pval, pen, f"thresh {pthresh}")

    pen, eval_, ethresh = attr_norm_penalty(d, 182, cap=40.0)
    if pen:
        score -= pen
        add_penalty(breakdown, "attr_182_EraseFailTotal_norm", eval_, pen, f"thresh {ethresh}")

    realloc = get_attr_count(d, 5, 0)
    pend = get_attr_count(d, 197, 0)
    offu = get_attr_count(d, 198, 0)
    repu = get_attr_count(d, 187, 0)
    if realloc > 0:
        pen = min(30.0, 5.0 + 2.0 * (float(realloc) ** 0.5))
        score -= pen
        add_penalty(breakdown, "attr_005_ReallocatedSectors", realloc, pen)
    if pend > 0:
        pen = min(35.0, 12.0 + 5.0 * (float(pend) ** 0.5))
        score -= pen
        add_penalty(breakdown, "attr_197_CurrentPending", pend, pen)
    # Split for the same reason as in score_hdd: 198 counts sectors, 187 counts errors
    # reported to the host. On SSDs 187 is usually named Uncorrectable_Error_Cnt.
    if offu > 0:
        pen = min(40.0, 10.0 + 3.0 * (float(offu) ** 0.5))
        score -= pen
        add_penalty(breakdown, "attr_198_OfflineUncorrectable", offu, pen)
    if repu > 0:
        pen = min(25.0, 6.0 + 2.0 * (float(repu) ** 0.5))
        score -= pen
        add_penalty(breakdown, "attr_187_UncorrectableErrorCnt", repu, pen)

    # The old attribute-177 term is gone. It double-charged the endurance already
    # counted above, and its ">200 cycles, 0.01 per cycle" shape was a bare guess:
    # 200 cycles is nothing for MLC (the 850 PRO here is rated around 6000) and a
    # large fraction of life for QLC. Attribute 177 now feeds the endurance block as
    # one of the normalised wear sources, where the vendor supplies the rating.

    crc = get_attr_count(d, 199, 0)
    pen = min(5.0, 0.05 * float(crc))
    score -= pen
    add_penalty(breakdown, "attr_199_CRC_Errors", crc, pen)

    return clamp(round(score, 1)), {"breakdown": breakdown}


def score_hdd(d):
    score = 100.0
    breakdown = []

    # Sector-level damage. These three raws are genuine counts on every vendor, so
    # they go through get_attr_count(), which refuses ids whose raw is packed.
    realloc = get_attr_count(d, 5, 0)
    pend = get_attr_count(d, 197, 0)
    offu = get_attr_count(d, 198, 0)

    pen = min(40.0, 4.0 * (float(realloc) ** 0.5))
    score -= pen
    add_penalty(breakdown, "attr_005_ReallocatedSectors", realloc, pen)

    pen = min(45.0, 7.0 * (float(pend) ** 0.5))
    score -= pen
    add_penalty(breakdown, "attr_197_CurrentPending", pend, pen)

    pen = min(35.0, 6.0 * (float(offu) ** 0.5))
    score -= pen
    add_penalty(breakdown, "attr_198_OfflineUncorrectable", offu, pen)

    # Attribute 187 Reported_Uncorrect is NOT a sector count and is no longer summed
    # with 198.
    #
    # The old term was min(35, 4*sqrt(attr198 + attr187)). That sum is a category
    # error: 198 counts uncorrectable SECTORS found by offline scanning, while 187
    # counts errors REPORTED TO THE HOST, which on Seagate includes link, cable and
    # command-timeout events that leave no media defect behind. Adding them asserts
    # sector damage the drive never claimed.
    #
    # host A /dev/sda on 2026-09-08: attribute 187 raw 121 with a
    # normalised value of 1 against threshold 0, the floor, while 5, 197 and 198 are
    # all zero and smartctl -H reports PASSED. The old sum charged 35.0 and produced
    # 56.7%, ranking that drive BELOW /dev/sdc, which has 8 genuinely
    # reallocated sectors and scored 69.8%. That inversion is the concrete misranking.
    #
    # Two independent facts deserve separate weight:
    #   - The normalised value is the vendor own statement of remaining margin. At the
    #     floor it is the strongest claim the drive makes short of failing -H, and the
    #     raw count cannot express it: /dev/sdc carries raw 18 at a normalised 82,
    #     nowhere near its limit, while sda carries raw 121 at 1. Larger cap.
    #   - The raw count still matters, but with no sector-level corroboration the
    #     indicated action is check cabling and run a long self-test, not replace.
    #     Uncorroborated, that term is halved so it cannot by itself drive a replace.
    repu = get_attr_count(d, 187, 0)
    corroborated = (realloc + pend + offu) > 0
    npen, nval, nth = attr_norm_penalty(d, 187, cap=25.0)
    if npen:
        score -= npen
        add_penalty(breakdown, "attr_187_ReportedUncorrect_norm", nval, npen,
                    f"thresh {nth}, vendor margin nearly gone")
    if repu > 0:
        pen = min(10.0, 1.0 * (float(repu) ** 0.5))
        note = "corroborated by sector damage"
        if not corroborated:
            pen *= 0.5
            note = "no sector damage; link/cable suspect, term halved"
        score -= pen
        add_penalty(breakdown, "attr_187_ReportedUncorrect_raw", repu, pen, note)

    re_event = get_attr_count(d, 196, 0)
    pen = min(10.0, float(re_event))
    score -= pen
    add_penalty(breakdown, "attr_196_ReallocEvents", re_event, pen)

    # Attribute 7 is scored on the NORMALISED value, not the raw.
    #
    # The previous version read the raw as an error count: min(8, 2*log10(1+raw)).
    # On Seagate the raw is a packed 48-bit field whose low 32 bits are total seeks,
    # so a healthy drive with zero seek errors still produced a huge number and took
    # the full 8-point cap. Measured on host A 2026-09-08: /dev/sda raw
    # 370282112, which is 0 errors once shifted right 32, while the normalised value
    # read 086 against a threshold of 045. That drive was losing 8 points for nothing.
    #
    # The normalised value is the vendor's own verdict and is comparable across makes.
    # No penalty while more than half the margin above the threshold remains; from
    # there it ramps to the 8-point cap as the value reaches the threshold.
    pen, seek_val, seek_thresh = attr_norm_penalty(d, 7, cap=8.0)
    if pen:
        score -= pen
        add_penalty(breakdown, "attr_007_SeekErrorRate_norm", seek_val, pen,
                    f"thresh {seek_thresh}")

    # Attribute 1 Raw_Read_Error_Rate is encoded exactly like attribute 7 and is now
    # scored the same way, through the same helper. Raws measured 2026-09-08:
    # 244073641 on host A sda, 30792939 on host C sda, 0 on the WD Green. Reading
    # any of those as a count would repeat the original defect, so only the normalised
    # value is used. Small cap, because on Seagate this attribute sits well above its
    # threshold for the whole of a healthy life: corroborating signal, not a primary.
    pen, rre_val, rre_thresh = attr_norm_penalty(d, 1, cap=6.0)
    if pen:
        score -= pen
        add_penalty(breakdown, "attr_001_RawReadErrorRate_norm", rre_val, pen,
                    f"thresh {rre_thresh}")

    spin_retry = get_attr_count(d, 10, 0)
    pen = min(20.0, 10.0 * float(spin_retry))
    score -= pen
    add_penalty(breakdown, "attr_010_SpinRetry", spin_retry, pen)

    # Load cycle count carries half its former weight: cap 5 rather than 10.
    #
    # It is an odometer, not an error counter, and nothing in this fleet links it to
    # damage. host A /dev/sdb (WD Green) has 1,584,397 load cycles over 81,879 hours
    # with zero reallocated, pending, uncorrectable and CRC, and took the full 10
    # points for it: a 90.0% score on a drive with no defect of any kind. Head parking
    # is what a WD Green does. A tenth of the whole scale for that is not defensible;
    # a nudge is.
    #
    # The rating guess is unreliable too. Seagate own normalised value for /dev/sdc
    # reads 43 at 114,853 cycles, implying a design rating near 200,000 rather than the
    # 600,000 assumed here. The raw-against-rating heuristic is kept because every 193
    # row in the fleet publishes threshold 0, leaving the normalised route no vendor
    # threshold to anchor against, but the weight now reflects that uncertainty.
    lcc = get_attr_count(d, 193, 0)
    model = d.get("model_name", "") or d.get("device", {}).get("model_name", "")
    family = d.get("model_family", "") or ""
    # Match on the model FAMILY for NAS and enterprise ratings. The old test included
    # a bare `"pro" in model.lower()` substring, which matches any product with "pro"
    # anywhere in its name.
    hay = f"{family} {model}".lower()
    lcc_cap = 600000 if any(
        k in hay for k in ("ironwolf", "red pro", "ultrastar", "exos", "enterprise")
    ) else 300000
    pen = min(5.0, 5.0 * min(1.5, lcc / max(1, lcc_cap)))
    score -= pen
    add_penalty(breakdown, "attr_193_LoadCycleCount", lcc, pen, f"rating {lcc_cap}")

    crc = get_attr_count(d, 199, 0)
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

    # A drive that fails its own overall health check cannot publish a good score.
    # smart_status.passed was only ever wired to the problem binary_sensor, so a drive
    # the firmware had already given up on could still publish a high health_percent,
    # and any automation watching the percentage rather than the problem flag would
    # have seen nothing wrong. The attribute penalties do not reliably cover this: the
    # firmware fails a drive on internal criteria that need not appear in any of the
    # attributes scored above.
    if not bool(get(d, ["smart_status", "passed"], True)):
        details.setdefault("breakdown", []).append({
            "key": "smart_status_failed",
            "value": False,
            "penalty": round(max(0.0, score - 5.0), 2),
            "note": "smartctl -H reports FAILED; score floored",
        })
        score = min(score, 5.0)

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
                # Interruptible sleep so stop() is honored promptly during backoff.
                if self._stop.wait(delay):
                    break
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

    state = {
        "worker": worker,
        "expected_disconnect": False,
        "force_cycle": threading.Event(),
        # Slugs this host published previously, learned from the retained manifest,
        # plus any slug seen live under this host subtree when --prune-stale-topics
        # is on. Drives the removal of vanished drives; see prune_vanished_drives().
        "known_slugs": set(),
        "manifest_seen": False,
    }

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
        if isinstance(userdata, dict):
            force = userdata.get("force_cycle")
            if isinstance(force, threading.Event):
                force.set()
        try:
            ha_publish_availability(client, args.base_topic, host, online=True, qos=args.qos, retain=True)
        except Exception:
            pass
        # Subscribe on every connect, not just the first: subscriptions do not survive
        # a reconnect, and without the manifest nothing can be pruned.
        try:
            client.subscribe(manifest_topic(args.base_topic, host), qos=1)
            if getattr(args, "prune_stale_topics", False):
                # Wildcard sweep of this host own subtree, used to clean up slugs that
                # predate the manifest, including the device-letter identities.
                client.subscribe(f"{args.base_topic}/{host}/+/health_percent", qos=1)
        except Exception:
            pass

    def _on_message(client, userdata, msg):
        if not isinstance(userdata, dict):
            return
        try:
            topic = msg.topic
            if topic == manifest_topic(args.base_topic, host):
                userdata["manifest_seen"] = True
                if not msg.payload:
                    return
                data = json.loads(msg.payload.decode("utf-8", "replace"))
                for sl in (data.get("slugs") or []):
                    userdata["known_slugs"].add(str(sl))
                return
            parts = topic.split("/")
            if len(parts) >= 2 and parts[-1] == "health_percent":
                userdata["known_slugs"].add(parts[-2])
        except Exception:
            pass

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
    client.on_message = _on_message

    # The initial connect must never be fatal. If the broker is unreachable at
    # startup (e.g. service and broker come up together after a reboot, or the
    # broker is briefly down), a synchronous connect() raises. Instead of letting
    # that kill the process, hand off to the reconnect worker, which retries
    # forever with backoff until the broker is reachable. connect_async() has
    # already stored host/port/keepalive on the client, so worker reconnects work.
    try:
        client.connect(args.broker, args.port, keepalive=keepalive)
    except Exception as e:
        print(f"MQTT initial connect failed ({e}); retrying in background until broker is reachable",
              file=sys.stderr)
        worker.schedule(reset_backoff=True)
    return client, worker, state


def publish(client, topic, payload, qos=0, retain=False):
    client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=qos, retain=retain)


def _slug(x):
    s = "".join(ch if ch.isalnum() else "_" for ch in str(x))
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_").lower()


# Serials that identify nothing. USB bridges and some OEM parts hand out placeholders.
_BOGUS_SERIALS = frozenset({
    "", "0", "00000000", "000000000000", "0123456789abcdef", "none", "unknown",
    "default", "n_a", "na", "notsupported", "0000000000000000",
})


def drive_identity(result):
    """
    Stable per-drive identity. The SERIAL, and nothing else.

    The old identity was f"{device_letter}_{serial}", which is not stable: Linux
    assigns sd* by discovery order, so a drive changes letter whenever cabling,
    controller enumeration or another drive changes. Every letter a drive ever held
    minted a separate retained topic tree and a separate Home Assistant entity, and
    nothing removed the old ones.

    Measured on the broker, 2026-09-08:
      host C carries 15 drive slugs for 6 physical drives. One drive has appeared as
      sda, sdb and sdd, another as sdb, sdc and sdd, and the NVMe pair swapped places,
      so one NVMe exists under both nvme0_ and nvme1_. One more slug belongs to a drive
      that left the machine and still replays health_percent 0.0 under an online host.
      host A carries 17, seven of which belong to other hosts.

    The evidence for serial-only is on the same broker: a parallel serial-keyed scheme
    (<host>_disk-<serial>) holds exactly 11 entities for the 11 drives host A has
    ever had, with no letter churn at all and one stale entry for one removed drive.

    Every one of the 18 drives across the three hosts publishes a serial, and all 18
    are distinct. WWN is the fallback because the two NVMe drives and the no-name NGFF
    SSD do not publish one. The device letter is last resort only, and is still
    published as an attribute, where a moving value is information rather than a new
    identity.
    """
    serial = str(result.get("serial") or "").strip()
    if _slug(serial) not in _BOGUS_SERIALS and len(_slug(serial)) >= 4:
        return _slug(serial), "serial"

    wwn = result.get("wwn") or ""
    if wwn:
        return _slug(wwn), "wwn"

    base = os.path.basename(str(result.get("device") or "")) or "drive"
    return _slug(base), "device_letter_unstable"


def drive_topics(base_topic, host, slug):
    root = f"{base_topic}/{host}/{slug}"
    return {
        "state": f"{root}/state",
        "health_percent": f"{root}/health_percent",
        "problem": f"{root}/problem",
    }


def drive_config_topics(ha_prefix, host, slug):
    return [
        f"{ha_prefix}/sensor/{_slug(f'{host}_{slug}_health')}/config",
        f"{ha_prefix}/binary_sensor/{_slug(f'{host}_{slug}_smart_problem')}/config",
    ]

def ha_discovery_publish(client, ha_prefix, base_topic, host, results, qos=0, retain=True, node_name=None,
                         heartbeat_expire=None, drive_expire=None):
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
        # Identity is the serial, not the device letter. See drive_identity().
        slug = r.get("slug") or drive_identity(r)[0]
        devbase = os.path.basename(str(r.get("device") or "")) or "drive"

        topics = drive_topics(base_topic, host, slug)
        state_json_topic = topics["state"]
        health_topic     = topics["health_percent"]
        problem_topic    = topics["problem"]

        # The entity name still carries the current device letter so the drive is easy
        # to find in Home Assistant, but the letter is NOT part of the unique_id: a
        # rename is harmless, a changed unique_id mints a whole new entity.
        label = f"{devbase} {r.get('model') or ''}".strip() or slug

        # sensor: health %
        sensor_uid = _slug(f"{host}_{slug}_health")
        sensor_cfg_topic = f"{ha_prefix}/sensor/{sensor_uid}/config"
        sensor_cfg = {
            "name": f"{label} health",
            "unique_id": sensor_uid,
            # object_id pins the Home Assistant entity_id instead of letting it be
            # derived from the friendly name and the device name.
            #
            # Without it the entity_id is generated once, at first discovery, from
            # "<device name> <entity name>" -- and then never revised. That produced two
            # naming generations live side by side on one host, because the device had
            # been renamed in the UI at some point: hostc_drives_smart_sdc_<serial>
            # and hostc_sdc_<serial> both existed, from the same publisher, for the
            # same drives. It also baked the device letter into the id permanently, so a
            # drive that moved from sdc to sdi kept an entity_id naming a letter it no
            # longer had.
            #
            # With object_id the id is a pure function of host and serial, so it is
            # stable across letter changes, device renames and rediscovery. The friendly
            # name below still carries the current device letter and model, where a
            # value that changes is useful rather than misleading.
            "object_id": sensor_uid,
            "state_topic": health_topic,
            "unit_of_measurement": "%",
            "state_class": "measurement",
            "availability_topic": avail_topic,
            "json_attributes_topic": state_json_topic,
            "device": device_obj,
            "icon": "mdi:harddisk",
        }
        # expire_after makes a drive that STOPS REPORTING go unavailable instead of
        # freezing at its last value. Without it the health topics are retained, so the
        # broker replays the last number forever: a removed or failed drive keeps a
        # plausible reading and nothing looks wrong.
        #
        # This is not hypothetical. On 2026-09-08 host A was found carrying 19 stale
        # retained slugs against 10 real drives, and its low-disk-life automation was
        # watching two serials that were not in the machine, both showing healthy frozen
        # values. Separately drives-smart-monitoring on host B had been dead for
        # seven weeks while Home Assistant displayed that host's drive at 100.0%.
        # availability_topic does not cover either case: it tracks the PUBLISHER's LWT,
        # so it only fires when the whole process dies, never when one drive goes away.
        if drive_expire:
            sensor_cfg["expire_after"] = int(drive_expire)
        publish(client, sensor_cfg_topic, sensor_cfg, qos=qos, retain=retain)

        # binary_sensor: overall SMART problem
        bin_uid = _slug(f"{host}_{slug}_smart_problem")
        bin_cfg_topic = f"{ha_prefix}/binary_sensor/{bin_uid}/config"
        bin_cfg = {
            "name": f"{label} SMART problem",
            "unique_id": bin_uid,
            "object_id": bin_uid,
            "state_topic": problem_topic,
            "device_class": "problem",
            "payload_on": "ON",
            "payload_off": "OFF",
            "availability_topic": avail_topic,
            "device": device_obj,
            "icon": "mdi:alert",
        }
        if drive_expire:
            bin_cfg["expire_after"] = int(drive_expire)
        publish(client, bin_cfg_topic, bin_cfg, qos=qos, retain=retain)

    # Host heartbeat binary_sensor
    hb_uid = _slug(f"{host}_heartbeat")
    hb_cfg_topic = f"{ha_prefix}/binary_sensor/{hb_uid}/config"
    hb_cfg = {
        "name": f"{node} heartbeat",
        "unique_id": hb_uid,
        "object_id": hb_uid,
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
        # Order matters. The old fallback called get_attr(s, 9), which returns the
        # PACKED raw on Seagate; get_attr_hours() reads the decoded string instead.
        poh = get(s, ["power_on_time", "hours"], None)
        if poh is None:
            poh = (get(s, ["nvme_smart_health_information_log", "power_on_hours"], 0)
                   or get_attr_hours(s) or 0)

        # Temperature comes from smartctl's own decoded `temperature.current`, which is
        # present on every drive in the fleet and works for both ATA and NVMe. It is NOT
        # read from attribute 190 or 194: those raws pack the current reading together
        # with min/max, e.g. 974782513 rendering as "49 (Min/Max 26/58)".
        #
        # Not scored, only published. A drive that runs warm is not a drive that is
        # failing, and an SSD with no heatsink sitting at 55 C is behaving normally. What
        # matters is whether the controller has ever had to act on temperature, which is
        # what the two NVMe counters below record: seconds spent above the vendor's
        # warning and critical composite thresholds. Those stay at 0 on a drive that has
        # never thermally throttled.
        nlog_t = s.get("nvme_smart_health_information_log") or {}
        m = {
            "power_on_hours": poh,
            "temperature_c": get(s, ["temperature", "current"], None),
            "warning_temp_time_min": nlog_t.get("warning_temp_time"),
            "critical_temp_time_min": nlog_t.get("critical_comp_time"),
            "reallocated": get_attr_count(s, 5, 0),
            "pending": get_attr_count(s, 197, 0),
            "offline_uncorrect": get_attr_count(s, 198, 0),
            "reported_uncorrect": get_attr_count(s, 187, 0),
            "crc_errors": get_attr_count(s, 199, 0),
            "breakdown": bdown,
            "total_penalty": total_penalty(bdown),
        }

        w = s.get("wwn") or {}
        wwn = ""
        try:
            if w:
                wwn = f"{int(w['naa']):x}{int(w['oui']):06x}{int(w['id']):09x}"
        except Exception:
            wwn = ""

        r = {
            "host": host,
            "device": devname,
            "model": model,
            "family": family,
            "serial": serial,
            "wwn": wwn,
            "type": dtype,
            "health_percent": score,
            "metrics": m,
            "smart_overall_passed": bool(get(s, ["smart_status", "passed"], True)),
            "timestamp": int(time.time()),
        }
        r["slug"], r["identity_source"] = drive_identity(r)
        results.append(r)
    return results


MANIFEST_SUFFIX = "known_drives"


def manifest_topic(base_topic, host):
    return f"{base_topic}/{host}/{MANIFEST_SUFFIX}"


def prune_vanished_drives(client, ha_prefix, base_topic, host, results, known_slugs, qos=0):
    """
    Delete the retained topics and discovery configs of drives that are no longer here.

    Nothing did this before. Every topic is published retained, so the broker replays a
    removed drive's last reading forever, and expire_after on its own only
    marks the ENTITY unavailable: the discovery config and the retained payloads stay
    on the broker and the entity stays in Home Assistant.

    What that left behind, measured 2026-09-08:
      - host C: 15 drive slugs for 6 drives, including sdb_<serial>, a drive that is
        no longer in the machine, pinned at health_percent 0.0 with the host
        availability topic reading online. A permanent alarm for a drive that does not
        exist, unclearable from Home Assistant.
      - host A: 17 slugs for 10 drives, 7 of them belonging to other hosts.
      - A discovery config for a serial the host no longer has.

    The set of drives published last run is kept in a retained manifest on the broker
    rather than in a local file, so it survives a host reinstall and still describes
    what the broker is actually holding. An empty retained payload is the MQTT way to
    delete a retained message, and removes the Home Assistant entity with it.
    """
    current = {r.get("slug") for r in results if r.get("slug")}
    stale = sorted(sl for sl in known_slugs if sl and sl not in current)
    for sl in stale:
        for t in drive_config_topics(ha_prefix, host, sl):
            client.publish(t, "", qos=qos, retain=True)
        for t in drive_topics(base_topic, host, sl).values():
            client.publish(t, "", qos=qos, retain=True)
        print(f"pruned vanished drive: {host}/{sl}", file=sys.stderr)
    return stale


def publish_all(client, base_topic, host, qos, retain, results, payload_root):
    for payload in results:
        slug = payload.get("slug") or drive_identity(payload)[0]
        topics = drive_topics(base_topic, host, slug)

        # JSON state
        publish(client, topics["state"], payload, qos=qos, retain=retain)
        # health% numeric
        client.publish(topics["health_percent"], str(payload.get("health_percent", "")),
                       qos=qos, retain=retain)
        # problem flag
        smart_ok = payload.get("smart_overall_passed", True)
        client.publish(topics["problem"], "OFF" if smart_ok else "ON", qos=qos, retain=retain)

    publish(client, f"{base_topic}/{host}/summary", payload_root, qos=qos, retain=retain)

    # Retained manifest of what this host published, so the next run can tell what has
    # gone away. Always retained regardless of --retain: it is bookkeeping, and a
    # manifest that vanishes on reconnect prunes nothing.
    publish(client, manifest_topic(base_topic, host),
            {"host": host, "slugs": sorted(r.get("slug") for r in results if r.get("slug")),
             "updated": int(time.time())},
            qos=qos, retain=True)


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
    ap.add_argument("--ignore-samsung-181", action="store_true",
                    help="Deprecated no-op. Attribute 181 is scored on its normalised value now, "
                         "so the false alarm this suppressed no longer happens. Accepted so "
                         "existing unit files keep working.")
    ap.add_argument("--ignore-nvme-used", action="store_true",
                    help="Damp the NVMe percentage_used endurance penalty to 40%% of its weight. It no longer "
                         "silences endurance entirely: a flag must not assert perfect health over the drive own end-of-life estimate.")
    ap.add_argument("--prune-stale-topics", action="store_true",
                    help="One-time cleanup: also sweep this host's own topic subtree for drive slugs "
                         "left behind by earlier versions (device-letter identities, removed drives) "
                         "and delete their retained topics and discovery configs.")

    args = ap.parse_args()

    settings = {
        "ignore_samsung_181": bool(args.ignore_samsung_181),
        "ignore_nvme_used": bool(args.ignore_nvme_used),
    }
    if args.ignore_samsung_181:
        print("note: --ignore-samsung-181 is a deprecated no-op; attribute 181 is now scored "
              "on its normalised value and no longer needs suppressing", file=sys.stderr)

    host = socket.gethostname()

    # The hostname reaches the topic path twice: once from socket.gethostname() here,
    # and once from whatever the operator typed into --base-topic. Nothing checked that
    # they agreed, and they did not.
    #
    # On 2026-09-08 the broker still held hostA/smart/hostB/* (2 drives) and
    # hostA/smart/hostC/* (5 drives): those two hosts had been deployed by
    # copying host A's systemd unit with --base-topic hostA/smart left unedited,
    # so each published its own drives, correctly named by gethostname(), into
    # host A's tree. That is the cross-host contamination. The retained values in the
    # host C branch (nvme 63.0 and 60.0, from percentage_used 37 and 40) date it to
    # before --ignore-nvme-used was added.
    #
    # A warning rather than a refusal, because a deliberately shared base topic is a
    # legitimate choice and refusing would take drive monitoring down on a host that
    # is otherwise fine. The full path is printed so the mistake is visible the first
    # time the service starts.
    first_seg = args.base_topic.strip("/").split("/")[0]
    if first_seg.lower() != host.lower() and any(
        ch.isalpha() for ch in first_seg
    ) and first_seg.lower() not in ("servers", "server", "smart", "homeassistant", "hosts"):
        print(f"warning: --base-topic starts with {first_seg!r} but this host is {host!r}. "
              f"Drives will publish under {args.base_topic}/{host}/. If {first_seg!r} is another "
              f"host's name, this unit file was copied without editing --base-topic and this "
              f"host's drives will land in that host's topic tree.", file=sys.stderr)
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
            # Give the retained manifest (and, with --prune-stale-topics, the wildcard
            # sweep) a bounded moment to arrive before deciding what has vanished.
            # Without this the first cycle after a start would prune nothing.
            deadline = time.time() + 3.0
            while time.time() < deadline and not (
                isinstance(mqtt_state, dict) and mqtt_state.get("manifest_seen")
            ):
                try:
                    client.loop(timeout=0.2)
                except Exception:
                    break

            if isinstance(mqtt_state, dict):
                known = set(mqtt_state.get("known_slugs") or ())
                if known:
                    try:
                        removed = prune_vanished_drives(
                            client, args.ha_prefix, args.base_topic, host, results,
                            known, qos=args.qos,
                        )
                        for sl in removed:
                            mqtt_state["known_slugs"].discard(sl)
                    except Exception as e:
                        print(f"prune failed: {e}", file=sys.stderr)

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
                    # Two missed polls plus slack, floored at 24h so a drive that stops
                    # reporting is flagged within a day whatever the interval is set to.
                    drive_expire=max(86400, 2 * int(args.interval) + 300),
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
            force_cycle = None
            if isinstance(mqtt_state, dict):
                force_cycle = mqtt_state.get("force_cycle")
            if isinstance(force_cycle, threading.Event) and force_cycle.is_set():
                force_cycle.clear()
                next_scan = 0.0
                next_hb = 0.0
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
