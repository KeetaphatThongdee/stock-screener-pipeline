"""Healthcheck script for Docker — checks if silver data is fresh.

Exits 0 if the most recent silver partition file was written within
MAX_AGE_HOURS (default 25h). Exits 1 otherwise.

Used by Dockerfile HEALTHCHECK to detect stale pipelines.
"""
import os
import sys
import time

silver = "/data/silver"
max_age_hours = 25
now = time.time()
freshest = 0

if os.path.isdir(silver):
    for cat in os.listdir(silver):
        cat_dir = os.path.join(silver, cat)
        if not os.path.isdir(cat_dir):
            continue
        # เช็คเฉพาะ partition ล่าสุด (sorted descending, take first)
        for part in sorted(os.listdir(cat_dir), reverse=True)[:1]:
            part_dir = os.path.join(cat_dir, part)
            if not os.path.isdir(part_dir):
                continue
            for f in os.listdir(part_dir):
                mtime = os.path.getmtime(os.path.join(part_dir, f))
                freshest = max(freshest, mtime)

age_hours = (now - freshest) / 3600 if freshest else 999
sys.exit(0 if age_hours < max_age_hours else 1)
