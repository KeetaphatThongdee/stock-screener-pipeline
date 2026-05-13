# Runbook — Operations & Troubleshooting

Guide สำหรับ operate และ troubleshoot Stock Screener Data Pipeline

---

## Table of Contents

1. [Daily Operations](#daily-operations)
2. [Common Issues & Fixes](#common-issues--fixes)
3. [Health Monitoring](#health-monitoring)
4. [Data Recovery](#data-recovery)
5. [Configuration Changes](#configuration-changes)
6. [Maintenance](#maintenance)

---

## Daily Operations

### Check pipeline status
```bash
# ดู container status
docker compose ps

# ดู logs ล่าสุด
docker logs --tail 100 stock-scraper

# ดู logs แบบ follow (real-time)
docker logs -f stock-scraper
```

### Check Docker healthcheck
```bash
docker inspect --format='{{.State.Health.Status}}' stock-scraper
# Expected: healthy
```

### Check data freshness
```bash
# ดูว่า silver partition ล่าสุดเมื่อไหร่
ls -lt data/silver/dividend/ | head -3
```

### Manual trigger (ไม่ต้องรอ schedule)
```bash
# Restart container → runs both modes immediately on startup
docker compose restart stock-scraper
```

---

## Common Issues & Fixes

### 1. Container keeps restarting

**Symptoms**: `docker compose ps` แสดง status = "Restarting"

**Possible causes**:
- Playwright/Chromium install ไม่ครบ
- Python dependency conflict

**Fix**:
```bash
# ดู error log
docker logs stock-scraper 2>&1 | head -50

# Rebuild image
docker compose build --no-cache stock-scraper
docker compose up -d stock-scraper
```

### 2. All APIs return 403

**Symptoms**: Log แสดง `403 session expired` สำหรับทุก API

**Possible causes**:
- stockanalysis.com เปลี่ยน anti-bot protection
- Chromium version ไม่ตรงกับ Playwright version
- IP ถูก block

**Fix**:
```bash
# ลบ session เก่า ให้ refresh ใหม่
rm data/session/session_cookies.json
docker compose restart stock-scraper
```

ถ้ายังไม่ได้ → ตรวจว่า Playwright version ใน `requirements.txt` ตรงกับ Chromium build ใน Dockerfile

### 3. 429 Too Many Requests (Rate Limited)

**Symptoms**: Log แสดง `429 Too Many Requests` หลาย API

**Possible causes**:
- Fetch เร็วเกินไป
- IP ถูก rate limit

**Fix**:
ลดจำนวน concurrent requests:
```bash
# ใน .env
MAX_CONCURRENT=3  # ลดจาก 5 เป็น 3
```

### 4. Dashboard แสดง "Down" (🔴)

**Symptoms**: Dashboard status badge แสดงสีแดง

**Possible causes**:
- Pipeline ไม่ได้รันนานเกิน 48 ชั่วโมง
- Data volume ไม่ได้ mount

**Fix**:
```bash
# ตรวจว่า pipeline container ทำงานอยู่
docker compose ps stock-scraper

# ตรวจ data volume
ls data/silver/

# Restart pipeline
docker compose restart stock-scraper
```

### 5. Silver layer มี row count drop warning

**Symptoms**: DQ warning `row count dropped X%`

**Possible causes**:
- stockanalysis.com ลบ/เปลี่ยน data จริง (ปกติ)
- API response ถูก truncate (ผิดปกติ)
- API parameters เปลี่ยน

**Fix**:
1. เทียบ row count กับ 2-3 วันก่อนหน้า
2. ถ้าลดเล็กน้อย (<15%) → ปกติ (stock delist, data update)
3. ถ้าลดมาก (>30%) → ตรวจ API URL ว่ายังถูกต้อง

### 6. Gold computed columns เป็น NaN 100%

**Symptoms**: DQ warning `computed column 'X' is 100% NaN`

**Possible causes**:
- Silver layer ขาด column ที่ Gold formula ต้องใช้
- API เปลี่ยน response format

**Fix**:
```bash
# ตรวจ columns ใน silver
python -c "
import pandas as pd
df = pd.read_parquet('data/silver/financials/date=2026-04-23/us.parquet')
print(df.columns.tolist())
"
```
เทียบกับ `EXPECTED_SCHEMA` ใน `main.py`

### 7. Disk space เต็ม

**Symptoms**: Write errors ใน log

**Fix**:
```bash
# ตรวจ disk usage
du -sh data/*

# ลด retention
# ใน .env
RETENTION_DAYS=30  # ลดจาก 90 เป็น 30

# Manual cleanup
docker compose restart stock-scraper  # cleanup runs on startup
```

---

## Health Monitoring

### Docker Healthcheck

Container มี built-in healthcheck ที่ตรวจว่า silver data ยังสด (ไม่เก่ากว่า 25 ชั่วโมง):

```bash
# Check health
docker inspect --format='{{json .State.Health}}' stock-scraper | python -m json.tool
```

### Monitoring Dashboard

```bash
# เปิด dashboard
docker compose up -d dashboard
# เปิด http://localhost:8501
```

Dashboard features:
- **Status badge**: 🟢 Healthy (<25h) / 🟡 Stale (<48h) / 🔴 Down (>48h)
- **KPI cards**: Total rows, categories, partitions, DQ warnings
- **Row Trends**: Time series charts
- **Data Quality**: Null ratios, coverage heatmap
- **Run History**: Recent runs with duration and results

### Webhook Alerts

ตั้ง `ALERT_WEBHOOK_URL` ใน `.env` เพื่อรับ notification:

```bash
# Discord
ALERT_WEBHOOK_URL=https://discord.com/api/webhooks/xxx/yyy

# Slack
ALERT_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
```

---

## Data Recovery

### Re-run สำหรับวันที่ specific

Pipeline ใช้ UTC date เป็น partition key — re-run ในวันเดียวกันจะ dedup อัตโนมัติ (Silver layer เก็บ row ล่าสุดต่อ ticker):

```bash
# Restart container → re-fetch ข้อมูลวันนี้
docker compose restart stock-scraper
```

### Restore จาก Bronze layer

ถ้า Silver/Gold corrupt แต่ Bronze ยังอยู่:
```bash
# ดู Bronze data ที่มี
ls data/bronze/dividend/date=2026-04-23/

# Bronze เก็บ raw JSON — สามารถ re-process ได้โดยลบ silver/gold partition แล้ว re-run
rm -rf data/silver/dividend/date=2026-04-23/
rm -rf data/gold/dividend/date=2026-04-23/
docker compose restart stock-scraper
```

### ลบ data ทั้งหมด (fresh start)

```bash
docker compose down
rm -rf data/bronze data/silver data/gold data/metrics
docker compose up -d
```

---

## Configuration Changes

### เปลี่ยนเวลา schedule

แก้ใน `.env` แล้ว restart:
```bash
RUN_HOUR=9         # เปลี่ยน daily เป็น 09:00
INTRADAY_HOUR_1=5  # เปลี่ยน intraday morning เป็น 05:00
```
```bash
docker compose restart stock-scraper
```

### เพิ่ม API endpoint ใหม่

แก้ใน `main.py`:
1. เพิ่ม dict ใน `DAILY_APIS` หรือ `INTRADAY_APIS`
2. เพิ่ม schema ใน `EXPECTED_SCHEMA`
3. (Optional) เพิ่ม DQ key columns ใน `DQ_KEY_COLUMNS`
4. Rebuild: `docker compose build && docker compose up -d`

### เปลี่ยน DQ thresholds

แก้ใน `main.py`:
```python
ROW_DROP_THRESHOLD = 0.10   # 10% — เพิ่มถ้า noisy data
NULL_THRESHOLD     = 0.50   # 50% — ลดถ้าต้องการ strict กว่า
```

---

## Maintenance

### Update dependencies

```bash
# Update requirements.txt
# แก้ version ใน requirements.txt

# Rebuild
docker compose build --no-cache
docker compose up -d
```

### Update Playwright + Chromium

```bash
# 1. Update playwright version ใน requirements.txt
# 2. Rebuild image (Dockerfile จะ install Chromium ที่ตรงกับ playwright version)
docker compose build --no-cache stock-scraper
```

### Backup data

```bash
# Backup ทั้ง data directory
tar -czf backup_$(date +%Y%m%d).tar.gz data/

# Backup เฉพาะ gold layer (ข้อมูลรวมที่สำคัญที่สุด)
tar -czf gold_backup_$(date +%Y%m%d).tar.gz data/gold/
```

### Check resource usage

```bash
# Memory + CPU usage
docker stats stock-scraper stock-dashboard

# Disk usage per layer
du -sh data/bronze data/silver data/gold data/metrics
```
