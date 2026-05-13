FROM python:3.11-slim

# ตั้งค่าโซนเวลา
ENV TZ=Asia/Bangkok
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# รวม layer เดียวเพื่อไม่ให้ cache layer ก่อนหน้าบวม และเพิ่ม tzdata เข้าไปแล้ว
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    # Chromium system dependencies
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxfixes3 libxrandr2 libgbm1 libasound2 \
    && rm -rf /var/lib/apt/lists/*

# ลง Python dependencies ก่อน COPY โค้ด
# Docker จะ cache layer นี้ไว้ถ้า requirements.txt ไม่เปลี่ยน -> build ครั้งถัดไปเร็วขึ้น
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pin Chromium build 1200 (v143.0.7499.4) ให้ตรงกับ playwright==1.57.0
# ถ้า upgrade playwright ใน requirements.txt ให้อัพ build number ด้วย:
#   playwright install --dry-run chromium → ดู build number ที่ต้องการ
RUN playwright install chromium --with-deps=false

# --- Security Hardening ---
# สร้าง non-root user สำหรับรัน application
# UID 1000 เป็น convention สำหรับ first non-root user
RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid 1000 --shell /bin/bash --create-home appuser \
    && mkdir -p /data/session /data/metrics/runs /data/metrics/changes \
    && chown -R appuser:appuser /data \
    && chown -R appuser:appuser /root/.cache/ms-playwright 2>/dev/null || true

# copy โค้ด (แยก layer ไว้ท้ายสุด เพราะเปลี่ยนบ่อยที่สุด)
COPY --chown=appuser:appuser . .

# สลับไปใช้ non-root user — ป้องกันไม่ให้ process ภายใน container
# มีสิทธิ์ root ซึ่งลดความเสี่ยงหาก container ถูก compromise
USER appuser

# HEALTHCHECK — ตรวจว่า silver partition ล่าสุดยังสดอยู่ (ไม่เกิน 25 ชม.)
# แยกเป็น healthcheck.py เพื่อให้อ่านง่ายและ maintain ได้
HEALTHCHECK --start-period=5m --interval=30m --timeout=10s --retries=2 \
    CMD ["python", "healthcheck.py"]

CMD ["python", "main.py"]