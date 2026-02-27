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

# ติด Chromium เท่านั้น (ไม่ใช่ --with-deps เพราะลงไปแล้วข้างบน)
RUN playwright install chromium

# copy โค้ด (แยก layer ไว้ท้ายสุด เพราะเปลี่ยนบ่อยที่สุด)
COPY . .

CMD ["python", "main.py"]