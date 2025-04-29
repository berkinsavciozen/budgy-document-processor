FROM python:3.9-slim

WORKDIR /app

# 1) System dependencies for PDF rendering, Magic, and Tesseract OCR
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-tur \
    libmagic1 \
    poppler-utils \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

# 2) Python environment settings
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 3) Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4) Copy the application code
COPY . .

# 5) Expose port & start
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "75"]
