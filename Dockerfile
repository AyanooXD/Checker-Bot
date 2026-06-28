FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cache bust: 2026-06-28-10:02
CMD ["python3", "-u", "bot.py"]

