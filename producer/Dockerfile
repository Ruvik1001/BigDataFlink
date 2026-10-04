FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY send_to_kafka.py .
CMD ["python", "send_to_kafka.py"]
