FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# Mount the data volume when running in docker-compose
CMD ["python", "app.py"]
