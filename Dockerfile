FROM python:3.11-slim

WORKDIR /app

# System deps (optional but helpful for pandas)
RUN apt-get update && apt-get install -y --no-install-recommends     build-essential curl &&     rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Expose and run
EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
