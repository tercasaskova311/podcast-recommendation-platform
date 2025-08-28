# Dockerfile
FROM python:3.11
WORKDIR /app

# system deps (optional but handy for pandas/pyarrow)
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

EXPOSE 8504
CMD ["streamlit", "run", "dashboard.py", "--server.port=8504", "--server.address=0.0.0.0"]
