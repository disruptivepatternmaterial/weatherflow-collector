# Use an official Python runtime as a parent image
FROM python:3.12.3-slim

# Set the working directory
WORKDIR /app/weatherflow-collector

# Copy the requirements file from the root of the build context
COPY requirements.txt ./

# Install PostgreSQL client libs needed by psycopg (binary wheels)
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and install required packages in one layer
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ .

# Run your Python script
CMD ["python3", "./weatherflow-collector.py"]
