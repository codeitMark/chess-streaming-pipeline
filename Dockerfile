FROM python:3.14

# JDK for PySpark and procps for Spark process monitoring
RUN apt-get update && apt-get install -y \
    default-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Stockfish, so the analyzer can also be run directly in this container
RUN apt-get update && apt-get install -y \
    stockfish \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /CS4010

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY . .

# Unbuffered output so logs appear immediately
ENV PYTHONUNBUFFERED=1