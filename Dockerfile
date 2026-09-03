FROM python:3.14

# With kafka-python-ng (new-gen), it supports python 3.12+

# Installing JDK for PySpark.
# procps necessary for PySpark monitoring. rm-rf is in best practices in the Docker Docs.
RUN apt-get update && apt-get install -y \
    default-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /CS4010

# --no-cache-dir is also part of best practice (Docker Docs). Reduces image size. 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Step 5: Copy application files
COPY . .

# Ensures Python outputs output instantly to the terminal
ENV PYTHONUNBUFFERED=1