# ==============================================================================
# Custom Container Image for Google Cloud Serverless for Apache Spark (4.x)
#
# Build & Push:
#   docker build -t "${REGION}-docker.pkg.dev/${PROJECT_ID}/spark-containers/pipeline:latest" .
#   docker push "${REGION}-docker.pkg.dev/${PROJECT_ID}/spark-containers/pipeline:latest"
# ==============================================================================
FROM debian:12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

# 1. Install minimal OS packages and Python 3 runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      procps \
      python3 \
      python3-pip \
      python3-venv \
      tini && \
    rm -rf /var/lib/apt/lists/*

# 2. Create the hardened non-root 'spark' user (UID/GID 1099 required by Serverless Spark)
RUN groupadd -g 1099 spark && \
    useradd -u 1099 -g 1099 -m -s /bin/bash spark && \
    mkdir -p /app && \
    chown -R 1099:1099 /app

WORKDIR /app

# 3. Install Python dependencies from requirements.txt
COPY --chown=1099:1099 requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /app/requirements.txt

# 4. Copy application source code
COPY --chown=1099:1099 src/ /app/src/

# 5. Drop privileges to non-root spark user
USER 1099:1099
ENTRYPOINT ["/usr/bin/tini", "--"]
