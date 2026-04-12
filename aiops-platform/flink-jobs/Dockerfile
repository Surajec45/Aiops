FROM flink:1.18-scala_2.12-java11

# ── Install Python 3.11 + pip + build tools ──────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3.11 \
        python3.11-dev \
        python3-pip \
        curl \
        gcc \
        g++ \
        build-essential \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && ln -sf /usr/bin/python3.11 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# ── PyFlink + job dependencies ────────────────────────────────────────────────
COPY flink-jobs/requirements.txt /opt/flink/jobs/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/jobs/requirements.txt

# ── Kafka connector JAR (must match Flink version exactly) ───────────────────
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
    -o /opt/flink/lib/flink-sql-connector-kafka.jar

# ── Copy job files ────────────────────────────────────────────────────────────
COPY flink-jobs/ /opt/flink/jobs/

# ── Flink config: Python executable + checkpointing ──────────────────────────
# Flink 1.18 uses flink-conf.yaml (config.yaml is 1.19+)
RUN { \
        echo "python.executable: python3"; \
        echo "state.checkpoints.dir: file:///tmp/flink-checkpoints"; \
        echo "execution.checkpointing.interval: 30000"; \
        echo "execution.checkpointing.mode: EXACTLY_ONCE"; \
    } >> /opt/flink/conf/flink-conf.yaml
