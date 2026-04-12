#!/bin/bash
# Waits for the Flink JobManager REST API and Kafka to be ready,
# then submits all PyFlink jobs in dependency order.

# Do NOT use set -e — a single failed submission should not abort the rest.

JOBMANAGER_URL="${JOBMANAGER_URL:-http://flink-jobmanager:8081}"
JOBMANAGER_ADDRESS="${JOBMANAGER_ADDRESS:-flink-jobmanager:8081}"
JOBS_DIR="/opt/flink/jobs"

echo "Waiting for Flink JobManager at ${JOBMANAGER_URL}..."
until curl -sf "${JOBMANAGER_URL}/overview" > /dev/null 2>&1; do
    echo "  JobManager not ready yet, retrying in 5s..."
    sleep 5
done
echo "Flink JobManager is ready."

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
echo "Waiting for Kafka at ${KAFKA_BOOTSTRAP}..."
until python3 -c "
from kafka import KafkaAdminClient
try:
    c = KafkaAdminClient(bootstrap_servers='${KAFKA_BOOTSTRAP}', request_timeout_ms=5000)
    c.close()
    exit(0)
except Exception:
    exit(1)
" 2>/dev/null; do
    echo "  Kafka not ready yet, retrying in 5s..."
    sleep 5
done
echo "Kafka is ready."

# Give the taskmanager a moment to register with the jobmanager
sleep 5

submit_job() {
    local job_file="$1"
    local job_name="$2"
    echo ""
    echo "Submitting: ${job_name} (${job_file})"
    flink run \
        --jobmanagerAddress "${JOBMANAGER_ADDRESS}" \
        --python "${JOBS_DIR}/${job_file}" \
        --detached
    local exit_code=$?
    if [ $exit_code -eq 0 ]; then
        echo "  ✓ ${job_name} submitted successfully"
    else
        echo "  ✗ ${job_name} failed to submit (exit code ${exit_code}) — check Flink UI at ${JOBMANAGER_URL}"
    fi
    sleep 2
}

# Submit detectors first, correlator last (it reads from raw-signals which detectors produce)
submit_job "metrics_anomaly.py"       "Metrics Anomaly Detection"
submit_job "trace_error_detection.py" "Trace Error Rate Detection"
submit_job "log_pattern_analysis.py"  "Log Pattern Analysis"
submit_job "dependency_discovery.py"  "Dependency Discovery"
submit_job "signal_correlator.py"     "Signal Correlator"

echo ""
echo "Job submission complete. Check job status at ${JOBMANAGER_URL}"
