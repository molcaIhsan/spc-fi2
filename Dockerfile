# Packages the SPC PyFlink job for submission to an existing Flink cluster.
#
# This does NOT run its own Flink cluster -- it's built on the official Flink
# image (which bundles the JVM + Flink client) so the same image can submit
# the job to your cluster with `flink run -py ...` (see the commands at the
# bottom of this file). If your cluster runs a different Flink version, change
# the FROM tag AND requirements.txt's apache-flink version to match -- PyFlink's
# client version must match the cluster's Flink version, or the job will fail
# to submit.
FROM flink:1.18.1-scala_2.12-java11

# The official Flink image is Debian-based without Python -- add it.
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /opt/spc

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY spc/ ./spc/

# No CMD/ENTRYPOINT: this image is meant to be used as a submission client,
# not run standalone. See the commands below.

# --------------------------------------------------------------------------
# Build:
#   docker build -t spc-detector:latest .
#
# Submit the job to an already-running Flink cluster from this image
# (replace <jobmanager-host:port> with your cluster's JobManager address):
#   docker run --rm \
#     -e KAFKA_BOOTSTRAP_SERVERS=<your-kafka:9092> \
#     -e SOURCE_TOPIC=<your-source-topic> \
#     -e SINK_TOPIC=<your-sink-topic> \
#     spc-detector:latest \
#     flink run -m <jobmanager-host:port> -py /opt/spc/spc/flink_job.py -pyfs /opt/spc
#
# Local smoke test (runs the job against a local mini-cluster inside the
# container -- fine for checking the job starts and doesn't error, not a
# substitute for testing against real Kafka topics):
#   docker run --rm spc-detector:latest \
#     flink run -py /opt/spc/spc/flink_job.py -pyfs /opt/spc
# --------------------------------------------------------------------------
