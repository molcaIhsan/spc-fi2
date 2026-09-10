# Packages the SPC PyFlink job for submission to an existing Flink cluster.
#
# This does NOT run its own Flink cluster -- it's built on the official Flink
# image (which bundles the JVM + Flink client) so the same image can submit
# the job to your cluster with `flink run -py ...` (see the commands at the
# bottom of this file). 1.20.1 confirmed as the real target cluster's version
# (checked directly against ajinomoto-etl-flink's docker/Dockerfile base
# image) -- if your cluster runs something else, change the FROM tag AND
# requirements.txt's apache-flink version to match.
FROM flink:1.20.1-scala_2.12-java11

# The official Flink image is Debian-based without Python -- add it.
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /opt/spc

# JDBC connector + Postgres driver for the spc_readings sink. NOT VERIFIED
# against Maven Central (no network access when this was written) -- confirm
# these artifacts actually resolve before building.
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.20/flink-connector-jdbc-3.2.0-1.20.jar -P /opt/flink/lib/ && \
    wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar -P /opt/flink/lib/ && \
    wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.20/flink-sql-connector-kafka-3.2.0-1.20.jar -P /opt/flink/lib/

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
#     -e SPC_KAFKA_BOOTSTRAP_SERVERS=<broker1:9092,broker2:9092> \
#     -e SPC_PG_HOST=<postgres-host> -e SPC_PG_USERNAME=<user> -e SPC_PG_PASSWORD=<pass> \
#     spc-detector:latest \
#     flink run -m <jobmanager-host:port> -py /opt/spc/spc/flink_job.py -pyfs /opt/spc
#
# Local smoke test (runs the job against a local mini-cluster inside the
# container -- fine for checking the job starts and doesn't error, not a
# substitute for testing against real Kafka/Postgres):
#   docker run --rm spc-detector:latest \
#     flink run -py /opt/spc/spc/flink_job.py -pyfs /opt/spc
# --------------------------------------------------------------------------
