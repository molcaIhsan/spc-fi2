"""
PyFlink streaming job: Kafka (raw weight readings) -> SPC sustained-breach
detector -> Kafka (per-item state + alert events).

REQUIRES VERIFICATION AGAINST YOUR ACTUAL FLINK CLUSTER VERSION before deploying
-- PyFlink's client version must match the cluster's Flink version, and
state-descriptor/connector builder APIs have moved across minor versions.
Check `pyflink.__version__` against your cluster before running.

Fill in KAFKA_BOOTSTRAP_SERVERS / SOURCE_TOPIC / SINK_TOPIC below (or set them
as environment variables -- see the os.environ.get() defaults).
"""

import json
import os

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.common import Types, Time
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

from .config import Config
from .preprocessing import MultiPackNormalizer
from .detector import DriftDetector

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "CHANGE_ME:9092")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "CHANGE_ME_raw_weight_readings")
SINK_TOPIC = os.environ.get("SINK_TOPIC", "CHANGE_ME_spc_drift_events")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "spc-detector")

_TTL_CONFIG = StateTtlConfig.new_builder(Time.hours(Config.STATE_TTL_HOURS)).build()


class MultiPackNormalizerFunction(KeyedProcessFunction):
    """Keyed by run_id. Wraps preprocessing.MultiPackNormalizer, unmodified logic."""

    def open(self, runtime_context):
        desc = ValueStateDescriptor("normalizer", Types.PICKLED_BYTE_ARRAY())
        desc.enable_time_to_live(_TTL_CONFIG)
        self.state = runtime_context.get_state(desc)

    def process_element(self, value, ctx):
        record = json.loads(value)
        normalizer = self.state.value() or MultiPackNormalizer()

        result = normalizer.update(float(record["weight"]))
        self.state.update(normalizer)

        if result is None:
            return  # first reading of the run, or an invalid/discarded delta
        unit_weight, pack_count = result

        out = dict(record)
        out["unit_weight"] = unit_weight
        out["pack_count"] = pack_count
        yield json.dumps(out)


class DriftDetectorFunction(KeyedProcessFunction):
    """Keyed by run_id. Wraps detector.DriftDetector, unmodified logic."""

    def open(self, runtime_context):
        detector_desc = ValueStateDescriptor("detector", Types.PICKLED_BYTE_ARRAY())
        detector_desc.enable_time_to_live(_TTL_CONFIG)
        self.detector_state = runtime_context.get_state(detector_desc)

        prev_desc = ValueStateDescriptor("prev_sustained", Types.BOOLEAN())
        prev_desc.enable_time_to_live(_TTL_CONFIG)
        self.prev_sustained_state = runtime_context.get_state(prev_desc)

    def process_element(self, value, ctx):
        record = json.loads(value)
        detector = self.detector_state.value() or DriftDetector()

        result = detector.update(record["unit_weight"])
        self.detector_state.update(detector)

        prev_sustained = bool(self.prev_sustained_state.value())
        is_onset_event = bool(result["sustained_drift"]) and not prev_sustained
        self.prev_sustained_state.update(bool(result["sustained_drift"]))

        out = {
            "run_id": record.get("run_id"),
            "hwcode": record.get("hwcode"),
            "sku": record.get("sku"),
            "timestamp": record.get("timestamp"),
            "unit_weight": record["unit_weight"],
            "pack_count": record.get("pack_count"),
            "ewma": result["ewma"],
            "kalman": result["kalman"],
            "is_breach": result["is_breach"],
            "sustained_drift": result["sustained_drift"],
            "is_onset_event": is_onset_event,  # the actual alarm event -- False->True transition only
            "k_threshold": Config.SUSTAINED_THRESHOLD,
            "n_window": Config.SUSTAINED_WINDOW,
        }
        yield json.dumps(out)


def build_job() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaSink.record_serializer_builder()
            .set_topic(SINK_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    raw_stream = env.from_source(source, watermark_strategy=None, source_name="raw-weight-readings")

    normalized = raw_stream.key_by(lambda s: json.loads(s)["run_id"]).process(
        MultiPackNormalizerFunction(), output_type=Types.STRING()
    )

    detected = normalized.key_by(lambda s: json.loads(s)["run_id"]).process(
        DriftDetectorFunction(), output_type=Types.STRING()
    )

    detected.sink_to(sink)
    return env


if __name__ == "__main__":
    build_job().execute("spc-sustained-breach-detector")
