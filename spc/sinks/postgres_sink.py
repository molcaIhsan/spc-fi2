"""
Postgres JDBC sink builder.

Uses PyFlink's flink-connector-jdbc (Java connector, driver-agnostic) plus the
Postgres JDBC driver jar -- see Dockerfile for where those are downloaded.
This has NOT been run against a real cluster yet -- verify against the actual
PyFlink JDBC connector docs for whatever Flink version the cluster runs.
"""

from pyflink.datastream.connectors.jdbc import (
    JdbcSink,
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)
from pyflink.common.typeinfo import Types

from ..config import PostgresConfig


class PostgresSinkBuilder:
    def __init__(self, config: PostgresConfig):
        self.config = config

    def build_sink(self, data_stream, insert_sql: str, field_types: list):
        """
        insert_sql: a plain INSERT ... VALUES (?, ?, ...) statement, schema-qualified.
        field_types: PyFlink Types.* list matching the tuple shape of data_stream's
                     elements, in the same order as insert_sql's columns.
        """
        return data_stream.add_sink(
            JdbcSink.sink(
                insert_sql,
                type_info=Types.ROW(field_types),
                jdbc_connection_options=(
                    JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .with_url(self.config.jdbc_url)
                    .with_driver_name("org.postgresql.Driver")
                    .with_user_name(self.config.username)
                    .with_password(self.config.password)
                    .build()
                ),
                jdbc_execution_options=(
                    JdbcExecutionOptions.builder()
                    .with_batch_size(200)
                    .with_batch_interval_ms(1000)
                    .with_max_retries(3)
                    .build()
                ),
            )
        )
