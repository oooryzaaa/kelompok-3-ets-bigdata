import logging
import os
import sys
from dataclasses import dataclass

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp, from_json
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


APP_NAME = "SahamApiStructuredStreaming"
SPARK_35_PREFIX = "3.5."
DEFAULT_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


@dataclass(frozen=True)
class StreamConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_starting_offsets: str
    hdfs_default_fs: str
    output_path: str
    checkpoint_path: str
    kafka_package: str
    trigger_interval: str


def load_config() -> StreamConfig:
    hdfs_default_fs = os.getenv("HDFS_DEFAULT_FS", "hdfs://100.74.49.87:8020")

    return StreamConfig(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "saham-api"),
        kafka_starting_offsets=os.getenv("KAFKA_STARTING_OFFSETS", "latest"),
        hdfs_default_fs=hdfs_default_fs,
        output_path=os.getenv(
            "HDFS_OUTPUT_PATH",
            f"{hdfs_default_fs}/data/saham/hasil",
        ),
        checkpoint_path=os.getenv(
            "SPARK_CHECKPOINT_PATH",
            "/tmp/spark-checkpoint-saham",
        ),
        kafka_package=os.getenv("SPARK_KAFKA_PACKAGE", DEFAULT_KAFKA_PACKAGE),
        trigger_interval=os.getenv("SPARK_TRIGGER_INTERVAL", "30 seconds"),
    )


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def build_spark(config: StreamConfig) -> SparkSession:
    validate_configured_pyspark(config.kafka_package)

    spark = (
        SparkSession.builder.appName(APP_NAME)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", config.kafka_package)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.hadoop.fs.defaultFS", config.hdfs_default_fs)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    validate_spark_runtime(spark, config.kafka_package)
    return spark


def validate_configured_pyspark(kafka_package: str) -> None:
    pyspark_version = pyspark.__version__
    if not pyspark_version.startswith(SPARK_35_PREFIX):
        raise RuntimeError(
            "This job must be submitted with Spark/PySpark 3.5.x. "
            f"Detected PySpark {pyspark_version}. Current Kafka package: {kafka_package}"
        )


def validate_spark_runtime(spark: SparkSession, kafka_package: str) -> None:
    spark_version = spark.version
    if not spark_version.startswith(SPARK_35_PREFIX):
        raise RuntimeError(
            "This job is configured for Spark 3.5.x. "
            f"Detected Spark {spark_version}. Use Spark 3.5.x, or update "
            f"the Kafka connector package accordingly. Current package: {kafka_package}"
        )

    kafka_package_version = kafka_package.rsplit(":", maxsplit=1)[-1]
    if kafka_package_version != spark_version:
        logging.getLogger(APP_NAME).warning(
            "Kafka package version (%s) does not exactly match Spark runtime (%s). "
            "For production, keep org.apache.spark:spark-sql-kafka-0-10_2.12 "
            "on the same Spark patch version.",
            kafka_package_version,
            spark_version,
        )


def saham_schema() -> StructType:
    return StructType(
        [
            StructField("symbol", StringType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
            StructField("time", StringType(), nullable=False),
        ]
    )


def kafka_payload_schema() -> StructType:
    schema = saham_schema()
    schema.add(StructField("ticker", StringType(), nullable=True))
    schema.add(StructField("harga", DoubleType(), nullable=True))
    schema.add(StructField("timestamp", StringType(), nullable=True))
    return schema


def read_kafka_stream(spark: SparkSession, config: StreamConfig) -> DataFrame:
    try:
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", config.kafka_starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )
    except Exception as exc:
        raise RuntimeError(
            "Failed to create Kafka streaming source. Make sure the Spark Kafka "
            f"connector is available: {config.kafka_package}"
        ) from exc


def decode_saham_json(kafka_df: DataFrame) -> DataFrame:
    schema = kafka_payload_schema()
    if not schema.fields:
        raise ValueError(
            "Streaming JSON schema is missing. Spark Structured Streaming cannot "
            "infer schema from Kafka values; define StructType explicitly."
        )

    parsed_df = kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_value"),
    ).withColumn("payload", from_json(col("raw_value"), schema))

    normalized_df = (
        parsed_df.withColumn(
            "symbol",
            coalesce(col("payload.symbol"), col("payload.ticker")),
        )
        .withColumn(
            "price",
            coalesce(col("payload.price"), col("payload.harga")),
        )
        .withColumn(
            "time",
            coalesce(col("payload.time"), col("payload.timestamp")),
        )
    )

    valid_df = normalized_df.where(
        col("payload").isNotNull()
        & col("symbol").isNotNull()
        & col("price").isNotNull()
        & col("time").isNotNull()
    )

    return valid_df.select(
        col("symbol"),
        col("price"),
        col("time"),
        col("kafka_timestamp"),
        current_timestamp().alias("processed_at"),
        col("topic"),
        col("partition"),
        col("offset"),
    )


def write_to_hdfs_parquet(stream_df: DataFrame, config: StreamConfig) -> StreamingQuery:
    return (
        stream_df.writeStream.format("parquet")
        .option("path", config.output_path)
        .option("checkpointLocation", config.checkpoint_path)
        .outputMode("append")
        .trigger(processingTime=config.trigger_interval)
        .start()
    )


def main() -> int:
    configure_logging()
    log = logging.getLogger(APP_NAME)
    config = load_config()

    log.info("Starting %s", APP_NAME)
    log.info("Kafka topic: %s", config.kafka_topic)
    log.info("Kafka bootstrap servers: %s", config.kafka_bootstrap_servers)
    log.info("HDFS output path: %s", config.output_path)
    log.info("Checkpoint path: %s", config.checkpoint_path)

    spark = build_spark(config)

    try:
        kafka_df = read_kafka_stream(spark, config)
        stream_df = decode_saham_json(kafka_df)
        query = write_to_hdfs_parquet(stream_df, config)

        log.info("Streaming query started: %s", query.id)
        query.awaitTermination()
        return 0
    except KeyboardInterrupt:
        log.info("Stopping streaming job")
        return 0
    except Exception:
        log.exception("Streaming job failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
