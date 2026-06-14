"""
Kafka Producer Module for Thunders BigData System.

Provides a reliable Kafka producer with serialization, batching,
delivery reports, and graceful shutdown support.
"""

import json
import logging
from typing import Any, Callable, Dict, List, Optional, Union

from confluent_kafka import KafkaException, Producer

logger = logging.getLogger(__name__)


class DeliveryReport:
    """Encapsulates a Kafka message delivery report.

    Attributes:
        topic: The topic the message was sent to.
        partition: The partition the message was assigned to.
        offset: The offset of the message within the partition.
        error: Error information if delivery failed, else None.
    """

    def __init__(self, topic: str, partition: int, offset: int, error: Optional[str] = None) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.error = error

    @property
    def success(self) -> bool:
        """Return True if the message was delivered successfully."""
        return self.error is None

    def __repr__(self) -> str:
        status = "SUCCESS" if self.success else f"FAILED ({self.error})"
        return f"DeliveryReport(topic={self.topic}, partition={self.partition}, offset={self.offset}, status={status})"


class KafkaProducer:
    """High-level Kafka producer with serialization and delivery tracking.

    This class wraps the confluent-kafka Producer to provide a simplified
    interface for publishing messages with automatic serialization, retry
    logic, and delivery confirmation.

    Attributes:
        bootstrap_servers: Comma-separated list of Kafka broker addresses.
        producer: Underlying confluent_kafka Producer instance.
        pending_reports: List of delivery reports for sent messages.
    """

    DEFAULT_CONFIG = {
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 100,
        "linger.ms": 10,
        "batch.size": 16384,
        "compression.type": "snappy",
        "max.in.flight.requests.per.connection": 5,
        "enable.idempotence": True,
    }

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the KafkaProducer.

        Args:
            bootstrap_servers: Kafka broker addresses (e.g., 'host1:9092,host2:9092').
            config: Additional Kafka producer configuration overrides.
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[Producer] = None
        self.pending_reports: List[DeliveryReport] = []

        self._config = {**self.DEFAULT_CONFIG, **(config or {})}
        self._config["bootstrap.servers"] = self.bootstrap_servers

    def connect(self) -> None:
        """Establish a connection to the Kafka cluster.

        Creates and configures the underlying confluent_kafka Producer instance.

        Raises:
            KafkaException: If the connection cannot be established.
        """
        try:
            self.producer = Producer(self._config)
            logger.info("Kafka producer connected to %s", self.bootstrap_servers)
        except KafkaException as exc:
            logger.error("Failed to connect Kafka producer: %s", exc)
            raise

    def _delivery_callback(self, err, msg) -> None:
        """Handle delivery reports from Kafka.

        Args:
            err: Error object if delivery failed, else None.
            msg: The Kafka Message object.
        """
        report = DeliveryReport(
            topic=msg.topic(),
            partition=msg.partition() if not err else -1,
            offset=msg.offset() if not err else -1,
            error=str(err) if err else None,
        )
        self.pending_reports.append(report)

        if err:
            logger.error(
                "Message delivery failed for topic %s: %s",
                msg.topic(),
                err,
            )
        else:
            logger.debug(
                "Message delivered to %s [%d] at offset %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def send(
        self,
        topic: str,
        value: Any,
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[List[tuple]] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        value_serializer: Callable[[Any], bytes] = lambda v: json.dumps(v).encode("utf-8"),
        key_serializer: Callable[[Union[str, bytes]], bytes] = lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    ) -> None:
        """Send a message to a Kafka topic.

        Args:
            topic: Target topic name.
            value: Message payload (will be serialized).
            key: Optional message key for partition routing.
            headers: Optional list of (key, value) header tuples.
            partition: Target partition (None for automatic).
            timestamp: Optional message timestamp in milliseconds.
            value_serializer: Function to serialize the message value.
            key_serializer: Function to serialize the message key.

        Raises:
            RuntimeError: If the producer is not connected.
            KafkaException: If the message cannot be enqueued.
            BufferError: If the producer queue is full.
        """
        if self.producer is None:
            raise RuntimeError("Producer not connected. Call connect() first.")

        serialized_value = value_serializer(value)
        serialized_key = key_serializer(key) if key is not None else None

        try:
            self.producer.produce(
                topic=topic,
                value=serialized_value,
                key=serialized_key,
                headers=headers,
                partition=partition,
                timestamp=timestamp,
                on_delivery=self._delivery_callback,
            )
            logger.debug("Message enqueued for topic %s", topic)
        except BufferError:
            logger.warning("Producer queue full; flushing before retry for topic %s", topic)
            self.flush()
            self.producer.produce(
                topic=topic,
                value=serialized_value,
                key=serialized_key,
                headers=headers,
                partition=partition,
                timestamp=timestamp,
                on_delivery=self._delivery_callback,
            )
        except KafkaException as exc:
            logger.error("Failed to send message to topic %s: %s", topic, exc)
            raise

    def send_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
        value_serializer: Callable[[Any], bytes] = lambda v: json.dumps(v).encode("utf-8"),
    ) -> int:
        """Send a batch of messages to a Kafka topic.

        Args:
            topic: Target topic name.
            messages: List of dicts, each containing 'value' and optionally 'key', 'headers'.
            value_serializer: Function to serialize message values.

        Returns:
            Number of messages successfully enqueued.
        """
        if self.producer is None:
            raise RuntimeError("Producer not connected. Call connect() first.")

        count = 0
        for msg in messages:
            try:
                self.send(
                    topic=topic,
                    value=msg.get("value"),
                    key=msg.get("key"),
                    headers=msg.get("headers"),
                    value_serializer=value_serializer,
                )
                count += 1
            except (KafkaException, BufferError) as exc:
                logger.error("Failed to send batch message: %s", exc)

        logger.info("Enqueued %d/%d messages for topic %s", count, len(messages), topic)
        return count

    def flush(self, timeout: Optional[float] = None) -> int:
        """Flush all pending messages and wait for delivery confirmations.

        Args:
            timeout: Maximum time in seconds to wait. None for infinite.

        Returns:
            Number of messages still pending after flush.
        """
        if self.producer is None:
            raise RuntimeError("Producer not connected. Call connect() first.")

        remaining = self.producer.flush(timeout=timeout if timeout else 30)
        if remaining > 0:
            logger.warning("Flush incomplete: %d messages still pending", remaining)
        else:
            logger.debug("All messages flushed successfully")
        return remaining

    def get_delivery_reports(self, clear: bool = True) -> List[DeliveryReport]:
        """Retrieve accumulated delivery reports.

        Args:
            clear: If True, clear the reports list after retrieval.

        Returns:
            List of DeliveryReport instances.
        """
        reports = list(self.pending_reports)
        if clear:
            self.pending_reports.clear()
        return reports

    def close(self, flush_timeout: float = 30.0) -> None:
        """Gracefully close the Kafka producer.

        Flushes any pending messages before closing.

        Args:
            flush_timeout: Maximum time in seconds to wait for pending messages.
        """
        if self.producer is not None:
            remaining = self.flush(timeout=flush_timeout)
            if remaining > 0:
                logger.warning(
                    "Closing producer with %d undelivered messages",
                    remaining,
                )
            self.producer = None
            logger.info("Kafka producer closed")

    def __enter__(self) -> "KafkaProducer":
        """Context manager entry: connect and return self."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit: close the producer."""
        self.close()

    def __repr__(self) -> str:
        return f"KafkaProducer(bootstrap_servers='{self.bootstrap_servers}')"
