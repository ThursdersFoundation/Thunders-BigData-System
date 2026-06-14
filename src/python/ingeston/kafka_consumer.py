"""
Kafka Consumer Module for Thunders BigData System.

Provides a robust Kafka consumer with connection management,
topic subscription, message consumption, and graceful shutdown.
"""

import json
import logging
from typing import Any, Callable, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """High-level Kafka consumer with automatic reconnection and error handling.

    This class wraps the confluent-kafka Consumer to provide a simplified
    interface for subscribing to topics, consuming messages, and managing
    the consumer lifecycle.

    Attributes:
        bootstrap_servers: Comma-separated list of Kafka broker addresses.
        group_id: Consumer group identifier for coordinated consumption.
        consumer: Underlying confluent_kafka Consumer instance.
        subscribed_topics: List of currently subscribed topics.
    """

    DEFAULT_CONFIG = {
        "session.timeout.ms": 30000,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "max.poll.interval.ms": 300000,
        "fetch.min.bytes": 1,
        "fetch.max.wait.ms": 500,
    }

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "thunders-consumer-group",
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the KafkaConsumer.

        Args:
            bootstrap_servers: Kafka broker addresses (e.g., 'host1:9092,host2:9092').
            group_id: Consumer group ID for partition assignment.
            config: Additional Kafka consumer configuration overrides.
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.subscribed_topics: List[str] = []
        self.consumer: Optional[Consumer] = None

        self._config = {**self.DEFAULT_CONFIG, **(config or {})}
        self._config.update({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
        })

    def connect(self) -> None:
        """Establish a connection to the Kafka cluster.

        Creates and configures the underlying confluent_kafka Consumer instance.

        Raises:
            KafkaException: If the connection cannot be established.
        """
        try:
            self.consumer = Consumer(self._config)
            logger.info(
                "Kafka consumer connected to %s (group: %s)",
                self.bootstrap_servers,
                self.group_id,
            )
        except KafkaException as exc:
            logger.error("Failed to connect Kafka consumer: %s", exc)
            raise

    def subscribe(
        self,
        topics: List[str],
        on_assign: Optional[Callable] = None,
        on_revoke: Optional[Callable] = None,
    ) -> None:
        """Subscribe to one or more Kafka topics.

        Args:
            topics: List of topic names to subscribe to.
            on_assign: Callback invoked when partitions are assigned.
            on_revoke: Callback invoked when partitions are revoked.

        Raises:
            RuntimeError: If the consumer is not connected.
            KafkaException: If subscription fails.
        """
        if self.consumer is None:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        try:
            self.consumer.subscribe(
                topics,
                on_assign=on_assign,
                on_revoke=on_revoke,
            )
            self.subscribed_topics = topics
            logger.info("Subscribed to topics: %s", topics)
        except KafkaException as exc:
            logger.error("Failed to subscribe to topics %s: %s", topics, exc)
            raise

    def consume(
        self,
        timeout: float = 1.0,
        max_messages: Optional[int] = None,
        value_deserializer: Callable[[bytes], Any] = json.loads,
    ) -> List[Dict[str, Any]]:
        """Consume messages from subscribed topics.

        Polls for messages until timeout is reached or max_messages is hit.
        Each message is deserialized and enriched with metadata.

        Args:
            timeout: Maximum time (in seconds) to wait for a message per poll.
            max_messages: Maximum number of messages to return. None for unlimited.
            value_deserializer: Function to deserialize message values.

        Returns:
            List of consumed messages as dictionaries with keys:
                - topic, partition, offset, key, value, timestamp

        Raises:
            RuntimeError: If the consumer is not connected.
        """
        if self.consumer is None:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        messages: List[Dict[str, Any]] = []

        while True:
            if max_messages is not None and len(messages) >= max_messages:
                break

            msg: Optional[Message] = self.consumer.poll(timeout)

            if msg is None:
                break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        "Reached end of partition %s [%d] at offset %d",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )
                    continue
                else:
                    logger.error("Kafka consumer error: %s", msg.error())
                    continue

            try:
                value = value_deserializer(msg.value()) if msg.value() else None
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning("Failed to deserialize message at offset %d: %s", msg.offset(), exc)
                value = msg.value()

            message_data = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": value,
                "timestamp": msg.timestamp()[1] if msg.timestamp()[0] != 0 else None,
            }
            messages.append(message_data)

        logger.debug("Consumed %d messages", len(messages))
        return messages

    def consume_stream(
        self,
        timeout: float = 1.0,
        value_deserializer: Callable[[bytes], Any] = json.loads,
    ):
        """Generator that yields messages continuously from subscribed topics.

        Args:
            timeout: Maximum time (in seconds) to wait per poll cycle.
            value_deserializer: Function to deserialize message values.

        Yields:
            Dictionary containing message data and metadata.

        Raises:
            RuntimeError: If the consumer is not connected.
        """
        if self.consumer is None:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        while True:
            msg: Optional[Message] = self.consumer.poll(timeout)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka consumer error: %s", msg.error())
                continue

            try:
                value = value_deserializer(msg.value()) if msg.value() else None
            except (json.JSONDecodeError, TypeError):
                value = msg.value()

            yield {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": value,
                "timestamp": msg.timestamp()[1] if msg.timestamp()[0] != 0 else None,
            }

    def commit(self, asynchronous: bool = True) -> None:
        """Commit the current offsets for all assigned partitions.

        Args:
            asynchronous: If True, commit asynchronously; otherwise, block until committed.
        """
        if self.consumer is None:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        self.consumer.commit(asynchronous=asynchronous)
        logger.debug("Offsets committed (async=%s)", asynchronous)

    def close(self) -> None:
        """Gracefully close the Kafka consumer.

        Unsubscribes from topics and releases all associated resources.
        """
        if self.consumer is not None:
            self.consumer.unsubscribe()
            self.consumer.close()
            logger.info(
                "Kafka consumer closed (group: %s, topics: %s)",
                self.group_id,
                self.subscribed_topics,
            )
            self.consumer = None
            self.subscribed_topics = []

    def __enter__(self) -> "KafkaConsumer":
        """Context manager entry: connect and return self."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit: close the consumer."""
        self.close()

    def __repr__(self) -> str:
        return (
            f"KafkaConsumer(bootstrap_servers='{self.bootstrap_servers}', "
            f"group_id='{self.group_id}', "
            f"topics={self.subscribed_topics})"
        )
