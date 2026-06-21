"""Create the `image-events` topic + DLQ (idempotent). Run once on setup:

    uv run python -m hybridsearch.events.admin

Topic is partitioned (= broker count) and keyed by image_id, so an image's
1st/2nd events stay ordered on one partition while load spreads across brokers.
"""
from __future__ import annotations

import logging

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from .. import config

log = logging.getLogger("hybridsearch.events.admin")


def ensure_topics(replication_factor: int = 2) -> None:
    if not config.KAFKA_BOOTSTRAP:
        raise RuntimeError("HS_KAFKA_BOOTSTRAP is not set")
    admin = KafkaAdminClient(
        bootstrap_servers=config.KAFKA_BOOTSTRAP.split(","),
        security_protocol=config.KAFKA_SECURITY_PROTOCOL,
    )
    topics = [
        NewTopic(
            config.KAFKA_TOPIC,
            num_partitions=config.KAFKA_TOPIC_PARTITIONS,
            replication_factor=replication_factor,
        ),
        # DLQ: single partition is plenty; ordering there doesn't matter.
        NewTopic(config.KAFKA_DLQ_TOPIC, num_partitions=1, replication_factor=replication_factor),
    ]
    try:
        for t in topics:
            try:
                admin.create_topics([t])
                log.info("created topic %s (partitions=%d, rf=%d)", t.name, t.num_partitions, replication_factor)
            except TopicAlreadyExistsError:
                log.info("topic %s already exists", t.name)
    finally:
        admin.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ensure_topics()


if __name__ == "__main__":
    main()
