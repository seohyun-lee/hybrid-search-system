"""Event-driven indexing: envelope schema, Kafka producer/consumer, DLQ.

Kept import-light: importing this package does NOT import kafka. The producer /
admin modules import it lazily so the schema can be used (and unit-tested)
without a broker.
"""
