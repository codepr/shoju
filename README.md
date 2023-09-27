Shoju
=====

A simplified implementation of a Kafka like library, a lightweight append-only commit log system
that behaves similarly to Kafka, in a pull-oriented mechanism based on offsets to (re)play a stream of events.

- CRC32 of the payloads
- Iterator, batch size to read efficiently
- Compaction
