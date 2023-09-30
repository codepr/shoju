Shoju
=====

A simplified implementation of a Kafka like library, a lightweight append-only commit log system
that behaves similarly to Kafka, in a pull-oriented mechanism based on offsets  and keys for ordering to (re)play a stream of events.

### On the radar

Non exhaustive and definitely underloved roadmap.

- CRC32 of the payloads
- Reduce insane IO, write in batches
- Iterator, batch size to read efficiently
- Log compaction
- Multiple partitions per topic
- Retention
