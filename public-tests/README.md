# Public Tests
Public tests for Message Layer Encryption for Kafka (Java)

## Usage

### Tests
This test carries out a full round-trip of encrypted data with a live broker. It execises many
components of KafakCrypto, including configuration file handling, open and close operations, etc. Relies on
group timeout not being "too long", since confluent-kafka-python does not gracefully support close operations
on all platforms.

  1. Install kafkacrypto-java
  1. Use `kafkacrypto/tools/simple-provision.py` to generate two configs: node1 (producer) and node2 (consumer).
  1. Adjust `node1.config` and `node2.config` to point to live broker.
  1. Make sure broker allows autocreation of topics, or create the topics `openmsitest.0`, `openmsitest.subs`, `openmsitest.reqs`, `openmsitest.keys`
  1. Move `Tests.java` to `src/main/java/org/kafkacrypto`
  1. `mvn compile assembly:single`
  1. `java -jar ../target/kafkacrypto-java-VERSION-jar-with-dependencies.jar`
