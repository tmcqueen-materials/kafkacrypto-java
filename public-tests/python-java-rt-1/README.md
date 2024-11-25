# Java-Python Interoperability Test
Public test for Message Layer Encryption for Kafka for communication-level interoperability between Java and Python clients.

## Usage

### Test
This test carries out a full round-trip of encrypted data with a live broker. 

  1. Install kafkacrypto-java
  1. Install kafkacrypto (python)
  1. Use `kafkacrypto/tools/simple-provision.py` to generate two configs: java-prodcon (java producer/consumer) and python-prodcon (python producer/consumer).
  1. Adjust `python-prodcon.config` and `java-prodcon.config` to point to live broker.
  1. Optionally also use tools like `kafkacrypto/tools/enable-pq-exchange.py` to restrict which key exchange types are in use.
  1. Make sure broker allows autocreation of topics, or create the topics `openmsitest.python`, `openmsitest.java`, `openmsitest.subs`, `openmsitest.reqs`, `openmsitest.keys`
  1. Move `Tests.java` to `src/main/java/org/kafkacrypto`
  1. `mvn compile assembly:single`
  1. `./roundstrip-java-python-test1.py`
  1. `java -jar ../target/kafkacrypto-java-VERSION-jar-with-dependencies.jar`

