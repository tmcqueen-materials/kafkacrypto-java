# kafkacrypto-java
End-to-End (E2EE) Message Layer Encryption for Kafka

This is a Java implementation of the encryption layer [kafkacrypto](https://github.com/tmcqueen-materials/kafkacrypto).

## Quick Start
Utilize the kafkacrypto-java library during your build process and package as part of your releases.

In your producer/consumer code:
```java
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.kafkacrypto.KafkaCrypto;
import org.kafkacrypto.KafkaCryptoMessage;
import org.kafkacrypto.exceptions.KafkaCryptoException;

String nodeId = "my-node-ID";

// setup separate consumer/producers for the crypto key passing messages. DO NOT use these for
// other messages.
KafkaConsumer<byte[],byte[]> kcc = new KafkaConsumer<byte[],byte[]>(...your server params and bytearray deserializers in normal form...);
KafkaProducer<byte[],byte[]> kcp = new KafkaProducer<byte[],byte[]>(...your server params and bytearray serializers in normal form...);
try {
  KafkaCrypto kc = new KafkaCrypto(nodeId,kcp,kcc,null);
} catch (KafkaCryptoException kce) {
  // handle
}

... Your code here ...

// Here is how you configure your producer/consumer objects to use the crypto (de)serializers
KafkaProducer<byte[],byte[]>  producer = new KafkaProducer<byte[],byte[]>(...,kc.getKeySerializer(), kc.getValueSerializer());
KafkaConsumer<KafkaCryptoMessage,KafkaCryptoMessage> consumer = new KafkaConsumer<KafkaCryptoMessage,KafkaCryptoMessage>(...,kc.getKeyDeserializer(), kc.getValueDeserializer());

... Your code here ...
```

And that's it! Your producers and consumers should function as normal, but all traffic within Kafka is encrypted. 

To generate the necessary cryptographic configuration files, you need to generate them first. There are many ways to accomplish this, for example through the use of [simple-provision.py](https://github.com/tmcqueen-materials/kafkacrypto/blob/master/tools/simple-provision.py).

If automatic topic creation is disabled, then one more action is needed. For each "root topic" you must create the requisite key-passing topics. By default these are `root.reqs`, `root.subs` and `root.keys`, where root is replaced with the root topic name. It is safe to enable regular log compaction on these topics.

This implementation is not idiomatic Java, rather a nearly direct mapping from the python implementation, to ease code maintenance. See the python code documentation for details of the implementation:

Available on PyPI at https://pypi.org/project/kafkacrypto/  
Available on Github at https://github.com/tmcqueen-materials/kafkacrypto

## Maven Central

This code is available on [Maven Central](https://search.maven.org/artifact/org.kafkacrypto/kafkacrypto-java) for use in projects. You can include in `pom.xml`

```xml
    <dependency>
      <groupId>org.kafkacrypto</groupId>
      <artifactId>kafkacrypto-java</artifactId>
      <version>0.9.11.1</version> <!-- change to latest version for best performance -->
    </dependency>
```

To automatically include kafkacrypto-java in your build processes. Version numbers are kept in synch with the [python](https://github.com/tmcqueen-materials/kafkacrypto) implementation. The latest kafkacrypo-java version is 0.9.11.1.

## Undecryptable Messages
kafkacrypto is designed so that messages being sent can **always** be encrypted once a KafkaCrypto object is successfully created. However, it is possible for a consumer to receive a message for which it does not have a decryption key, i.e. an undecryptable message. This is most often because the asynchronous key exchange process has not completed before the message is received, or because the consumer is not authorized to receive on that topic. 

To handle this scenario, all deserialized messages are returned as [KafkaCryptoMessage](https://github.com/tmcqueen-materials/kafkacrypto-java/blob/master/src/main/java/org/kafkacrypto/KafkaCryptoMessage.java) objects. The `.isCleartext()` method can be used to determine whether the message component was successfully decrypted or not:
```java
// consumer is setup with KafkaCrypto deserializers as shown above
// 'key' refers to the key of key->value pairs from Kafka, not a cryptographic key
for (ConsumerRecord<KafkaCryptoMessage,KafkaCryptoMessage> msg : consumer) {
  if (msg.key().isCleartext()) {
    // message key was decrypted. msg.key().getMessage() returns a byte[] of the cleartext
  } else {
    // message key was not decrypted. msg.key().toWire() is the raw (undecrypted) message key
    // It can be discarded, or saved and decryption attempted at a later time
  }
  if (msg.value().isCleartext()) {
    // message value was decrypted. msg.value().getMessage() returns a byte[] of the cleartext
  } else {
    // message value was not decrypted. msg.value().toWire() is the raw (undecrypted) message value
    // It can be discarded, or saved and decryption attempted at a later time
  }
  // Note that for single messages due to how per-message encryption keys are derived, either both 
  // key and value will be successfully decrypted, or neither will.
```
The convenience method `.getMessage()` can be used instead to return the message as bytes if successfully decrypted, or to raise a `KafkaCryptoMessageException` if decryption failed.

## Universal Configuration File
kafkacrypto separates the storage of cryptographic secrets and non-secret configuration information:
  1. `my-node-ID.config`: Non-secret parameters, in Python ConfigParser / Windows ini format.
  1. `my-node-ID.seed`: Next ratchet seed, when using default implementation of Ratchet. Key secret, should never be saved or transmitted plaintext.
  1. `my-node-ID.crypto`: Identification private key, when using default implementation of Cryptokey. Key secret, should never be saved or transmitted plaintext.

Alternative implementations of Ratchet and Cryptokey enable secrets to be managed by specialized hardware (e.g. TPMs/HSMs).

It is also possible to use `my-node-ID.config` to manage all configuration directives, including those that control Kafka, using the load_value/store_value directives. A sample implementation is:
```java
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.kafkacrypto.KafkaCrypto;
import org.kafkacrypto.KafkaCryptoMessage;
import org.kafkacrypto.KafkaCryptoStore;
import org.kafkacrypto.exceptions.KafkaCryptoException;

... in main ...
// May throw KafkaCryptoException, handle accordingly.
KafkaCryptoStore kcs = new KafkaCryptoStore("nodeID.config", "nodeID");

// Setup KafkaCrypto
KafkaConsumer<byte[],byte[]> kcc = new KafkaConsumer<byte[],byte[]>(kcs.get_kafka_config("consumer","crypto"));
KafkaProducer<byte[],byte[]> kcp = new KafkaProducer<byte[],byte[]>(kcs.get_kafka_config("producer","crypto"));
KafkaCrypto kc = new KafkaCrypto("nodeID",kcp,kcc,kcs);

// read program specific values
// may throw KafkaCryptoException, handle accordingly.
TYPE value1 = kcs.load_value("value1", "", default1);
TYPE value2 = kcs.load_value("value2", "", default2);

// Setup Kafka Consumer and Producer
KafkaConsumer<KafkaCryptoMessage,KafkaCryptoMessage> consumer = new KafkaConsumer<KafkaCryptoMessage,KafkaCryptoMessage>(kcs.get_kafka_config("consumer"), kc.getKeyDeserializer(), kc.getValueDeserializer());
KafkaProducer<byte[],byte[]> producer = new KafkaProducer<byte[],byte[]>(kcs.get_kafka_config("producer"), kc.getKeySerializer(), kc.getValueSerializer());

... your code here ...

// Save new values
kcs.store_value("value1", "", "value-of-value1");
kcs.store_value("value2", "", "value-of-value2");
```
## Post Quantum Secure Cryptography
Support exists as of v0.9.11.0, with the same structure and caveats as for the python version of [kafkacrypto](https://github.com/tmcqueen-materials/kafkacrypto). Additionally,
the java version requires availability of the [liboqs-java](https://github.com/open-quantum-safe/liboqs-java) dependency. This has
some cross-platform challenges. It can be installed using Maven by doing:
1. `git clone https://github.com/open-quantum-safe/liboqs-java.git`
2. `cd liboqs-java`
3. Make sure JAVA_HOME is set.
4. `mvn compile`
5. `mvn package`
6. There will be a file `./target/liboqs-java.jar` that should be added to the java classpath.
7. For packaging as a dependency as part of a larger project, it can be installed in your local maven repository with: `mvn install:install-file -Dfile=./target/liboqs-java.jar -DgroupId=org.openquantumsafe -DartifactId=liboqs-java -Dversion=1.0 -Dpackaging=jar`
