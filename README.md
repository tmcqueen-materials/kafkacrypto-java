# kafkacrypto-java
Message Layer Encryption for Kafka

This is a Java implementation of the encryption layer kafkacrypto.

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
KafkaConsumer<byte[],byte[]> kcc = new KafkaConsumer<byte[],byte[]>(...your server params in normal form...);
KafkaProducer<byte[],byte[]> kcp = new KafkaProducer<byte[],byte[]>(...your server params in normal form...);
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

This code is available on [Maven Central](https://mvnrepository.com/artifact/org.kafkacrypto/kafkacrypto-java) for use in projects. You can include in `pom.xml`

```xml
    <dependency>
      <groupId>org.kafkacrypto</groupId>
      <artifactId>kafkacrypto-java</artifactId>
      <version>0.9.9.3</version> <!-- change to latest version for best performance -->
    </dependency>
```

To automatically include kafkacrypto-java in your build processes.

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
```
The convenience method `.getMessage()` can be used instead to return the message as bytes if successfully decrypted, or to raise a `KafkaCryptoMessageException` if decryption failed.

## Universal Configuration File
kafkacrypto separates the storage of cryptographic secrets and non-secret configuration information:
  1. `my-node-ID.config`: Non-secret parameters, in Python ConfigParser / Windows ini format.
  1. `my-node-ID.seed`: Next ratchet seed, when using default implementation of Ratchet. Key secret, should never be saved or transmitted plaintext.
  1. `my-node-ID.crypto`: Identification private key, when using default implementation of Cryptokey. Key secret, should never be saved or transmitted plaintext.

Alternative implementations of Ratchet and Cryptokey enable secrets to be managed by specialized hardware (e.g. HSMs).

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

## Security Contact

Potential security issues can be reported to <securty@kafkacrypto.org>. Encryption is not required, but if you want to encrypt the email you can use the following PGP key.
```
-----BEGIN PGP PUBLIC KEY BLOCK-----

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
NZ1gD4wDwSGlNMe+vuxj
=kElQ
-----END PGP PUBLIC KEY BLOCK-----
```

