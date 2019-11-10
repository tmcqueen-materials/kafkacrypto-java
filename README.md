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

String nodeId = 'my-node-ID';

# setup separate consumer/producers for the crypto key passing messages. DO NOT use these for
# other messages.
KafkaConsumer<byte[],byte[]> kcc = new KafkaConsumer(...your server params in normal form...)
KafkaProducer<byte[],byte[]> kcp = new KafkaProducer(...your server params in normal form...)
KafkaCrypto kc = new KafkaCrypto(nodeId,kcp,kcc,null)

... Your code here ...

# Here is how you configure your producer/consumer objects to use the crypto (de)serializers
KafkaProducer<byte[],byte[]>  producer = KafkaProducer(...,kc.getKeySerializer(), kc.getValueSerializer())
KafkaConsumer<KafkaCryptoMessage,KafkaCryptoMessage> consumer = KafkaConsumer(...,kc.getKeyDeserializer(), kc.getValueDeserializer())

... Your code here ...
```

And that's it! Your producers and consumers should function as normal, but all traffic within Kafka is encrypted. 

If automatic topic creation is disabled, then one more action is needed. For each "root topic" you must create the requisite key-passing topics. By default these are `root.reqs`, `root.subs` and `root.keys`, where root is replaced with the root topic name. It is safe to enable regular log compaction on these topics.

This implementation is not idiomatic Java, rather a nearly direct mapping from the python implementation, to ease code maintenance. See the python code documentation for details of the implementation:

Available on PyPI at https://pypi.org/project/kafkacrypto/  
Available on Github at https://github.com/tmcqueen-materials/kafkacrypto

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
