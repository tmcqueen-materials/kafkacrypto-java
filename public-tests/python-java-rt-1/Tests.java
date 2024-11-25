package org.kafkacrypto;

import java.lang.Thread;

import java.util.ArrayList;
import java.util.List;

import org.kafkacrypto.Utils;
import org.kafkacrypto.CryptoStore;
import org.kafkacrypto.KafkaCryptoStore;
import org.kafkacrypto.KafkaCrypto;
import org.kafkacrypto.KafkaCryptoMessage;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.exceptions.KafkaCryptoMessageException;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Tests
{
    public static void main( String[] args )
    {
      try {
          KafkaCryptoStore hpfz = new KafkaCryptoStore("java-prodcon.config");
          KafkaConsumer kcc = new KafkaConsumer(hpfz.get_kafka_config("consumer", "crypto"));
          KafkaProducer kcp = new KafkaProducer(hpfz.get_kafka_config("producer", "crypto"));
          KafkaCrypto kc = new KafkaCrypto(null,kcp,kcc,hpfz);
          KafkaConsumer consumer = new KafkaConsumer(hpfz.get_kafka_config("consumer"), kc.getKeyDeserializer(), kc.getValueDeserializer());
          List<String> topics = new ArrayList();
          topics.add("openmsitest.python");
          consumer.subscribe(topics);
          int num_received = 0;
          while (num_received < 1) {
            ConsumerRecords<KafkaCryptoMessage,KafkaCryptoMessage> msgs = consumer.poll(500);
            for (ConsumerRecord<KafkaCryptoMessage,KafkaCryptoMessage> msg : msgs) {
              while (num_received == 0)
                try {
                  System.out.println("Got message: " + new String(msg.value().getMessage()));
                  num_received++;
                } catch (KafkaCryptoMessageException kcme) {
                  System.out.println("Error with message: " + kcme);
                  Thread.sleep(1000);
                }
            }
          }
          consumer.close();

          KafkaProducer producer = new KafkaProducer(hpfz.get_kafka_config("producer"), kc.getKeySerializer(), kc.getValueSerializer());
          producer.send(new ProducerRecord<byte[],byte[]>("openmsitest.java", "key".getBytes(), "Hello World from Java!".getBytes()));
          producer.flush();
          Thread.sleep(10000); // wait for consumer and us to exchange keys.
          producer.close();

          kc.close();

        } catch (Exception e) {
          e.printStackTrace();
        }
    }
}

