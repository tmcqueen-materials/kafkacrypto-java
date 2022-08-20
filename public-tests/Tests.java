package org.kafkacrypto;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.kafkacrypto.KafkaCryptoStore;
import org.kafkacrypto.KafkaCrypto;
import org.kafkacrypto.exceptions.KafkaCryptoException;
import org.kafkacrypto.exceptions.KafkaCryptoMessageException;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

    class DeadHandler implements Thread.UncaughtExceptionHandler
    {
        public void uncaughtException(Thread t, Throwable e)
        {
            e.printStackTrace();
            System.err.println("Thread died! Exiting.");
            System.exit(1);
        }
    }

    class ProdThread implements Runnable
    {
        private Thread _thrd;
        private byte[] sendval;

        public ProdThread(byte[] uval)
        {
          this.sendval = uval;
          this._thrd = new Thread(this, "producer thread");
          this._thrd.setDaemon(true);
          this._thrd.setDefaultUncaughtExceptionHandler(new DeadHandler());
          this._thrd.start();
        }

        public void run()
        {
          try {
            KafkaCryptoStore kcs = new KafkaCryptoStore("node1.config");
            KafkaConsumer kcc = new KafkaConsumer(kcs.get_kafka_config("consumer", "crypto"));
            KafkaProducer kcp = new KafkaProducer(kcs.get_kafka_config("producer", "crypto"));
            KafkaCrypto kc = new KafkaCrypto(null,kcp,kcc,kcs);
            KafkaProducer producer = new KafkaProducer(kcs.get_kafka_config("producer"), kc.getKeySerializer(), kc.getValueSerializer());
            Thread.sleep(5000);
            producer.send(new ProducerRecord("openmsitest.0",this.sendval));
            producer.flush();
            while (true)
              Thread.sleep(100);
          } catch (Throwable e) {
            e.printStackTrace();
            System.err.println("Producer thread died! Exiting.");
            System.exit(1);
          }
        }
    }

    class ConsumeThread implements Runnable
    {
        private Thread _thrd;
        private boolean _running;
        private byte[] recvval;

        public ConsumeThread(byte[] uval)
        {
          this.recvval = uval;
          this._running = true;
          this._thrd = new Thread(this, "consumer thread");
          this._thrd.setDefaultUncaughtExceptionHandler(new DeadHandler());
          this._thrd.start();
        }

        public void run()
        {
          try {
            KafkaCryptoStore kcs = new KafkaCryptoStore("node2.config");
            KafkaConsumer kcc = new KafkaConsumer(kcs.get_kafka_config("consumer", "crypto"));
            KafkaProducer kcp = new KafkaProducer(kcs.get_kafka_config("producer", "crypto"));
            KafkaCrypto kc = new KafkaCrypto(null,kcp,kcc,kcs);
            KafkaConsumer consumer = new KafkaConsumer(kcs.get_kafka_config("consumer"), kc.getKeyDeserializer(), kc.getValueDeserializer());
            List<String> topics = new ArrayList();
            topics.add("openmsitest.0");
            consumer.subscribe(topics);
            Thread.sleep(5000);
            while (this._running) {
              ConsumerRecords<KafkaCryptoMessage,KafkaCryptoMessage> msgs = consumer.poll(500);
              for (ConsumerRecord<KafkaCryptoMessage,KafkaCryptoMessage> msg : msgs) {
                byte[] decmsg = null;
                try {
                  decmsg = msg.value().getMessage();
                } catch (KafkaCryptoMessageException kcme) {
                  System.out.println("Got undecryptable message.");
                }
/*                while (decmsg == null)
                  try {
                    decmsg = msg.value().getMessage();
                  } catch (KafkaCryptoMessageException kcme) {
                    System.out.println("Waiting for decryption...");
                    Thread.sleep(1000);
                  } */
                if (decmsg != null && Arrays.equals(decmsg, this.recvval)) {
                  System.out.println("Success!");
                  this._running = false;
                }
              }
            }
          } catch (Throwable e) {
            e.printStackTrace();
            System.err.println("Consumer thread died! Exiting.");
            System.exit(1);
          }
        }
    }


public class Tests
{
    public static void main( String[] args )
    {
        /* the random message */
        byte[] rnd = jasodium.randombytes(64);
        ProdThread prod = new ProdThread(rnd);
        ConsumeThread con = new ConsumeThread(rnd);
        try {
          synchronized(con) {
            con.wait(65);
          }
        } catch (Throwable e) {
            e.printStackTrace();
            System.err.println("Main thread died! Exiting.");
            System.exit(1);
        }
    }
}

