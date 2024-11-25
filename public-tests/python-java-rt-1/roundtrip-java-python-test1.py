#!/usr/bin/env python3

from threading import Thread
from kafkacrypto import KafkaCryptoStore, KafkaCrypto, KafkaConsumer, KafkaProducer
from time import sleep,time
from pysodium import randombytes

class ProdThread(object):
  def __init__(self,uval,kcs,joinon=None):
    self._uval = uval
    self._joinon = joinon
    self.kcs = kcs
    self._my_thread = Thread(target=self._my_thread_proc)
    self._my_thread.start()

  def _my_thread_proc(self):
    # Setup KafkaCrypto
    self.kcc = KafkaConsumer(**self.kcs.get_kafka_config('consumer',extra='crypto'))
    self.kcp = KafkaProducer(**self.kcs.get_kafka_config('producer',extra='crypto'))
    self.kc = KafkaCrypto(None,self.kcp,self.kcc,config=self.kcs)
    kafka_config = self.kcs.get_kafka_config('producer')
    kafka_config['key_serializer'] = self.kc.getKeySerializer()
    kafka_config['value_serializer'] = self.kc.getValueSerializer()
    self.producer = KafkaProducer(**kafka_config)
    sleep(5)
    self.producer.send("openmsitest.python",value=self._uval)
    self.producer.poll(1)
    sleep(5)
    self.producer.poll(1)
    if not (self._joinon is None):
      self._joinon.join()
    self.producer.close()
    self.kcc.close()
    self.kcp.close()
    self.kc.close()

class ConsumeThread(object):
  def __init__(self,uval,kcs):
    self._uval = uval
    self.kcs = kcs
    self._my_thread = Thread(target=self._my_thread_proc)
    self._my_thread.start()

  def _my_thread_proc(self):
    # Setup KafkaCrypto
    self.kcc = KafkaConsumer(**self.kcs.get_kafka_config('consumer',extra='crypto'))
    self.kcp = KafkaProducer(**self.kcs.get_kafka_config('producer',extra='crypto'))
    self.kc = KafkaCrypto(None,self.kcp,self.kcc,config=self.kcs)
    kafka_config = self.kcs.get_kafka_config('consumer')
    kafka_config['key_deserializer'] = self.kc.getKeyDeserializer()
    kafka_config['value_deserializer'] = self.kc.getValueDeserializer()
    self.consumer = KafkaConsumer(**kafka_config)
    self.consumer.subscribe(["openmsitest.java"])
    end_time = time()+600
    rcvd = False
    while not rcvd and time() < end_time:
      rv = self.consumer.poll(timeout_ms=1000)
      for tp,msgs in rv.items():
        for msg in msgs:
          print("Message:",bytes(msg.value),flush=True)
          if bytes(msg.value) == self._uval:
            print("Message Successfully Received!")
            rcvd = True
    self.consumer.close()
    self.kcc.close()
    self.kcp.close()
    self.kc.close()

kcs = KafkaCryptoStore("python-prodcon.config")
uval = b'Hello World from Java!'
t2 = ConsumeThread(uval,kcs)
uval = b'Hello World from Python!'
t1 = ProdThread(uval,t2.kcs,t2._my_thread)
t2._my_thread.join(60+5)
kcs.close()
