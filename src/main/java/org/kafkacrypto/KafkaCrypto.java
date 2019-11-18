package org.kafkacrypto;

import org.kafkacrypto.Ratchet;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.ChainCert;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.EncryptionKey;
import org.kafkacrypto.msgs.EncryptionKeys;
import org.kafkacrypto.msgs.CryptoState;
import org.kafkacrypto.msgs.SignedChain;
import org.kafkacrypto.msgs.KafkaPlainWireMessage;
import org.kafkacrypto.msgs.KafkaCryptoWireMessage;
import org.kafkacrypto.exceptions.KafkaCryptoException;
import org.kafkacrypto.exceptions.KafkaCryptoInternalError;
import org.kafkacrypto.msgs.ByteString;
import org.kafkacrypto.KafkaCryptoMessage;
import org.kafkacrypto.KafkaCryptoMessageDecryptor;

import org.msgpack.value.Value;
import org.msgpack.value.Variable;
import org.msgpack.core.MessageTypeCastException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import java.time.Duration;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

public class KafkaCrypto extends KafkaCryptoBase implements Runnable
{
  private class DeadHandler implements Thread.UncaughtExceptionHandler
  {
    public void uncaughtException(Thread t, Throwable e)
    {
      e.printStackTrace();
      System.err.println("Management thread died! Exiting.");
      // TODO: call kafkaconsumer/producer close?
      System.exit(1);
    }
  }

  private Ratchet _seed;
  private double _last_time;
  private Map<TopicPartition,OffsetAndMetadata> _tps;
  private boolean _tps_updated;

  private Map<String,EncryptionKey> _cur_pgens;
  private EncryptionKeys _pgens;
  private boolean _pgens_updated;
  private EncryptionKeys _cgens;

  private Thread _mgmt_thread;
  private boolean _running;

  public KafkaCrypto(String nodeID, KafkaProducer<byte[],byte[]> kp, KafkaConsumer<byte[],byte[]> kc, Object config) throws KafkaCryptoException, IOException
  {
    this(nodeID, kp, kc, config, null, null);
  }
  public KafkaCrypto(String nodeID, KafkaProducer<byte[],byte[]> kp, KafkaConsumer<byte[],byte[]> kc, Object config, Object cryptokey) throws KafkaCryptoException, IOException
  {
    this(nodeID, kp, kc, config, cryptokey, null);
  }
  public KafkaCrypto(String nodeID, KafkaProducer<byte[],byte[]> kp, KafkaConsumer<byte[],byte[]> kc, Object config, Object cryptokey, Object seed) throws KafkaCryptoException, IOException
  {
    super(nodeID, kp, kc, config, cryptokey);

    if (seed == null) {
      seed = (String)this._cryptostore.load_value("ratchet", null, (String)null);
      if (seed != null && ((String)seed).startsWith("file#")) {
        seed = ((String)seed).substring(5);
      } else {
        seed = this._nodeID + ".seed";
        this._cryptostore.store_value("ratchet", null, "file#" + (String)seed);
      }
    }
    if (Ratchet.class.isAssignableFrom(seed.getClass())) {
      this._seed = (Ratchet)seed;
    } else {
      this._seed = new Ratchet((String)seed);
    }

    this._last_time = Utils.currentTime();

    this._tps = new HashMap<TopicPartition,OffsetAndMetadata>();
    this._tps.put(new TopicPartition(this._config.getProperty("MGMT_TOPIC_CHAINS"),0), new OffsetAndMetadata(0));
    this._tps.put(new TopicPartition(this._config.getProperty("MGMT_TOPIC_ALLOWLIST"),0), new OffsetAndMetadata(0));
    this._tps.put(new TopicPartition(this._config.getProperty("MGMT_TOPIC_DENYLIST"),0), new OffsetAndMetadata(0));
    this._tps_updated = true;

    ByteString oldkeybytestring = this._cryptostore.load_opaque_value("oldkeys", "crypto", null);
    byte[] oldkeybytes = (oldkeybytestring != null)?(oldkeybytestring.getBytes()):(null);
    CryptoState cs = new CryptoState().unpackb(oldkeybytes);
    if (cs.containsKey("pgens")) {
      this._pgens = cs.get("pgens");
    } else {
      this._pgens = new EncryptionKeys();
    }
    this._pgens_updated = true;
    this._cur_pgens = new HashMap<String,EncryptionKey>();

    this._cgens = new EncryptionKeys();

    this._running = true;
    this._mgmt_thread = new Thread(this, "process_mgmt_messages");
    this._mgmt_thread.setDaemon(true);
    this._mgmt_thread.setDefaultUncaughtExceptionHandler(new DeadHandler());
    this._mgmt_thread.start();
  }

  public void close()
  {
    this._running = false;
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {
      throw new KafkaCryptoInternalError("Interrupted during close.", ie);
    }
  }

  public void run()
  {
    ConsumerRecords<byte[],byte[]> msgs;
    Duration mgmt_poll_interval = Duration.ofMillis(Long.parseLong(this._config.getProperty("MGMT_POLL_INTERVAL")));
    while (this._running) {
     this._logger.debug("At beginning of management loop");
     try {
      if (this._kc.assignment().size() > 0) {
        this._logger.debug("Polling");
        msgs = this._kc.poll(mgmt_poll_interval);
      } else {
        this._logger.debug("No assignments, so no poll");
        msgs = ConsumerRecords.empty();
        Thread.sleep(mgmt_poll_interval.toMillis());
      }
      this._logger.debug("Got {} messages", msgs.count());
      for (ConsumerRecord<byte[],byte[]> msg : msgs) {
        this._lock.lock();
        try {
          TopicPartition tp = new TopicPartition(msg.topic(),msg.partition());
          if (!this._tps.containsKey(tp) || this._tps.get(tp).offset() <= msg.offset()) {
            this._tps.put(tp, new OffsetAndMetadata(msg.offset()+1));
            if (msg.topic().endsWith(this._config.getProperty("TOPIC_SUFFIX_REQS"))) {
              String root = msg.topic().substring(0,msg.topic().lastIndexOf(this._config.getProperty("TOPIC_SUFFIX_REQS")));
              List<Value> kreq = msgpack.unpackb(msg.key());
              if (this._pgens.containsKey(root)) {
                List<byte[]> sendki = new ArrayList<byte[]>();
                List<byte[]> sendk = new ArrayList<byte[]>();
                for (Value v : kreq) {
                  byte[] ki = v.asRawValue().asByteArray();
                  if (this._pgens.get(root).containsKey(ki)) {
                    sendki.add(ki);
                    sendk.add(this._pgens.get(root).get(ki).key);
                  }
                }
                if (sendki.size() > 0) {
                  byte[] ski = msgpack.packb(sendki), sk = msgpack.packb(sendk);
                  this._kp.send(new ProducerRecord<byte[],byte[]>(root + this._config.getProperty("TOPIC_SUFFIX_KEYS"), ski, this._cryptoexchange.encrypt_keys(sendki,sendk,root,msg.value())));
                }
              }
            } else if (msg.topic().endsWith(this._config.getProperty("TOPIC_SUFFIX_KEYS"))) {
              String root = msg.topic().substring(0,msg.topic().lastIndexOf(this._config.getProperty("TOPIC_SUFFIX_KEYS")));
              this._cgens.ensureContains(root);
              Map<byte[],byte[]> newkeys = this._cryptoexchange.decrypt_keys(root,msg.value());
              for (byte[] ki : newkeys.keySet()) {
                if (!this._cgens.get(root).containsKey(ki))
                  this._cgens.get(root).put(ki, new EncryptionKey(root, ki, newkeys.get(ki)));
                else
                  this._cgens.get(root).get(ki).setKey(newkeys.get(ki));
              }
            } else if (msg.topic().equals(this._config.getProperty("MGMT_TOPIC_CHAINS"))) {
              if (this._cryptokey.get_spk().equals(msg.key())) {
                SignedChain sc = this._cryptoexchange.replace_spk_chain(new SignedChain().unpackb(msg.value()));
                if (sc != null)
                  this._cryptostore.store_value("chain","crypto",msgpack.packb(sc));
              }
            } else if (msg.topic().equals(this._config.getProperty("MGMT_TOPIC_ALLOWLIST"))) {
              ChainCert cc = this._cryptoexchange.add_allowlist(new SignedChain().unpackb(msg.value()));
              if (cc != null)
                this._cryptostore.store_value(jasodium.crypto_hash_sha256(cc.pk),"allowlist",msgpack.packb(cc));
            } else if (msg.topic().equals(this._config.getProperty("MGMT_TOPIC_DENYLIST"))) {
              ChainCert cc = this._cryptoexchange.add_denylist(new SignedChain().unpackb(msg.value()));
              if (cc != null)
                this._cryptostore.store_value(jasodium.crypto_hash_sha256(cc.pk),"denylist",msgpack.packb(cc));
            } else {
              this._logger.warn("Help! I'm lost. Unknown message received on topic={}", msg.topic());
            }
          }
        } catch (Throwable e) {
          this._logger.info("Exception during message processin", e);
        } finally {
          this._lock.unlock();
        }
      }

      this._logger.debug("Flushing producer");
      this._kp.flush();

      this._logger.debug("Updating assignment");
      this._lock.lock();
      try {
        if (this._tps_updated) {
          this._kc.assign(this._tps.keySet());
          this._tps_updated = false;
        }
      } finally {
        this._lock.unlock();
      }

      this._logger.debug("(Re)subscribing and updating assignments accordingly");
      this._lock.lock();
      try {
        for (String root : this._cgens.keySet()) {
          String subtopic = root + this._config.getProperty("TOPIC_SUFFIX_KEYS");
          TopicPartition tp = new TopicPartition(subtopic,0);
          if (!this._tps.containsKey(tp)) {
            this._tps.put(tp,new OffsetAndMetadata(0));
            this._tps_updated = true;
          }
          List<byte[]> needed = new ArrayList<byte[]>();
          for (byte[] keyidx : this._cgens.get(root).keySet())
            try {
              if (this._cgens.get(root).get(keyidx).resubNeeded(Double.parseDouble(this._config.getProperty("CRYPTO_SUB_INTERVAL"))))
                needed.add(this._cgens.get(root).get(keyidx).keyIndex);
            } catch (Throwable e) {
              this._logger.warn("Exception determining new subscriptions", e);
            }
          if (needed.size() > 0) {
            byte[] needmsg = msgpack.packb(needed);
            byte[] needrequest = this._cryptoexchange.signed_epk(root,null);
            if (needmsg != null && needrequest != null) {
              this._kp.send(new ProducerRecord<byte[],byte[]>(root + this._config.getProperty("TOPIC_SUFFIX_SUBS"), needmsg, needrequest));
              if (!this._cryptoexchange.valid_spk_chain()) {
                // If using default/temp ROT, send directly as well
                this._kp.send(new ProducerRecord<byte[],byte[]>(root + this._config.getProperty("TOPIC_SUFFIX_REQS"), needmsg, needrequest));
              }
              for (byte[] ki : needed)
                this._cgens.get(root).get(ki).resub(Utils.currentTime());
            }
          }
        }
      } catch (Throwable e) {
        this._logger.info("Exception during preparing new subscriptions", e);
      } finally {
        this._lock.unlock();
      }

      this._logger.debug("Flushing producer(2)");
      this._kp.flush();

      this._logger.debug("Checking ratchet");
      this._lock.lock();
      try {
        if (this._last_time + Double.parseDouble(this._config.getProperty("CRYPTO_RATCHET_INTERVAL")) < Utils.currentTime()) {
          this._seed.increment();
          this._last_time = Utils.currentTime();
          this._cur_pgens.clear();
        }
      } catch (Throwable e) {
        this._logger.warn("Exception incrementing ratchet", e);
      } finally {
        this._lock.unlock();
      }

      this._logger.debug("Updating assignments and clearing old keys");
      this._lock.lock();
      try {
        if (this._pgens_updated) {
          for (String root : this._pgens.keySet()) {
            String subtopic = root + this._config.getProperty("TOPIC_SUFFIX_REQS");
            TopicPartition tp = new TopicPartition(subtopic,0);
            if (!this._tps.containsKey(tp)) {
              this._tps.put(tp, new OffsetAndMetadata(0));
              this._tps_updated = true;
            }
            List<byte[]> torem = new ArrayList<byte[]>();
            for (byte[] ki : this._pgens.get(root).keySet())
              if (this._pgens.get(root).get(ki).birth+Double.parseDouble(this._config.getProperty("CRYPTO_MAX_PGEN_AGE"))<Utils.currentTime())
                torem.add(ki);
            for (byte[] ki : torem)
                this._pgens.get(root).remove(ki);
          }
          CryptoState oldkeys = new CryptoState();
          oldkeys.put("pgens", this._pgens);
          this._cryptostore.store_opaque_value("oldkeys",msgpack.packb(oldkeys),"crypto");
          this._pgens_updated = false;
        }
      } catch (Throwable e) {
        this._logger.warn("Exception writing oldkeys", e);
      } finally {
        this._lock.unlock();
      }
     } catch (InterruptedException | InterruptException ie) {
      this.close();
     }
    }
    this._kc.close();
    this._kp.close();
  }

  private class ByteSerializer implements Serializer<byte[]>
  {
    private KafkaCrypto _parent;
    private boolean _key;
    public ByteSerializer(KafkaCrypto parent, boolean key)
    {
      this._parent = parent;
      this._key = key;
    }
    public void close()
    {
      this._parent = null;
    }
    public void configure(Map<String,?> configs, boolean isKey)
    {
      this._key = isKey;
    }
    public byte[] serialize(String topic, byte[] data)
    {
      String root = this._parent.get_root(topic);
      byte[] rv = null;
      this._parent._lock.lock();
      try {
        EncryptionKey ek;
        if (!this._parent._cur_pgens.containsKey(root)) {
          ek = this._parent._seed.get_key_value_generators(root, this._parent._cryptokey.get_spk());
          this._parent._cur_pgens.put(root, ek);
          this._parent._pgens.ensureContains(root);
          this._parent._pgens.get(root).put(ek.keyIndex, ek);
        } else
          ek = this._parent._cur_pgens.get(root);
        KeyGenerator gen = (this._key)?(ek.keygen):(ek.valgen);
        KafkaCryptoWireMessage msg = new KafkaCryptoWireMessage(ek.keyIndex, gen.salt(), null);
        byte[][] kn = gen.generate();
        msg.msg = jasodium.crypto_secretbox(data,kn[1],kn[0]);
        rv = msg.toWire();
      } catch (Throwable e) {
        this._parent._logger.warn("Exception during serialization", e);
      } finally {
        this._parent._lock.unlock();
      }
      return rv;
    }
  }

  private class KafkaCryptoMessageDeserializer implements Deserializer<KafkaCryptoMessage>, KafkaCryptoMessageDecryptor<KafkaCryptoWireMessage>
  {
    private KafkaCrypto _parent;
    private boolean _key;
    private int _kwintervals;
    private long _kwi;
    private double _ikw;

    public KafkaCryptoMessageDeserializer(KafkaCrypto parent, boolean key, int max_key_wait_intervals, long key_wait_interval)
    {
      this._parent = parent;
      this._key = key;
      this._kwintervals = max_key_wait_intervals;
      this._kwi = key_wait_interval;
      this._ikw = key_wait_interval*Double.parseDouble(this._parent._config.getProperty("DESER_INITIAL_WAIT_INTERVALS"))/1000.0;
    }
    public void close()
    {
      this._parent = null;
    }
    public void configure(Map<String,?> configs, boolean isKey)
    {
      this._key = isKey;
    }
    public KafkaCryptoMessage deserialize(String topic, byte[] bytes_)
    {
      String root = this._parent.get_root(topic);
      try {
        KafkaCryptoMessage msg = KafkaCryptoMessage.fromWire(bytes_);
        if (msg == null) return null;
        if (KafkaPlainWireMessage.class.isAssignableFrom(msg.getClass()))
          return msg;
        if (KafkaCryptoWireMessage.class.isAssignableFrom(msg.getClass())) {
          KafkaCryptoWireMessage msg2 = (KafkaCryptoWireMessage)msg;
          msg2.root = root;
          msg2.deser = this;
          return this.decrypt(msg2);
        }
      } catch (Throwable e) {
        this._parent._logger.warn("Exception during deserialization", e);
      }
      return null;
    }

    public KafkaCryptoWireMessage decrypt(KafkaCryptoWireMessage msg)
    {
      try {
        this._parent._lock.lock();
        try {
          int i = this._kwintervals;
          this._parent._cgens.ensureContains(msg.root);
          if (!this._parent._cgens.get(msg.root).containsKey(msg.keyIndex))
            this._parent._cgens.get(msg.root).put(msg.keyIndex, new EncryptionKey(msg.root,msg.keyIndex));
          EncryptionKey ek = this._parent._cgens.get(msg.root).get(msg.keyIndex);
          while (!ek.acquired && (ek.creation+this._ikw >= Utils.currentTime() || i > 0)) {
            this._parent._lock.unlock();
            Thread.sleep(this._kwi);
            this._parent._lock.lock();
            ek = this._parent._cgens.get(msg.root).get(msg.keyIndex);
            i--;
          }
          if (ek.acquired) {
            KeyGenerator gen = (this._key)?(ek.keygen):(ek.valgen);
            byte[][] kn = gen.generate(msg.salt);
            msg.setCleartext(jasodium.crypto_secretbox_open(msg.msg,kn[1],kn[0]));
          }
        } finally {
          this._parent._lock.unlock();
        }
      } catch (InterruptedException ie) {
        throw new KafkaCryptoInternalError("Interrupted!", ie);
      } catch (Throwable e) {
        this._parent._logger.warn("Exception during deserialization decryption", e);
      }
      return msg;
    }
  }

  public Serializer getKeySerializer()
  {
    return new KafkaCrypto.ByteSerializer(this,true);
  }
  public Serializer getValueSerializer()
  {
    return new KafkaCrypto.ByteSerializer(this,false);
  }
  public Deserializer<KafkaCryptoMessage> getKeyDeserializer()
  {
    return this.getKeyDeserializer(0, 1000);
  }
  public Deserializer<KafkaCryptoMessage> getValueDeserializer()
  {
    return this.getValueDeserializer(0, 1000);
  }
  public Deserializer<KafkaCryptoMessage> getKeyDeserializer(int max_key_wait_intervals, long key_wait_interval)
  {
    return new KafkaCrypto.KafkaCryptoMessageDeserializer(this,true, max_key_wait_intervals, key_wait_interval);
  }
  public Deserializer<KafkaCryptoMessage> getValueDeserializer(int max_key_wait_intervals, long key_wait_interval)
  {
    return new KafkaCrypto.KafkaCryptoMessageDeserializer(this,false, max_key_wait_intervals, key_wait_interval);
  }
}
