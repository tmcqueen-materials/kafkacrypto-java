package org.kafkacrypto;

import org.kafkacrypto.CryptoStore;
import org.kafkacrypto.msgs.ByteString;
import org.kafkacrypto.exceptions.KafkaCryptoException;
import org.kafkacrypto.exceptions.KafkaCryptoStoreException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

public class KafkaCryptoStore extends CryptoStore
{
  public KafkaCryptoStore(String file) throws KafkaCryptoException
  {
    this(file,null);
  }

  public KafkaCryptoStore(String file, String nodeID) throws KafkaCryptoException
  {
    super(file, nodeID);
    if (this.need_init)
      this.__init_kafkacryptostore();
    long log_level = this.load_value("log_level",null,30L);
    if (log_level <= 50L && log_level >= 40L)
      System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "error");
    else if (log_level >= 30L)
      System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "warning");
    else if (log_level >= 20L)
      System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "info");
    else if (log_level >= 10L)
      System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug");
  }

  public Properties get_kafka_config(String use, String... extra)
  {
    Properties rv = new Properties();
    String extra0 = null;
    Map<ByteString,ByteString> base_config = this.load_section("kafka",false);
    List<String> extras = new ArrayList<String>();
    if (use != null)
      extras.add("kafka-" + use);
    for (String e : extra) {
      if (extra0 == null)
        extra0 = e;
      extras.add("kafka-" + extra);
      if (use != null)
        extras.add("kafka-" + extra + "-" + use);
    }
    for (String e : extras) {
      Map<ByteString,ByteString> add_config = this.load_section(e,false);
      for (ByteString bs : add_config.keySet()) {
        ByteString bsv = add_config.get(bs);
        if (bsv == null || bsv.length() < 1 && base_config.containsKey(bs))
          base_config.remove(bs);
        else
          base_config.put(bs, bsv);
      }
    }
    if (!base_config.containsKey(new ByteString("group_id"))) {
      if (extra0 != null && extra0.equals("crypto"))
        base_config.put(new ByteString("group_id"), new ByteString(this.get_nodeID()+".kafkacrypto"));
      else
        base_config.put(new ByteString("group_id"), new ByteString(this.get_nodeID()));
    }
    if (!use.equals("consumer"))
      base_config.remove(new ByteString("group_id"));

    if (extra0 != null && extra0.equals("crypto")) {
      if (use.equals("consumer")) {
        if (!base_config.containsKey(new ByteString("key_deserializer")))
          base_config.put(new ByteString("key_deserializer"), new ByteString("org.apache.kafka.common.serialization.ByteArrayDeserializer"));
        if (!base_config.containsKey(new ByteString("value_deserializer")))
          base_config.put(new ByteString("value_deserializer"), new ByteString("org.apache.kafka.common.serialization.ByteArrayDeserializer"));
      }
      if (use.equals("producer")) {
        if (!base_config.containsKey(new ByteString("key_serializer")))
          base_config.put(new ByteString("key_serializer"), new ByteString("org.apache.kafka.common.serialization.ByteArraySerializer"));
        if (!base_config.containsKey(new ByteString("value_serializer")))
          base_config.put(new ByteString("value_serializer"), new ByteString("org.apache.kafka.common.serialization.ByteArraySerializer"));
      }
    }
    for (ByteString bs : base_config.keySet()) {
      String key = bs.toString().replace('_','.');
      // more filtering?
      rv.setProperty(key, base_config.get(bs).toString());
    }
    return rv;
  }

  private void __init_kafkacryptostore() throws KafkaCryptoStoreException
  {
    this.store_value("bootstrap_servers", "kafka", "");
    this.store_value("security_protocol", "kafka", "SSL");
    this.store_value("test", "kafka-consumer", "test");
    this.store_value("test", "kafka-consumer", (ByteString)null);
    this.store_value("test", "kafka-producer", "test");
    this.store_value("test", "kafka-producer", (ByteString)null);
    this.store_value("test", "kafka-crypto", "test");
    this.store_value("test", "kafka-crypto", (ByteString)null);
    this.store_value("test", "kafka-crypto-consumer", "test");
    this.store_value("test", "kafka-crypto-consumer", (ByteString)null);
    this.store_value("test", "kakfa-crypto-producer", "test");
    this.store_value("test", "kakfa-crypto-producer", (ByteString)null);
  }
}
