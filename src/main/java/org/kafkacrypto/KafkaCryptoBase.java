package org.kafkacrypto;

import org.kafkacrypto.CryptoStore;
import org.kafkacrypto.CryptoExchange;
import org.kafkacrypto.CryptoKey;
import org.kafkacrypto.msgs.ChainCert;
import org.kafkacrypto.msgs.SignedChain;
import org.kafkacrypto.exceptions.KafkaCryptoException;
import org.kafkacrypto.exceptions.KafkaCryptoBaseException;
import org.kafkacrypto.msgs.ByteString;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import java.util.Properties;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

class KafkaCryptoBase
{
  protected Logger _logger;
  protected String _nodeID;
  protected CryptoStore _cryptostore;
  protected boolean _cryptostore_close;
  protected CryptoKey _cryptokey;
  protected CryptoExchange _cryptoexchange;
  protected Properties _config;
  protected Lock _lock;
  protected KafkaConsumer<byte[],byte[]> _kc;
  protected KafkaProducer<byte[],byte[]> _kp;

  public KafkaCryptoBase(String nodeID, KafkaProducer<byte[],byte[]> kp, KafkaConsumer<byte[],byte[]> kc, Object config, Object cryptokey) throws KafkaCryptoException, IOException
  {
    this._logger = LoggerFactory.getLogger("kafkacrypto-java");
    if ((nodeID==null || nodeID.length() < 1) && config==null)
      throw new KafkaCryptoBaseException("At least one of Node ID and Config file must be specified.");
    if (config == null)
      config = nodeID + ".config";
    if (CryptoStore.class.isAssignableFrom(config.getClass())) {
      this._cryptostore = (CryptoStore)config;
      this._cryptostore_close = false;
    } else {
      this._cryptostore = new CryptoStore((String)config, nodeID);
      this._cryptostore_close = true;
    }
    nodeID = this._cryptostore.get_nodeID();
    this._nodeID = nodeID;

    this.__configure();
    if (cryptokey == null) {
      cryptokey = this._cryptostore.load_value("cryptokey",null,(String)null);
      if (cryptokey != null && ((String)cryptokey).startsWith("file#")) {
        cryptokey = ((String)cryptokey).substring(5);
      } else {
        cryptokey = nodeID + ".crypto";
        this._cryptostore.store_value("cryptokey", "", "file#" + (String)cryptokey);
      }
    }
    if (CryptoKey.class.isAssignableFrom(cryptokey.getClass()))
      this._cryptokey = (CryptoKey)cryptokey;
    else
      this._cryptokey = new CryptoKey((String)cryptokey);
    this._cryptostore.set_cryptokey(this._cryptokey);

    Map<ByteString,ByteString> allow = this._cryptostore.load_section("allowlist",false);
    List<ChainCert> allowlist = null;
    if (allow != null) {
      allowlist = new ArrayList<ChainCert>();
      for (ByteString bs : allow.values())
        allowlist.add(new ChainCert().unpackb(bs.getBytes()));
    }
    Map<ByteString,ByteString> deny = this._cryptostore.load_section("denylist",false);
    List<ChainCert> denylist = null;
    if (deny != null) {
      denylist = new ArrayList<ChainCert>();
      for (ByteString bs : deny.values())
        denylist.add(new ChainCert().unpackb(bs.getBytes()));
    }

    SignedChain chain = new SignedChain().unpackb(this._cryptostore.load_value("chain","crypto",new byte[0]));
    int max_age = (int)(this._cryptostore.load_value("maxage","crypto",0));
    this._cryptoexchange = new CryptoExchange(chain, this._cryptokey, allowlist, denylist, max_age);
    this._lock = new ReentrantLock();
    this._kp = kp;
    this._kc = kc;
  }

  public String get_root(String topic)
  {
    int lio = topic.lastIndexOf(this._config.getProperty("TOPIC_SEPARATOR"));
    return topic.substring(0,(lio>0)?(lio):(topic.length()));
  }

  public void close()
  {
    this._kp.close();
    this._kp = null;
    this._kc.close();
    this._kc = null;
    if (this._cryptostore_close)
      this._cryptostore.close();
    this._cryptostore = null;
    // Never close these two, just release objects.
    this._cryptokey = null;
    this._cryptoexchange = null;
  }

  private void __configure()
  {
    this._config = new Properties();
    this._config.setProperty("TOPIC_SEPARATOR", ".");
    this._config.setProperty("TOPIC_SUFFIX_SUBS", ".subs");
    this._config.setProperty("TOPIC_SUFFIX_KEYS", ".keys");
    this._config.setProperty("TOPIC_SUFFIX_REQS", ".reqs");
    this._config.setProperty("CRYPTO_MAX_PGEN_AGE", "604800");
    this._config.setProperty("CRYPTO_SUB_INTERVAL", "60");
    this._config.setProperty("CRYPTO_RATCHET_INTERVAL", "86400");
    this._config.setProperty("MGMT_TOPIC_CHAINS", "chains");
    this._config.setProperty("MGMT_TOPIC_ALLOWLIST", "allowlist");
    this._config.setProperty("MGMT_TOPIC_DENYLIST", "denylist");
    this._config.setProperty("MGMT_POLL_INTERVAL", "500");
    this._config.setProperty("MGMT_POLL_RECORDS", "8");
    this._config.setProperty("MGMT_SUBSCRIBE_INTERVAL", "300");
    this._config.setProperty("MGMT_LONG_KEYINDEX", "true");
    this._config.setProperty("DESER_INITIAL_WAIT_INTERVALS", "10");
    for (String e : this._config.stringPropertyNames()) {
      ByteString r = this._cryptostore.load_value(e,null,(ByteString)null);
      this._logger.info("Loading config: {},{},{}",e,this._config.getProperty(e),r);
      if (r != null)
        this._config.setProperty(e, new String(r.getBytes()));
    }
  }
}
