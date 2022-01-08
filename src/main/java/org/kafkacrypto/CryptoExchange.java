package org.kafkacrypto;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.kafkacrypto.jasodium;
import org.kafkacrypto.CryptoKey;
import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.SignedChain;
import org.kafkacrypto.msgs.ChainCert;
import org.kafkacrypto.msgs.TopicsPoison;
import org.kafkacrypto.msgs.UsagesPoison;
import org.kafkacrypto.msgs.PathlenPoison;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.exceptions.KafkaCryptoExchangeException;
import org.kafkacrypto.exceptions.KafkaCryptoInternalError;
import org.msgpack.value.Value;
import org.msgpack.value.Variable;
import org.msgpack.core.MessageTypeCastException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;

import org.kafkacrypto.types.ByteHashMap;

import java.io.IOException;

public class CryptoExchange
{
  protected Logger _logger;
  private int __maxage = 86400;
  private int __randombytes = 32;
  private CryptoKey __cryptokey;
  private SignedChain __spk_chain = new SignedChain();
  private ChainCert __spk = null;
  private boolean __spk_direct_request = true;
  private Lock __spk_lock = new ReentrantLock();
  private List<ChainCert> __allowlist = new ArrayList<ChainCert>(), __denylist = new ArrayList<ChainCert>();
  private Lock __allowdenylist_lock = new ReentrantLock();

  public CryptoExchange(SignedChain chain, CryptoKey cryptokey, List<ChainCert> allowlist, List<ChainCert> denylist)
  {
    this(chain, cryptokey, allowlist, denylist, 0, 32);
  }

  public CryptoExchange(SignedChain chain, CryptoKey cryptokey, List<ChainCert> allowlist, List<ChainCert> denylist, int maxage)
  {
    this(chain, cryptokey, allowlist, denylist, maxage, 32);
  }

  public CryptoExchange(SignedChain chain, CryptoKey cryptokey, List<ChainCert> allowlist, List<ChainCert> denylist, int maxage, int randombytes)
  {
    this._logger = LoggerFactory.getLogger("kafkacrypto-java.cryptoexchange");
    if (maxage > 0)
      this.__maxage = maxage;
    if (randombytes > this.__randombytes)
      this.__randombytes = randombytes;
    this.__cryptokey = cryptokey;
    if (allowlist != null)
      this.__allowlist = allowlist;
    if (denylist != null)
      this.__denylist = denylist;
    this.__update_spk_chain(chain);
  }

  public byte[] encrypt_keys(List<byte[]> keyidxs, List<byte[]> keys, String topic, byte[] msgval)
  {
    this.__allowdenylist_lock.lock();
    try {
      ChainCert pk = (new SignedChain().unpackb(msgval)).process_chain(topic,"key-encrypt-request",this.__allowlist,this.__denylist);
      byte[] epk = this.__cryptokey.get_epk(topic, "encrypt_keys");
      byte[][] pks = new byte[1][0];
      pks[0] = pk.pk;
      byte[][] eks = this.__cryptokey.use_epk(topic, "encrypt_keys", pks, true);
      byte[] ek = eks[0];
      eks[0] = epk;
      byte[] random0 = pk.getExtra(0).asRawValue().asByteArray();
      byte[] random1 = jasodium.randombytes(this.__randombytes);
      byte[] ss = Utils.splitArray(jasodium.crypto_hash_sha256(Utils.concatArrays(topic.getBytes(),random0,random1,ek)),jasodium.CRYPTO_SECRETBOX_KEYBYTES)[0];
      List<byte[]> kiks = new ArrayList<byte[]>();
      for (int i = 0; i < keyidxs.size(); i++) {
        kiks.add(keyidxs.get(i));
        kiks.add(keys.get(i));
      }
      byte[] msg0 = msgpack.packb(kiks);
      msg0 = jasodium.crypto_secretbox_auto(msg0, ss);
      ChainCert cc = new ChainCert();
      cc.max_age = Utils.currentTime() + this.__maxage;
      cc.poisons.add(new TopicsPoison(topic));
      cc.poisons.add(new UsagesPoison("key-encrypt"));
      cc.pk = eks[0];
      cc.pk_array = eks;
      List<Value> randoms = new ArrayList<Value>();
      randoms.add(new Variable().setStringValue(random0));
      randoms.add(new Variable().setStringValue(random1));
      cc.extra.add(new Variable().setArrayValue(randoms));
      cc.extra.add(new Variable().setStringValue(msg0));
      byte[] msg = this.__cryptokey.sign_spk(msgpack.packb(cc));
      this.__spk_lock.lock();
      try {
        SignedChain rv = new SignedChain(this.__spk_chain);
        if (rv.chain.size() == 0) {
          ChainCert tempcc = new ChainCert(), tempcc2 = new ChainCert();
          tempcc.max_age = Utils.currentTime() + this.__maxage;
          tempcc.poisons.add(new TopicsPoison(topic));
          tempcc.poisons.add(new UsagesPoison("key-encrypt"));
          tempcc.poisons.add(new PathlenPoison(1));
          tempcc.pk = this.__cryptokey.get_spk();
          rv.append(jasodium.crypto_sign(msgpack.packb(tempcc), jasodium.crypto_sign_seed_keypair(Utils.hexToBytes("4c194f7de97c67626cc43fbdaf93dffbc4735352b37370072697d44254e1bc6c"))[1]));
          SignedChain provision = new SignedChain();
          tempcc2.max_age = 0;
          tempcc2.pk = this.__cryptokey.get_spk();
          provision.append(msgpack.packb(tempcc2));
          provision.append(this.__cryptokey.sign_spk(msgpack.packb(tempcc)));
          this._logger.warn("Current signing chain is empty. Use {} to provision access and then remove temporary root of trust from allowedlist.", Utils.bytesToHex(msgpack.packb(provision)));
        }
        rv.append(msg);
        return msgpack.packb(rv);
      } catch (Throwable t) {
        this._logger.info("Could not build reply chain", t);
      } finally {
        this.__spk_lock.unlock();
      }
    } catch (Throwable t) {
      this._logger.info("Error replying to encrypt_keys message", t);
    } finally {
      this.__allowdenylist_lock.unlock();
    }
    return null;
  }

  public Map<byte[], byte[]> decrypt_keys(String topic, byte[] msgval)
  {
    this.__allowdenylist_lock.lock();
    try {
      ChainCert pk = (new SignedChain().unpackb(msgval)).process_chain(topic,"key-encrypt",this.__allowlist,this.__denylist);
      List<Value> rnd = pk.getExtra(0).asArrayValue().list();
      byte[] msg = pk.getExtra(1).asRawValue().asByteArray();
      byte[] random0 = rnd.get(0).asRawValue().asByteArray();
      byte[] random1 = rnd.get(1).asRawValue().asByteArray();
      byte[][] pks = pk.pk_array;
      byte[][] eks = this.__cryptokey.use_epk(topic, "decrypt_keys", pks, false);
      for (byte[] ck : eks) {
        try {
          byte[] ss = Utils.splitArray(jasodium.crypto_hash_sha256(Utils.concatArrays(topic.getBytes(),random0,random1,ck)),jasodium.CRYPTO_SECRETBOX_KEYBYTES)[0];
          List<Value> rvs = msgpack.unpackb(jasodium.crypto_secretbox_open_auto(msg,ss));
          if (rvs.size() % 2 != 0 || rvs.size() < 2)
            throw new KafkaCryptoInternalError("Invalid encryption key set!");
          Map<byte[],byte[]> rv = new ByteHashMap<byte[]>();
          for (int i = 0; i < rvs.size(); i += 2)
            rv.put(rvs.get(i).asRawValue().asByteArray(), rvs.get(i+1).asRawValue().asByteArray());
          this.__cryptokey.use_epk(topic, "decrypt_keys", new byte[0][], true);
          return rv;
        } catch (Throwable t) {
          this._logger.debug("Failure to decrypt keys", t);
        }
      }
    } catch (Throwable t) {
      this._logger.info("Unable to interpret decrypt_keys message", t);
    } finally {
      this.__allowdenylist_lock.unlock();
    }
    return null;
  }

  public byte[] signed_epk(String topic, byte[] epk) throws IOException
  {
    if (epk == null)
      epk = this.__cryptokey.get_epk(topic,"decrypt_keys");
    byte[] random0 = jasodium.randombytes(this.__randombytes);
    ChainCert cc = new ChainCert();
    cc.poisons.add(new TopicsPoison(topic));
    cc.poisons.add(new UsagesPoison("key-encrypt-request", "key-encrypt-subscribe"));
    cc.max_age = Utils.currentTime() + this.__maxage;
    cc.pk = epk;
    cc.extra.add(new Variable().setStringValue(random0));
    byte[] msg = this.__cryptokey.sign_spk(msgpack.packb(cc));
    this.__spk_lock.lock();
    try {
      SignedChain rv = new SignedChain(this.__spk_chain);
      if (rv.chain.size() == 0) {
        this.__spk_direct_request = true;
        ChainCert tempcc = new ChainCert(), tempcc2 = new ChainCert();
        tempcc.max_age = Utils.currentTime() + this.__maxage;
        tempcc.poisons.add(new TopicsPoison(topic));
        tempcc.poisons.add(new UsagesPoison("key-encrypt-request","key-encrypt-subscribe"));
        tempcc.poisons.add(new PathlenPoison(1));
        tempcc.pk = this.__cryptokey.get_spk();
        rv.append(jasodium.crypto_sign(msgpack.packb(tempcc), jasodium.crypto_sign_seed_keypair(Utils.hexToBytes("4c194f7de97c67626cc43fbdaf93dffbc4735352b37370072697d44254e1bc6c"))[1]));
        SignedChain provision = new SignedChain();
       	tempcc2.max_age = 0;
        tempcc2.pk = this.__cryptokey.get_spk();
        provision.append(msgpack.packb(tempcc2));
        provision.append(this.__cryptokey.sign_spk(msgpack.packb(tempcc)));
        this._logger.warn("Current signing chain is empty. Use {} to provision access and then remove temporary root of trust from allowedlist.", Utils.bytesToHex(msgpack.packb(provision)));
      }
      rv.append(msg);
      return msgpack.packb(rv);
    } finally {
      this.__spk_lock.unlock();
    }
  }

  public ChainCert add_allowlist(SignedChain allow)
  {
    this.__allowdenylist_lock.lock();
    try {
      ChainCert pk = allow.process_chain(null, "key-allowlist", this.__allowlist, this.__denylist);
      ChainCert apk = new ChainCert().unpackb(pk.getExtra(0).asRawValue().asByteArray());
      if (!pk.pk.equals(apk.pk)) throw new KafkaCryptoInternalError("Mismatch in keys for allowlist.");
      if (!this.__allowlist.contains(apk)) {
        this.__allowlist.add(apk);
        return apk;
      }
    } catch (NullPointerException npe) {
      this._logger.info("add_allowlist error", npe);
    } catch (KafkaCryptoInternalError kcie) {
      this._logger.warn("add_allowlist error", kcie);
    } catch (IOException ioe) {
      this._logger.info("add_allowlist error", ioe);
    } finally {
      this.__allowdenylist_lock.unlock();
    }
    return null;
  }

  public ChainCert add_denylist(SignedChain deny)
  {
    this.__allowdenylist_lock.lock();
    try {
      ChainCert pk = deny.process_chain(null, "key-denylist", this.__allowlist, this.__denylist);
      ChainCert apk = new ChainCert().unpackb(pk.getExtra(0).asRawValue().asByteArray());
      if (!pk.pk.equals(apk.pk)) throw new KafkaCryptoInternalError("Mismatch in keys for denylist.");
      if (!this.__denylist.contains(apk)) {
        this.__denylist.add(apk);
        return apk;
      }
    } catch (NullPointerException npe) {
      this._logger.info("add_denylist error", npe);
    } catch (KafkaCryptoInternalError kcie) {
      this._logger.warn("add_denylist error", kcie);
    } catch (IOException ioe) {
      this._logger.info("add_denylist error", ioe);
    } finally {
      this.__allowdenylist_lock.unlock();
    }
    return null;
  }

  public boolean direct_request_spk_chain()
  {
    this.__spk_lock.lock();
    try {
      return this.__spk_direct_request;
    } finally {
      this.__spk_lock.unlock();
    }
  }

  public SignedChain replace_spk_chain(SignedChain newchain)
  {
    return this.__update_spk_chain(newchain);
  }

  private SignedChain __update_spk_chain(SignedChain chain)
  {
    if (chain == null || chain.chain.size() < 1)
      return null;
    this.__allowdenylist_lock.lock();
    try {
      ChainCert new_spk = chain.process_chain(null, null, this.__allowlist, this.__denylist);
      if (new_spk == null)
        return null;
      this.__spk_lock.lock();
      try {
        if (!Arrays.equals(this.__cryptokey.get_spk(), new_spk.pk)) {
          this._logger.warn("Key mismatch: {} vs {}", this.__cryptokey.get_spk(), new_spk.pk);
          throw new KafkaCryptoExchangeException("New chain does not match current signing public key!");
        }
        if (this.__spk == null || new_spk.max_age > this.__spk.max_age) {
          this.__spk_chain = chain;
          this.__spk = new_spk;
          // check if direct requests will work. default to false
          this.__spk_direct_request = false;
          try {
            chain.process_chain(null,"key-encrypt-request",this.__allowlist,this.__denylist);
            this.__spk_direct_request = true;
          } catch (KafkaCryptoExchangeException kcee) {
            this.__spk_direct_request = false;
          }
          return chain;
        } else {
          this._logger.warn("Non-superior chain: {} vs {}", this.__spk.max_age, new_spk.max_age);
          throw new KafkaCryptoExchangeException("New chain has sooner expiry time than current chain!");
        }
      } finally {
        this.__spk_lock.unlock();
      }
    } catch (KafkaCryptoExchangeException kcee) {
      this._logger.warn("__update_spk_chain error", kcee);
    } finally {
      this.__allowdenylist_lock.unlock();
    }
    return null;
  }

}
