package org.kafkacrypto;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.kafkacrypto.jasodium;
import org.kafkacrypto.CryptoKey;
import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.SignPublicKey;
import org.kafkacrypto.msgs.SignSecretKey;
import org.kafkacrypto.msgs.KEMPublicKey;
import org.kafkacrypto.msgs.SignedChain;
import org.kafkacrypto.msgs.ChainCert;
import org.kafkacrypto.msgs.TopicsPoison;
import org.kafkacrypto.msgs.UsagesPoison;
import org.kafkacrypto.msgs.PathlenPoison;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.types.ByteArrayWrapper;
import org.kafkacrypto.exceptions.KafkaCryptoExchangeException;
import org.kafkacrypto.exceptions.KafkaCryptoInternalError;
import org.msgpack.value.Value;
import org.msgpack.value.Variable;
import org.msgpack.core.MessageTypeCastException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

import org.kafkacrypto.types.ByteHashMap;

import java.io.IOException;

public class CryptoExchange
{
  protected Logger _logger;
  private int __maxage = 86400;
  private int __randombytes = 32;
  private Map<String,ArrayList<ByteArrayWrapper>> __randoms = new HashMap<String,ArrayList<ByteArrayWrapper>>();
  private Lock __randoms_lock = new ReentrantLock();
  private CryptoKey __cryptokey;
  private List<SignedChain> __spk_chains = new ArrayList<SignedChain>();
  private List<ChainCert> __spks = new ArrayList<ChainCert>();
  private List<Boolean> __spk_direct_requests = new ArrayList<Boolean>();
  private Lock __spk_lock = new ReentrantLock();
  private List<ChainCert> __allowlist = new ArrayList<ChainCert>(), __denylist = new ArrayList<ChainCert>();
  private Lock __allowdenylist_lock = new ReentrantLock();

  public CryptoExchange(List<SignedChain> chains, CryptoKey cryptokey, List<ChainCert> allowlist, List<ChainCert> denylist)
  {
    this(chains, cryptokey, allowlist, denylist, 0, 32);
  }

  public CryptoExchange(List<SignedChain> chains, CryptoKey cryptokey, List<ChainCert> allowlist, List<ChainCert> denylist, int maxage)
  {
    this(chains, cryptokey, allowlist, denylist, maxage, 32);
  }

  public CryptoExchange(List<SignedChain> chains, CryptoKey cryptokey, List<ChainCert> allowlist, List<ChainCert> denylist, int maxage, int randombytes)
  {
    this._logger = LoggerFactory.getLogger("kafkacrypto-java.cryptoexchange");
    if (maxage > 0)
      this.__maxage = maxage;
    if (randombytes > this.__randombytes)
      this.__randombytes = randombytes;
    this.__cryptokey = cryptokey;
    for (int idx = 0; idx < this.__cryptokey.get_num_spk(); idx++) {
      this.__spk_chains.add(null);
      this.__spks.add(null);
      this.__spk_direct_requests.add(false);
    }
    if (allowlist != null)
      this.__allowlist = allowlist;
    if (denylist != null)
      this.__denylist = denylist;
    for (SignedChain chain : chains)
      this.__update_spk_chain(chain);
  }

  public List<byte[]> encrypt_keys(List<byte[]> keyidxs, List<byte[]> keys, String topic, byte[] msgval)
  {
    this.__allowdenylist_lock.lock();
    try {
      ChainCert pk = (new SignedChain().unpackb(msgval)).process_chain(topic,"key-encrypt-request",this.__allowlist,this.__denylist);
      List<KEMPublicKey> epks = this.__cryptokey.get_epks(topic, "encrypt_keys");
      Map<byte[],KEMPublicKey> eks = this.__cryptokey.use_epks(topic, "encrypt_keys", pk.pk_array, true);
      byte[] ek = eks.keySet().iterator().next(); // use first common KEM key
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
      cc.pk_array = epks;
      List<Value> randoms = new ArrayList<Value>();
      randoms.add(new Variable().setStringValue(random0));
      randoms.add(new Variable().setStringValue(random1));
      cc.extra.add(new Variable().setArrayValue(randoms));
      cc.extra.add(new Variable().setStringValue(msg0));
      // One message for each signing chain
      List<byte[]> msgs = new ArrayList<byte[]>();
      boolean doall = true;
      for (int idx = 0; idx < this.__cryptokey.get_num_spk(); idx++)
        if (pk.parentpk != null && this.__cryptokey.get_spk(idx).getType() == pk.parentpk.getType()) {
          doall = false;
          break; // found a matching type
        }
      for (int idx = 0; idx < this.__cryptokey.get_num_spk(); idx++) {
        if (!doall && pk.parentpk != null && this.__cryptokey.get_spk(idx).getType() != pk.parentpk.getType()) // if we have a matching type as request, use only that
          continue;
        byte[] msg = this.__cryptokey.sign_spk(msgpack.packb(cc),idx);
        this.__spk_lock.lock();
        try {
          SignedChain rv = new SignedChain(this.__spk_chains.get(idx));
          if (rv.chain.size() == 0) {
            ChainCert tempcc = new ChainCert(), tempcc2 = new ChainCert();
            tempcc.max_age = Utils.currentTime() + this.__maxage;
            tempcc.poisons.add(new TopicsPoison(topic));
            tempcc.poisons.add(new UsagesPoison("key-encrypt"));
            tempcc.poisons.add(new PathlenPoison(1));
            tempcc.pk = this.__cryptokey.get_spk(idx);
            rv.append(jasodium.crypto_sign(msgpack.packb(tempcc), jasodium.crypto_sign_seed_keypair(Utils.hexToBytes("4c194f7de97c67626cc43fbdaf93dffbc4735352b37370072697d44254e1bc6c"))[1]));
            SignedChain provision = new SignedChain();
            tempcc2.max_age = 0;
            tempcc2.pk = this.__cryptokey.get_spk(idx);
            provision.append(msgpack.packb(tempcc2));
            provision.append(this.__cryptokey.sign_spk(msgpack.packb(tempcc),idx));
            this._logger.warn("Current signing chain is empty. Use {} to provision access and then remove temporary root of trust from allowedlist.", Utils.bytesToHex(msgpack.packb(provision)));
          }
          rv.append(msg);
          msgs.add(msgpack.packb(rv));
        } catch (Throwable t) {
          this._logger.info("Could not build reply chain", t);
        } finally {
          this.__spk_lock.unlock();
        }
      }
      return msgs;
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
      this.__randoms_lock.lock();
      try {
        if (this.__randoms.containsKey(topic)) {
          if (!this.__randoms.get(topic).contains(new ByteArrayWrapper(random0)))
            return null;
        }
      } finally {
        this.__randoms_lock.unlock();
      }
      byte[] random1 = rnd.get(1).asRawValue().asByteArray();
      Map<byte[],KEMPublicKey> eks = this.__cryptokey.use_epks(topic, "decrypt_keys", pk.pk_array, false);
      for (byte[] ck : eks.keySet()) {
        try {
          byte[] ss = Utils.splitArray(jasodium.crypto_hash_sha256(Utils.concatArrays(topic.getBytes(),random0,random1,ck)),jasodium.CRYPTO_SECRETBOX_KEYBYTES)[0];
          List<Value> rvs = msgpack.unpackb(jasodium.crypto_secretbox_open_auto(msg,ss));
          if (rvs.size() % 2 != 0 || rvs.size() < 2)
            throw new KafkaCryptoInternalError("Invalid encryption key set!");
          Map<byte[],byte[]> rv = new ByteHashMap<byte[]>();
          for (int i = 0; i < rvs.size(); i += 2)
            rv.put(rvs.get(i).asRawValue().asByteArray(), rvs.get(i+1).asRawValue().asByteArray());
          this.__cryptokey.use_epks(topic, "decrypt_keys", (List<KEMPublicKey>)(new ArrayList<KEMPublicKey>()), true);
          this.__randoms_lock.lock();
          try {
            this.__randoms.get(topic).remove(new ByteArrayWrapper(random0));
          } finally {
            this.__randoms_lock.unlock();
          }
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

  public List<byte[]> signed_epks(String topic) throws IOException
  {
    List<byte[]> rv = new ArrayList<byte[]>();
    List<KEMPublicKey> epks = this.__cryptokey.get_epks(topic,"decrypt_keys");
    byte[] random0 = jasodium.randombytes(this.__randombytes);
    this.__randoms_lock.lock();
    try {
      if (!this.__randoms.containsKey(topic))
        this.__randoms.put(topic, new ArrayList<ByteArrayWrapper>());
      this.__randoms.get(topic).add(new ByteArrayWrapper(random0));
    } finally {
      this.__randoms_lock.unlock();
    }
    ChainCert cc = new ChainCert();
    cc.poisons.add(new TopicsPoison(topic));
    cc.poisons.add(new UsagesPoison("key-encrypt-request", "key-encrypt-subscribe"));
    cc.max_age = Utils.currentTime() + this.__maxage;
    cc.extra.add(new Variable().setStringValue(random0));
    for (int idx = 0; idx < this.__cryptokey.get_num_spk(); idx++) {
      if (this.__cryptokey.use_legacy() && this.__cryptokey.get_spk(idx).getType() == 1) { // Legacy format if required
        for (KEMPublicKey epk : epks) {
          if (epk.getType() == 1) {
            cc.pk = new SignPublicKey(epk);
            break;
          }
        }
        cc.pk_array = null;
        byte[] msg = this.__cryptokey.sign_spk(msgpack.packb(cc), idx);
        this.__spk_lock.lock();
        try {
          SignedChain sc = new SignedChain(this.__spk_chains.get(idx));
          if (sc.chain.size() == 0) {
            this.__spk_direct_requests.set(idx,true);
            ChainCert tempcc = new ChainCert(), tempcc2 = new ChainCert();
            tempcc.max_age = Utils.currentTime() + this.__maxage;
            tempcc.poisons.add(new TopicsPoison(topic));
            tempcc.poisons.add(new UsagesPoison("key-encrypt-request","key-encrypt-subscribe"));
            tempcc.poisons.add(new PathlenPoison(1));
            tempcc.pk = this.__cryptokey.get_spk(idx);
            sc.append(jasodium.crypto_sign(msgpack.packb(tempcc), jasodium.crypto_sign_seed_keypair(Utils.hexToBytes("4c194f7de97c67626cc43fbdaf93dffbc4735352b37370072697d44254e1bc6c"))[1]));
            SignedChain provision = new SignedChain();
            tempcc2.max_age = 0;
            tempcc2.pk = this.__cryptokey.get_spk(idx);
            provision.append(msgpack.packb(tempcc2));
            provision.append(this.__cryptokey.sign_spk(msgpack.packb(tempcc), idx));
            this._logger.warn("Current signing chain is empty. Use {} to provision access and then remove temporary root of trust from allowedlist.", Utils.bytesToHex(msgpack.packb(provision)));
          }
          sc.append(msg);
          rv.add(msgpack.packb(sc));
        } finally {
          this.__spk_lock.unlock();
        }
      }
      cc.pk = null;
      cc.pk_array = epks;
      byte[] msg = this.__cryptokey.sign_spk(msgpack.packb(cc), idx);
      this.__spk_lock.lock();
      try {
        SignedChain sc = new SignedChain(this.__spk_chains.get(idx));
        if (sc.chain.size() == 0) {
          this.__spk_direct_requests.set(idx,true);
          ChainCert tempcc = new ChainCert(), tempcc2 = new ChainCert();
          tempcc.max_age = Utils.currentTime() + this.__maxage;
          tempcc.poisons.add(new TopicsPoison(topic));
          tempcc.poisons.add(new UsagesPoison("key-encrypt-request","key-encrypt-subscribe"));
          tempcc.poisons.add(new PathlenPoison(1));
          tempcc.pk = this.__cryptokey.get_spk(idx);
          sc.append(jasodium.crypto_sign(msgpack.packb(tempcc), jasodium.crypto_sign_seed_keypair(Utils.hexToBytes("4c194f7de97c67626cc43fbdaf93dffbc4735352b37370072697d44254e1bc6c"))[1]));
          SignedChain provision = new SignedChain();
          tempcc2.max_age = 0;
          tempcc2.pk = this.__cryptokey.get_spk(idx);
          provision.append(msgpack.packb(tempcc2));
          provision.append(this.__cryptokey.sign_spk(msgpack.packb(tempcc), idx));
          this._logger.warn("Current signing chain is empty. Use {} to provision access and then remove temporary root of trust from allowedlist.", Utils.bytesToHex(msgpack.packb(provision)));
        }
        sc.append(msg);
        rv.add(msgpack.packb(sc));
      } finally {
        this.__spk_lock.unlock();
      }
    }
    return rv;
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

  public boolean direct_request_spk_chain(int idx)
  {
    this.__spk_lock.lock();
    try {
      return this.__spk_direct_requests.get(idx);
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
        for (int idx = 0; idx < this.__cryptokey.get_num_spk(); idx++) {
          if (!this.__cryptokey.get_spk(idx).equals(new_spk.pk))
            continue;
          if (this.__spks.get(idx) == null || new_spk.max_age > this.__spks.get(idx).max_age) {
            this.__spk_chains.set(idx,chain);
            this.__spks.set(idx,new_spk);
            // check if direct requests will work. default to false
            this.__spk_direct_requests.set(idx,false);
            try {
              chain.process_chain(null,"key-encrypt-request",this.__allowlist,this.__denylist);
              this.__spk_direct_requests.set(idx,true);
            } catch (KafkaCryptoInternalError kcie) {
              this.__spk_direct_requests.set(idx,false);
            }
            return chain;
          } else {
            this._logger.warn("Non-superior chain: {} vs {}", this.__spks.get(idx).max_age, new_spk.max_age);
            throw new KafkaCryptoExchangeException("New chain has sooner expiry time than current chain!");
          }
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
