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
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.exceptions.KafkaCryptoInternalError;
import org.msgpack.value.Value;
import org.msgpack.value.Variable;
import org.msgpack.core.MessageTypeCastException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.kafkacrypto.types.ByteHashMap;

import java.io.IOException;

public class CryptoExchange
{
  private int __maxage = 86400;
  private int __randombytes = 32;
  private CryptoKey __cryptokey;
  private SignedChain __spk_chain = new SignedChain();
  private ChainCert __spk = null;
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
        rv.append(msg);
        return msgpack.packb(rv);
      } catch (Throwable t) {
        t.printStackTrace();
      } finally {
        this.__spk_lock.unlock();
      }
    } catch (Throwable t) {
      t.printStackTrace();
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
          t.printStackTrace();
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
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
      ChainCert pk = allow.process_chain(null, "key-denylist", this.__allowlist, this.__denylist);
      ChainCert apk = new ChainCert().unpackb(pk.getExtra(0).asRawValue().asByteArray());
      if (!pk.pk.equals(apk.pk)) throw new KafkaCryptoInternalError("Mismatch in keys for allowlist.");
      if (!this.__allowlist.contains(apk)) {
        this.__allowlist.add(apk);
        return apk;
      }
    } catch (NullPointerException npe) {
    } catch (KafkaCryptoInternalError kcie) {
    } catch (IOException ioe) {
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
    } catch (KafkaCryptoInternalError kcie) {
    } catch (IOException ioe) {
    } finally {
      this.__allowdenylist_lock.unlock();
    }
    return null;
  }

  public SignedChain replace_spk_chain(SignedChain newchain)
  {
    return this.__update_spk_chain(newchain);
  }

  private SignedChain __update_spk_chain(SignedChain chain)
  {
    this.__allowdenylist_lock.lock();
    try {
      ChainCert new_spk = chain.process_chain(null, null, this.__allowlist, this.__denylist);
      this.__spk_lock.lock();
      try {
        if (this.__spk == null || new_spk.max_age > this.__spk.max_age) {
          this.__spk_chain = chain;
          this.__spk = new_spk;
          return chain;
        }
      } finally {
        this.__spk_lock.unlock();
      }
    } finally {
      this.__allowdenylist_lock.unlock();
    }
    return null;
  }
}
