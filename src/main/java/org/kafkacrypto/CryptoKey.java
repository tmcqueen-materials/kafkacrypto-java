package org.kafkacrypto;

import org.kafkacrypto.jasodium;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.CryptoKeyFileFormat;
import org.kafkacrypto.msgs.SignSecretKey;
import org.kafkacrypto.msgs.SignPublicKey;
import org.kafkacrypto.msgs.KEMPublicKey;
import org.kafkacrypto.msgs.KEMSecretKey;
import org.kafkacrypto.exceptions.KafkaCryptoException;
import org.kafkacrypto.exceptions.KafkaCryptoKeyException;

import java.io.File;
import java.nio.file.Paths;
import java.nio.file.Files;

import java.io.IOException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class CryptoKey
{
  private byte __version;
  private boolean __use_legacy;
  private byte[] __ek;
  private List<SignSecretKey> __ssk_all = new ArrayList<SignSecretKey>();
  private List<SignPublicKey> __spk_all = new ArrayList<SignPublicKey>();
  private List<SignSecretKey> __ssk = new ArrayList<SignSecretKey>();
  private List<SignPublicKey> __spk = new ArrayList<SignPublicKey>();
  private List<Byte> __versions;

  private Map<String,Map<String,Map<Byte,KEMSecretKey>>> __eskepk;
  private Lock __eskepklock;

  public CryptoKey(String file) throws KafkaCryptoException
  {
    this(file, (List<Byte>)(new ArrayList<Byte>()));
  }

  public CryptoKey(String file, List<Byte> keytypes) throws KafkaCryptoException
  {
    Logger _logger = LoggerFactory.getLogger("kafkacrypto-java.CryptoKey");

    if (!(new File(file).exists()))
      this.__init_empty_cryptokey(file);

    try {
      CryptoKeyFileFormat ckff = new CryptoKeyFileFormat().unpackb(Files.readAllBytes(Paths.get(file)));
      List<Byte> nkts = new ArrayList<Byte>();
      this.__version = ckff.version;
      for (int i = 0; i < ckff.ssk.size(); i++) {
        this.__ssk_all.add(ckff.ssk.get(i));
        this.__ssk.add(ckff.ssk.get(i));
        SignPublicKey spk = new SignPublicKey(ckff.ssk.get(i));
        this.__spk_all.add(spk);
        this.__spk.add(spk);
      }
      this.__ek = ckff.ek;
      this.__use_legacy = ckff.use_legacy;
      this.__versions = ckff.versions;
      for (int i = 0; i < keytypes.size(); i++) {
        int j = 0;
        for (; j < this.__ssk_all.size(); j++)
          if (this.__ssk_all.get(j).getType() == keytypes.get(i))
            break;
        if (j >= this.__ssk_all.size()) // Not found
          nkts.add(keytypes.get(i));
      }
      // Generate new keytypes
      for (int i = 0; i < nkts.size(); i++) {
        ckff.needs_update = true;
        SignSecretKey ssk = new SignSecretKey((byte)nkts.get(i));
        ckff.ssk.add(ssk);
        this.__ssk_all.add(ssk);
        this.__ssk.add(ssk);
        SignPublicKey spk = new SignPublicKey(ssk);
        this.__spk_all.add(spk);
        this.__spk.add(spk);
        _logger.warn("  Adding New Public Key: {}", spk.toString());
      }
      if (ckff.needs_update) {
        _logger.warn("Updating CryptoKey file {}.", file);
        try {
          Files.write(Paths.get(file), msgpack.packb(ckff));
        } catch (IOException ioe) {
          _logger.warn("  Error updating file.",ioe);
        }
      }
    } catch (IOException ioe) {
      throw new KafkaCryptoKeyException("Could not read CryptoKey file!", ioe);
    }
    this.__eskepklock = new ReentrantLock();
    this.__eskepk = new HashMap<String,Map<String,Map<Byte,KEMSecretKey>>>();
  }

//  public SignPublicKey get_spk()
//  {
//    return this.get_spk(0);
//  }

  public void limit_spk(List<Byte> keytypes)
  {
    this.__ssk = new ArrayList<SignSecretKey>();
    this.__spk = new ArrayList<SignPublicKey>();
    for (int i = 0; i < this.__ssk_all.size(); i++) {
      int j = 0;
      for (; j < keytypes.size(); j++)
        if (this.__ssk_all.get(i).getType() == keytypes.get(j));
      if (j < keytypes.size()) {// found!
        this.__ssk.add(this.__ssk_all.get(i));
        this.__spk.add(this.__spk_all.get(i));
      }
    }
  }

  public int get_num_spk()
  {
    return this.__ssk.size();
  }

  public byte[] get_id_spk()
  {
    byte[] rv = {};
    for (SignPublicKey spk : this.__spk)
      rv = Utils.concatArrays(rv, spk.getBytes());
    return rv;
  }

  public SignPublicKey get_spk(int idx)
  {
    return this.__spk.get(idx);
  }

  public byte[] sign_spk(byte[] msg, int idx)
  {
    return this.__ssk.get(idx).crypto_sign(msg);
  }

  public List<KEMPublicKey> get_epks(String topic, String usage)
  {
    List<KEMPublicKey> rv = new ArrayList<KEMPublicKey>();
    this.__eskepklock.lock();
    try {
      if (!this.__eskepk.containsKey(topic))
        this.__eskepk.put(topic, new HashMap<String,Map<Byte,KEMSecretKey>>());
      if (!this.__eskepk.get(topic).containsKey(usage))
        this.__eskepk.get(topic).put(usage, new HashMap<Byte,KEMSecretKey>());
      for (int v = 0; v < this.__versions.size(); v++) {
        KEMSecretKey ksk = new KEMSecretKey(this.__versions.get(v));
        this.__eskepk.get(topic).get(usage).put(this.__versions.get(v), ksk);
        rv.add(new KEMPublicKey(ksk));
      }
      return rv;
    } finally {
      this.__eskepklock.unlock();
    }
  }

  public Map<byte[],KEMPublicKey> use_epks(String topic, String usage, List<KEMPublicKey> pks, boolean clear)
  {
    Map<byte[],KEMPublicKey> rv = new HashMap<byte[],KEMPublicKey>();
    this.__eskepklock.lock();
    try {
      if (!this.__eskepk.containsKey(topic) || !this.__eskepk.get(topic).containsKey(usage))
        return rv;
      for (KEMPublicKey pk : pks) {
        if (this.__eskepk.get(topic).get(usage).containsKey(pk.getType())) {
          KEMSecretKey ksk = this.__eskepk.get(topic).get(usage).get(pk.getType());
          rv.put(ksk.complete_kem(pk), new KEMPublicKey(ksk));
        }
      }
      if (clear)
        this.__eskepk.get(topic).remove(usage);
    } finally {
      this.__eskepklock.unlock();
    }
    return rv;
  }

  public byte[] wrap_opaque(byte[] opaque)
  {
    return jasodium.crypto_secretbox_auto(opaque,this.__ek);
  }

  public byte[] unwrap_opaque(byte[] opaque)
  {
    return jasodium.crypto_secretbox_open_auto(opaque,this.__ek);
  }

  public boolean use_legacy()
  {
    return this.__use_legacy;
  }

  private void __init_empty_cryptokey(String file)
  {
    Logger _logger = LoggerFactory.getLogger("kafkacrypto-java.CryptoKey");
    _logger.warn("Initializing new CryptoKey file {}", file);
    CryptoKeyFileFormat ckff = new CryptoKeyFileFormat();
    ckff.ek = jasodium.randombytes(jasodium.CRYPTO_SECRETBOX_KEYBYTES);
    ckff.use_legacy = true;
    ckff.versions.add(Byte.valueOf((byte)1));
    try {
      Files.write(Paths.get(file), msgpack.packb(ckff));
    } catch (IOException ioe) {
      _logger.warn("  Error writing file.",ioe);
    }
    _logger.warn("  CryptoKey Initialized. Provisioning required for successful operation.");
  }
}
