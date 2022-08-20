package org.kafkacrypto;

import org.kafkacrypto.jasodium;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.CryptoKeyFileFormat;
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
  private byte[] __ssk, __ek;
  private SignPublicKey __spk;
  private List<Byte> __versions;

  private Map<String,Map<String,KEMSecretKey>> __eskepk;
  private Lock __eskepklock;

  public CryptoKey(String file) throws KafkaCryptoException
  {
    Logger _logger = LoggerFactory.getLogger("kafkacrypto-java.CryptoKey");

    if (!(new File(file).exists()))
      this.__init_cryptokey(file);

    try {
      CryptoKeyFileFormat ckff = new CryptoKeyFileFormat().unpackb(Files.readAllBytes(Paths.get(file)));
      this.__version = ckff.version;
      this.__ssk = ckff.ssk;
      this.__spk = new SignPublicKey(jasodium.crypto_sign_sk_to_pk(this.__ssk));
      this.__ek = ckff.ek;
      this.__use_legacy = ckff.use_legacy;
      this.__versions = ckff.versions;
      if (ckff.needs_update) {
        _logger.warn("Updating CryptoKey file {} to versioned format.", file);
        try {
          Files.write(Paths.get(file), msgpack.packb(ckff));
        } catch (IOException ioe) {
          _logger.warn("  Error updating file to versioned format.",ioe);
        }
      }
    } catch (IOException ioe) {
      throw new KafkaCryptoKeyException("Could not read CryptoKey file!", ioe);
    }
    this.__eskepklock = new ReentrantLock();
    this.__eskepk = new HashMap<String,Map<String,KEMSecretKey>>();
  }

  public SignPublicKey get_spk()
  {
    return this.__spk;
  }

  public byte[] sign_spk(byte[] msg)
  {
    return jasodium.crypto_sign(msg, this.__ssk);
  }

  public KEMPublicKey get_epk(String topic, String usage)
  {
    this.__eskepklock.lock();
    try {
      if (!this.__eskepk.containsKey(topic))
        this.__eskepk.put(topic, new HashMap<String,KEMSecretKey>());
      this.__eskepk.get(topic).put(usage, new KEMSecretKey(jasodium.randombytes(jasodium.CRYPTO_SCALARMULT_CURVE25519_BYTES)));
      return new KEMPublicKey(this.__eskepk.get(topic).get(usage));
    } finally {
      this.__eskepklock.unlock();
    }
  }

  public byte[][] use_epk(String topic, String usage, KEMPublicKey[] pks, boolean clear)
  {
    byte[][] rv = new byte[pks.length][];
    this.__eskepklock.lock();
    try {
      if (!this.__eskepk.containsKey(topic) || !this.__eskepk.get(topic).containsKey(usage))
        return rv;
      int i = 0;
      for (KEMPublicKey pk : pks) {
        rv[i] = this.__eskepk.get(topic).get(usage).complete_kem(pk);
        i++;
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

  private void __init_cryptokey(String file)
  {
    Logger _logger = LoggerFactory.getLogger("kafkacrypto-java.CryptoKey");
    _logger.warn("Initializing new CryptoKey file {}", file);
    byte[][] pksk = jasodium.crypto_sign_keypair();
    CryptoKeyFileFormat ckff = new CryptoKeyFileFormat();
    _logger.warn("  Public Key: {}", Utils.bytesToHex(pksk[0]));
    ckff.ek = jasodium.randombytes(jasodium.CRYPTO_SECRETBOX_KEYBYTES);
    ckff.ssk = pksk[1];
    ckff.version = 1;
    ckff.use_legacy = true;
    ckff.versions = new ArrayList<Byte>();
    ckff.versions.add(Byte.valueOf((byte)1));
    try {
      Files.write(Paths.get(file), msgpack.packb(ckff));
    } catch (IOException ioe) {
      _logger.warn("  Error writing file.",ioe);
    }
    _logger.warn("  CryptoKey Initialized. Provisioning required for successful operation.");
  }
}
