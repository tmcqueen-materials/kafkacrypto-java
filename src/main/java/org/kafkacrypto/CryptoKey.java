package org.kafkacrypto;

import org.kafkacrypto.jasodium;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.CryptoKeyFileFormat;
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
  private byte[] __ssk, __spk, __ek;
  private List<Byte> __versions;

  private Map<String,Map<String,byte[][]>> __eskepk;
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
      this.__spk = jasodium.crypto_sign_sk_to_pk(this.__ssk);
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
    this.__eskepk = new HashMap<String,Map<String,byte[][]>>();
  }

  public byte[] get_spk()
  {
    return this.__spk;
  }

  public byte[] sign_spk(byte[] msg)
  {
    return jasodium.crypto_sign(msg, this.__ssk);
  }

  public byte[] get_epk(String topic, String usage)
  {
    this.__eskepklock.lock();
    try {
      if (!this.__eskepk.containsKey(topic))
        this.__eskepk.put(topic, new HashMap<String,byte[][]>());
      if (!this.__eskepk.get(topic).containsKey(usage))
        this.__eskepk.get(topic).put(usage, new byte[2][]);
      this.__eskepk.get(topic).get(usage)[0] = jasodium.randombytes(jasodium.CRYPTO_SCALARMULT_CURVE25519_BYTES);
      this.__eskepk.get(topic).get(usage)[1] = jasodium.crypto_scalarmult_curve25519_base(this.__eskepk.get(topic).get(usage)[0]);
      return this.__eskepk.get(topic).get(usage)[1];
    } finally {
      this.__eskepklock.unlock();
    }
  }

  public byte[][] use_epk(String topic, String usage, byte[][] pks, boolean clear)
  {
    byte[][] rv = new byte[pks.length][];
    this.__eskepklock.lock();
    try {
      if (!this.__eskepk.containsKey(topic) || !this.__eskepk.get(topic).containsKey(usage))
        return rv;
      int i = 0;
      for (byte[] pk : pks) {
        rv[i] = jasodium.crypto_scalarmult_curve25519(this.__eskepk.get(topic).get(usage)[0],pk);
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
    ckff.versions.add(new Byte((byte)1));
    try {
      Files.write(Paths.get(file), msgpack.packb(ckff));
    } catch (IOException ioe) {
      _logger.warn("  Error writing file.",ioe);
    }
    _logger.warn("  CryptoKey Initialized. Provisioning required for successful operation.");
  }
}
