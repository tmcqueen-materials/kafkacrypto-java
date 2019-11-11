package org.kafkacrypto;

import org.abstractj.kalium.NaCl;
import jnr.ffi.byref.LongLongByReference;

import org.kafkacrypto.Utils;
import java.util.Arrays;

import org.kafkacrypto.exceptions.KafkaCryptoInternalError;

public class jasodium
{
  // libsodium helpful wrapper
  public final static NaCl.Sodium kalium = NaCl.sodium();
  // make sure libsodium is initialized once
  private final static int kaliuminit = NaCl.init();

  private static void _check(int rv)
  {
    if (rv != 0)
      throw new KafkaCryptoInternalError("libsodium return value check failed!");
  }

  public static byte[] crypto_generichash_blake2b_salt_personal(byte[] message, int outlen, byte[] key, byte [] salt, byte[] personal)
  {
    byte[] rv = new byte[(outlen>0)?(outlen):(32)];
    jasodium._check(kalium.crypto_generichash_blake2b_salt_personal(rv, rv.length, message, (message!=null)?(message.length):(0), key, (key!=null)?(key.length):(0), salt, personal));
    return rv;
  }

  public static byte[] crypto_generichash(byte[] message, byte[] key, int outlen)
  {
    byte[] rv = new byte[(outlen>0)?(outlen):(32)];
    jasodium._check(kalium.crypto_generichash_blake2b(rv, rv.length, message, (message!=null)?(message.length):(0), key, (key!=null)?(key.length):(0)));
    return rv;
  }

  public static byte[] crypto_hash_sha256(byte[] message)
  {
    byte[] rv = new byte[32];
    jasodium._check(kalium.crypto_hash_sha256(rv, message, (message!=null)?(message.length):(0)));
    return rv;
  }

  public static byte[] randombytes(int size)
  {
    byte[] rv = new byte[size];
    kalium.randombytes(rv, rv.length);
    return rv;
  }

  public static byte[] crypto_sign(byte[] message, byte[] key)
  {
    byte[] rv = new byte[kalium.CRYPTO_SIGN_ED25519_BYTES+((message!=null)?(message.length):(0))];
    LongLongByReference len = new LongLongByReference(rv.length);
    jasodium._check(kalium.crypto_sign_ed25519(rv, len, message, (message!=null)?(message.length):(0), key));
    if (len.getValue() != rv.length) throw new KafkaCryptoInternalError("Signed message not expected size.");
    return rv;
  }

  public static byte[] crypto_sign_open(byte[] cipher, byte[] pk)
  {
    if (cipher.length < jasodium.CRYPTO_SIGN_SIGNATURE_BYTES) throw new KafkaCryptoInternalError("Signed message too short.");
    byte[] rv = new byte[cipher.length-jasodium.CRYPTO_SIGN_SIGNATURE_BYTES];
    LongLongByReference len = new LongLongByReference(rv.length);
    jasodium._check(kalium.crypto_sign_ed25519_open(rv, len, cipher, cipher.length, pk));
    if (len.getValue() != rv.length) throw new KafkaCryptoInternalError("Unsigned message not expected size.");
    return rv;
  }

  public static byte[][] crypto_sign_seed_keypair(byte[] seed)
  {
    byte[][] rv = new byte[2][];
    rv[0] = new byte[kalium.CRYPTO_SIGN_ED25519_PUBLICKEYBYTES];
    rv[1] = new byte[kalium.CRYPTO_SIGN_ED25519_SECRETKEYBYTES];
    jasodium._check(kalium.crypto_sign_ed25519_seed_keypair(rv[0], rv[1], seed));
    return rv;
  }

  public static byte[][] crypto_sign_keypair()
  {
    return jasodium.crypto_sign_seed_keypair(jasodium.randombytes(jasodium.CRYPTO_SIGN_SEED_BYTES));
  }

  private static final int CRYPTO_SIGN_SEED_BYTES = 32;
  private static final int CRYPTO_SIGN_SIGNATURE_BYTES = 64;

  public static final int CRYPTO_SECRETBOX_KEYBYTES = 32;

  private static final int CRYPTO_SECRETBOX_XSALSA20POLY1305_BOXZEROBYTES = 16;
  private static final int CRYPTO_SECRETBOX_XSALSA20POLY1305_MACBYTES = 16;
  private static final int CRYPTO_SECRETBOX_XSALSA20POLY1305_ZEROBYTES = CRYPTO_SECRETBOX_XSALSA20POLY1305_MACBYTES + CRYPTO_SECRETBOX_XSALSA20POLY1305_BOXZEROBYTES;
  public static byte[] crypto_secretbox_auto(byte[] message, byte[] key)
  {
    byte[] nonce = jasodium.randombytes(kalium.CRYPTO_SECRETBOX_XSALSA20POLY1305_NONCEBYTES);
    return Utils.concatArrays(nonce, jasodium.crypto_secretbox(message, nonce, key));
  }

  public static byte[] crypto_box_seal(byte[] message, byte[] pk)
  {
    byte[] rv = new byte[kalium.CRYPTO_BOX_SEALBYTES+message.length];
    jasodium._check(kalium.crypto_box_seal(rv, message, message.length, pk));
    return rv;
  }

  public static byte[] crypto_secretbox(byte[] message, byte[] nonce, byte[] key)
  {
    byte[] inp = (message!=null)?(Utils.concatArrays(new byte[jasodium.CRYPTO_SECRETBOX_XSALSA20POLY1305_ZEROBYTES], message)):(new byte[jasodium.CRYPTO_SECRETBOX_XSALSA20POLY1305_ZEROBYTES]);
    byte[] out = new byte[inp.length];
    jasodium._check(kalium.crypto_secretbox_xsalsa20poly1305(out, inp, inp.length, nonce, key));
    return Utils.splitArray(out,jasodium.CRYPTO_SECRETBOX_XSALSA20POLY1305_BOXZEROBYTES)[1];
  }

  public static byte[] crypto_secretbox_open_auto(byte[] cipher, byte[] key)
  {
    byte[][] bits = Utils.splitArray(cipher,kalium.CRYPTO_SECRETBOX_XSALSA20POLY1305_NONCEBYTES);
    return crypto_secretbox_open(bits[1],bits[0],key);
  }

  public static byte[] crypto_secretbox_open(byte[] cipher, byte[] nonce, byte[] key)
  {
    byte[] inp = Utils.concatArrays(new byte[jasodium.CRYPTO_SECRETBOX_XSALSA20POLY1305_BOXZEROBYTES], cipher);
    byte[] rv = new byte[inp.length];
    jasodium._check(kalium.crypto_secretbox_xsalsa20poly1305_open(rv, inp, inp.length, nonce, key));
    return Utils.splitArray(rv,jasodium.CRYPTO_SECRETBOX_XSALSA20POLY1305_ZEROBYTES)[1];
  }

  public static final int CRYPTO_SCALARMULT_CURVE25519_BYTES=kalium.CRYPTO_SCALARMULT_CURVE25519_BYTES;
  public static byte[] crypto_scalarmult_curve25519_base(byte[] key)
  {
    byte[] rv = new byte[jasodium.CRYPTO_SCALARMULT_CURVE25519_BYTES];
    jasodium._check(kalium.crypto_scalarmult_base(rv,key));
    return rv;
  }
  public static byte[] crypto_scalarmult_curve25519(byte[] scalar, byte[] point)
  {
    byte[] rv = new byte[jasodium.CRYPTO_SCALARMULT_CURVE25519_BYTES];
    jasodium._check(kalium.crypto_scalarmult_curve25519(rv, scalar, point));
    return rv;
  }

  // low level function not exposed by libsodium
  private static final int CRYPTO_SIGN_ED25519_SEEDBYTES = 32;
  public static byte[] crypto_sign_sk_to_pk(byte[] sk)
  {
    return Utils.splitArray(sk,jasodium.CRYPTO_SIGN_ED25519_SEEDBYTES)[1];
  }
}
