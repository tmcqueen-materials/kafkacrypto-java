package org.kafkacrypto;

import org.kafkacrypto.exceptions.KafkaCryptoGeneratorException;
import org.kafkacrypto.jasodium;
import org.kafkacrypto.Utils;
import java.util.Arrays;

public class KeyGenerator
{
  public final static int SECRETSIZE = 32;
  public final static int KEYSIZE = 32;
  public final static int NONCESIZE = 24;
  public final static int SALTSIZE = 16;
  public final static byte[] MSG = new byte[0];

  private byte[] __secret = null;
  private byte[] __ctx = null;
  private byte[] __salt = null;

  public KeyGenerator() throws KafkaCryptoGeneratorException
  {
    this(null, new byte[]{'g','e','n','e','r','a','t','o','r',0,0,0,0,0,0,0});
  }

  public KeyGenerator(byte[] secret, byte[] ctx) throws KafkaCryptoGeneratorException
  {
    this.__secret = null;
    this.__ctx = ctx;
    if (secret != null)
      this.rekey(secret);
  }

  public void rekey(byte[] secret) throws KafkaCryptoGeneratorException
  {
    if ((secret.length != this.SECRETSIZE) || (this.__secret != null && !Arrays.equals(this.__secret, secret)))
      throw new KafkaCryptoGeneratorException("Secret is malformed!");
    this.__secret = secret;
    this.__salt = new byte[this.SALTSIZE];
  }

  public byte[][] generate() throws KafkaCryptoGeneratorException
  {
    return this.generate(this.__ctx, this.KEYSIZE, this.NONCESIZE);
  }

  public byte[][] generate(byte[] salt) throws KafkaCryptoGeneratorException
  {
    return this.generate(salt, this.__ctx, this.KEYSIZE, this.NONCESIZE);
  }

  public byte[][] generate(byte[] ctx, int keysize, int noncesize) throws KafkaCryptoGeneratorException
  {
    return this.generate(this.__salt, ctx, keysize, noncesize);
  }

  public byte[][] generate(byte[] salt, byte[] ctx, int keysize, int noncesize) throws KafkaCryptoGeneratorException
  {
    return this.generate(this.MSG, salt, ctx, keysize, noncesize);
  }

  public byte[][] generate(byte[] msg, byte[] salt, byte[] ctx, int keysize, int noncesize) throws KafkaCryptoGeneratorException
  {
    if (ctx == null || ctx.length < 1)
      ctx = this.__ctx;
    if (salt == null || salt.length < 1)
      salt = this.__salt;
    if (msg == null)
      throw new KafkaCryptoGeneratorException("Message is null!");

    byte[] keynonce = jasodium.crypto_generichash_blake2b_salt_personal(msg, keysize+noncesize, this.__secret, salt, ctx);
    // salt is incremented little endian
    this.__salt = Utils.littleIncrement(this.__salt);

    byte[][] kn = Utils.splitArray(keynonce, keysize);
    return kn;
  }

  public byte[] salt()
  {
    return this.__salt;
  }

  public static KeyGenerator[] get_key_value_generators(byte[] secret) throws KafkaCryptoGeneratorException
  {
    KeyGenerator[] kg = new KeyGenerator[2];
    kg[0] = new KeyGenerator(secret,new byte[]{'k','e','y',0,0,0,0,0,0,0,0,0,0,0,0,0});
    kg[1] = new KeyGenerator(secret,new byte[]{'v','a','l','u','e',0,0,0,0,0,0,0,0,0,0,0});
    return kg;
  }
}
