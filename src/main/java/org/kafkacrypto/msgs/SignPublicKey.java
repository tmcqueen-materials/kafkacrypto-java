package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.msgpack;

import org.kafkacrypto.Utils;
import org.kafkacrypto.jasodium;

import java.util.List;
import org.msgpack.value.Value;

import org.msgpack.core.MessagePacker;
import java.io.IOException;

import java.util.Arrays;

public class SignPublicKey implements Msgpacker<SignPublicKey>
{
  protected byte version;
  protected byte[] key;

  public SignPublicKey()
  {
    this.version = 0;
  }

  public SignPublicKey(byte[] inp)
  {
    this.version = 1;
    this.key = inp;
  }

  public SignPublicKey(KEMPublicKey kpk)
  {
    this.version = kpk.version;
    this.key = kpk.key;
  }

  public SignPublicKey unpackb(List<Value> src) throws IOException
  {
    // This is new style (aka list of version,keys pairs)
    if (src == null || src.size() < 2)
      return null;
    this.version = src.get(0).asIntegerValue().asByte();
    if (this.version == 1) {
      this.key = src.get(1).asRawValue().asByteArray();
    } // silently ignore unsupported options
    return this;
  }

  public SignPublicKey unpackb(byte[] src) throws IOException
  {
    // this is old style (aka byte array public key)
    this.version = 1;
    this.key = src;
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    msgpack.packb_recurse(packer, this.key);
  }

  public boolean equals(Object obj)
  {
    SignPublicKey spk = (SignPublicKey)obj;
    if (this.version == spk.version && this.version == 1 && Arrays.equals(this.key,spk.key))
      return true;
    return false;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    if (this.version == 1) {
      sb.append("Ed25519, ");
      sb.append(Utils.bytesToHex(this.key));
    }
    sb.append(")");
    return sb.toString();
  }

  public byte[] getBytes()
  {
    if (this.version == 1)
      return this.key;
    return null;
  }

  public byte[] crypto_sign_open(byte[] inp)
  {
    if (this.version == 1)
      return jasodium.crypto_sign_open(inp, this.key);
    return null;
  }
}
