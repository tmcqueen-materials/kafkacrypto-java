package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.SignPublicKey;
import org.kafkacrypto.msgs.KEMSecretKey;

import org.kafkacrypto.Utils;
import org.kafkacrypto.jasodium;

import java.util.List;
import org.msgpack.value.Value;

import org.msgpack.core.MessagePacker;
import java.io.IOException;

public class KEMPublicKey implements Msgpacker<KEMPublicKey>
{
  protected byte version = 0;
  protected byte[] key = null;

  public KEMPublicKey()
  {
    this.version = 0;
  }

  public KEMPublicKey(byte[] inp)
  {
    this.version = 1;
    this.key = inp;
  }

  public KEMPublicKey(SignPublicKey spk)
  {
    this.version = 1;
    this.key = spk.key;
  }

  public KEMPublicKey(KEMPublicKey kpk)
  {
    this.version = kpk.version;
    this.key = kpk.key;
  }

  public KEMPublicKey(KEMSecretKey ksk)
  {
    if (ksk.version == 1) {
      this.version = ksk.version;
      this.key = jasodium.crypto_scalarmult_curve25519_base(ksk.key);
    }
  }

  public KEMPublicKey unpackb(List<Value> src) throws IOException
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

  public KEMPublicKey unpackb(byte[] src) throws IOException
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
    KEMPublicKey kpk = (KEMPublicKey)obj;
    if (this.version == kpk.version && this.version == 1 && this.key.equals(kpk.key))
      return true;
    return false;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(String.format("KEMPublicKey-%d", this.version));
    if (this.version == 1) {
      sb.append(", ");
      sb.append(Utils.bytesToHex(this.key));
    }
    sb.append("]");
    return sb.toString();
  }
}
