package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.SignPublicKey;
import org.kafkacrypto.msgs.KEMPublicKey;

import org.kafkacrypto.Utils;
import org.kafkacrypto.jasodium;

import java.util.List;
import org.msgpack.value.Value;

import org.msgpack.core.MessagePacker;
import java.io.IOException;

public class KEMSecretKey implements Msgpacker<KEMSecretKey>
{
  protected byte version = 0;
  protected byte[] key = null;

  public KEMSecretKey()
  {
    this.version = 0;
  }

  public KEMSecretKey(byte[] inp)
  {
    this.version = 1;
    this.key = inp;
  }

  public KEMSecretKey(SignPublicKey spk)
  {
    // we allow polymorphism of v1 keys *only*
    if (spk.version == 1) {
      this.version = 1;
      this.key = spk.key;
    }
  }

  public KEMSecretKey(KEMSecretKey ksk)
  {
    this.version = ksk.version;
    this.key = ksk.key;
  }

  public byte[] complete_kem(KEMPublicKey kpk)
  {
    if (kpk.version == 1 && this.version == 1)
      return jasodium.crypto_scalarmult_curve25519(this.key,kpk.key);
    return null;
  }

  public KEMSecretKey unpackb(List<Value> src) throws IOException
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

  public KEMSecretKey unpackb(byte[] src) throws IOException
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
    KEMSecretKey ksk = (KEMSecretKey)obj;
    if (this.version == ksk.version && this.version == 1 && this.key.equals(ksk.key))
      return true;
    return false;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(String.format("KEMSecretKey-%d", this.version));
    if (this.version == 1) {
      sb.append(", ");
      sb.append(Utils.bytesToHex(this.key));
    }
    sb.append("]");
    return sb.toString();
  }
}
