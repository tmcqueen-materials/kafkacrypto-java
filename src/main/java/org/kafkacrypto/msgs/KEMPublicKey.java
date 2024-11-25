package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.SignPublicKey;
import org.kafkacrypto.msgs.KEMSecretKey;

import org.kafkacrypto.Utils;
import org.kafkacrypto.jasodium;
import org.openquantumsafe.KeyEncapsulation;
import org.openquantumsafe.Pair;

import java.util.List;
import java.util.Arrays;
import org.msgpack.value.Value;

import org.msgpack.core.MessagePacker;
import java.io.IOException;

public class KEMPublicKey implements Msgpacker<KEMPublicKey>
{
  protected byte version = 0;
  protected byte[] key = null;
  protected byte[] key2 = null;

  public KEMPublicKey()
  {
    this.version = 0;
  }

  public KEMPublicKey(byte[] inp) throws IOException
  {
    this.unpackb(inp);
  }

  public KEMPublicKey(List<Value> src)
  {
    this.__from_list(src);
  }

  private void __from_list(List<Value> src)
  {
    // This is new style (aka list of version,keys pairs)
    this.version = src.get(0).asIntegerValue().asByte();
    if (this.version == 1) {
      this.key = src.get(1).asRawValue().asByteArray();
    } else {
      List<Value> keys = src.get(1).asArrayValue().list();
      this.key = keys.get(0).asRawValue().asByteArray();
      this.key2 = keys.get(1).asRawValue().asByteArray();
    }
  }

  public KEMPublicKey(SignPublicKey spk)
  {
    // we permit polymorphism of v1 keys *only*
    if (spk.version == 1) {
      this.version = 1;
      this.key = spk.key;
    }
  }

  public KEMPublicKey(KEMPublicKey kpk)
  {
    this.version = kpk.version;
    this.key = kpk.key;
    this.key2 = kpk.key2;
  }

  public KEMPublicKey(KEMSecretKey ksk)
  {
    if (ksk.version == 1) {
      this.version = ksk.version;
      this.key = jasodium.crypto_scalarmult_curve25519_base(ksk.key);
    } else {
      this.version = ksk.version;
      this.key = jasodium.crypto_scalarmult_curve25519_base(ksk.key);
      if (ksk.version == 2 || ksk.version == 5)
        this.key2 = ksk.key2.export_public_key();
      if (ksk.version == 3 || ksk.version == 6)
        this.key2 = ksk.key3;
    }
  }

  public KEMPublicKey unpackb(List<Value> src) throws IOException
  {
    // This is new style (aka list of version,keys pairs)
    if (src == null || src.size() < 2)
      return null;
    this.__from_list(src);
    return this;
  }

  public KEMPublicKey unpackb(byte[] src) throws IOException
  {
    if (src.length == 32) {
      // this is old style (aka byte array public key)
      this.version = 1;
      this.key = src;
    } else {
      this.__from_list(msgpack.unpackb(src));
    }
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    if (this.version == 1) {
      msgpack.packb_recurse(packer, this.key);
    } else {
      packer.packArrayHeader(2);
      msgpack.packb_recurse(packer, this.version);
      packer.packArrayHeader(2);
      msgpack.packb_recurse(packer, this.key);
      msgpack.packb_recurse(packer, this.key2);
    }
  }

  public boolean equals(Object obj)
  {
    if (obj == null) return false;
    KEMPublicKey kpk = (KEMPublicKey)obj;
    if (this.version == kpk.version) {
      switch (this.version) {
        case 1:
          if (Arrays.equals(this.key,kpk.key)) return true;
          break;
        case 2:
        case 3:
        case 5:
        case 6:
          if (Arrays.equals(this.key,kpk.key) && Arrays.equals(this.key2,kpk.key2)) return true;
          break;
        default:
          return false;
      }
    }
    return false;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(String.format("KEMPublicKey-%d", this.version));
    if (this.version > 0) {
      sb.append(", ");
      sb.append(Utils.bytesToHex(this.key));
    }
    if (this.version > 1) {
      sb.append(", ");
      sb.append(Utils.bytesToHex(this.key2));
    }
    sb.append("]");
    return sb.toString();
  }

  public byte getType() {
    if (this.version == 1)
      return 1;
    if (this.version == 2 || this.version == 3) // form a single unit
      return 2;
    if (this.version == 5 || this.version == 6) // form a single unit
      return 5;
    return this.version; // fallback
  }
}
