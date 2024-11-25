package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.SignPublicKey;
import org.kafkacrypto.msgs.KEMPublicKey;

import org.kafkacrypto.Utils;
import org.kafkacrypto.jasodium;

import org.openquantumsafe.KeyEncapsulation;
import org.openquantumsafe.Pair;

import java.util.List;
import java.util.Arrays;
import org.msgpack.value.Value;

import org.msgpack.core.MessagePacker;
import java.io.IOException;

public class KEMSecretKey implements Msgpacker<KEMSecretKey>
{
  protected byte version = 0;
  protected byte[] key = null;
  protected KeyEncapsulation key2 = null;
  protected byte[] key3 = null;

  public KEMSecretKey()
  {
    this.version = 0;
  }

  public KEMSecretKey(byte ver)
  {
    if (ver == 1)
      this.version = 1;
      this.key = jasodium.randombytes(jasodium.CRYPTO_SCALARMULT_CURVE25519_BYTES);
    if (ver == 2) {
      this.version = 2;
      this.key = jasodium.randombytes(jasodium.CRYPTO_SCALARMULT_CURVE25519_BYTES);
      this.key2 = new KeyEncapsulation("sntrup761");
      this.key2.generate_keypair();
    }
    if (ver == 5) {
      this.version = 5;
      this.key = jasodium.randombytes(jasodium.CRYPTO_SCALARMULT_CURVE25519_BYTES);
      this.key2 = new KeyEncapsulation("ML-KEM-1024");
      this.key2.generate_keypair();
    }
  }

  public KEMSecretKey(byte[] inp) throws IOException
  {
    this.unpackb(inp);
  }

  public KEMSecretKey(List<Value> src)
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
      if (this.version == 3 || this.version == 6)
        this.key3 = keys.get(2).asRawValue().asByteArray();
      if (this.version == 2 || this.version == 3)
        this.key2 = new KeyEncapsulation("sntrup761", keys.get(1).asRawValue().asByteArray());
      if (this.version == 5 || this.version == 6)
        this.key2 = new KeyEncapsulation("ML-KEM-1024", keys.get(1).asRawValue().asByteArray());
    }
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
    this.key2 = ksk.key2;
    this.key3 = ksk.key3;
  }

  public byte[] complete_kem(KEMPublicKey kpk)
  {
    if (kpk.version == 1 && this.version == 1)
      return jasodium.crypto_scalarmult_curve25519(this.key,kpk.key);
    if (((this.version == 2 || this.version == 3) && kpk.version == 2) ||
        ((this.version == 5 || this.version == 6) && kpk.version == 5) ) {
      byte[] part0 = jasodium.crypto_scalarmult_curve25519(this.key,kpk.key);
      Pair<byte[],byte[]> part1 = this.key2.encap_secret(kpk.key2);
      if (this.version == 2)
        this.version = 3;
      if (this.version == 5)
        this.version = 6;
      this.key3 = part1.getLeft();
      return Utils.concatArrays(part0,part1.getRight());
    }
    if ((this.version == 2 && kpk.version == 3) ||
        (this.version == 5 && kpk.version == 6) ) {
      byte[] part0 = jasodium.crypto_scalarmult_curve25519(this.key,kpk.key);
      byte[] part1 = this.key2.decap_secret(kpk.key2);
      return Utils.concatArrays(part0,part1);
    }
    return null;
  }

  public KEMSecretKey unpackb(List<Value> src) throws IOException
  {
    // This is new style (aka list of version,keys pairs)
    if (src == null || src.size() < 2)
      return null;
    this.__from_list(src);
    return this;
  }

  public KEMSecretKey unpackb(byte[] src) throws IOException
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
    } else if (this.version == 2 || this.version == 5) {
      packer.packArrayHeader(2);
      msgpack.packb_recurse(packer, this.version);
      packer.packArrayHeader(2);
      msgpack.packb_recurse(packer, this.key);
      msgpack.packb_recurse(packer, this.key2.export_secret_key());
    } else if (this.version == 3 || this.version == 6) {
      packer.packArrayHeader(2);
      msgpack.packb_recurse(packer, this.version);
      packer.packArrayHeader(3);
      msgpack.packb_recurse(packer, this.key);
      msgpack.packb_recurse(packer, this.key2.export_secret_key());
      msgpack.packb_recurse(packer, this.key3);
    }
  }

  public boolean equals(Object obj)
  {
    if (obj == null) return false;
    KEMSecretKey ksk = (KEMSecretKey)obj;
    if (this.version == ksk.version) {
      switch (this.version) {
        case 1:
          if (Arrays.equals(this.key,ksk.key)) return true;
          break;
        case 2:
        case 5:
          if (Arrays.equals(this.key,ksk.key) && Arrays.equals(this.key2.export_secret_key(),ksk.key2.export_secret_key())) return true;
          break;
        case 3:
        case 6:
          if (Arrays.equals(this.key,ksk.key) && Arrays.equals(this.key2.export_secret_key(),ksk.key2.export_secret_key()) && Arrays.equals(this.key3,ksk.key3)) return true;
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
    sb.append(String.format("KEMSecretKey-%d", this.version));
    if (this.version == 1) {
      sb.append(", ");
      sb.append(Utils.bytesToHex(this.key));
    } else {
      sb.append(", ");
      sb.append(Utils.bytesToHex(this.key));
      sb.append(", ");
      sb.append(Utils.bytesToHex(this.key2.export_secret_key()));
      if (this.version == 3 || this.version == 6) {
        sb.append(", ");
        sb.append(Utils.bytesToHex(this.key3));
      }
    }
    sb.append("]");
    return sb.toString();
  }
}
