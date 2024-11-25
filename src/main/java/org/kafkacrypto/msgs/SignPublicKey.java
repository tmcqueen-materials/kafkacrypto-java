package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.PQSignature;

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
  protected byte[] key2;

  public SignPublicKey()
  {
    this.version = 0;
  }

  public SignPublicKey(byte[] inp)
  {
    this.version = 1;
    this.key = inp;
  }

  public SignPublicKey(List<Value> src)
  {
    this.version = src.get(0).asIntegerValue().asByte();
    if (this.version == 1) {
      this.key = src.get(1).asRawValue().asByteArray();
    } else {
      List<Value> keys = (List<Value>)src.get(1).asArrayValue();
      this.key = keys.get(0).asRawValue().asByteArray();
      this.key2 = keys.get(1).asRawValue().asByteArray();
    }
  }

  public SignPublicKey(KEMPublicKey kpk)
  {
    this.version = kpk.version;
    this.key = kpk.key;
    this.key2 = kpk.key2;
  }

  public SignPublicKey(SignSecretKey ssk)
  {
    if (ssk.version == 1) {
      this.version = ssk.version;
      this.key = jasodium.crypto_sign_sk_to_pk(ssk.key);
    } else {
      this.version = ssk.version;
      this.key = jasodium.crypto_sign_sk_to_pk(ssk.key);
      if (ssk.version == 4)
        this.key2 = ssk.key2.export_public_key();
    }
  }

  public SignPublicKey unpackb(List<Value> src) throws IOException
  {
    // This is new style (aka list of version,keys pairs)
    if (src == null || src.size() < 2)
      return null;
    this.version = src.get(0).asIntegerValue().asByte();
    if (this.version != 1 && this.version != 4)
      return null;
    if (this.version == 1) {
      this.key = src.get(1).asRawValue().asByteArray();
    } else {
      List<Value> keys = src.get(1).asArrayValue().list();
      this.key = keys.get(0).asRawValue().asByteArray();
      this.key2 = keys.get(1).asRawValue().asByteArray();
    }
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
    SignPublicKey spk = (SignPublicKey)obj;
    if (this.version == spk.version) {
      switch (this.version) {
        case 1:
          if (Arrays.equals(this.key,spk.key)) return true;
          break;
        case 4:
          if (Arrays.equals(this.key,spk.key) && Arrays.equals(this.key2,spk.key2)) return true;
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
    sb.append(String.format("SignPublicKey-%d", this.version));
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

  public byte[] getBytes()
  {
    if (this.version == 1)
      return this.key;
    if (this.version == 4)
      try {
        return msgpack.packb(this);
      } catch (IOException ioe) {
        return null;
      }
    return null;
  }

  public byte getType() {
    return this.version;
  }

  public byte[] crypto_sign_open(byte[] inp)
  {
    if (this.version == 1)
      return jasodium.crypto_sign_open(inp, this.key);
    if (this.version == 4) {
      byte[][] sigmsg = Utils.splitArray(inp, 17088);
      byte[] dsctx = { 0, 0 };
      if ((new PQSignature("SPHINCS+-SHAKE-128f-simple")).verify(Utils.concatArrays(dsctx,sigmsg[1]), sigmsg[0], this.key2))
        return jasodium.crypto_sign_open(sigmsg[1], this.key);
    }
    return null;
  }
}
