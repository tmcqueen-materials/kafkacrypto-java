package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.msgpack;

import org.kafkacrypto.Utils;
import org.kafkacrypto.jasodium;
import org.openquantumsafe.Signature;

import java.util.List;
import org.msgpack.value.Value;

import org.msgpack.core.MessagePacker;
import java.io.IOException;

import java.util.Arrays;

public class SignSecretKey implements Msgpacker<SignSecretKey>
{
  protected byte version;
  protected byte[] key;
  protected Signature key2;

  public SignSecretKey()
  {
    this.version = 0;
  }

  public SignSecretKey(byte ver)
  {
    if (ver == 1)
      this.version = 1;
      this.key = jasodium.crypto_sign_keypair()[1];
    if (ver == 4) {
      this.version = 4;
      this.key = jasodium.crypto_sign_keypair()[1];
      this.key2 = new Signature("SPHINCS+-SHAKE-128f-simple");
      this.key2.generate_keypair();
    }
  }

  public SignSecretKey(byte[] inp)
  {
    this.version = 1;
    this.key = inp;
  }

  public SignSecretKey(List<Value> src)
  {
    this.version = src.get(0).asIntegerValue().asByte();
    if (this.version == 1) {
      this.key = src.get(1).asRawValue().asByteArray();
    } else {
      List<Value> keys = (List<Value>)src.get(1).asArrayValue();
      this.key = keys.get(0).asRawValue().asByteArray();
      if (this.version == 4)
        this.key2 = new Signature("SPHINCS+-SHAKE-128f-simple", keys.get(1).asRawValue().asByteArray());
    }
  }

  public SignSecretKey(SignSecretKey ssk)
  {
    this.version = ssk.version;
    this.key = ssk.key;
    this.key2 = ssk.key2;
  }

  public SignSecretKey unpackb(List<Value> src) throws IOException
  {
    // This is new style (aka list of version,keys pairs)
    if (src == null || src.size() < 2)
      return null;
    this.version = src.get(0).asIntegerValue().asByte();
    if (this.version == 1) {
      this.key = src.get(1).asRawValue().asByteArray();
    } else {
      List<Value> keys = (List<Value>)src.get(1).asArrayValue();
      this.key = keys.get(0).asRawValue().asByteArray();
      if (this.version == 4)
        this.key2 = new Signature("SPHINCS+-SHAKE-128f-simple", keys.get(1).asRawValue().asByteArray());
    }
    return this;
  }

  public SignSecretKey unpackb(byte[] src) throws IOException
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
      msgpack.packb_recurse(packer, this.key2.export_secret_key());
    }
  }

  public boolean equals(Object obj)
  {
    SignSecretKey ssk = (SignSecretKey)obj;
    if (this.version == ssk.version) {
      switch (this.version) {
        case 1:
          if (Arrays.equals(this.key,ssk.key)) return true;
          break;
        case 2:
        case 3:
        case 5:
        case 6:
          if (Arrays.equals(this.key,ssk.key) && Arrays.equals(this.key2.export_secret_key(),ssk.key2.export_secret_key())) return true;
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
    sb.append(String.format("SignSecretKey-%d", this.version));
    if (this.version > 0) {
      sb.append(", ");
      sb.append(Utils.bytesToHex(this.key));
    }
    if (this.version > 1) {
      sb.append(", ");
      sb.append(Utils.bytesToHex(this.key2.export_secret_key()));
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

  public byte[] crypto_sign(byte[] inp)
  {
    if (this.version == 1)
      return jasodium.crypto_sign(inp, this.key);
    if (this.version == 4) {
      byte[] edmsg = jasodium.crypto_sign(inp, this.key);
      byte[] dsctx = {0,0};
      byte[] sig = this.key2.sign(Utils.concatArrays(dsctx,edmsg));
      return Utils.concatArrays(sig,edmsg);
    }
    return null;
  }

  public byte getType() {
    return this.version;
  }
}
