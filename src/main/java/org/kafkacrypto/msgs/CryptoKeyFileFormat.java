package org.kafkacrypto.msgs;

import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.SignSecretKey;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;

import org.msgpack.core.MessagePacker;
import java.util.List;
import java.util.ArrayList;
import org.msgpack.value.Value;

import java.io.IOException;

public class CryptoKeyFileFormat implements Msgpacker<CryptoKeyFileFormat>
{
  public byte version = 2;
  public List<SignSecretKey> ssk = new ArrayList<SignSecretKey>();
  public byte[] ek = null;
  public boolean use_legacy;
  public List<Byte> versions = new ArrayList<Byte>();
  public boolean needs_update = false;

  public CryptoKeyFileFormat unpackb(List<Value> src) throws IOException
  {
    if (src.size() == 2) {
      // Unversioned legacy format, so update
      this.needs_update = true;
      this.version = 2;
      this.use_legacy = true;
      this.versions.add(Byte.valueOf((byte)1));
      this.ssk.add(new SignSecretKey(src.get(0).asRawValue().asByteArray()));
      this.ek = src.get(1).asRawValue().asByteArray();
    } else {
      this.version = src.get(0).asIntegerValue().asByte();
      this.ek = src.get(2).asRawValue().asByteArray();
      this.use_legacy = src.get(3).asBooleanValue().getBoolean();
      List<Value> vers = src.get(4).asArrayValue().list();
      for (int i = 0; i < vers.size(); i++)
        this.versions.add(Byte.valueOf(vers.get(i).asIntegerValue().asByte()));
      if (this.version == 1) {
        this.version = 2;
        this.ssk.add(new SignSecretKey(src.get(1).asRawValue().asByteArray()));
        this.needs_update = true;
      } else {
        List<Value> sks = src.get(1).asArrayValue().list();
        for (int i = 0; i < sks.size(); i++) {
          this.ssk.add(new SignSecretKey(sks.get(i).asRawValue().asByteArray()));
        }
      }
    }
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(5);
    msgpack.packb_recurse(packer, this.version);
    packer.packArrayHeader(this.ssk.size());
    for (int i = 0; i < this.ssk.size(); i++)
      if (this.ssk.get(i).getType() == 1)
        msgpack.packb_recurse(packer, this.ssk.get(i));
      else
        msgpack.packb_recurse(packer, msgpack.packb(this.ssk.get(i)));
    msgpack.packb_recurse(packer, this.ek);
    msgpack.packb_recurse(packer, this.use_legacy);
    msgpack.packb_recurse(packer, this.versions);
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CKFF: [");
    sb.append(this.version + ", [");
    for (int i = 0; i < this.ssk.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(this.ssk.get(i).toString());
    }
    sb.append("], ");
    sb.append(Utils.bytesToHex(this.ek));
    sb.append(", " + this.use_legacy);
    sb.append(", " + this.versions);
    sb.append("]");
    return sb.toString();
  }
}
