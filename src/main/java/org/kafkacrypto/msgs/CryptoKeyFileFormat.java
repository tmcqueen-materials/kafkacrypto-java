package org.kafkacrypto.msgs;

import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;

import org.msgpack.core.MessagePacker;
import java.util.List;
import java.util.ArrayList;
import org.msgpack.value.Value;

import java.io.IOException;

public class CryptoKeyFileFormat implements Msgpacker<CryptoKeyFileFormat>
{
  public byte version;
  public byte[] ssk;
  public byte[] ek;
  public boolean use_legacy;
  public List<Byte> versions;
  public boolean needs_update = false;

  public CryptoKeyFileFormat unpackb(List<Value> src)
  {
    if (src.size() == 2) {
      // Unversioned legacy format, so update
      this.needs_update = true;
      this.version = 1;
      this.use_legacy = true;
      this.versions = new ArrayList<Byte>();
      this.versions.add(Byte.valueOf((byte)1));
      this.ssk = src.get(0).asRawValue().asByteArray();
      this.ek = src.get(1).asRawValue().asByteArray();
    } else {
      this.version = src.get(0).asIntegerValue().asByte();
      this.ssk = src.get(1).asRawValue().asByteArray();
      this.ek = src.get(2).asRawValue().asByteArray();
      this.use_legacy = src.get(3).asBooleanValue().getBoolean();
      List<Value> vers = src.get(4).asArrayValue().list();
      this.versions = new ArrayList<Byte>();
      for (int i = 0; i < vers.size(); i++)
        this.versions.add(Byte.valueOf(vers.get(i).asIntegerValue().asByte()));
    }
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(5);
    msgpack.packb_recurse(packer, this.version);
    msgpack.packb_recurse(packer, this.ssk);
    msgpack.packb_recurse(packer, this.ek);
    msgpack.packb_recurse(packer, this.use_legacy);
    msgpack.packb_recurse(packer, this.versions);
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CKFF: [");
    sb.append(this.version + ", ");
    sb.append(Utils.bytesToHex(this.ssk));
    sb.append(", ");
    sb.append(Utils.bytesToHex(this.ek));
    sb.append(", " + this.use_legacy);
    sb.append(", " + this.versions);
    sb.append("]");
    return sb.toString();
  }
}
