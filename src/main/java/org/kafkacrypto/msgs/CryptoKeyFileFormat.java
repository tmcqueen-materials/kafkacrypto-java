package org.kafkacrypto.msgs;

import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;

import org.msgpack.core.MessagePacker;
import java.util.List;
import org.msgpack.value.Value;

import java.io.IOException;

public class CryptoKeyFileFormat implements Msgpacker<CryptoKeyFileFormat>
{
  public byte[] ssk;
  public byte[] ek;

  public CryptoKeyFileFormat unpackb(List<Value> src)
  {
    this.ssk = src.get(0).asRawValue().asByteArray();
    this.ek = src.get(1).asRawValue().asByteArray();
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(2);
    msgpack.packb_recurse(packer, this.ssk);
    msgpack.packb_recurse(packer, this.ek);
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CKFF: [");
    sb.append(Utils.bytesToHex(this.ssk));
    sb.append(", ");
    sb.append(Utils.bytesToHex(this.ek));
    sb.append("]");
    return sb.toString();
  }
}
