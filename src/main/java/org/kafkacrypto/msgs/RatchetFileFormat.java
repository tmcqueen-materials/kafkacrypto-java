package org.kafkacrypto.msgs;

import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;

import org.msgpack.core.MessagePacker;
import java.util.List;
import org.msgpack.value.Value;

import java.math.BigInteger;
import java.io.IOException;

public class RatchetFileFormat implements Msgpacker<RatchetFileFormat>
{
  public BigInteger keyidx;
  public byte[] secret;

  public RatchetFileFormat unpackb(List<Value> src)
  {
    this.keyidx = src.get(0).asIntegerValue().asBigInteger();
    this.secret = src.get(1).asRawValue().asByteArray();
    return this;
  }
  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(2);
    msgpack.packb_recurse(packer, this.keyidx);
    msgpack.packb_recurse(packer, this.secret);
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("RFF: [");
    sb.append(this.keyidx.toString());
    sb.append(", ");
    sb.append(Utils.bytesToHex(this.secret));
    sb.append("]");
    return sb.toString();
  }
}
