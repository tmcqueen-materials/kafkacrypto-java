package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.CertPoison;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePacker;
import java.util.List;
import java.util.ArrayList;
import org.msgpack.value.Value;

import java.io.IOException;

public class CertPoisons extends ArrayList<CertPoison> implements Msgpacker<CertPoisons>
{

  public CertPoisons unpackb(List<Value> src) throws IOException
  {
    for (int i = 0; i < src.size(); i++)
      this.add(CertPoison.unpackb(src.get(i).asArrayValue().list()));
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    msgpack.packb_recurse(packer, this.packb_internal());
  }

  public byte[] packb_internal() throws IOException
  {
    MessageBufferPacker packer2 = MessagePack.DEFAULT_PACKER_CONFIG.withStr8FormatSupport(false).newBufferPacker();
    packer2.packArrayHeader(this.size());
    for (int i = 0; i < this.size(); i++)
      msgpack.packb_recurse(packer2, this.get(i));
    packer2.close();
    return packer2.toByteArray();
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < this.size(); i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(this.get(i).toString());
    }
    sb.append("]");
    return sb.toString();
  }
}
