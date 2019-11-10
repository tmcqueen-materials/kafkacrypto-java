package org.kafkacrypto.msgs;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

import java.io.IOException;

import org.kafkacrypto.types.ByteHashMap;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.EncryptionKey;

public class EncryptionKeys extends LinkedHashMap<String,Map<byte[],EncryptionKey>> implements Msgpacker<EncryptionKeys>
{
  public EncryptionKeys unpackb(List<Value> src) throws IOException
  {
    if (src == null) return this;
    for (Value oldkey : src) {
      if (oldkey != null && oldkey.isArrayValue()) {
        EncryptionKey newkey = new EncryptionKey().unpackb(oldkey.asArrayValue().list());
        if (newkey != null)
            this.ensureContains(newkey.root);
        if (newkey != null)
          this.get(newkey.root).put(newkey.keyIndex, newkey);
      }
    }
    return this;
  }

  public void ensureContains(String s)
  {
    if (!this.containsKey(s))
      this.put(s, new ByteHashMap<EncryptionKey>());
  }

  public void packb(MessagePacker packer) throws IOException
  {
    int total = 0;
    for (String r : this.keySet())
      total += this.get(r).size();
    packer.packArrayHeader(total);
    for (String r : this.keySet())
      for (EncryptionKey ek : this.get(r).values())
        ek.packb(packer);
  }

}
