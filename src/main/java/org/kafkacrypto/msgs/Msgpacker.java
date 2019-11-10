package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.msgpack;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;
import java.util.List;
import java.io.IOException;

interface Msgpacker<E>
{
  E unpackb(List<Value> src) throws IOException;
  void packb(MessagePacker packer) throws IOException;

  default E unpackb(byte[] src) throws IOException
  {
    return this.unpackb(msgpack.unpackb(src));
  }
  default byte[] packb() throws IOException
  {
    return msgpack.packb(this);
  }
}
