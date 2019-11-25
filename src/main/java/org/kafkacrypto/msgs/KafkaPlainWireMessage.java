package org.kafkacrypto.msgs;

import java.util.List;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;

import org.kafkacrypto.KafkaCryptoMessage;
import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;

import java.io.IOException;

public class KafkaPlainWireMessage implements Msgpacker<KafkaPlainWireMessage>, KafkaCryptoMessage
{
  public byte[] msg = null;

  public KafkaPlainWireMessage()
  {
  }

  public KafkaPlainWireMessage(byte[] msg)
  {
    this();
    this.msg = msg;
  }

  public KafkaPlainWireMessage unpackb(List<Value> src) throws IOException
  {
    if (src.size() == 1 && src.get(0).isRawValue()) {
      this.msg = src.get(0).asRawValue().asByteArray();
      return this;
    }
    return null;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    msgpack.packb_recurse(packer, this.msg);
  }

  public byte[] getMessage()
  {
    return msg;
  }

  public boolean isCleartext(boolean retry)
  {
    return true;
  }

  public boolean isCleartext()
  {
    return true;
  }

  public byte[] toWire() throws IOException
  {
    return Utils.concatArrays(new byte[]{0}, msgpack.packb(this));
  }
}
