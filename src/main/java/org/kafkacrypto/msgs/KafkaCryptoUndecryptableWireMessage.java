package org.kafkacrypto.msgs;

import java.util.List;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;

import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.KafkaCryptoWireMessage;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.exceptions.KafkaCryptoMessageException;

import java.io.IOException;

public class KafkaCryptoUndecryptableWireMessage extends KafkaCryptoWireMessage
{
  public byte[] rawBytes = null;

  public KafkaCryptoUndecryptableWireMessage()
  {
  }

  public KafkaCryptoUndecryptableWireMessage(byte[] rawBytes)
  {
    this();
    this.rawBytes = rawBytes;
  }

  public void setCleartext(byte[] msg)
  {
    return;
  }

  public boolean isCleartext()
  {
    return false;
  }

  public boolean isCleartext(boolean retry)
  {
    return false;
  }

  public byte[] getMessage() throws KafkaCryptoMessageException
  {
    throw new KafkaCryptoMessageException("Message not decryptable!");
  }

  public byte[] toWire() throws IOException
  {
    return this.rawBytes;
  }

  public KafkaCryptoWireMessage unpackb(List<Value> src) throws IOException
  {
    return null;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    return;
  }
}
