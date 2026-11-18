package org.kafkacrypto.msgs;

import java.util.List;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;

import org.kafkacrypto.Utils;
import org.kafkacrypto.KafkaCryptoMessage;
import org.kafkacrypto.KafkaCryptoMessageDecryptor;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.exceptions.KafkaCryptoMessageException;

import java.io.IOException;

public class KafkaCryptoUndecryptableWireMessage implements KafkaCryptoMessage
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

}
