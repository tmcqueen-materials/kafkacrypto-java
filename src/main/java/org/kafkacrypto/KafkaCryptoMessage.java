package org.kafkacrypto;

import org.kafkacrypto.msgs.KafkaCryptoWireMessage;
import org.kafkacrypto.msgs.KafkaPlainWireMessage;

import org.kafkacrypto.exceptions.KafkaCryptoMessageException;

import java.io.IOException;

public interface KafkaCryptoMessage
{
  public boolean isCleartext();
  public byte[] getMessage() throws KafkaCryptoMessageException;
  public byte[] toWire() throws IOException;

  public static KafkaCryptoMessage fromWire(byte[] wire)
  {
    if (wire == null || wire.length < 1) return null;
    byte[][] msgs = Utils.splitArray(wire,1);
    try {
      if (msgs[0][0] == 1)
        return new KafkaCryptoWireMessage().unpackb(msgs[1]);
      if (msgs[0][0] == 0)
        return new KafkaPlainWireMessage().unpackb(msgs[1]);
    } catch (IOException ioe) {
    }
    return null;
  }
}
