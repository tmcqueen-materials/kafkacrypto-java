package org.kafkacrypto;

import org.kafkacrypto.msgs.KafkaCryptoWireMessage;
import org.kafkacrypto.msgs.KafkaPlainWireMessage;

import org.kafkacrypto.exceptions.KafkaCryptoMessageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public interface KafkaCryptoMessage
{
  static final Logger _logger = LoggerFactory.getLogger("kafkacrypto-java.kafkacryptomessage");
  public boolean isCleartext();
  public boolean isCleartext(boolean retry);
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
      KafkaCryptoMessage._logger.info("Error unpacking message", ioe);
    }
    return null;
  }
}
