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
    return KafkaCryptoMessage.fromWire(wire, false);
  }

  public static KafkaCryptoMessage fromWire(byte[] wire, boolean allowPlainWireMsgs)
  {
    if (wire == null || wire.length < 1) return null;
    byte[][] msgs = Utils.splitArray(wire,1);
    try {
      if (msgs[0][0] == 1)
        return new KafkaCryptoWireMessage().unpackb(msgs[1]);
      // Allowing a mix of encrypted and plain wire messages enables a third party to
      // inject arbitrary frames that are indistinguishable from encrypted ones, which
      // is a full integrity break. Thus we do not process plain messages unless
      // explicitly enabled by users (who can check the class type to determine whether
      // it was plain on the wire or not)
      if (msgs[0][0] == 0 && allowPlainWireMsgs)
        return new KafkaPlainWireMessage().unpackb(msgs[1]);
    } catch (IOException ioe) {
      KafkaCryptoMessage._logger.info("Error unpacking message", ioe);
    }
    return null;
  }
}
