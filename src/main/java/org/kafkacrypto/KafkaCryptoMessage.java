package org.kafkacrypto;

import org.kafkacrypto.msgs.KafkaCryptoWireMessage;
import org.kafkacrypto.msgs.KafkaCryptoUndecryptableWireMessage;
import org.kafkacrypto.msgs.KafkaPlainWireMessage;

import org.kafkacrypto.exceptions.KafkaCryptoMessageException;

import org.msgpack.core.MessagePackException;

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
    KafkaCryptoMessage rv = null;
    if (wire != null && wire.length > 0) {
      byte[][] msgs = Utils.splitArray(wire,1);
      try {
        if (msgs[0][0] == 1) {
          rv = new KafkaCryptoWireMessage().unpackb(msgs[1]);
        } else if (msgs[0][0] == 0 && allowPlainWireMsgs) {
          // Allowing a mix of encrypted and plain wire messages enables a third party to
          // inject arbitrary frames that are indistinguishable from encrypted ones, which
          // is a full integrity break. Thus we do not process plain messages unless
          // explicitly enabled by users (who can check the class type to determine whether
          // it was plain on the wire or not)
          rv = new KafkaPlainWireMessage().unpackb(msgs[1]);
        }
      } catch (IOException ioe) {
        KafkaCryptoMessage._logger.info("Error unpacking message", ioe);
      } catch (MessagePackException mpe) {
        KafkaCryptoMessage._logger.info("Error unpacking message", mpe);
      }
    }
    if (rv != null)
      return rv;
    // If we get here, permanently undecryptable
    return new KafkaCryptoUndecryptableWireMessage(wire);
  }
}
