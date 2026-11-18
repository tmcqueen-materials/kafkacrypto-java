package org.kafkacrypto.exceptions;

import org.apache.kafka.common.errors.SerializationException;

public class KafkaCryptoSerializeError extends SerializationException
{
  public KafkaCryptoSerializeError(String s)
  {
    super(s);
  }
  public KafkaCryptoSerializeError(String s, Throwable cause)
  {
    super(s, cause);
  }
}
