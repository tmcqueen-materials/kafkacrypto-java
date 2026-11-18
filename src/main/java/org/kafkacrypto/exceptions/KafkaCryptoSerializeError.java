package org.kafkacrypto.exceptions;

import org.kafkacrypto.exceptions.KafkaCryptoInternalError;

public class KafkaCryptoSerializeError extends KafkaCryptoInternalError
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
