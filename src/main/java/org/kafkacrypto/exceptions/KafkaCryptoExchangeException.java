package org.kafkacrypto.exceptions;

import org.kafkacrypto.exceptions.KafkaCryptoException;

public class KafkaCryptoExchangeException extends KafkaCryptoException
{
  public KafkaCryptoExchangeException(String s)
  {
    super(s);
  }
  public KafkaCryptoExchangeException(String s, Throwable cause)
  {
    super(s, cause);
  }
}
