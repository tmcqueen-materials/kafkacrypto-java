package org.kafkacrypto.exceptions;

import org.kafkacrypto.exceptions.KafkaCryptoException;

public class KafkaCryptoBaseException extends KafkaCryptoException
{
  public KafkaCryptoBaseException(String s)
  {
    super(s);
  }
  public KafkaCryptoBaseException(String s, Throwable cause)
  {
    super(s, cause);
  }
}
