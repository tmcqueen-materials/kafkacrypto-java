package org.kafkacrypto.exceptions;

import org.kafkacrypto.exceptions.KafkaCryptoException;

public class KafkaCryptoKeyException extends KafkaCryptoException
{
  public KafkaCryptoKeyException(String s)
  {
    super(s);
  }
  public KafkaCryptoKeyException(String s, Throwable cause)
  {
    super(s, cause);
  }
}
