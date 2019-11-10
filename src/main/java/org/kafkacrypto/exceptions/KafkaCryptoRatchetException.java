package org.kafkacrypto.exceptions;

import org.kafkacrypto.exceptions.KafkaCryptoException;

public class KafkaCryptoRatchetException extends KafkaCryptoException
{
  public KafkaCryptoRatchetException(String s)
  {
    super(s);
  }
  public KafkaCryptoRatchetException(String s, Throwable cause)
  {
    super(s, cause);
  }
}
