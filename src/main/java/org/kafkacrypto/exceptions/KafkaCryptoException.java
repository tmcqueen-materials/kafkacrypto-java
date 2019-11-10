package org.kafkacrypto.exceptions;

public class KafkaCryptoException extends Exception
{
  public KafkaCryptoException(String s)
  {
    super(s);
  }
  public KafkaCryptoException(String s, Throwable cause)
  {
    super(s, cause);
  }
}
