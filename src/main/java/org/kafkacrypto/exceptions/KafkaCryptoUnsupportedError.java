package org.kafkacrypto.exceptions;

public class KafkaCryptoUnsupportedError extends Error
{
  public KafkaCryptoUnsupportedError(String s)
  {
    super(s);
  }
  public KafkaCryptoUnsupportedError(String s, Throwable cause)
  {
    super(s, cause);
  }
}
