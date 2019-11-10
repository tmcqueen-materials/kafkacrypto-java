package org.kafkacrypto.exceptions;

public class KafkaCryptoInternalError extends Error
{
  public KafkaCryptoInternalError(String s)
  {
    super(s);
  }
  public KafkaCryptoInternalError(String s, Throwable cause)
  {
    super(s, cause);
  }
}
