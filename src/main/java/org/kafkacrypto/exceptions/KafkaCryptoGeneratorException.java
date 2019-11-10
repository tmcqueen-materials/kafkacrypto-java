package org.kafkacrypto.exceptions;

import org.kafkacrypto.exceptions.KafkaCryptoException;

public class KafkaCryptoGeneratorException extends KafkaCryptoException
{
  public KafkaCryptoGeneratorException(String s)
  {
    super(s);
  }
  public KafkaCryptoGeneratorException(String s, Throwable cause)
  {
    super(s, cause);
  }
}
