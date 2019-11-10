package org.kafkacrypto.exceptions;

import org.kafkacrypto.exceptions.KafkaCryptoException;

public class KafkaCryptoMessageException extends KafkaCryptoException
{
  public KafkaCryptoMessageException(String s)
  {
    super(s);
  }
  public KafkaCryptoMessageException(String s, Throwable cause)
  {
    super(s, cause);
  }
}
