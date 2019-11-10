package org.kafkacrypto.exceptions;

import org.kafkacrypto.exceptions.KafkaCryptoException;

public class KafkaCryptoStoreException extends KafkaCryptoException
{
  public KafkaCryptoStoreException(String s)
  {
    super(s);
  }
  public KafkaCryptoStoreException(String s, Throwable cause)
  {
    super(s, cause);
  }
}
