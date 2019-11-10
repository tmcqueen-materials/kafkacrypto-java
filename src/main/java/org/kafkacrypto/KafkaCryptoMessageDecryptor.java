package org.kafkacrypto;

public interface KafkaCryptoMessageDecryptor<E>
{
  public E decrypt(E msg);
}
