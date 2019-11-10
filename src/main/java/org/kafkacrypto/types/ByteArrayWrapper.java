package org.kafkacrypto.types;

import java.util.Arrays;

class ByteArrayWrapper {
  protected final byte[] data;

  public ByteArrayWrapper(byte[] data)
  {
    if (data == null)
      throw new NullPointerException();
    this.data = data;
  }

  public boolean equals(Object other)
  {
    if (!(other instanceof ByteArrayWrapper))
      return false;
    return Arrays.equals(data, ((ByteArrayWrapper) other).data);
  }

  public int hashCode()
  {
    return Arrays.hashCode(data);
  }
}

