package org.kafkacrypto.msgs;

import java.util.Base64;
import java.util.Arrays;
import java.util.List;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.msgpack;
import org.msgpack.value.Value;
import org.msgpack.core.MessagePacker;

import java.io.IOException;

public class ByteString implements Comparable<ByteString>, Msgpacker<ByteString[]>
{
  private String strval = "";
  private byte[] byteval = new byte[0];
  private boolean caseSensitive = false;

  private static byte[] value_to_bytes(Value v)
  {
    if (v != null && v.isRawValue())
     return v.asRawValue().asByteArray();
    else
     return null;
  }

  public ByteString(Value v)
  {
    this(ByteString.value_to_bytes(v));
  }

  public ByteString(byte[] bytes)
  {
    if (bytes != null) {
      this.byteval = bytes;
      this.strval = "base64#" + Base64.getEncoder().encodeToString(bytes);
      this.caseSensitive = true;
    }
  }

  public ByteString(String str)
  {
    if (str != null) {
      this.strval = str;
      if (str.startsWith("base64#")) {
        this.byteval = Base64.getDecoder().decode(str.substring(7));
        this.caseSensitive = true;
      } else {
        this.byteval = str.getBytes();
        this.caseSensitive = false;
      }
    }
  }

  public ByteString[] unpackb(List<Value> src)
  {
    if (src == null) return null;
    ByteString[] rv = new ByteString[src.size()];
    for (int i = 0; i < src.size(); i++)
      rv[i] = new ByteString(src.get(i));
    return rv;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    msgpack.packb_recurse(packer, this.byteval);
  }

  public String toString()
  {
    return this.strval;
  }

  public int length()
  {
    if (this.caseSensitive == true)
      return this.byteval.length;
    return this.strval.length();
  }

  public byte[] getBytes()
  {
    return this.byteval;
  }

  public boolean equals(byte[] ref)
  {
    if (ref == null) return false;
    return Arrays.equals(this.byteval, ref);
  }

  public boolean equals(String ref)
  {
    if (ref == null) return false;
    if (this.caseSensitive)
      return this.strval.equals(ref);
    return this.strval.toLowerCase().equals(ref.toLowerCase());
  }

  public boolean equals(ByteString ref)
  {
    if (ref == null) return false;
    if (this.caseSensitive == true && ref.caseSensitive == true)
      return this.equals(ref.strval);
    else if (this.caseSensitive == true && ref.caseSensitive == false)
      return ref.equals(this.strval);
    return this.equals(ref.strval);
  }

  public boolean equals(Object ref)
  {
    if (this == ref) return true;
    if (ref == null) return false;
    if (this.getClass() != ref.getClass()) return false;
    return this.equals((ByteString)ref);
  }

  public int hashCode()
  {
    if (this.caseSensitive == true)
      return this.strval.hashCode();
    return this.strval.toLowerCase().hashCode();
  }

  public int compareTo(ByteString ref)
  {
    if (this.caseSensitive == true && ref.caseSensitive == true)
      return this.strval.compareTo(ref.strval);
    return this.strval.toLowerCase().compareTo(ref.strval.toLowerCase());
  }
}
