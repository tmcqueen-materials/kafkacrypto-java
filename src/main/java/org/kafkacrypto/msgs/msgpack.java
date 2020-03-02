package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.ValueType;
import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.math.BigInteger;
import java.io.IOException;
import java.lang.reflect.Array;

// msgpack
public class msgpack
{

  public static List<Value> unpackb(byte[] input) throws IOException
  {
    MessagePack.UnpackerConfig cfg = new MessagePack.UnpackerConfig();
    cfg.withAllowReadingBinaryAsString(false);
    MessageUnpacker unpacker = cfg.newUnpacker(input);
    List<Value> rv = new ArrayList<Value>();
    while (unpacker.hasNext())
      rv.add(unpacker.unpackValue());
    unpacker.close();
    if (rv.size() == 1 && rv.get(0).isArrayValue())
      rv = rv.get(0).asArrayValue().list();
    return rv;
  }

  protected static void packb_recurse(MessagePacker packer, boolean input) throws IOException
  {
    packer.packBoolean(input);
  }
  protected static void packb_recurse(MessagePacker packer, byte input) throws IOException
  {
    packer.packByte(input);
  }
  protected static void packb_recurse(MessagePacker packer, short input) throws IOException
  {
    packer.packShort(input);
  }
  protected static void packb_recurse(MessagePacker packer, int input) throws IOException
  {
    packer.packInt(input);
  }
  protected static void packb_recurse(MessagePacker packer, long input) throws IOException
  {
    packer.packLong(input);
  }
  protected static void packb_recurse(MessagePacker packer, float input) throws IOException
  {
    packer.packFloat(input);
  }
  protected static void packb_recurse(MessagePacker packer, double input) throws IOException
  {
    packer.packDouble(input);
  }
  protected static void packb_recurse(MessagePacker packer, byte[] input) throws IOException
  {
    if (input == null) {
      packer.packNil();
    } else {
      packer.packBinaryHeader(input.length);
      packer.writePayload(input);
    }
  }
  protected static <E> void packb_recurse(MessagePacker packer, Msgpacker<E> input) throws IOException
  {
    if (input == null) {
      packer.packNil();
    } else {
      input.packb(packer);
    }
  }
  protected static <E> void packb_recurse(MessagePacker packer, Collection<E> input) throws IOException
  {
    if (input == null) {
      packer.packNil();
    } else {
      packer.packArrayHeader(input.size());
      for (E o : input)
        msgpack.packb_recurse(packer, o);
    }
  }
  protected static <K,V> void packb_recurse(MessagePacker packer, Map<K,V> input) throws IOException
  {
    if (input == null) {
      packer.packNil();
    } else {
      packer.packMapHeader(input.size());
      for (K ok : input.keySet()) {
        msgpack.packb_recurse(packer, ok);
        msgpack.packb_recurse(packer, input.get(ok));
      }
    }
  }
  protected static void packb_recurse(MessagePacker packer, BigInteger input) throws IOException
  {
    if (input == null) {
      packer.packNil();
    } else {
      packer.packBigInteger(input);
    }
  }
  protected static void packb_recurse(MessagePacker packer, String input) throws IOException
  {
    if (input == null) {
      packer.packNil();
    } else {
      packer.packString(input);
    }
  }
  protected static <T> void packb_recurse(MessagePacker packer, T input) throws IOException
  {
    if (input == null) {
      packer.packNil();
    } else if (input instanceof byte[]) {
      msgpack.packb_recurse(packer, (byte[])input);
    } else if (input instanceof Byte) {
      msgpack.packb_recurse(packer, (byte)input);
    } else if (input instanceof Short) {
      msgpack.packb_recurse(packer, (short)input);
    } else if (input instanceof Integer) {
      msgpack.packb_recurse(packer, (int)input);
    } else if (input instanceof Long) {
      msgpack.packb_recurse(packer, (long)input);
    } else if (input instanceof BigInteger) {
      msgpack.packb_recurse(packer, (BigInteger)input);
    } else if (input instanceof Float) {
      msgpack.packb_recurse(packer, (float)input);
    } else if (input instanceof Double) {
      msgpack.packb_recurse(packer, (double)input);
    } else if (input instanceof Msgpacker<?>) {
      ((Msgpacker<?>)input).packb(packer);
    } else if (input instanceof Collection<?>) {
      msgpack.packb_recurse(packer, (Collection<?>)input);
    } else if (input instanceof Map<?,?>) {
      msgpack.packb_recurse(packer, (Map<?,?>)input);
    } else if (input.getClass().isArray()) {
      List<Object> tmp = new ArrayList<Object>();
      for (int i = 0; i < Array.getLength(input); i++)
        tmp.add(Array.get(input, i));
      msgpack.packb_recurse(packer, tmp);
    } else if (input instanceof Value) {
      // Handle MessagePack Value/Variable formats for seamless read/modify/writes
      Value i = (Value)input;
      switch (i.getValueType()) {
        case BOOLEAN:
          msgpack.packb_recurse(packer,i.asBooleanValue().getBoolean());
          break;
        case INTEGER:
          IntegerValue iv = i.asIntegerValue();
          if (iv.isInByteRange()) {
            msgpack.packb_recurse(packer,iv.asByte());
          } else if (iv.isInShortRange()) {
            msgpack.packb_recurse(packer,iv.asShort());
          } else if (iv.isInIntRange()) {
            msgpack.packb_recurse(packer,iv.asInt());
          } else if (iv.isInLongRange()) {
            msgpack.packb_recurse(packer,iv.asLong());
          } else {
            msgpack.packb_recurse(packer,iv.asBigInteger());
          }
          break;
        case FLOAT:
          // This one case is a loss of information as we cannot directly tell if it was a float or a double.
          if (i.asRawValue().asByteArray().length > 5)
            msgpack.packb_recurse(packer,i.asFloatValue().toDouble());
          else
            msgpack.packb_recurse(packer,i.asFloatValue().toFloat());
          break;
        case ARRAY:
          msgpack.packb_recurse(packer,i.asArrayValue().list());
          break;
        case MAP:
          msgpack.packb_recurse(packer,i.asMapValue().map());
          break;
        case BINARY:
        case STRING:
          msgpack.packb_recurse(packer,i.asRawValue().asByteArray());
          break;
        case EXTENSION:
          packer.packExtensionTypeHeader((byte)1,i.asExtensionValue().getType());
          packer.writePayload(i.asExtensionValue().getData());
          break;
        case NIL:
        default:
          packer.packNil();
          break;
      }
    } else {
      msgpack.packb_recurse(packer, (String)input);
    }
  }

  public static <T> byte[] packb(T input) throws IOException
  {
    MessageBufferPacker packer = MessagePack.DEFAULT_PACKER_CONFIG.withStr8FormatSupport(false).newBufferPacker(); //MessagePack.newDefaultBufferPacker();
    msgpack.packb_recurse(packer, input);
    packer.close();
    return packer.toByteArray();
  }
}
