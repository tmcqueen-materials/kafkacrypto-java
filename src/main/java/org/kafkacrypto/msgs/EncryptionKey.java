package org.kafkacrypto.msgs;

import java.util.List;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;

import org.kafkacrypto.KeyGenerator;
import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.exceptions.KafkaCryptoException;

import java.io.IOException;

public class EncryptionKey implements Msgpacker<EncryptionKey>
{
  public String root = null;
  public byte[] keyIndex = null;
  public byte[] key = null;
  public double birth = 0.0;
  /* these items are for tracking filling out this key and never persisted to disk */
  public double creation = 0.0;
  public boolean acquired = false;
  public double last_acquire_time = 0;
  /* these two items are constructed from the key and thus are also not persisted to disk */
  public KeyGenerator keygen = null;
  public KeyGenerator valgen = null;

  public EncryptionKey()
  {
    this.creation = Utils.currentTime();
  }

  public EncryptionKey(String root, byte[] ki)
  {
    this();
    this.root = root;
    this.keyIndex = ki;
    this.birth = Utils.currentTime();
  }

  public void setKey(byte[] k) throws KafkaCryptoException
  {
    this.key = k;
    KeyGenerator[] kgs = KeyGenerator.get_key_value_generators(k);
    this.keygen = kgs[0];
    this.valgen = kgs[1];
    this.acquired = true;
    this.birth = Utils.currentTime();
  }

  public EncryptionKey(String root, byte[] ki, byte[] k) throws KafkaCryptoException
  {
    this(root, ki);
    this.setKey(k);
  }

  public EncryptionKey unpackb(List<Value> src) throws IOException
  {
    if (src.size() == 4 && src.get(0).isStringValue() && src.get(1).isRawValue() &&
        src.get(2).isRawValue() && src.get(3).isNumberValue()) {
      this.root = src.get(0).asStringValue().asString();
      this.keyIndex = src.get(1).asRawValue().asByteArray();
      this.key = src.get(2).asRawValue().asByteArray();
      this.birth = src.get(3).asNumberValue().toDouble();
      this.acquired = true;
      this.creation = Utils.currentTime();
      System.out.println("Attempting load of root=" + this.root + ", ki=" + Utils.bytesToHex(this.keyIndex) + ", b=" + String.format("%.2f", this.birth));
      return this;
    }
    return null;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(4);
    msgpack.packb_recurse(packer, this.root);
    msgpack.packb_recurse(packer, this.keyIndex);
    msgpack.packb_recurse(packer, this.key);
    msgpack.packb_recurse(packer, this.birth);
  }

  public boolean resubNeeded(double interval)
  {
    if (!this.acquired && this.last_acquire_time + interval < Utils.currentTime()) return true;
    return false;
  }
  public void resub(double time)
  {
    this.last_acquire_time = time;
  }
}
