package org.kafkacrypto.msgs;

import java.util.List;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;

import org.kafkacrypto.Utils;
import org.kafkacrypto.KafkaCryptoMessage;
import org.kafkacrypto.KafkaCryptoMessageDecryptor;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.exceptions.KafkaCryptoMessageException;

import java.io.IOException;

public class KafkaCryptoWireMessage implements Msgpacker<KafkaCryptoWireMessage>, KafkaCryptoMessage
{
  public byte[] keyIndex = null;
  public byte[] salt = null;
  public byte[] msg = null;
  public boolean ipt = false;
  public KafkaCryptoMessageDecryptor<KafkaCryptoWireMessage> deser = null;
  public String root = null;

  public KafkaCryptoWireMessage()
  {
  }

  public KafkaCryptoWireMessage(byte[] ki, byte[] salt, byte[] msg)
  {
    this();
    this.keyIndex = ki;
    this.salt = salt;
    this.msg = msg;
  }

  public KafkaCryptoWireMessage unpackb(List<Value> src) throws IOException
  {
    if (src.size() == 3 && src.get(0).isRawValue() && src.get(1).isRawValue() &&
        src.get(2).isRawValue()) {
      this.keyIndex = src.get(0).asRawValue().asByteArray();
      this.salt = src.get(1).asRawValue().asByteArray();
      this.msg = src.get(2).asRawValue().asByteArray();
      return this;
    }
    return null;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(3);
    msgpack.packb_recurse(packer, this.keyIndex);
    msgpack.packb_recurse(packer, this.salt);
    msgpack.packb_recurse(packer, this.msg);
  }

  public void setCleartext(byte[] msg)
  {
    if (msg != null) {
      this.msg = msg;
      this.ipt = true;
      this.root = null;
      this.deser = null;
    }
  }

  public boolean isCleartext()
  {
    if (this.ipt) return true;
    if (this.root != null && this.deser != null)
      this.deser.decrypt(this);
    return this.ipt;
  }

  public byte[] getMessage() throws KafkaCryptoMessageException
  {
    if (!this.isCleartext())
      throw new KafkaCryptoMessageException("Message not decrypted!");
    return msg;
  }

  public byte[] toWire() throws IOException
  {
    return Utils.concatArrays(new byte[]{1}, msgpack.packb(this));
  }

}
