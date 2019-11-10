package org.kafkacrypto.msgs;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

import java.io.IOException;

import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.EncryptionKeys;

public class CryptoState extends LinkedHashMap<String,EncryptionKeys> implements Msgpacker<CryptoState>
{
  public CryptoState unpackb(List<Value> src) throws IOException
  {
    if (src == null || src.size() != 1 || !src.get(0).isMapValue()) return this;
    Map<Value,Value> cryptostate = src.get(0).asMapValue().map();
    for (Value state : cryptostate.keySet())
      if (state.isStringValue() && cryptostate.get(state).isArrayValue())
        this.put(state.asStringValue().asString(), new EncryptionKeys().unpackb(cryptostate.get(state).asArrayValue().list()));
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packMapHeader(this.size());
    for (String r : this.keySet()) {
      msgpack.packb_recurse(packer, r);
      msgpack.packb_recurse(packer, (Msgpacker<EncryptionKeys>)(this.get(r)));
    }
  }

}
