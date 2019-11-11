package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.CertPoison;

import org.msgpack.core.MessagePacker;
import java.util.List;
import java.util.ArrayList;
import org.msgpack.value.Value;

import java.io.IOException;

public class UsagesPoison implements Msgpacker<UsagesPoison>, CertPoison
{
  public List<String> usages;

  public UsagesPoison()
  {
    this.usages = new ArrayList<String>();
  }

  public UsagesPoison(String... sa)
  {
    this();
    for (String s : sa)
      this.usages.add(s);
  }

  public UsagesPoison unpackb(List<Value> src)
  {
    usages = new ArrayList<String>();
    for (Value v : src)
      usages.add(v.asStringValue().toString());
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(2);
    msgpack.packb_recurse(packer, "usages");
    packer.packArrayHeader(usages.size());
    for (String t : usages)
      msgpack.packb_recurse(packer, t);
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("[usages, [");
    for (int i = 0; i < usages.size(); i++) {
      if (i != 0) sb.append(",");
      sb.append(usages.get(i));
    }
    sb.append("]]");
    return sb.toString();
  }

  public String poisonName()
  {
    return "usages";
  }

  public boolean multimatch(String match)
  {
    return CertPoison.multimatch(match, this.usages);
  }

  public CertPoison intersect_poison(CertPoison c2, boolean same_pk)
  {
    if (c2 == null) return this;
    UsagesPoison c2c = (UsagesPoison)c2;
    UsagesPoison rv = new UsagesPoison();
    for (String w : c2c.usages)
      if (CertPoison.multimatch(w, this.usages))
        rv.usages.add(w);
    return rv;
  }
}
