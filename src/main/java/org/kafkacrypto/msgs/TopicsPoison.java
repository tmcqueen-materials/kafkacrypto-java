package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.CertPoison;

import org.msgpack.core.MessagePacker;
import java.util.List;
import java.util.ArrayList;
import org.msgpack.value.Value;

import java.io.IOException;

public class TopicsPoison implements Msgpacker<TopicsPoison>, CertPoison
{
  public List<String> topics;

  public TopicsPoison()
  {
    this.topics = new ArrayList<String>();
  }

  public TopicsPoison(String... sa)
  {
    this();
    for (String s : sa)
      this.topics.add(s);
  }

  public TopicsPoison unpackb(List<Value> src)
  {
    topics = new ArrayList<String>();
    for (Value v : src)
      topics.add(v.asStringValue().toString());
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(2);
    msgpack.packb_recurse(packer, "topics");
    packer.packArrayHeader(topics.size());
    for (String t : topics)
      msgpack.packb_recurse(packer, t);
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("[topics, [");
    for (int i = 0; i < topics.size(); i++) {
      if (i != 0) sb.append(",");
      sb.append(topics.get(i));
    }
    sb.append("]]");
    return sb.toString();
  }

  public String poisonName()
  {
    return "topics";
  }

  public boolean multimatch(String match)
  {
    return CertPoison.multimatch(match, this.topics);
  }

  public CertPoison intersect_poison(CertPoison c2, boolean same_pk)
  {
    if (c2 == null) return this;
    TopicsPoison c2c = (TopicsPoison)c2;
    TopicsPoison rv = new TopicsPoison();
    for (String w : c2c.topics)
      if (CertPoison.multimatch(w, this.topics))
        rv.topics.add(w);
    return rv;
  }
}
