package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.msgpack;
import org.msgpack.value.Value;
import java.util.List;

import java.util.regex.Pattern;

import org.kafkacrypto.msgs.PathlenPoison;
import org.kafkacrypto.msgs.UsagesPoison;
import org.kafkacrypto.msgs.TopicsPoison;

import java.io.IOException;

public interface CertPoison<E>
{
  public String poisonName();

  public boolean multimatch(String match);

  public CertPoison intersect_poison(CertPoison c2);

  public static CertPoison unpackb(List<Value> src) throws IOException
  {
    String poison = src.get(0).asStringValue().asString();
    if (poison.equals("pathlen"))
      return new PathlenPoison(src.get(1).asIntegerValue().asInt());
    if (poison.equals("usages"))
      return new UsagesPoison().unpackb(src.get(1).asArrayValue().list());
    if (poison.equals("topics"))
      return new TopicsPoison().unpackb(src.get(1).asArrayValue().list());
    throw new IOException("Unknown poison value!");
  }

  public static boolean multimatch(String wanted, List<String> choices)
  {
    for (String c : choices) {
      if (!wanted.startsWith("^")) {
        // Wanted is a Literal
        if (!c.startsWith("^")) {
          // c is a literal
          if (wanted.equals(c)) return true;
        } else {
          // c is a regex
          if (Pattern.matches(c, wanted)) return true;
        }
      } else {
        // Wanted is a regex.
        if (wanted.equals(c)) return true;
      }
    }
    // did not find it.
    return false;
  }
}
