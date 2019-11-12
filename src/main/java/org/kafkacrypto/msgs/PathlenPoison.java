package org.kafkacrypto.msgs;

import org.kafkacrypto.msgs.Msgpacker;
import org.kafkacrypto.msgs.CertPoison;

import org.msgpack.core.MessagePacker;
import java.util.List;
import org.msgpack.value.Value;

import java.io.IOException;

public class PathlenPoison implements Msgpacker<PathlenPoison>, CertPoison
{
  public int max_pathlen = 0;

  public PathlenPoison()
  {
  }

  public PathlenPoison(int pathlen)
  {
    this.max_pathlen = pathlen;
  }

  public PathlenPoison unpackb(List<Value> src)
  {
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(2);
    msgpack.packb_recurse(packer, "pathlen");
    msgpack.packb_recurse(packer, this.max_pathlen);
  }

  public String toString()
  {
    return "[pathlen, " + this.max_pathlen + "]";
  }

  public String poisonName()
  {
    return "pathlen";
  }

  public boolean multimatch(String match)
  {
    if (this.max_pathlen >= Integer.parseInt(match)) return true;
    return false;
  }

  public CertPoison intersect_poison(CertPoison c2, boolean same_pk)
  {
    if (c2 == null) return this;
    PathlenPoison c2c = (PathlenPoison)c2;
    PathlenPoison rv = new PathlenPoison();
    if (this.max_pathlen-1 < (c2c).max_pathlen && !same_pk) rv.max_pathlen = this.max_pathlen-1;
    else if (this.max_pathlen < (c2c).max_pathlen) rv.max_pathlen = this.max_pathlen;
    else rv.max_pathlen = c2c.max_pathlen;
    return rv;
  }
}
