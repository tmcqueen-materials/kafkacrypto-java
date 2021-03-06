package org.kafkacrypto.msgs;

import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.CertPoisons;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePacker;
import java.util.List;
import java.util.ArrayList;
import org.msgpack.value.Value;

import java.io.IOException;

public class ChainCert implements Msgpacker<ChainCert>
{
  public double max_age = 0;
  public CertPoisons poisons;
  public byte[] pk = null;
  public byte[][] pk_array = null;
  public List<Value> extra;

  public ChainCert()
  {
    this.poisons = new CertPoisons();
    this.extra = new ArrayList<Value>();
  }

  public ChainCert unpackb(List<Value> src) throws IOException
  {
    if (src == null || src.size() < 3)
      return null;
    this.max_age = src.get(0).asNumberValue().toDouble();
    this.poisons = new CertPoisons().unpackb(src.get(1).asRawValue().asByteArray());
    if (src.get(2).isArrayValue()) {
      List<Value> vals = src.get(2).asArrayValue().list();
      this.pk_array = new byte[vals.size()][];
      for (int i = 0; i < vals.size(); i++)
        this.pk_array[i] = vals.get(i).asRawValue().asByteArray();
      if (this.pk_array.length > 0)
        this.pk = pk_array[0];
    } else {
      this.pk = src.get(2).asRawValue().asByteArray();
    }
    this.extra = new ArrayList<Value>();
    for (int i = 3; i < src.size(); i++)
      this.extra.add(src.get(i));
    return this;
  }

  public byte[] toSignable() throws IOException
  {
    MessageBufferPacker packer2 = MessagePack.DEFAULT_PACKER_CONFIG.withStr8FormatSupport(false).newBufferPacker();
    this.packb(packer2);
    packer2.close();
    return packer2.toByteArray();
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(3+this.extra.size());
    if (this.max_age > 0)
      msgpack.packb_recurse(packer, this.max_age);
    else
      msgpack.packb_recurse(packer, (byte)0);
    msgpack.packb_recurse(packer, (Msgpacker<CertPoisons>)(this.poisons));
    if (this.pk_array == null)
      msgpack.packb_recurse(packer, this.pk);
    else
      msgpack.packb_recurse(packer, this.pk_array);
    for (int i = 0; i < this.extra.size(); i++)
      msgpack.packb_recurse(packer, this.extra.get(i));
  }

  public Value getExtra(int item)
  {
    if (item >= 0 && item < this.extra.size()) return this.extra.get(item);
    return null;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(String.format("%.2f", this.max_age));
    sb.append(", ");
    sb.append(this.poisons.toString());
    sb.append(", ");
    if (this.pk_array == null) {
      sb.append(Utils.bytesToHex(this.pk));
    } else {
      sb.append("[");
      for (int i = 0; i < this.pk_array.length; i++) {
        if (i > 0) sb.append(", ");
        sb.append(Utils.bytesToHex(this.pk_array[i]));
      }
      sb.append("]");
    }
    for (int i = 0; i < this.extra.size(); i++) {
      sb.append(", ");
      if (this.extra.get(i).isRawValue())
        sb.append(Utils.bytesToHex(this.extra.get(i).asRawValue().asByteArray()));
      else
        sb.append(this.extra.get(i).toString());
    }
    sb.append("]");
    return sb.toString();
  }

  public boolean validate_time()
  {
    if (System.currentTimeMillis()/1000.0 <= this.max_age)
      return true;
    return false;
  }

  public boolean validate_poison(String poison, String match)
  {
    CertPoison cp = this.get_poison(poison);
    if (cp == null && poison.equals("pathlen")) return true;
    if (match == null) return true;
    if (cp == null) return false;
    if (cp.multimatch(match)) return true;
    return false;
  }

  public static ChainCert key_in_list(ChainCert wanted, List<ChainCert> choices)
  {
    if (wanted == null || choices == null) return null;
    for (ChainCert c : choices)
      if (wanted.pk.equals(c.pk))
        return c;
    return null;
  }

  public static ChainCert intersect_certs(ChainCert c1, ChainCert c2, boolean same_pk)
  {
    ChainCert rv = new ChainCert();
    if (c2.max_age > 0 && (c2.max_age < c1.max_age || c1.max_age == 0))
      rv.max_age = c2.max_age;
    else
      rv.max_age = c1.max_age;
    rv.poisons = new CertPoisons();
    for (CertPoison cp : c1.poisons)
      rv.poisons.add(cp.intersect_poison(c2.get_poison(cp.poisonName()),same_pk));
    for (CertPoison cp : c2.poisons)
      if (c1.get_poison(cp.poisonName()) == null)
        rv.poisons.add(cp);
    rv.pk = c2.pk;
    rv.pk_array = c2.pk_array;
    rv.extra = c2.extra;
    return rv;
  }

  private CertPoison get_poison(String poison)
  {
    for (CertPoison cp : this.poisons)
      if (cp.poisonName().equals(poison))
        return cp;
    return null;
  }
}
