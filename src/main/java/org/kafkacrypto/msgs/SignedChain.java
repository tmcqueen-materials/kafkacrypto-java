package org.kafkacrypto.msgs;

import org.kafkacrypto.Utils;
import org.kafkacrypto.msgs.ChainCert;
import org.kafkacrypto.msgs.msgpack;
import org.kafkacrypto.msgs.Msgpacker;

import org.kafkacrypto.jasodium;

import org.kafkacrypto.exceptions.KafkaCryptoInternalError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.msgpack.core.MessagePacker;
import java.util.List;
import java.util.ArrayList;
import org.msgpack.value.Value;

import java.io.IOException;

public class SignedChain implements Msgpacker<SignedChain>
{
  static final Logger _logger = LoggerFactory.getLogger("kafkacrypto-java.signedchain");
  public ArrayList<byte[]> chain;

  public SignedChain()
  {
    this.chain = new ArrayList<byte[]>();
  }

  @SuppressWarnings("unchecked")
  public SignedChain(SignedChain src)
  {
    this.chain = (ArrayList<byte[]>)(src.chain.clone());
  }

  public SignedChain unpackb(List<Value> src) throws IOException
  {
    this.chain = new ArrayList<byte[]>();
    for (int i = 0; i < src.size(); i++) {
      this.chain.add(src.get(i).asRawValue().asByteArray());
    }
    return this;
  }

  public void packb(MessagePacker packer) throws IOException
  {
    packer.packArrayHeader(this.chain.size());
    for (int i = 0; i < this.chain.size(); i++)
      msgpack.packb_recurse(packer, this.chain.get(i));
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < this.chain.size(); i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(Utils.bytesToHex(this.chain.get(i)));
    }
    sb.append("]");
    return sb.toString();
  }

  public void append(byte[] cert)
  {
    this.chain.add(cert);
  }

  public ChainCert process_chain(String topic, String usage, List<ChainCert> allowed, List<ChainCert> denied)
  {
    return SignedChain.process_chain(this.chain, topic, usage, allowed, denied);
  }

  public static ChainCert process_chain(List<byte[]> chain, String topic, String usage, List<ChainCert> allowed, List<ChainCert> denied)
  {
    if (allowed.size() < 1) throw new KafkaCryptoInternalError("No roots of trust specified!");
    if (chain.size() < 1) throw new KafkaCryptoInternalError("Signed chain has no entries!");
    // See python process_chain for logic description.
    for (ChainCert rot : allowed) {
      ChainCert lpk = rot;
      boolean denylisted = false;
      try {
        for (byte[] next : chain) {
          ChainCert npk = new ChainCert().unpackb(jasodium.crypto_sign_open(next,lpk.pk));
          if (ChainCert.key_in_list(lpk, denied) != null) {
            denylisted = true;
          } else if (ChainCert.key_in_list(lpk, allowed) != null) {
            denylisted = false;
            lpk = ChainCert.intersect_certs(lpk, ChainCert.key_in_list(lpk, allowed), true);
          }
          lpk = ChainCert.intersect_certs(lpk, npk, false);
        }
        if (ChainCert.key_in_list(lpk, allowed) != null)
          denylisted = false;
        if (ChainCert.key_in_list(lpk, denied) != null)
          denylisted = true
        if (denylisted) throw new KafkaCryptoInternalError("Chain has denylisted key.");
        if (!lpk.validate_time())
          throw new KafkaCryptoInternalError("Chain is expired!");
        if (topic != null && !lpk.validate_poison("topics",topic))
          throw new KafkaCryptoInternalError("Chain does not match required topic.");
        if (usage != null && !lpk.validate_poison("usages",usage))
          throw new KafkaCryptoInternalError("Chain does not match required usage.");
        if (!lpk.validate_poison("pathlen", Integer.toString(0)))
          throw new KafkaCryptoInternalError("Chain exceeds allowed pathlen.");
        return lpk;
      } catch (KafkaCryptoInternalError kcie) {
        SignedChain._logger.info("Issues encountered with chain", kcie);
      } catch (IOException ioe) {
        SignedChain._logger.info("Issues encountered with chain", ioe);
      }
    }
    //TODO: Add self-signed denylisting.
    throw new KafkaCryptoInternalError("No valid chain path found.");
  }
}
