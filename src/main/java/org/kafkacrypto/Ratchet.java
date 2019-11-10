package org.kafkacrypto;

import org.kafkacrypto.msgs.RatchetFileFormat;
import org.kafkacrypto.msgs.EncryptionKey;

import org.kafkacrypto.jasodium;
import org.kafkacrypto.Utils;
import org.kafkacrypto.KeyGenerator;
import org.kafkacrypto.exceptions.KafkaCryptoException;
import org.kafkacrypto.exceptions.KafkaCryptoRatchetException;

import java.math.BigInteger;

import java.io.RandomAccessFile;
import java.io.IOException;

public class Ratchet extends KeyGenerator
{
  private final static byte[] __ctx = {'r','a','t','c','h','e','t',0,0,0,0,0,0,0,0,0};
  private RandomAccessFile __file;
  private BigInteger __keyidx;

  private static RandomAccessFile open_file(String file) throws KafkaCryptoRatchetException
  {
    try {
      return new RandomAccessFile(file, "rws");
    } catch (IOException ioe) {
      throw new KafkaCryptoRatchetException("Could not open file!", ioe);
    }
  }

  public Ratchet(String file) throws KafkaCryptoException
  {
    this(Ratchet.open_file(file));
  }

  public Ratchet(RandomAccessFile file) throws KafkaCryptoException
  {
    super();
    try {
      this.__file = file;
      this.increment();
    } catch (Exception e) {
      throw new KafkaCryptoRatchetException(e.getMessage());
    }
  }

  public void increment() throws KafkaCryptoException
  {
    try {
      int size = (int)this.__file.length();
      byte[] bcontents = new byte[size];
      this.__file.seek(0);
      this.__file.read(bcontents);
      RatchetFileFormat contents = new RatchetFileFormat().unpackb(bcontents);
      this.__keyidx = contents.keyidx;
      this.rekey(contents.secret);
      this.__file.seek(0);
      byte[][] newkn = this.generate(this.__ctx,this.SECRETSIZE,0);
      /* In general there is no guarantee that a write and then sync will atomically overwrite
       * the previous values. However, in this specific case, this is the best approach:
       * the total size of data being written is << 512 bytes (a single sector), meaning that
       * on any block-based device either the new data will be written, or it won't be, with no
       * intermediate possibilities, and thus is in practice atomic.
       * We do not use atomic write approaches based on renames due to the possibility of leaving
       * secret key material on disk if temporary files are not appropriately cleaned up.
       */
      contents.keyidx = contents.keyidx.add(BigInteger.valueOf(1));
      contents.secret = newkn[0];
      this.__file.write(contents.packb());
      this.__file.setLength(this.__file.getFilePointer());
      this.__file.getFD().sync();
      this.__file.seek(0);
    } catch (IOException ioe) {
      throw new KafkaCryptoRatchetException("Failed to increment ratchet!", ioe);
    } catch (KafkaCryptoException kce) {
      throw kce;
    }
  }

  public EncryptionKey get_key_value_generators(String topic, String node) throws KafkaCryptoException
  {
    EncryptionKey rv = new EncryptionKey();
    byte[][] hashparts = Utils.splitArray(jasodium.crypto_hash_sha256(topic.getBytes()),this.SALTSIZE);
    byte[][] kn = this.generate(hashparts[0],hashparts[1],this.SECRETSIZE,0);
    rv.root = topic;
    if (node != null) {
      byte[] ki = this.__keyidx.toByteArray();
      byte[] nki = new byte[16+node.getBytes().length];
      for (int i = 0; i < ki.length; i++)
        nki[i+(16-ki.length)+node.getBytes().length] = ki[i];
      for (int i = 0; i < node.getBytes().length; i++)
        nki[i] = node.getBytes()[i];
      ki = jasodium.crypto_generichash(nki, null, 0);
      rv.keyIndex = ki;
    } else
      rv.keyIndex = this.__keyidx.toByteArray();
    rv.setKey(kn[0]);
    return rv;
  }
}
