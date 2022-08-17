package org.kafkacrypto;

import org.kafkacrypto.msgs.SignPublicKey;
import org.kafkacrypto.msgs.RatchetFileFormat;
import org.kafkacrypto.msgs.EncryptionKey;
import org.kafkacrypto.msgs.msgpack;

import org.kafkacrypto.jasodium;
import org.kafkacrypto.Utils;
import org.kafkacrypto.KeyGenerator;
import org.kafkacrypto.exceptions.KafkaCryptoException;
import org.kafkacrypto.exceptions.KafkaCryptoRatchetException;

import java.math.BigInteger;

import java.io.RandomAccessFile;
import java.io.IOException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ratchet extends KeyGenerator
{
  private final static byte[] __ctx = {'r','a','t','c','h','e','t',0,0,0,0,0,0,0,0,0};
  private RandomAccessFile __file;
  private boolean __file_close;
  private BigInteger __keyidx;

  private static RandomAccessFile open_file(String file) throws KafkaCryptoRatchetException
  {
    if (!(new File(file).exists()))
      Ratchet.__init_ratchet(file);
    try {
      return new RandomAccessFile(file, "rws");
    } catch (IOException ioe) {
      throw new KafkaCryptoRatchetException("Could not open file!", ioe);
    }
  }

  public Ratchet(String file) throws KafkaCryptoException
  {
    this(Ratchet.open_file(file), true);
  }

  public Ratchet(RandomAccessFile file) throws KafkaCryptoException
  {
    this(file, false);
  }

  public Ratchet(RandomAccessFile file, boolean toclose) throws KafkaCryptoException
  {
    super();
    try {
      this.__file = file;
      this.__file_close = toclose;
      this.increment();
    } catch (Exception e) {
      throw new KafkaCryptoRatchetException(e.getMessage());
    }
  }

  public void close()
  {
    try {
      if (this.__file_close)
        this.__file.close();
    } catch (IOException ioe) {
      // Do nothing
    } finally {
      this.__file = null;
      super.close();
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

  public EncryptionKey get_key_value_generators(String topic, SignPublicKey node) throws KafkaCryptoException
  {
    EncryptionKey rv = new EncryptionKey();
    byte[][] hashparts = Utils.splitArray(jasodium.crypto_hash_sha256(topic.getBytes()),this.SALTSIZE);
    byte[][] kn = this.generate(hashparts[0],hashparts[1],this.SECRETSIZE,0);
    rv.root = topic;
    if (node != null) {
      byte[] nodebytes = node.getBytes();
      byte[] ki = this.__keyidx.toByteArray();
      byte[] nki = new byte[16+nodebytes.length];
      for (int i = 0; i < ki.length; i++)
        nki[i+(16-ki.length)+nodebytes.length] = ki[i];
      for (int i = 0; i < nodebytes.length; i++)
        nki[i] = nodebytes[i];
      ki = jasodium.crypto_generichash(nki, null, 0);
      rv.keyIndex = ki;
    } else
      rv.keyIndex = this.__keyidx.toByteArray();
    rv.setKey(kn[0]);
    return rv;
  }

  private static void __init_ratchet(String file)
  {
    Logger _logger = LoggerFactory.getLogger("kafkacrypto-java.Ratchet");
    _logger.warn("Initializing new Ratchet file {}", file);
    RatchetFileFormat rff = new RatchetFileFormat();
    rff.secret = jasodium.randombytes(Ratchet.SECRETSIZE);
    byte[] _ss0_escrow = Utils.hexToBytes("7e301be3922d8166e30be93c9ecc2e18f71400fe9e6407fd744f4a542bcab934");
    _logger.warn("  Escrow public key: {}", Utils.bytesToHex(_ss0_escrow));
    _logger.warn("  Escrow value: {}", Utils.bytesToHex(jasodium.crypto_box_seal(rff.secret, _ss0_escrow)));
    rff.keyidx = BigInteger.valueOf(0);
    try {
      Files.write(Paths.get(file), msgpack.packb(rff));
    } catch (IOException ioe) {
      _logger.warn("  Error writing file.",ioe);
    }
    _logger.warn("  Ratchet Initialized.");    
  }
}
