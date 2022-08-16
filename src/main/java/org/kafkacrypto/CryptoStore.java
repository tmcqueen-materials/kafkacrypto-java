package org.kafkacrypto;

import org.kafkacrypto.CryptoKey;
import org.kafkacrypto.AtomicFile;
import org.kafkacrypto.exceptions.KafkaCryptoException;
import org.kafkacrypto.exceptions.KafkaCryptoStoreException;
import org.kafkacrypto.msgs.ByteString;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Map;
import java.util.LinkedHashMap;

import java.util.Base64;

class CryptoStore
{
  protected Logger _logger;
  protected boolean need_init;
  private Lock lock;
  private Lock keylock;
  private CryptoKey cryptokey;
  private String nodeID;
  private AtomicFile file;
  private Map<ByteString,Map<ByteString,ByteString>> config;

  public CryptoStore(String file) throws KafkaCryptoException
  {
    this(file, "");
  }
  public CryptoStore(String file, String nodeID) throws KafkaCryptoException
  {
    this.need_init = false;
    this._logger = LoggerFactory.getLogger("kafkacrypto-java.cryptostore");
    if (!(new File(file).exists()) && nodeID != null && nodeID.length() > 0) {
      try {
        Files.write(Paths.get(file), new byte[0]);
      } catch (IOException ioe) {
        this._logger.warn("  Error writing file.",ioe);
      }
      this.need_init = true;
    }
    try {
      this.file = new AtomicFile(file);
      this.config = CryptoStore.parseFile(this.file);
      this.config = config;
      this.lock = new ReentrantLock();
      this.keylock = new ReentrantLock();
      if (this.need_init) {
        this.nodeID = nodeID;
        this.__init_cryptostore(nodeID);
      }
      String nodeIDFile = (this.config.get(new ByteString("DEFAULT")).containsKey(new ByteString("node_id")))
                          ?(this.config.get(new ByteString("DEFAULT")).get(new ByteString("node_id")).toString()):("");
      if ((nodeID != null && nodeID.length() > 0) && nodeIDFile.length() > 0) {
        if (!nodeID.equals(nodeIDFile))
          throw new KafkaCryptoStoreException("Mismatch in nodeID in file versus constructor call.");
      } else if ((nodeID == null || nodeID.length() == 0) && nodeIDFile.length() == 0) {
        throw new KafkaCryptoStoreException("No nodeID specified!");
      } else if (nodeIDFile.length() == 0) {
        Map<ByteString,ByteString> nvs = new LinkedHashMap<ByteString,ByteString>();
        nvs.put(new ByteString("node_id"), new ByteString(nodeID));
        this.store_values(new ByteString("DEFAULT"),nvs,true);
      } else { // nodeID == null || nodeID.length() == 0
        nodeID = nodeIDFile;
      }
      this.nodeID = nodeID;
    } catch (IOException ioe) {
      throw new KafkaCryptoStoreException("Could not open or read file!", ioe);
    }
  }

  public void close()
  {
    this.lock.lock();
    try {
      this.file.close();
    } catch (IOException ioe) {
      // Do nothing
    } finally {
      this.file = null;
      this.keylock.lock();
      this.cryptokey = null;
      this.keylock.unlock();
      this.keylock = null;
      this.lock.unlock();
      this.lock = null;
    }
  }

  public String get_nodeID()
  {
    return this.nodeID;
  }

  public long load_value(String name, String section, long def)
  {
    ByteString rvs = this.load_value(new ByteString(name), new ByteString(section), null);
    if (rvs!=null && rvs.length() > 0)
      return Long.parseLong(rvs.toString());
    return def;
  }
  public double load_value(String name, String section, double def)
  {
    ByteString rvs = this.load_value(new ByteString(name), new ByteString(section), null);
    if (rvs!=null && rvs.length() > 0)
      return Double.parseDouble(rvs.toString());
    return def;
  }
  public boolean load_value(String name, String section, boolean def)
  {
    ByteString rvs = this.load_value(new ByteString(name), new ByteString(section), null);
    if (rvs!=null && rvs.length() > 0)
      return Boolean.parseBoolean(rvs.toString());
    return def;
  }
  public byte[] load_value(String name, String section, byte[] def)
  {
    ByteString rvs = this.load_value(new ByteString(name), new ByteString(section), null);
    if (rvs!=null && rvs.length() > 0)
      return rvs.getBytes();
    return def;
  }
  public String load_value(String name, String section, String def)
  {
    ByteString rvs = this.load_value(new ByteString(name), new ByteString(section), null);
    if (rvs!=null)
      return rvs.toString();
    return def;
  }
  public ByteString load_value(String name, String section, ByteString def)
  {
    return this.load_value(new ByteString(name), new ByteString(section), def);
  }
  public ByteString load_value(ByteString name, ByteString section, ByteString def)
  {
    ByteString rv = def;
    this._logger.debug("Attempting to load name={} from section={}, default={}", name, section, def);
    this.lock.lock();
    try {
      if (section==null || section.length() < 1)
        section = new ByteString(this.nodeID);
      else
        section = new ByteString(this.nodeID + "-" + section);
      if (this.config.containsKey(section) && this.config.get(section).containsKey(name))
        rv = this.config.get(section).get(name);
      else if (this.config.get(new ByteString("DEFAULT")).containsKey(name))
        rv = this.config.get(new ByteString("DEFAULT")).get(name);
    } finally {
      this.lock.unlock();
    }
    this._logger.debug("Loaded {} for name={}, section={}", rv, name, section);
    return rv;
  }

  public Map<ByteString,ByteString> load_section(String section, boolean defaults)
  {
    return this.load_section(new ByteString(section), defaults);
  }

  public Map<ByteString,ByteString> load_section(ByteString section, boolean defaults)
  {
    Map<ByteString,ByteString> rv = new LinkedHashMap<ByteString,ByteString>();
    this._logger.debug("Attempting to load section={} with defaults={}", section, defaults);
    this.lock.lock();
    try {
      if (section==null || section.length() < 1)
        section = new ByteString(this.nodeID);
      else
        section = new ByteString(this.nodeID + "-" + section);
      Map<ByteString,ByteString> sect = this.config.get(section);
      if (sect != null) {
        for (ByteString k : sect.keySet())
          rv.put(k,sect.get(k));
      }
      if (defaults) {
        sect = this.config.get(new ByteString("DEFAULT"));
        for (ByteString k : sect.keySet())
          if (!rv.containsKey(k)) {
            this._logger.debug("Adding default name={}, value={}", k, sect.get(k));
            rv.put(k,sect.get(k));
          } else if (rv.get(k) == null || rv.get(k).length() == 0) {
            this._logger.debug("Removing name={}", k);
            rv.remove(k);
          }
      }
    } finally {
      this.lock.unlock();
    }
    return rv;
  }

  public void store_value(String name, String section, long value) throws KafkaCryptoStoreException
  {
    this.store_value(name, section, Long.toString(value));
  }
  public void store_value(String name, String section, double value) throws KafkaCryptoStoreException
  {
    this.store_value(name, section, Double.toString(value));
  }
  public void store_value(String name, String section, boolean value) throws KafkaCryptoStoreException
  {
    this.store_value(name, section, Boolean.toString(value));
  }
  public void store_value(String name, String section, String value) throws KafkaCryptoStoreException
  {
    this.store_value(new ByteString(name), new ByteString(section), new ByteString(value));
  }
  public void store_value(String name, String section, ByteString value) throws KafkaCryptoStoreException
  {
    this.store_value(new ByteString(name), new ByteString(section), value);
  }
  public void store_value(String name, String section, byte[] value) throws KafkaCryptoStoreException
  {
    this.store_value(new ByteString(name), new ByteString(section), new ByteString(value));
  }
  public void store_value(byte[] name, String section, byte[] value) throws KafkaCryptoStoreException
  {
    this.store_value(new ByteString(name), new ByteString(section), new ByteString(value));
  }

  public void store_value(ByteString name, ByteString section, ByteString value) throws KafkaCryptoStoreException
  {
    Map<ByteString,ByteString> nvs = new LinkedHashMap<ByteString,ByteString>();
    nvs.put(name, value);
    this.store_values(section,nvs,false);
  }

  public void store_values(ByteString section, Map<ByteString,ByteString> namevals) throws KafkaCryptoStoreException
  {
    this.store_values(section,namevals,false);
  }

  private void store_values(ByteString section, Map<ByteString,ByteString> namevals, boolean rawSection) throws KafkaCryptoStoreException
  {
    this.lock.lock();
    this._logger.debug("Attempting to store values for section={}", section);
    try {
      if (section==null || section.length() < 1)
        section = new ByteString(this.nodeID);
      else if (!rawSection)
        section = new ByteString(this.nodeID + "-" + section);
      if (!this.config.containsKey(section))
        this.config.put(section, new LinkedHashMap<ByteString,ByteString>());
      Map<ByteString,ByteString> sect = this.config.get(section);
      for (ByteString k : namevals.keySet()) {
        this._logger.debug("  Storing name={}, value={}", k, namevals.get(k));
        if (namevals.get(k) == null)
          sect.remove(k);
        else
          sect.put(k, namevals.get(k));
      }
      this.writeConfig();
    } catch (IOException ioe) {
      throw new KafkaCryptoStoreException("IOError storing values!", ioe);
    } finally {
      this.lock.unlock();
    }
  }

  public void set_cryptokey(CryptoKey key)
  {
    this.keylock.lock();
    try {
      this.cryptokey = key;
    } finally {
      this.keylock.unlock();
    }
  }

  public ByteString load_opaque_value(String name, String section, String def)
  {
    return this.load_opaque_value(new ByteString(name), new ByteString(section), new ByteString(def));
  }

  public ByteString load_opaque_value(ByteString name, ByteString section, ByteString def)
  {
    ByteString rv;
    this._logger.debug("Loading opaque value for name={}, section={}", name, section);
    this.keylock.lock();
    try {
      rv = this.load_value(name, section, null);
      if (rv != null) {
        this._logger.debug("  Decrypting opaque value.");
        rv = new ByteString(this.cryptokey.unwrap_opaque(rv.getBytes()));
        if (rv.length() < 1)
          rv = null;
      }
      if (rv == null)
        rv = def;
    } finally {
      this.keylock.unlock();
    }
    this._logger.debug("Loaded opaque value for name={}, section={}", name, section);
    return rv;
  }

  public void store_opaque_value(String name, byte[] value, String section) throws KafkaCryptoException
  {
    this.store_opaque_value(new ByteString(name), new ByteString(value), new ByteString(section));
  }

  public void store_opaque_value(ByteString name, ByteString value, ByteString section) throws KafkaCryptoException
  {
    this._logger.debug("Storing opaque value for name={}, section={}", name, section);
    this.keylock.lock();
    try {
      value = new ByteString(this.cryptokey.wrap_opaque(value.getBytes()));
      this.store_value(name, value, section);
      this._logger.debug("  Successfully stored opaque value for name={}, section={}", name, section);
    } finally {
      this.keylock.unlock();
    }
  }

  private static String trimLine(String line)
  {
    if (line==null)
      return line;
    line = line.trim();
    if (line.startsWith(";"))
      return "";
    return line;
  }

  private static String sectionLine(String line)
  {
    if (line.startsWith("[") && line.lastIndexOf("]")>0)
      return line.substring(1,line.lastIndexOf("]"));
    return null;
  }

  private static String[] keyvalueLine(String line)
  {
    if (line.indexOf(":") > 0) {
      String[] parts = new String[2];
      parts[0] = line.substring(0,line.indexOf(":")).trim();
      if (line.indexOf(":") < line.length()-1)
        parts[1] = line.substring(line.indexOf(":")+1).trim();
      else
        parts[1] = "";
      if (parts[0].length() > 0)
        return parts;
    }
    return null;
  }

  private static Map<ByteString,Map<ByteString,ByteString>> parseFile(AtomicFile file) throws IOException
  {
    file.seek(0);
    ByteString section = null;
    Map<ByteString,Map<ByteString,ByteString>> rv = new LinkedHashMap<ByteString,Map<ByteString,ByteString>>();
    for (String rs = CryptoStore.trimLine(file.readLine()); rs != null; rs = CryptoStore.trimLine(file.readLine())) {
      if (sectionLine(rs) != null) {
        // New Section
        section = new ByteString(sectionLine(rs));
        if (!rv.containsKey(section))
          rv.put(section, new LinkedHashMap<ByteString,ByteString>());
      } else if (keyvalueLine(rs) != null) {
        // Key Value Line
        String[] kv = keyvalueLine(rs);
        if (kv[0].startsWith("string64#"))
          kv[0] = new String(Base64.getDecoder().decode(kv[0].substring(9)));
        if (kv[1].startsWith("string64#"))
          kv[1] = new String(Base64.getDecoder().decode(kv[1].substring(9)));
        rv.get(section).put(new ByteString(kv[0]), new ByteString(kv[1]));
      }
    }
    if (!rv.containsKey(new ByteString("DEFAULT")))
      rv.put(new ByteString("DEFAULT"), new LinkedHashMap<ByteString,ByteString>());
    return rv;
  }

  private void writeConfig() throws IOException
  {
    this.file.seek(0);
    for (ByteString sect : this.config.keySet()) {
      this.file.writeLine("[" + sect + "]");
      Map<ByteString,ByteString> sect_map = this.config.get(sect);
      for (ByteString k : sect_map.keySet()) {
        String value = sect_map.get(k).toString();
        String key = k.toString();
        if (key.indexOf(":") >= 0 || key.startsWith("string64#"))
          key = "string64#" + Base64.getEncoder().encodeToString(key.getBytes());
        if (value.startsWith("string64#"))
          value = "string64#" + Base64.getEncoder().encodeToString(value.getBytes());
        this.file.writeLine(key + " : " + value);
      }
      this.file.writeLine("");
    }
    this.file.truncate();
    this.file.flush();
  }

  public void __init_cryptostore(String nodeID) throws KafkaCryptoStoreException
  {
    this._logger.warn("Initializing new CryptoStore nodeID={}", nodeID);
    this.store_value("test", "", "test");
    this.store_value("test", "", (ByteString)null);
    this._logger.warn("  Including a default/temporary root of trust. Once proper access is provisioned, this root of trust should be removed or distrusted.");
    this.store_value("temporary", "allowlist", Utils.hexToBytes("9300ab9192a7706174686c656e02da00201a13b0aecdd6751c7dfa43e43284326ad01dbc20a8a00b1566092ab0a542620f"));
    this.store_value("test", "denylist", "test");
    this.store_value("test", "denylist", (ByteString)null);
    this.store_value("test", "crypto", "test");
    this.store_value("test", "crypto", (ByteString)null);
    this._logger.warn("  CryptoStore Initialized.");
  }
}
