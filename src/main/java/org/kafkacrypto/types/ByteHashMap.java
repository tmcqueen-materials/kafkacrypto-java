package org.kafkacrypto.types;

import org.kafkacrypto.types.ByteArrayWrapper;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.HashSet;

public class ByteHashMap<V> implements Map<byte[], V>, Cloneable, Serializable
{
  private Map<ByteArrayWrapper, V> internalMap = new LinkedHashMap<ByteArrayWrapper, V>();

  public void clear()
  {
    internalMap.clear();
  }

  public boolean containsKey(Object key)
  {
    if (key instanceof byte[])
      return internalMap.containsKey(new ByteArrayWrapper((byte[]) key));
    return internalMap.containsKey(key);
  }

  public boolean containsValue(Object value)
  {
    return internalMap.containsValue(value);
  }

  public Set<java.util.Map.Entry<byte[], V>> entrySet()
  {
    Iterator<java.util.Map.Entry<ByteArrayWrapper, V>> iterator = internalMap.entrySet().iterator();
    HashSet<Entry<byte[], V>> hashSet = new HashSet<java.util.Map.Entry<byte[], V>>();
    while (iterator.hasNext()) {
      Entry<ByteArrayWrapper, V> entry = iterator.next();
      hashSet.add(new ByteEntry(entry.getKey().data, entry.getValue()));
    }
    return hashSet;
  }

  public V get(Object key)
  {
    if (key instanceof byte[])
      return internalMap.get(new ByteArrayWrapper((byte[]) key));
    return internalMap.get(key);
  }

  public boolean isEmpty()
  {
    return internalMap.isEmpty();
  }

  public Set<byte[]> keySet()
  {
    Set<byte[]> keySet = new HashSet<byte[]>();
    Iterator<ByteArrayWrapper> iterator = internalMap.keySet().iterator();
    while (iterator.hasNext()) {
      keySet.add(iterator.next().data);
    }
    return keySet;
  }

  public V put(byte[] key, V value)
  {
    return internalMap.put(new ByteArrayWrapper(key), value);
  }

  @SuppressWarnings("unchecked")
  public void putAll(Map<? extends byte[], ? extends V> m)
  {
    Iterator<?> iterator = m.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<? extends byte[], ? extends V> next = (Entry<? extends byte[], ? extends V>) iterator.next();
      internalMap.put(new ByteArrayWrapper(next.getKey()), next.getValue());
    }
  }

  public V remove(Object key)
  {
    if (key instanceof byte[])
      return internalMap.remove(new ByteArrayWrapper((byte[]) key));
    return internalMap.remove(key);
  }

  public int size()
  {
    return internalMap.size();
  }

  public Collection<V> values()
  {
    return internalMap.values();
  }

  private final class ByteEntry implements Entry<byte[], V>
  {
    private V value;
    private byte[] key;

    public ByteEntry(byte[] key, V value)
    {
      this.key = key;
      this.value = value;
    }

    public byte[] getKey()
    {
      return this.key;
    }

    public V getValue()
    {
      return this.value;
    }

    public V setValue(V value)
    {
      this.value = value;
      return value;
    }
  }
}
